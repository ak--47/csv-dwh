const { BigQuery } = require('@google-cloud/bigquery');
const u = require('ak-tools');
const { prepHeaders } = require('../components/inference');

const client = new BigQuery(); // use application default credentials; todo: override with service account
let datasetId = '';
let tableId = '';


/**
 * BigQuery middleware
 * implements this contract
 * @param  {import('../types').Schema} schema
 * @param  {import('../types').csvBatch[]} batches
 * @param  {import('../types').JobConfig} PARAMS
 * @returns {Promise<import('../types').WarehouseUploadResult>}
 */
async function loadToBigQuery(schema, batches, PARAMS) {
	const {
		bigquery_dataset = '',
		table_name = '',
	} = PARAMS;

	if (!bigquery_dataset) throw new Error('bigquery_dataset is required');
	if (!table_name) throw new Error('table_name is required');
	if (!schema) throw new Error('schema is required');
	if (!batches) throw new Error('batches is required');
	if (batches.length === 0) throw new Error('batches is empty');

	datasetId = bigquery_dataset;
	tableId = table_name;
	// ensure column headers are clean in schema and batches
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

	// build a specific schema for BigQuery
	schema = schemaToBQS(schema);

	// do work
	const dataset = await createDataset();
	const table = await createTable(schema);
	const upload = await insertData(schema, batches);

	const uploadJob = { schema, dataset, table, upload };

	// @ts-ignore
	return uploadJob;
}


async function createDataset() {
	const datasets = await client.getDatasets();
	const datasetExists = datasets[0].some(dataset => dataset.id === datasetId);

	if (!datasetExists) {
		const [dataset] = await client.createDataset(datasetId);
		console.log(`Dataset ${dataset.id} created.\n`);
		return datasetId;
	} else {
		console.log(`Dataset ${datasetId} already exists.\n`);
		return datasetId;
	}
}


async function createTable(schema) {
	const dataset = client.dataset(datasetId);
	const table = dataset.table(tableId);
	const [tableExists] = await table.exists();

	if (tableExists) {
		console.log(`Table ${tableId} already exists. Deleting existing table.`);
		await table.delete();
		console.log(`Table ${tableId} has been deleted.`);
	}

	// Proceed to create a new table with the new schema
	const options = { schema: schemaToBQS(schema) };
	const [newTable] = await dataset.createTable(tableId, options);
	console.log(`New table ${newTable.id} created with the updated schema.\n`);
	const newTableExists = await newTable.exists();
	return newTable.id;
}


function schemaToBQS(schema) {
	const transformedSchema = schema.map(field => {
		let bqType;

		// Determine BigQuery type
		// ? https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
		switch (field.type.toUpperCase()) {
			case 'OBJECT': // Treat objects as RECORD
				bqType = 'STRING';
				break;
			case 'ARRAY': // Arrays require a type specification of the elements
				bqType = 'STRING';
				break;
			case 'JSON': // Store JSON as a string if not decomposed into a schema
				bqType = 'STRING';
				break;
			case 'INT':
				bqType = 'INT64';
				break;
			case 'FLOAT':
				bqType = 'FLOAT64';
				break;
			default:
				bqType = field.type.toUpperCase();
				break;
		}

		// Build field definition
		const fieldSchema = {
			name: field.name,
			type: bqType,
			mode: field.mode || 'NULLABLE'
		};

		// arrays require an item type
		// if (field.type.toUpperCase() === 'ARRAY') {
		// 	fieldSchema.type = 'JSON';
		// 	fieldSchema.mode = 'REPEATED';
		// }
		// // For RECORD types, handle subfields if any
		// if (field.type.toUpperCase() === 'OBJECT' && field.fields) {
		//     fieldSchema.fields = schemaToBQS(field.fields);
		// }

		return fieldSchema;
	});

	return transformedSchema;
}


async function waitForTableToBeReady(table, retries = 20, maxInsertAttempts = 20) {
	console.log('Checking if table exits...');

	tableExists: for (let i = 0; i < retries; i++) {
		const [exists] = await table.exists();
		if (exists) {
			console.log(`\tTable is confirmed to exist on attempt ${i + 1}.`);
			break tableExists;
		}
		const sleepTime = u.rand(1000, 5000);
		console.log(`sleeping for ${u.prettyTime(sleepTime)}; waiting for table exist; attempt ${i + 1}`);
		await u.sleep(sleepTime);

		if (i === retries - 1) {
			console.log(`Table does not exist after ${retries} attempts.`);
			return false;
		}
	}

	console.log('\nChecking if table is ready for operations...');
	let insertAttempt = 0;
	while (insertAttempt < maxInsertAttempts) {
		try {
			// Attempt a dummy insert that SHOULD fail, but not because 404
			const dummyRecord = { [u.uid()]: u.uid() };
			await table.insert([dummyRecord]);
			console.log('...should never get here...');
			return true; // If successful, return true immediately
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = u.rand(1000, 5000);
				console.log(`\tTable not ready for operations, sleeping ${u.prettyTime(sleepTime)} retrying... attempt #${insertAttempt + 1}`);
				await u.sleep(sleepTime);
				insertAttempt++;
			}

			else if (error.name === 'PartialFailureError') {
				console.log('\tTable is ready for operations\n');
				return true;
			}

			else {
				console.log('should never get here either');
				debugger;
			}



		}
	}
	return false; // Return false if all attempts fail
}

async function insertData(schema, batches) {
	const table = client.dataset(datasetId).table(tableId);

	// Check if table is ready
	const tableReady = await waitForTableToBeReady(table);
	if (!tableReady) {
		console.log('\nTable is NOT ready after all attempts. Aborting data insertion.');
		process.exit(1);
	}
	console.log('Starting data insertion...\n');
	const results = [];

	/** @type {import('@google-cloud/bigquery').InsertRowsOptions} */
	const options = {
		skipInvalidRows: false,
		ignoreUnknownValues: false,
		raw: false,
		partialRetries: 3,
		schema: schema,
	};
	// Continue with data insertion if the table is ready
	let currentBatch = 0;
	for (const batch of batches) {
		currentBatch++;
		const start = Date.now();
		try {
			const rows = prepareRowsForInsertion(batch, schema);
			const [job] = await table.insert(rows, options);
			const duration = Date.now() - start;
			results.push({ status: 'success', insertedRows: rows.length, failedRows: 0, duration });
			u.progress(`\tsent batch ${u.comma(currentBatch)} of ${u.comma(batches.length)} batches`);

		} catch (error) {
			const duration = Date.now() - start;
			results.push({ status: 'error', errorMessage: error.message, errors: error, duration });
			console.error(`\n\nError inserting batch ${currentBatch}; ${error.message}\n\n`);
			debugger;
		}
	}
	console.log('\n\tData insertion complete.\n');
	return results;
}

function prepareRowsForInsertion(batch, schema) {
	return batch.map(row => {
		const newRow = {};
		schema.forEach(field => {
			//sparse CSVs will have missing fields
			if (row[field.name] !== '') {
				newRow[field.name] = convertField(row[field.name], field.type.toUpperCase());
			}
			if (row[field.name] === '') delete newRow[field.name];
		});
		return newRow;
	});
}

function convertField(value, type) {
	switch (type) {
		case 'STRING':
			return value.toString();
		case 'TIMESTAMP':
			return value;
		case 'DATE':
			return value;
		case 'INT64':
			return parseInt(value);
		case 'FLOAT64':
			return parseFloat(value);
		case 'BOOLEAN':
			return value.toLowerCase() === 'true';
		case 'RECORD':
			return JSON.parse(value);
		case 'JSON':
			return JSON.parse(value);
		case 'REPEATED':
			return JSON.parse(value);
		default:
			return value;
	}
}


module.exports = loadToBigQuery;