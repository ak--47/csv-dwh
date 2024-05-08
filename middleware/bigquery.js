const { BigQuery } = require('@google-cloud/bigquery');
const u = require('ak-tools');
//todo: authentication
const client = new BigQuery();
let datasetId = '';
let tableId = '';

async function main(schema, batches, PARAMS) {
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
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));


	const bigQuerySchema = schemaToBQS(schema);

	const dataset = await createDataset();
	const table = await createTable(schema);
	debugger;
	const upload = await insertData(schema, batches);
	return { dataset, table, upload };
}


async function createDataset() {
	const datasets = await client.getDatasets();
	const datasetExists = datasets[0].some(dataset => dataset.id === datasetId);

	if (!datasetExists) {
		const [dataset] = await client.createDataset(datasetId);
		console.log(`Dataset ${dataset.id} created.`);
		return dataset;
	} else {
		console.log(`Dataset ${datasetId} already exists.`);
		return datasetId;
	}
}


async function createTable(schema) {
	const dataset = client.dataset(datasetId);
	const [tables] = await dataset.getTables();
	const tableExists = tables.some(table => table.id === tableId);
	const options = { schema: schemaToBQS(schema) };

	if (!tableExists) {
		const [table] = await dataset.createTable(tableId, options);
		console.log(`Table ${table.id} created.`);
		return table;
	} else {
		console.log(`Table ${tableId} already exists. Overwriting schema.`);
		// @ts-ignore
		const table = await dataset.table(tableId);
		const [metaData] = await table.setMetadata(options);
		console.log(`Table ${table.id} schema updated.`);
		return metaData;
	}
}



function schemaToBQS(schema) {
	return schema.map(field => {
		let bqType;

		// Determine BigQuery type
		// ? https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
		switch (field.type.toUpperCase()) {
			case 'OBJECT': // Treat objects as RECORD
				bqType = 'JSON';
				break;
			case 'ARRAY': // Arrays require a type specification of the elements
				bqType = 'REPEATED';
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
		if (field.type.toUpperCase() === 'ARRAY') {
			// fieldSchema.itemType = "foos"
			// fieldSchema.type = field.itemType.toUpperCase();
			fieldSchema.type = 'JSON';
			fieldSchema.mode = 'REPEATED';
		}

		// // For RECORD types, handle subfields if any
		// if (field.type.toUpperCase() === 'OBJECT' && field.fields) {
		//     fieldSchema.fields = schemaToBQS(field.fields);
		// }

		return fieldSchema;
	});
}

// BQ restriction on column names
// 
/**
 * Prepares and cleans header names according to BigQuery's naming restrictions. Optionally returns
 * ? see: https://cloud.google.com/bigquery/docs/schemas#column_names
 * @param {string[]} headers - The array of header names to be cleaned.
 * @param {boolean} [asArray=false] - Whether to return the result as an array of arrays.
 * @returns {(Object|string[][])|{}} If asArray is true, returns an array of arrays, each containing
 * the original header name and the cleaned header name. If false, returns an object mapping
 * original header names to cleaned header names.
 */
function prepHeaders(headers, asArray = false) {
	const headerMap = {};
	const usedNames = new Set();

	headers.forEach(originalName => {
		let cleanName = originalName.trim();

		// Replace invalid characters
		cleanName = cleanName.replace(/[^a-zA-Z0-9_]/g, '_');

		// Ensure it starts with a letter or underscore
		if (!/^[a-zA-Z_]/.test(cleanName)) {
			cleanName = '_' + cleanName;
		}

		// Trim to maximum length
		cleanName = cleanName.substring(0, 300);

		// Ensure uniqueness
		let uniqueName = cleanName;
		let suffix = 1;
		while (usedNames.has(uniqueName)) {
			uniqueName = cleanName + '_' + suffix++;
		}
		cleanName = uniqueName;

		// Add to used names set
		usedNames.add(cleanName);

		// Map original name to clean name
		headerMap[originalName] = cleanName;
	});

	if (asArray) {
		const oldNames = Object.keys(headerMap);
		const newNames = Object.values(headerMap);
		const result = oldNames.map((key, i) => [key, newNames[i]]);
		return result;
	}

	return headerMap;
}



async function insertData(schema, batches) {
	// Inserts data of various BigQuery-supported types into a table.

	/**
	 * TODO(developer): Uncomment the following lines before running the sample.
	 */
	// const datasetId = 'my_dataset';
	// const tableId = 'my_table';

	// Describe the schema of the table
	// For more information on supported data types, see
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
	const schemaEx = [
		{
			name: 'name',
			type: 'STRING',
		},
		{
			name: 'age',
			type: 'INTEGER',
		},
		{
			name: 'school',
			type: 'BYTES',
		},
		{
			name: 'metadata',
			type: 'JSON',
		},
		{
			name: 'location',
			type: 'GEOGRAPHY',
		},
		{
			name: 'measurements',
			mode: 'REPEATED',
			type: 'FLOAT',
		},
		{
			name: 'datesTimes',
			type: 'RECORD',
			fields: [
				{
					name: 'day',
					type: 'DATE',
				},
				{
					name: 'firstTime',
					type: 'DATETIME',
				},
				{
					name: 'secondTime',
					type: 'TIME',
				},
				{
					name: 'thirdTime',
					type: 'TIMESTAMP',
				},
			],
		},
	];

	// For all options, see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
	const options = {
		schema: schema,
	};

	// Create a new table in the dataset
	const [table] = await bigquery
		.dataset(datasetId)
		.createTable(tableId, options);

	console.log(`Table ${table.id} created.`);

	// The DATE type represents a logical calendar date, independent of time zone.
	// A DATE value does not represent a specific 24-hour time period.
	// Rather, a given DATE value represents a different 24-hour period when
	// interpreted in different time zones, and may represent a shorter or longer
	// day during Daylight Savings Time transitions.
	const bqDate = bigquery.date('2019-1-12');
	// A DATETIME object represents a date and time, as they might be
	// displayed on a calendar or clock, independent of time zone.
	const bqDatetime = bigquery.datetime('2019-02-17 11:24:00.000');
	// A TIME object represents a time, as might be displayed on a watch,
	// independent of a specific date and timezone.
	const bqTime = bigquery.time('14:00:00');
	// A TIMESTAMP object represents an absolute point in time,
	// independent of any time zone or convention such as Daylight
	// Savings Time with microsecond precision.
	const bqTimestamp = bigquery.timestamp('2020-04-27T18:07:25.356Z');
	const bqGeography = bigquery.geography('POINT(1 2)');
	const schoolBuffer = Buffer.from('Test University');
	// a JSON field needs to be converted to a string
	const metadata = JSON.stringify({
		owner: 'John Doe',
		contact: 'johndoe@example.com',
	});
	// Rows to be inserted into table
	const rows = [
		{
			name: 'Tom',
			age: '30',
			location: bqGeography,
			school: schoolBuffer,
			metadata: metadata,
			measurements: [50.05, 100.5],
			datesTimes: {
				day: bqDate,
				firstTime: bqDatetime,
				secondTime: bqTime,
				thirdTime: bqTimestamp,
			},
		},
		{
			name: 'Ada',
			age: '35',
			measurements: [30.08, 121.7],
		},
	];

	// Insert data into table
	await client.dataset(datasetId).table(tableId).insert(rows);

	console.log(`Inserted ${rows.length} rows`);
}

module.exports = main;