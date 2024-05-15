const { RedshiftDataClient, ExecuteStatementCommand, BatchExecuteStatementCommand } = require('@aws-sdk/client-redshift-data');
const u = require('ak-tools');
const { prepHeaders, cleanName } = require('../components/inference');
require('dotenv').config();

/**
 * Main Function: Inserts data into Redshift Serverless
 * @param {import('../types').Schema} schema
 * @param {import('../types').csvBatch[]} batches
 * @param {import('../types').JobConfig} PARAMS
 * @returns {Promise<import('../types').WarehouseUploadResult>}
 */
async function loadToRedshift(schema, batches, PARAMS) {
	let {
		redshift_workgroup,
		redshift_database,
		table_name,
		dry_run,
		AWS_ACCESS_KEY_ID,
		AWS_SECRET_ACCESS_KEY,
		AWS_SESSION_TOKEN,
		AWS_REGION = "us-west-2"
	} = PARAMS;

	if (!redshift_database) throw new Error('redshift_database is required');
	if (!redshift_workgroup) throw new Error('redshift_workgroup is required');
	if (!table_name) throw new Error('table_name is required');

	// override with environment variables if available
	if (process.env.AWS_ACCESS_KEY_ID) AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
	if (process.env.AWS_SECRET_ACCESS_KEY) AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
	if (process.env.AWS_SESSION_TOKEN) AWS_SESSION_TOKEN = process.env.AWS_SESSION_TOKEN;
	if (process.env.AWS_REGION) AWS_REGION = process.env.AWS_REGION;

	// set environment variables for AWS SDK
	process.env.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
	process.env.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY
	process.env.AWS_SESSION_TOKEN = AWS_SESSION_TOKEN
	process.env.AWS_REGION = AWS_REGION


	table_name = cleanName(table_name);

	schema = schema.map(field => prepareRedshiftSchema(field));
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));


	// // Explicitly configure AWS credentials
	// const credentials = new AWS.Credentials({
	// 	accessKeyId: process.env.AWS_ACCESS_KEY_ID, // Defined in your environment variables
	// 	secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
	// 	sessionToken: process.env.AWS_SESSION_TOKEN // Optional: needed only if using temporary credentials
	// });

	/** @type {import('@aws-sdk/client-redshift-data').RedshiftDataClientConfig} */
	const clientConfig = {
		region: AWS_REGION		
	};

	const redshiftClient = new RedshiftDataClient(clientConfig);
	const tableSchemaSQL = schemaToRedshiftSQL(schema);

	/** @type {import('../types').InsertResult[]} */
	const upload = [];

	let createTableSQL;
	try {
		createTableSQL = `CREATE TABLE IF NOT EXISTS ${table_name} (${tableSchemaSQL})`;
		await executeSQL(redshiftClient, redshift_database, createTableSQL, redshift_workgroup);
		console.log(`Table ${table_name} created or verified successfully`);
	} catch (error) {
		console.error(`Error creating table; ${error.message}`);
		debugger;
	}

	const columnNames = schema.map(f => f.name).join(", ");
	const placeholders = schema.map(() => '?').join(", ");
	const insertSQL = `INSERT INTO ${table_name} (${columnNames}) VALUES (${placeholders})`;

	console.log('\n\n');

	if (dry_run) {
		console.log('Dry run requested. Skipping Redshift upload.');
		return { schema, database: redshift_database, table: table_name || "", upload: [] };
	}

	// Insert data
	for (const batch of batches) {
		const bindArray = batch.map(row => schema.map(f => formatBindValue(row[f.name], f.type)));
		const start = Date.now();
		try {
			const task = await executeSQL(redshiftClient, redshift_database, insertSQL, redshift_workgroup, bindArray);
			const duration = Date.now() - start;
			const numRows = task?.numberOfRecordsUpdated || batch.length;
			upload.push({ duration, status: 'success', insertedRows: numRows, failedRows: 0 });
			u.progress(`\tsent batch ${u.comma(batches.indexOf(batch) + 1)} of ${u.comma(batches.length)} batches`);
		} catch (error) {
			const duration = Date.now() - start;
			upload.push({ status: 'error', errorMessage: error.message, errors: error, duration });
			console.error(`\n\nError inserting batch ${batches.indexOf(batch) + 1}; ${error.message}\n\n`);
		}
	}

	console.log('\n\nData insertion complete.\n');

	/** @type {import('../types').WarehouseUploadResult} */
	const uploadJob = { schema, database: redshift_database, table: table_name || "", upload };

	return uploadJob;
}

/**
 * Executes a given SQL query on the Redshift Serverless connection
 * @param {RedshiftDataClient} client 
 * @param {string} database 
 * @param {string} sql 
 * @param {string} workgroup 
 * @param {any[]} [binds] 
 * @returns {Promise<any>}
 */
async function executeSQL(client, database, sql, workgroup, binds) {
	const command = binds ? new BatchExecuteStatementCommand({
		// @ts-ignore
		sql,
		database,
		workgroup,
		parameterSets: binds
	}) : new ExecuteStatementCommand({
		// @ts-ignore
		sql,
		database,
		workgroup
	});

	try {
		const response = await client.send(command);
		return response;
	} catch (error) {
		console.error('Failed executing SQL:', error);
		throw error;
	}
}

module.exports = loadToRedshift;



/**
 * Translates a schema definition to Redshift SQL data types
 * @param {import('../types').Schema} schema 
 * @returns {string}
 */
function schemaToRedshiftSQL(schema) {
	return schema.map(field => {
		let type = field.type.toUpperCase();
		switch (type) {
			case 'INT': type = 'INTEGER'; break;
			case 'FLOAT': type = 'REAL'; break;
			case 'STRING': type = 'VARCHAR'; break;
			case 'BOOLEAN': type = 'BOOLEAN'; break;
			case 'DATE': type = 'DATE'; break;
			case 'TIMESTAMP': type = 'TIMESTAMP'; break;
			case 'JSON': type = 'SUPER'; break;
			// Add other type mappings as necessary
		}
		return `${field.name} ${type}`;
	}).join(', ');
}

/**
 * Formats a value based on its schema definition type for Redshift.
 * @param {any} value
 * @param {string} type
 * @returns {any}
 */
function formatBindValue(value, type) {
	if (value === null || value === undefined || value === "null" || value.trim() === "") {
		return null;
	}
	switch (type) {
		case 'INTEGER':
			return parseInt(value);
		case 'REAL':
			return parseFloat(value);
		case 'BOOLEAN':
			return value.toLowerCase() === 'true';
		case 'SUPER':
			return JSON.stringify(value);
		default:
			return value;
	}
}


/**
 * Prepares the schema for Redshift based on the field types provided.
 * @param {import('../types').SchemaField} field
 * @returns {import('../types').SchemaField}
 */
function prepareRedshiftSchema(field) {
	const redshiftTypes = {
		'INT': 'INTEGER',
		'FLOAT': 'REAL',
		'STRING': 'VARCHAR',
		'BOOLEAN': 'BOOLEAN',
		'DATE': 'DATE',
		'TIMESTAMP': 'TIMESTAMP',
		'JSON': 'SUPER'  // Redshift supports SUPER for semi-structured data
	};

	let redshiftType = redshiftTypes[field.type.toUpperCase()] || 'VARCHAR';  // Default to VARCHAR if not mapped
	return { ...field, type: redshiftType };
}

module.exports = loadToRedshift;
