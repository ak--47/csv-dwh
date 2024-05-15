const { RedshiftDataClient, ExecuteStatementCommand, BatchExecuteStatementCommand } = require('@aws-sdk/client-redshift-data');
const u = require('ak-tools');
const { prepHeaders, cleanName } = require('../components/inference');
require('dotenv').config();

let workgroup;
let database;
let region;


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
		redshift_access_key_id,
		redshift_secret_access_key,
		redshift_session_token,
		redshift_region = "us-east-1",
		redshift_schema_name = "public"
	} = PARAMS;

	// override with environment variables if available
	if (process.env.redshift_workgroup) redshift_workgroup = process.env.redshift_workgroup;
	if (process.env.redshift_database) redshift_database = process.env.redshift_database;
	if (process.env.redshift_access_key_id) redshift_access_key_id = process.env.redshift_access_key_id;
	if (process.env.redshift_secret_access_key) redshift_secret_access_key = process.env.redshift_secret_access_key;
	if (process.env.redshift_session_token) redshift_session_token = process.env.redshift_session_token;
	if (process.env.redshift_region) redshift_region = process.env.redshift_region;
	if (process.env.redshift_schema) redshift_schema_name = process.env.redshift_schema;

	const credentials = {
		accessKeyId: redshift_access_key_id || "",
		secretAccessKey: redshift_secret_access_key || "",
	};
	if (redshift_session_token) credentials.sessionToken = redshift_session_token;


	if (!redshift_database) throw new Error('redshift_database is required');
	if (!redshift_workgroup) throw new Error('redshift_workgroup is required');
	if (!table_name) throw new Error('table_name is required');
	if (!redshift_schema_name) throw new Error('redshift_schema_name is required');

	database = redshift_database;
	workgroup = redshift_workgroup;
	region = redshift_region;

	table_name = cleanName(table_name);

	schema = schema.map(field => prepareRedshiftSchema(field));
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));


	/** @type {import('@aws-sdk/client-redshift-data').RedshiftDataClientConfig} */
	const clientConfig = { region: redshift_region, credentials };

	const redshiftClient = new RedshiftDataClient(clientConfig);
	const tableSchemaSQL = schemaToRedshiftSQL(schema);

	/** @type {import('../types').InsertResult[]} */
	const upload = [];

	let createTableSQL;
	try {
		createTableSQL = `CREATE TABLE IF NOT EXISTS ${redshift_schema_name}.${table_name} (${tableSchemaSQL})`;
		const tableCreate = await executeSQL(redshiftClient, createTableSQL);
		console.log(`Table ${table_name} created or verified successfully`);
	} catch (error) {
		console.error(`Error creating table; ${error.message}`);
		debugger;
	}

	//insert statements
	const columnNames = schema.map(f => f.name).join(", ");
	// const placeholders = schema.map(() => '?').join(", ");
	// const insertSQL = `INSERT INTO ${table_name} (${columnNames}) VALUES (${placeholders})`;

	console.log('\n\n');

	if (dry_run) {
		console.log('Dry run requested. Skipping Redshift upload.');
		return { schema, database: redshift_database, table: table_name || "", upload: [] };
	}

	// Process each batch
	for (const batch of batches) {
		let valuesArray = [];
		for (const row of batch) {
			let rowValues = schema.map(field => {
				let value = row[field.name];
				return formatSQLValue(value, field.type);
			}).join(", ");
			valuesArray.push(`(${rowValues})`);
		}
		const valuesString = valuesArray.join(", ");
		const insertSQL = `INSERT INTO ${table_name} (${columnNames}) VALUES ${valuesString}`;

		// Execute the batch insert
		const start = Date.now();
		try {
			const response = await executeSQL(redshiftClient, insertSQL);
			const duration = Date.now() - start;
			const numRows = batch.length; // todo better parsing of response
			upload.push({ duration, status: 'success', insertedRows: numRows, failedRows: 0, meta: response?.$metadata });
			console.log(`Batch ${batches.indexOf(batch) + 1} of ${batches.length} sent successfully.`);
		} catch (error) {
			const duration = Date.now() - start;
			upload.push({ status: 'error', errorMessage: error.message, errors: error, duration });
			console.error(`Error inserting batch ${batches.indexOf(batch) + 1}: ${error.message}`);
			debugger;
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
 * @param {string} sql 
 * @param {any[] | any} [binds] 
 * @returns {Promise<any>}
 */
async function executeSQL(client, sql, binds = false) {
	console.log(`Executing SQL: ${sql}`);  // Debug output to inspect SQL value
	if (!sql) {
		console.error('SQL command is null');
		throw new Error('SQL command is null');
	}

	let command;
	// if (binds) {
	// 	command = new BatchExecuteStatementCommand({
	// 		Sqls: sql,
	// 		Database: database,
	// 		WorkgroupName: workgroup,
	// 		// parameterSets: binds 
	// 	});
	// }

	if (!binds) {
		command = new ExecuteStatementCommand({
			Sql: sql,
			Database: database,
			WorkgroupName: workgroup
		});

	}

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

function formatSQLValue(value, type) {
    if (value === null || value === undefined) return 'NULL';
    switch (type) {
        case 'INTEGER':
        case 'REAL':
            return value;
        case 'BOOLEAN':
            return value ? 'TRUE' : 'FALSE';
        case 'STRING':
			return `'${value}'`
		case 'VARCHAR':
			return `'${value}'`
        case 'DATE':
        case 'TIMESTAMP':
            return `'${value.replace(/'/g, "''")}'`; // Escape single quotes
        case 'SUPER': // For JSON types
            return `'${JSON.stringify(value).replace(/'/g, "''")}'`; // Escape and convert to JSON string
        default:
            return value;
    }
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
