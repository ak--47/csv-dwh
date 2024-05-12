const snowflake = require('snowflake-sdk');
const u = require('ak-tools');
const { prepHeaders, cleanName } = require('../components/inference');
require('dotenv').config();


/**
 * Inserts data into Snowflake
 * @param {import('../types').Schema} schema
 * @param {import('../types').csvBatch[]} batches
 * @param {import('../types').JobConfig} PARAMS
 * @returns {Promise<import('../types').WarehouseUploadResult>}
 */
async function loadToSnowflake(schema, batches, PARAMS) {
	PARAMS.snowflake_account = PARAMS.snowflake_account || process.env.snowflake_account;
	PARAMS.snowflake_user = PARAMS.snowflake_user || process.env.snowflake_user;
	PARAMS.snowflake_password = PARAMS.snowflake_password || process.env.snowflake_password;
	PARAMS.snowflake_database = PARAMS.snowflake_database || process.env.snowflake_database;
	PARAMS.snowflake_schema = PARAMS.snowflake_schema || process.env.snowflake_schema;
	PARAMS.snowflake_warehouse = PARAMS.snowflake_warehouse || process.env.snowflake_warehouse;
	PARAMS.snowflake_role = PARAMS.snowflake_role || process.env.snowflake_role;

	const conn = await createSnowflakeConnection(PARAMS);
	const isConnectionValid = await conn.isValidAsync();

	let { snowflake_database, table_name, dry_run } = PARAMS;
	if (!snowflake_database) throw new Error('snowflake_database is required');
	if (!table_name) throw new Error('table_name is required');

	table_name = cleanName(table_name);
	// @ts-ignore
	schema = schema.map(prepareSnowflakeSchema);
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

	const snowflake_table_schema = schemaToSnowflakeSQL(schema);
	/** @type {import('../types').InsertResult[]} */
	const upload = [];

	let createTableSQL;
	try {
		// Create or replace table
		createTableSQL = `CREATE OR REPLACE TABLE ${table_name} (${snowflake_table_schema})`;
		const tableCreation = await executeSQL(conn, createTableSQL);
		console.log(`Table ${table_name} created (or replaced) successfully`);
	}
	catch (error) {
		console.error(`Error creating table; ${error.message}`);
		debugger;
	}

	// Prepare insert statement
	const columnNames = schema.map(f => f.name).join(", ");
	const placeholders = schema.map(() => '?').join(", ");
	const insertSQL = `INSERT INTO ${table_name} (${columnNames}) VALUES (${placeholders})`;
	console.log('\n\n');

	let currentBatch = 0;
	if (dry_run) {
		console.log('Dry run requested. Skipping Snowflake upload.');
		return { schema, database: snowflake_database, table: table_name, upload: [] };
	}
	// Insert data
	for (const batch of batches) {
		currentBatch++;
		const bindArray = batch.map(row => schema.map(f => formatBindValue(row[f.name])));
		const start = Date.now();
		try {
			const task = await executeSQL(conn, insertSQL, bindArray);
			const duration = Date.now() - start;
			const numRows = task?.[0]?.['number of rows inserted'] || batch.length;
			upload.push({ duration, status: 'success', insertedRows: numRows, failedRows: 0 });
			u.progress(`\tsent batch ${u.comma(currentBatch)} of ${u.comma(batches.length)} batches`);
		} catch (error) {
			const duration = Date.now() - start;
			upload.push({ status: 'error', errorMessage: error.message, errors: error, duration });
			console.error(`\n\nError inserting batch ${currentBatch}; ${error.message}\n\n`);
		}
	}

	/** @type {import('../types').WarehouseUploadResult} */
	const uploadJob = { schema, database: snowflake_database, table: table_name, upload };

	console.log('\n\nData insertion complete; Terminating Connection...\n');

	//conn.destroy(terminationHandler);
	// @ts-ignore
	conn.destroy();


	return uploadJob;

}

/**
 * Properly formats a value for SQL statement.
 * @param {any} value The value to be formatted.
 * @returns {string} The formatted value for SQL.
 */
function formatSQLValue(value) {
	if (value === null || value === "" || value === undefined || value === "null") {
		return 'NULL';
	} else if (u.isJSONStr(value)) {
		const parsed = JSON.parse(value);
		if (Array.isArray(parsed)) {
			const formattedArray = parsed.map(item =>
				item === null ? 'NULL' : (typeof item === 'string' ? `'${item.replace(/'/g, "''")}'` : item)
			);
			return `ARRAY_CONSTRUCT(${formattedArray.join(", ")})`;
		}
		return `PARSE_JSON('${value.replace(/'/g, "''")}')`;
	} else if (typeof value === 'string') {
		return `'${value.replace(/'/g, "''")}'`;
	} else {
		return value;
	}
}
/**
 * @param  {import('../types').SchemaField} field
 */
function prepareSnowflakeSchema(field) {
	const { name, type } = field;
	let snowflakeType = type.toUpperCase();
	switch (type) {
		case 'INT': snowflakeType = 'NUMBER'; break;
		case 'FLOAT': snowflakeType = 'FLOAT'; break;
		case 'STRING': snowflakeType = 'VARCHAR'; break;
		case 'BOOLEAN': snowflakeType = 'BOOLEAN'; break;
		case 'DATE': snowflakeType = 'DATE'; break;
		case 'TIMESTAMP': snowflakeType = 'TIMESTAMP'; break;
		case 'JSON': snowflakeType = 'VARIANT'; break;
		case 'ARRAY': snowflakeType = 'VARIANT'; break;
		case 'OBJECT': snowflakeType = 'VARIANT'; break;
		// Add other type mappings as necessary
	}
	return { name, type: snowflakeType };
}

function formatBindValue(value) {
	if (value === null || value === undefined || value === "null" || value.trim() === "") {
		return null; // Convert null-like strings to actual null
	} else if (typeof value === 'string' && u.isJSONStr(value)) {
		// Check if the string is JSON, parse it to actual JSON
		try {
			const parsed = JSON.parse(value);
			if (Array.isArray(parsed)) {
				// If it's an array, return it as-is so Snowflake interprets it as an array
				return parsed;
			} else {
				// If it's any other kind of JSON, return the parsed JSON
				return parsed;
			}
		} catch (e) {
			// If JSON parsing fails, return the original string (should not happen since you check with isJSONStr)
			return value;
		}
	} else {
		return value; // Return the value directly if not a JSON string
	}
}


function terminationHandler(err, conn) {
	if (err) {
		console.error('Unable to disconnect: ' + err.message);
	} else {
		console.log('\tDisconnected connection with id: ' + conn.getId());
	}
}


/**
 * Translates a schema definition to Snowflake SQL data types
 * @param {import('../types').Schema} schema 
 * @returns {string}
 */
function schemaToSnowflakeSQL(schema) {
	return schema.map(field => {
		let type = field.type.toUpperCase();
		switch (type) {
			case 'INT': type = 'NUMBER'; break;
			case 'FLOAT': type = 'FLOAT'; break;
			case 'STRING': type = 'VARCHAR'; break;
			case 'BOOLEAN': type = 'BOOLEAN'; break;
			case 'DATE': type = 'DATE'; break;
			case 'TIMESTAMP': type = 'TIMESTAMP'; break;
			case 'JSON': type = 'VARIANT'; break;
			// Add other type mappings as necessary
		}
		return `${field.name} ${type}`;
	}).join(', ');
}

/**
 * Executes a given SQL query on the Snowflake connection
 * ? https://docs.snowflake.com/en/developer-guide/node-js/nodejs-driver-execute#binding-an-array-for-bulk-insertions
 * @param {snowflake.Connection} conn 
 * @param {string} sql 
 * @param {any[]} [binds] pass binds to bulk insert
 * @returns {Promise<snowflake.StatementStatus | any[] | undefined>}
 */
function executeSQL(conn, sql, binds) {
	return new Promise((resolve, reject) => {

		const options = { sqlText: sql };
		if (binds) options.binds = binds;
		conn.execute({
			...options,
			complete: (err, stmt, rows) => {
				if (err) {
					console.error('Failed executing SQL:', err);
					reject(err);
				} else {
					// console.log('SQL executed successfully');
					resolve(rows);
				}
			}
		});
	});


}





/**
 * Establishes a connection to Snowflake
 * @param {import('../types').JobConfig} PARAMS 
 * @returns {Promise<snowflake.Connection>}
 */
async function createSnowflakeConnection(PARAMS) {
	console.log('Attempting to connect to Snowflake...');
	const { snowflake_account = "",
		snowflake_user = "",
		snowflake_password = "",
		snowflake_database = "",
		snowflake_schema = "",
		snowflake_warehouse = "",
		snowflake_role = "" } = PARAMS;
	if (!snowflake_account) throw new Error('snowflake_account is required');
	if (!snowflake_user) throw new Error('snowflake_user is required');
	if (!snowflake_password) throw new Error('snowflake_password is required');
	if (!snowflake_database) throw new Error('snowflake_database is required');
	if (!snowflake_schema) throw new Error('snowflake_schema is required');
	if (!snowflake_warehouse) throw new Error('snowflake_warehouse is required');
	if (!snowflake_role) throw new Error('snowflake_role is required');

	snowflake.configure({ keepAlive: true, logLevel: 'WARN' });

	return new Promise((resolve, reject) => {
		const connection = snowflake.createConnection({
			account: snowflake_account,
			username: snowflake_user,
			password: snowflake_password,
			database: snowflake_database,
			schema: snowflake_schema,
			warehouse: snowflake_warehouse,
			role: snowflake_role,
			accessUrl: 'https://xjcqygf-pj72746.snowflakecomputing.com',
			// jsonColumnVariantParser: rawColumnValue => JSON.parse(rawColumnValue),


		});


		connection.connect((err, conn) => {
			if (err) {
				debugger;
				console.error('Unable to connect to Snowflake:', err);
				reject(err);
			} else {
				console.log('Successfully connected to Snowflake');
				resolve(conn);
			}
		});
	});
}



module.exports = loadToSnowflake;
