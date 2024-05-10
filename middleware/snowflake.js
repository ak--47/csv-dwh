const snowflake = require('snowflake-sdk');
const u = require('ak-tools');
const { prepHeaders } = require('../components/inference');


/**
 * Inserts data into Snowflake
 * @param {import('../types').Schema} schema
 * @param {import('../types').csvBatch[]} batches
 * @param {import('../types').JobConfig} PARAMS
 * @returns {Promise<import('../types').WarehouseUploadResult>}
 */
async function loadToSnowflake(schema, batches, PARAMS) {
	const conn = await createSnowflakeConnection(PARAMS);
	const isConnectionValid = await conn.isValidAsync();

	const { snowflake_database, table_name } = PARAMS;
	if (!snowflake_database) throw new Error('snowflake_database is required');
	if (!table_name) throw new Error('table_name is required');
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

	const snowflake_table_schema = schemaToSnowflakeSQL(schema);
	/** @type {import('../types').InsertResult[]} */
	const upload = [];

	try {
		// Create or replace table
		const createTableSQL = `CREATE OR REPLACE TABLE ${table_name} (${snowflake_table_schema})`;
		const tableCreation = await executeSQL(conn, createTableSQL);
	}
	catch (error) {
		console.error(`Error creating table; ${error.message}`);
		debugger;
	}


	// Insert data
	for (const batch of batches) {
		const statements = [];
		for (const row of batch) {
			const columnNames = schema.map(f => f.name).join(", ");
			const values = schema.map(f => formatSQLValue(row[f.name])).join(", ");
			statements.push(`INSERT INTO ${PARAMS.table_name} (${columnNames}) VALUES (${values});`);
		}
		const combinedSQL = `BEGIN;\n${statements.join("\n")}\nCOMMIT;`;
		try {
			const start = Date.now();
			const task = await executeSQL(conn, combinedSQL);
			const duration = Date.now() - start;
			upload.push({ duration, status: 'success', insertedRows: batch.length, failedRows: 0 });
		} catch (error) {
			upload.push({ status: 'error', errorMessage: error.message, errors: error, duration: Date.now() - start });
			console.error(`\n\nError during batch insert; ${error.message}\n\n`);
		}
	}

	// @ts-ignore
	conn.destroy();

	const uploadJob = { schema, database: snowflake_database, table: table_name, upload };
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
 * @returns {Promise<void>}
 */
function executeSQL(conn, sql, binds) {
	if (binds) {
		return new Promise((resolve, reject) => {
			conn.execute({
				sqlText: sql,
				binds: binds, //todo 
				complete: (err, stmt, rows) => {
					if (err) {
						console.error('Failed executing SQL:', err);
						reject(err);
					} else {
						console.log('SQL executed successfully');
						resolve(rows);
					}
				}
			});
		});
	}
	return new Promise((resolve, reject) => {
		conn.execute({
			sqlText: sql,
			complete: (err, stmt, rows) => {
				if (err) {
					console.error('Failed executing SQL:', err);
					reject(err);
				} else {
					console.log('SQL executed successfully');
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

	snowflake.configure({ keepAlive: true });

	return new Promise((resolve, reject) => {
		const connection = snowflake.createConnection({
			account: snowflake_account,
			username: snowflake_user,
			password: snowflake_password,
			database: snowflake_database,
			schema: snowflake_schema,
			warehouse: snowflake_warehouse,
			role: snowflake_role,
			accessUrl: 'https://xjcqygf-pj72746.snowflakecomputing.com'

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
