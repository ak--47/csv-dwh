/*
----
DATABRICKS MIDDLEWARE
----
*/
const { DBSQLClient } = require('@databricks/sql');
const u = require('ak-tools');
const { prepHeaders, cleanName } = require('../components/inference');
const log = require('../components/logger.js');
require('dotenv').config();

/**
 * Main Function: Inserts data into Databricks
 * @param {import('../types').Schema} schema
 * @param {import('../types').csvBatch[]} batches
 * @param {import('../types').JobConfig} PARAMS
 * @returns {Promise<import('../types').WarehouseUploadResult>}
 */
async function loadToDatabricks(schema, batches, PARAMS) {
    let {
        databricks_host,
        databricks_http_path,
        databricks_token,
        table_name,
        databricks_database = 'default',
        dry_run
    } = PARAMS;

    // Override with environment variables if available
    if (process.env.databricks_host) databricks_host = process.env.databricks_host;
    if (process.env.databricks_http_path) databricks_http_path = process.env.databricks_http_path;
    if (process.env.databricks_token) databricks_token = process.env.databricks_token;

    // Ensure required parameters are provided
    if (!databricks_host) throw new Error('databricks_host is required');
    if (!databricks_http_path) throw new Error('databricks_http_path is required');
    if (!databricks_token) throw new Error('databricks_token is required');
    if (!table_name) throw new Error('table_name is required');

    table_name = cleanName(table_name);

    schema = schema.map(prepareDatabricksSchema);
    const columnHeaders = schema.map(field => field.name);
    const headerMap = prepHeaders(columnHeaders);
    const headerReplacePairs = prepHeaders(columnHeaders, true);
    // @ts-ignore
    schema = schema.map(field => u.rnVals(field, headerReplacePairs));
    batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

    const databricks_table_schema = schemaToDatabricksSQL(schema);
    /** @type {import('../types').InsertResult[]} */
    const upload = [];

    const client = new DBSQLClient();

    try {
        await client.connect({
            host: databricks_host,
            path: databricks_http_path,
            token: databricks_token,
        });

        const session = await client.openSession();

        const createTableSQL = `
            CREATE TABLE IF NOT EXISTS ${databricks_database}.${table_name} (${databricks_table_schema})
            USING delta
        `;

        await executeSQL(session, createTableSQL);
        log(`Table ${table_name} created or verified successfully`);

        if (dry_run) {
            log('Dry run requested. Skipping Databricks upload.');
            await session.close();
            await client.close();
            return { schema, database: databricks_database, table: table_name || "", upload: [], logs: log.getLog() };
        }

        let currentBatch = 0;
        for (const batch of batches) {
            currentBatch++;
            const insertSQL = prepareInsertSQL(schema, table_name, databricks_database);
            const data = batch.map(row => schema.map(f => formatBindValue(row[f.name], f.type)));
            const start = Date.now();
            try {
                await executeSQL(session, insertSQL, data);
                const duration = Date.now() - start;
                upload.push({ duration, status: 'success', insertedRows: batch.length, failedRows: 0 });
                if (log.isVerbose()) u.progress(`\tsent batch ${u.comma(currentBatch)} of ${u.comma(batches.length)} batches`);
            } catch (error) {
                const duration = Date.now() - start;
                upload.push({ status: 'error', errorMessage: error.message, errors: error, duration });
                log(`Error inserting batch ${currentBatch}: ${error.message}`, error, batch);
                debugger;
            }
        }

        await session.close();
        await client.close();
        log('\n\nData insertion complete.\n');

        const logs = log.getLog();
        /** @type {import('../types').WarehouseUploadResult} */
        const uploadJob = { schema, database: databricks_database, table: table_name || "", upload, logs };

        return uploadJob;

    } catch (error) {
        log(`Failed to connect or execute SQL: ${error.message}`, error);
        throw error;
    }
}

/**
 * @param {import('../types').SchemaField} field
 */
function prepareDatabricksSchema(field) {
    const { name, type } = field;
    let databricksType = type.toUpperCase();
    switch (type) {
        case 'INT': databricksType = 'INT'; break;
        case 'FLOAT': databricksType = 'FLOAT'; break;
        case 'STRING': databricksType = 'STRING'; break;
        case 'BOOLEAN': databricksType = 'BOOLEAN'; break;
        case 'DATE': databricksType = 'DATE'; break;
        case 'TIMESTAMP': databricksType = 'TIMESTAMP'; break;
        case 'JSON': databricksType = 'STRING'; break; // Use STRING for JSON in Databricks
        case 'ARRAY': databricksType = 'STRING'; break;
        case 'OBJECT': databricksType = 'STRING'; break;
    }
    return { name, type: databricksType };
}

/**
 * Prepares the schema for Databricks based on the field types provided.
 * @param {import('../types').Schema} schema
 * @returns {string}
 */
function schemaToDatabricksSQL(schema) {
    return schema.map(field => {
        let type = field.type.toUpperCase();
        switch (type) {
            case 'INT': type = 'INT'; break;
            case 'FLOAT': type = 'FLOAT'; break;
            case 'STRING': type = 'STRING'; break;
            case 'BOOLEAN': type = 'BOOLEAN'; break;
            case 'DATE': type = 'DATE'; break;
            case 'TIMESTAMP': type = 'TIMESTAMP'; break;
            case 'JSON': type = 'STRING'; break;
            case 'ARRAY': type = 'STRING'; break;
            case 'OBJECT': type = 'STRING'; break;
        }
        return `${field.name} ${type}`;
    }).join(', ');
}

/**
 * Prepares an insert SQL statement for Databricks.
 * @param {import('../types').Schema} schema
 * @param {string} tableName
 * @param {string} database
 * @returns {string}
 */
function prepareInsertSQL(schema, tableName, database) {
    const columnNames = schema.map(f => f.name).join(", ");
    const placeholders = schema.map(() => '?').join(", ");
    return `INSERT INTO ${database}.${tableName} (${columnNames}) VALUES (${placeholders})`;
}

/**
 * Formats a value for SQL statement.
 * @param {any} value
 * @param {string} type
 * @returns {any}
 */
function formatBindValue(value, type) {
    if (value === null || value === undefined || value === "null" || value === "" || value?.toString()?.trim() === "") {
        return null;
    } else if (typeof value === 'string' && u.isJSONStr(value)) {
        return value;
    } else {
        return value;
    }
}

/**
 * Executes a given SQL query on the Databricks connection.
 * @param {any} session
 * @param {string} sql
 * @param {Array<any>} [data]
 * @returns {Promise<any>}
 */
async function executeSQL(session, sql, data) {
    try {
        const queryOperation = await session.executeStatement(sql, { parameters: data });
        await queryOperation.fetchAll();
        await queryOperation.close();
    } catch (error) {
        log(`Failed executing SQL: ${error.message}`, error);
        throw error;
    }
}

module.exports = loadToDatabricks;
