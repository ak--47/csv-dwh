#! /usr/bin/env node
const u = require("ak-tools");
const { version } = require('./package.json');
const { existsSync } = require('fs');

const path = require('path');
require('dotenv').config({ debug: false, override: false });
// const dataMaker = require('make-mp-data');
const Papa = require("papaparse");

const cli = require('./components/cli');
const log = require('./components/logger.js');
const { generateSchema, checkEnv, validate } = require('./components/inference.js');


const loadToBigQuery = require('./middleware/bigquery');
const loadToSnowflake = require('./middleware/snowflake');
const loadToRedshift = require('./middleware/redshift');


/**
 * @typedef {import('./types').JobConfig} Config
 * @typedef {import('./types').WarehouseUploadResult} WarehouseResult
 * @typedef {import('./types').Schema} Schema
 * @typedef {import('./types').JobResult} JobResult
 * 
 */


/**
 * main program
 * @param {Config} PARAMS
 * @returns {Promise<JobResult>}
 * 
 */
async function main(PARAMS) {
	const startTime = Date.now();

	//% ENV VARS
	const keysToCheck = [
		// generic
		'table_name',

		// bigquery
		'bigquery_dataset',
		'bigquery_project',
		'bigquery_keyfile',
		'bigquery_service_account',
		'bigquery_service_account_pass',

		// snowflake
		'snowflake_account',
		'snowflake_user',
		'snowflake_password',
		'snowflake_database',
		'snowflake_schema',
		'snowflake_warehouse',
		'snowflake_role',
		'snowflake_access_url',

		// redshift
		"redshift_workgroup",
		"redshift_database",
		"redshift_access_key_id",
		"redshift_secret_access_key",
		"redshift_session_token",
		"redshift_region",
		"redshift_schema_name"
	];

	const { valsFound, valsMissing, vars } = checkEnv(keysToCheck, PARAMS);
	log('FOUND ENV VARS', Object.keys(valsFound));

	let {
		//everything requires:
		warehouse,
		batch_size = 1000,
		// demoDataConfig,

		//byo data requires:
		table_name = "",
		csv_file = '',
		json_file = '',

		//options
		verbose = false,
		dry_run = false,
		write_logs = false,
		...rest
	} = PARAMS;

	// set verbose logging
	log.verbose(verbose);
	if (!warehouse) throw new Error('warehouse is required');
	if (!Array.isArray(warehouse)) warehouse = [warehouse];
	PARAMS.warehouse = warehouse;
	if (!table_name) console.warn('no table name specified; i will make one up'); table_name = u.makeName(2, '_');

	// clean + validate PARAMS
	validate(PARAMS);

	let data;
	let schema;
	let intermediateSchema;
	let parsed;

	// user supplied csv or JSON file
	let file;
	if (csv_file) file = csv_file;
	if (json_file) file = json_file;
	if (file) {
		const filePath = path.resolve(csv_file || json_file);
		const isCSV = filePath.endsWith('.csv');
		const isJSON = filePath.endsWith('.json');

		if (isCSV) {
			const fileData = await u.load(filePath);
			const parseJob = Papa.parse(fileData, { header: true, skipEmptyLines: false, fastMode: false });
			parsed = parseJob.data;
		}

		if (isJSON) {
			parsed = (await u.load(filePath)).split('\n').filter(a => a).map(JSON.parse);
		}

		schema = generateSchema(parsed);
		intermediateSchema = u.clone(schema);
		data = { sourceFileData: parsed };
	}


	const batched = u.objMap(data, (value) => batchData(value, batch_size));
	const { sourceFileData } = batched;
	const results = [];
	for (const wh of warehouse) {
		results.push(await loadCSVtoDataWarehouse(schema, sourceFileData, wh, PARAMS));	
	}
	

	const endTime = Date.now();
	const e2eDuration = endTime - startTime;
	const clockTime = u.prettyTime(e2eDuration);
	const totalRows = results.reduce((acc, result) => acc + result?.insert?.success || 0 + result?.insert?.failed || 0, 0);
	const recordsPerSec = Math.floor(Number(totalRows) / (e2eDuration / 1000));

	const jobSummary = {
		version,
		PARAMS,
		results,
		e2eDuration,
		clockTime,
		recordsPerSec,
		totalRows,
		intermediateSchema
	};

	log('JOB SUMMARY:\n\n', jobSummary);

	return jobSummary;
}


/**
 * @param  {Schema} schema
 * @param  {any[][]} batches
 * @param  {import('./types').Warehouses} warehouse
 * @param  {Config} PARAMS
 */
async function loadCSVtoDataWarehouse(schema, batches, warehouse, PARAMS) {
	let result;
	try {
		switch (warehouse) {
			case 'bigquery':
				result = await loadToBigQuery(schema, batches, PARAMS);
				break;
			case 'snowflake':
				result = await loadToSnowflake(schema, batches, PARAMS);
				break;
			case 'redshift':
				result = await loadToRedshift(schema, batches, PARAMS);
				break;
			// case 'databricks':
			// 	// todo
			// 	// result = await loadToDatabricks(records, schema, PARAMS);
			// 	break;
			default:
				throw new Error(`Unknown warehouse: ${warehouse}`);
		}
	}
	catch (e) {
		log('WAREHOUSE ERROR', { warehouse }, e);
		debugger;
	}

	const summary = summarize(result, PARAMS);
	return summary;
}


/**
 * @param  {any[]} data
 * @param  {number} batchSize=0
 */
function batchData(data, batchSize = 0) {
	if (!batchSize) return [data];
	const batches = [];
	for (let i = 0; i < data.length; i += batchSize) {
		batches.push(data.slice(i, i + batchSize));
	}
	return batches;
}

/**
 * @param  {WarehouseResult | undefined} results
 * @param  {Config} PARAMS
 */
function summarize(results, PARAMS) {
	if (!results) return {};
	let { upload, dataset = "", schema, table } = results;
	if (!dataset) dataset = PARAMS.bigquery_dataset || PARAMS.snowflake_database || PARAMS.redshift_database || 'unknown';
	const uploadSummary = upload.reduce((acc, batch) => {
		acc.success += batch.insertedRows || 0;
		acc.failed += batch.failedRows || 0;
		acc.duration += batch.duration || 0;
		return acc;

	}, { success: 0, failed: 0, duration: 0, errors: [] });

	const summary = {
		insert: uploadSummary,
		dataset,
		schema,
		table
	};

	return summary;
}





// this is for CLI
if (require.main === module) {
	log.cli(true);
	const params = cli().then((params) => {

		// CLI is always verbose
		main({ ...params, verbose: true })
			.then((results) => {
				log('\n\nRESULTS:\n\n');
				log(JSON.stringify(results));
				if (params.write_logs) {
					const logText = log.getLog();
					let logPath;
					if (typeof params.write_logs === 'string') logPath = path.resolve(params.write_logs);
					else logPath = path.resolve('./log.txt');
					const writtenLog = u.touch(logPath, logText).then(() => {
						log(`\nwrote log to ${logPath}`);
					});
				}



			})
			.catch((e) => {
				log('\n\nUH OH! something went wrong; the error is:\n\n');
				console.error(e);
				process.exit(1);
			})
			.finally(() => {
				process.exit(0);
			});
	});
}




main.bigquery = loadToBigQuery;
main.snowflake = loadToSnowflake;
main.redshift = loadToRedshift;
module.exports = main;

