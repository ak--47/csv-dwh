#! /usr/bin/env node
const u = require("ak-tools");
const { version } = require('./package.json');
const { existsSync } = require('fs');

const path = require('path');
require('dotenv').config({ debug: false, override: false });
const dataMaker = require('make-mp-data');
const Papa = require("papaparse");

const cli = require('./components/cli');
const log = require('./components/logger.js');
const { generateSchema } = require('./components/inference.js');


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
	let {
		//everything requires:
		warehouse = "",
		batch_size = 1000,

		//generated data requires:
		demoDataConfig,
		event_table_name = '',
		user_table_name = '',
		scd_table_name = '',
		lookup_table_name = '',
		group_table_name = '',

		//byo data requires:
		table_name = "",
		csv_file = '',
		json_file = '',

		//bigquery requires:
		bigquery_dataset = '',

		//snowflake requires:
		snowflake_account = '',
		snowflake_user = '',
		snowflake_password = '',
		snowflake_database = '',
		snowflake_schema = '',
		snowflake_warehouse = '',
		snowflake_role = '',
		snowflake_access_url = '',

		//redshift requires:
		redshift_workgroup = '',
		redshift_database = '',
		redshift_access_key_id = '',
		redshift_secret_access_key = '',
		redshift_schema_name = '',
		redshift_region = '',
		redshift_session_token = '',

		//options
		verbose = false,
		dry_run = false,
		...rest

	} = PARAMS;

	// set verbose logging
	log.verbose(verbose);

	if (!warehouse) throw new Error('warehouse is required');
	if (!table_name) console.warn('no table name specified; i will make one up'); table_name = u.makeName(2, '_');
	if (demoDataConfig && !event_table_name) {
		console.warn('no table name specified; i will make one up');
		const prefix = u.makeName(2, '_');
		event_table_name = prefix + '_events';
		user_table_name = prefix + '_users';
		scd_table_name = prefix + '_scd';
		lookup_table_name = prefix + '_lookup';
		group_table_name = prefix + '_groups';
	}

	//CSV or JSON (or generated data)
	if (csv_file && json_file) throw new Error('cannot specify both csv_file and json_file');
	if (!csv_file && !json_file) throw new Error('csv_file or json_file is required');
	if (csv_file) if (!existsSync(csv_file)) throw new Error(`file not found: ${csv_file}`);
	if (json_file) if (!existsSync(json_file)) throw new Error(`file not found: ${json_file}`);
	let file;
	if (csv_file) file = csv_file;
	if (json_file) file = json_file;
	if (!file && !demoDataConfig) throw new Error('no data source specified');

	// clean PARAMS
	for (const key in PARAMS) {
		if (PARAMS[key] === undefined) delete PARAMS[key];
		if (PARAMS[key] === '') delete PARAMS[key];
		if (PARAMS[key] === null) delete PARAMS[key];
	}

	let data;
	let schema;
	let intermediateSchema;
	let parsed;

	// user supplied csv or JSON file
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

	// generated data essentially needs to treated as multiple csvs
	if (demoDataConfig) {
		data = await dataMaker(demoDataConfig);
		//todo: multiple schemas YIKES
		intermediateSchema = {};
	}

	const batched = u.objMap(data, (value) => batchData(value, batch_size));
	const { eventData, userProfilesData, scdTableData, groupProfilesData, lookupTableData, sourceFileData } = batched;


	const results = [];
	if (event_table_name) results.push(await loadCSVtoDataWarehouse(eventData, warehouse, PARAMS));
	if (user_table_name) results.push(await loadCSVtoDataWarehouse(userProfilesData, warehouse, PARAMS));
	if (scd_table_name) results.push(await loadCSVtoDataWarehouse(scdTableData, warehouse, PARAMS));
	if (lookup_table_name) results.push(await loadCSVtoDataWarehouse(lookupTableData, warehouse, PARAMS));
	if (group_table_name) results.push(await loadCSVtoDataWarehouse(groupProfilesData, warehouse, PARAMS));

	//this is the only one that matters
	if (table_name) results.push(await loadCSVtoDataWarehouse(schema, sourceFileData, warehouse, PARAMS));

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


	// @ts-ignore
	return jobSummary;

}


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
				//todo: write a log file?

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

