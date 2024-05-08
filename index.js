#! /usr/bin/env node
const u = require("ak-tools");
const fetch = require("ak-fetch");
const { version } = require('./package.json');

const path = require('path');
require('dotenv').config({ debug: false, override: false });
const dataMaker = require('make-mp-data');
const Papa = require("papaparse");

const cli = require('./components/cli');
const { inferType, getUniqueKeys, generateSchema } = require('./components/inference.js');


const loadToBigQuery = require('./middleware/bigquery');

/**
 * @typedef {import('./types').JobConfig} Config
 * 
 * 
 */


/**
 * main program
 * @param {Config} PARAMS
 * 
 */
async function main(PARAMS) {
	const startTime = Date.now();
	const {
		//general
		warehouse = "bigquery",
		batch_size = 0,

		//generated data
		demoDataConfig,
		event_table_name = '',
		user_table_name = '',
		scd_table_name = '',
		lookup_table_name = '',
		group_table_name = '',

		//byo data
		table_name = "foo",
		csv_file = '',

		//bigquery
		bigquery_dataset = 'hello-world',



	} = PARAMS;

	PARAMS = {
		// @ts-ignore
		warehouse,
		event_table_name,
		batch_size: 0,
		bigquery_dataset,
		// @ts-ignore
		csv_file,
		demoDataConfig,
		table_name,
		...PARAMS
	};



	let data;
	let schema;

	// user supplied csv file
	if (csv_file) {
		const fileData = await u.load(path.resolve(csv_file));
		const { data: parsed, errors } = Papa.parse(fileData, { header: true });
		schema = generateSchema(parsed, 'csv');
		data = { csvData: parsed };
	}

	// generated data
	if (demoDataConfig) {
		data = await dataMaker(demoDataConfig);
		//todo: multiple schemas
	}

	const batched = u.objMap(data, (value) => batchData(value, batch_size));
	const { eventData, userProfilesData, scdTableData, groupProfilesData, lookupTableData, csvData } = batched;


	const results = [];
	if (event_table_name) results.push(await loadCSVtoDataWarehouse(eventData, warehouse, PARAMS));
	if (user_table_name) results.push(await loadCSVtoDataWarehouse(userProfilesData, warehouse, PARAMS));
	if (scd_table_name) results.push(await loadCSVtoDataWarehouse(scdTableData, warehouse, PARAMS));
	if (lookup_table_name) results.push(await loadCSVtoDataWarehouse(lookupTableData, warehouse, PARAMS));
	if (group_table_name) results.push(await loadCSVtoDataWarehouse(groupProfilesData, warehouse, PARAMS));

	//this is the only one that matters
	if (table_name) results.push(await loadCSVtoDataWarehouse(schema, csvData, warehouse, PARAMS));

	debugger;

}


async function loadCSVtoDataWarehouse(schema, batches, warehouse, PARAMS) {
	let result;
	try {
		switch (warehouse) {
			case 'bigquery':
				result = await loadToBigQuery(schema, batches, PARAMS);
				break;
			case 'snowflake':
				// todo
				// result = await loadToSnowflake(records, schema, PARAMS);
				break;
			case 'databricks':
				// todo
				// result = await loadToDatabricks(records, schema, PARAMS);
				break;
			default:
				result = Promise.resolve(null);
		}
		return result;
	}
	catch (e) {
		console.log('WAREHOUSE ERROR', warehouse);
		console.error(e);
		debugger;
	}
}


function batchData(data, batchSize = 0) {
	if (!batchSize) return [data];
	const batches = [];
	for (let i = 0; i < data.length; i += batchSize) {
		batches.push(data.slice(i, i + batchSize));
	}
	return batches;
}


// this is for CLI
if (require.main === module) {
	const params = cli().then((params) => {
		// @ts-ignore
		main(params)
			.then((results) => {
				if (params.verbose) console.log('\n\nRESULTS:\n\n', u.json(results));
			})
			.catch((e) => {
				console.log('\n\nUH OH! something went wrong; the error is:\n\n');
				console.error(e);
				process.exit(1);
			})
			.finally(() => {
				process.exit(0);
			});
	});
}






module.exports = main;

