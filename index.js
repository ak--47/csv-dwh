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
		batch_size = 100,
		
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

	if (!PARAMS || Object.keys(PARAMS).length === 0) {
		PARAMS = {
			warehouse,
			event_table_name,
			batch_size: 100,
			bigquery_dataset,
			csv_file,
			demoDataConfig,
			table_name,
		};
	}

	//todo: simulation OR CSV file...
	let data;
	let schema;
	if (csv_file) {
		const fileData = await u.load(path.resolve(csv_file));
		const { data: parsed, errors } = Papa.parse(fileData, { header: true });
		schema = generateSchema(parsed, 'csv');
		data = { csvData: parsed };
	}
	if (demoDataConfig) data = await dataMaker(demoDataConfig);

	const batched = u.objMap(data, (value) => batchData(value, batch_size));
	const { eventData, userProfilesData, scdTableData, groupProfilesData, lookupTableData, csvData } = batched;


	const results = [];
	if (event_table_name) results.push(await loadCSVtoDataWarehouse(eventData, warehouse, PARAMS));
	if (user_table_name) results.push(await loadCSVtoDataWarehouse(userProfilesData, warehouse, PARAMS));
	if (scd_table_name) results.push(await loadCSVtoDataWarehouse(scdTableData, warehouse, PARAMS));
	if (lookup_table_name) results.push(await loadCSVtoDataWarehouse(lookupTableData, warehouse, PARAMS));
	if (group_table_name) results.push(await loadCSVtoDataWarehouse(groupProfilesData, warehouse, PARAMS));
	//this is the only one that matters
	if (table_name) results.push(await loadCSVtoDataWarehouse(csvData, warehouse, PARAMS));

	debugger;

}


async function loadCSVtoDataWarehouse(batches, warehouse, PARAMS) {
	let result;
	switch (warehouse) {
		case 'bigquery':
			result = await loadToBigQuery(batches, PARAMS);
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


function batchData(data, batchSize) {
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


const values = ['123', '45.67', 'true', 'False', '2022-01-01', 'hello', '2022-01-01T00:00:00Z'];

values.forEach(value => {
	console.log(`${value}: ${inferType(value)}`);
});






module.exports = main;

