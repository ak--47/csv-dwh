#! /usr/bin/env node
const u = require("ak-tools");
const fetch = require("ak-fetch");
const { version } = require('./package.json');
const cli = require('./cli');
const path = require('path');
require('dotenv').config({ debug: false, override: false });
const dataMaker = require('make-mp-data');
const  loadToBigQuery  = require('./middleware/bigquery');

/**
 * @typedef {Object} Config
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
		warehouse = "bigquery",
		batch_size = 100,
		event_table_name = 'events',
		user_table_name = 'users',
		scd_table_name = 'scd',
		lookup_table_name = '',
		group_table_name = '',

		bigquery_dataset = 'hello-world',

	} = PARAMS;

	if (!PARAMS || Object.keys(PARAMS).length === 0) {
		PARAMS = {
			warehouse,
			event_table_name,
			batch_size: 100,
			bigquery_dataset,
		};
	}

	//todo: simulation OR CSV file...
	const simulation = await generateSampleData();
	const batched = u.objMap(simulation, (value) => batchData(value, batch_size));
	const { eventData, userProfilesData, scdTableData, groupProfilesData, lookupTableData } = batched;


	const results = [];
	if (event_table_name) results.push(await loadCSVtoDataWarehouse(eventData, warehouse, PARAMS));
	if (user_table_name) results.push(await loadCSVtoDataWarehouse(userProfilesData, warehouse, PARAMS));
	if (scd_table_name) results.push(await loadCSVtoDataWarehouse(scdTableData, warehouse, PARAMS));
	if (lookup_table_name) results.push(await loadCSVtoDataWarehouse(lookupTableData, warehouse, PARAMS));
	if (group_table_name) results.push(await loadCSVtoDataWarehouse(groupProfilesData, warehouse, PARAMS));

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

async function generateSampleData() {
	/** @type {import('make-mp-data').Config} */
	const simulationConfig = {
		anonIds: true,
		sessionIds: true,
		numEvents: 100,
		numUsers: 10,
		seed: "off you pop",
		writeToDisk: false,
		verbose: true,
	};
	const data = await dataMaker(simulationConfig);

	const { eventData, groupProfilesData, lookupTableData, scdTableData, userProfilesData } = data;

	return {
		eventData,
		groupProfilesData,
		lookupTableData,
		scdTableData,
		userProfilesData,
	};

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



module.exports = main;

