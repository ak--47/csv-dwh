// @ts-nocheck
require('dotenv').config();
const { version } = require('../package.json');
const { execSync } = require("child_process");
const u = require('ak-tools');
const timeout = 60000;

const TIMEOUT = process.env.TIMEOUT || 1000 * 60 * 5; // 5 minutes
const BATCH_SIZE = process.env.BATCH_SIZE || 200;

/** @typedef {import('../types').JobConfig} PARAMS */



const csvFile = './tests/data/cli-simple.csv';
const csvTable = 'cli-simple-csv';
const jsonFile = './tests/data/cli-simple.json';
const jsonTable = 'cli-simple-json';


const {

	snowflake_account = "",
	snowflake_user = "",
	snowflake_password = "",
	snowflake_database = "",
	snowflake_schema = "",
	snowflake_warehouse = "",
	snowflake_role = "",
	snowflake_access_url = "",

	bigquery_project = "",
	bigquery_dataset = "",
	bigquery_service_account = "",
	bigquery_service_account_pass = "",

	redshift_workgroup = "",
	redshift_database = "",
	redshift_access_key_id = "",
	redshift_secret_access_key = "",
	redshift_schema_name = "",
	redshift_region = "",

} = process.env;

test('help', async () => {
	console.log('COMPLEX CLI TEST');
	const run = execSync(`node ./index.js --help`);
	const byline = `by AK (ak@mixpanel.com) v${version || 2}`;
	expect(run.toString()).toContain(byline);
}, TIMEOUT);

test('bq: csv', async () => {
	const env = { bigquery_service_account_pass, ...process.env };
	const prefix = `node ./index.js ${csvFile}`;
	const params = `--batch_size ${BATCH_SIZE} --verbose true --warehouse bigquery --bigquery_project ${bigquery_project} --bigquery_dataset ${bigquery_dataset} --bigquery_service_account ${bigquery_service_account} --table_name ${csvTable}`;
	const run = execSync(`${prefix} ${params}`, { env });
	const exec = JSON.parse(run.toString().split('RESULTS:').pop().trim());	
	const {insert} = exec.results[0];
	const {success, failed, errors} = insert;
	expect(success).toBe(1111);
	expect(failed).toBe(0);
	expect(errors.length).toBe(0);	
}, TIMEOUT);

test('bq: json', async () => {
	const env = { bigquery_service_account_pass, ...process.env };
	const prefix = `node ./index.js ${jsonFile}`;
	const params = `--batch_size ${BATCH_SIZE} --verbose true --warehouse bigquery --bigquery_project ${bigquery_project} --bigquery_dataset ${bigquery_dataset} --bigquery_service_account ${bigquery_service_account} --table_name ${jsonTable}`;
	const run = execSync(`${prefix} ${params}`, { env });
	const exec = JSON.parse(run.toString().split('RESULTS:').pop().trim());	
	const {insert} = exec.results[0];
	const {success, failed, errors} = insert;
	expect(success).toBe(1111);
	expect(failed).toBe(0);
	expect(errors.length).toBe(0);	
}, TIMEOUT);