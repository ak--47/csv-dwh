// @ts-nocheck
const u = require('ak-tools');
const main = require("../../index.js");
require('dotenv').config();

const TIMEOUT = process.env.TIMEOUT || 1000 * 60 * 5; // 5 minutes
const BATCH_SIZE = process.env.BATCH_SIZE || 500;

/** @typedef {import('../types').JobConfig} PARAMS */




const { snowflake_account,
	snowflake_user,
	snowflake_password,
	snowflake_database,
	snowflake_schema,
	snowflake_warehouse,
	snowflake_role } = process.env;

/** @type {PARAMS} */
const commonParams = {
	snowflake_account,
	snowflake_user,
	snowflake_password,
	snowflake_database,
	snowflake_schema,
	snowflake_warehouse,
	snowflake_role,
	batch_size: BATCH_SIZE,
	warehouse: "snowflake"
};

test("simple: events", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./testData/simple-EVENTS.csv",
		table_name: "test_simple_EVENTS",
		...commonParams
	};
	const expectedRows = 1080;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.snowflake_database);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);
	const expectedSchema = await u.load('./testData/simple-EVENTS-snowflake.json', true);
	expect(schema).toEqual(expectedSchema);

}, TIMEOUT);


test("simple: users", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./testData/simple-USERS.csv",
		table_name: "test_simple_USERS",
		...commonParams
	};
	const expectedRows = 100;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.snowflake_database);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);
	const expectedSchema = await u.load('./testData/simple-USERS-snowflake.json', true);
	expect(schema).toEqual(expectedSchema);

}, TIMEOUT);




