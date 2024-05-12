// @ts-nocheck
const u = require('ak-tools');
const main = require("../../index.js");
require('dotenv').config();

const TIMEOUT = process.env.TIMEOUT || 1000 * 60 * 5; // 5 minutes
const BATCH_SIZE = process.env.BATCH_SIZE || 500;

/** @typedef {import('../types').JobConfig} PARAMS */



/** @type {PARAMS} */
const commonParams = {
	bigquery_dataset: "csv_dwh",
	batch_size: BATCH_SIZE,
	warehouse: "bigquery"
};

test("simple: events", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/simple-EVENTS.csv",
		table_name: "test-simpleEvents",
		...commonParams
	};
	const expectedRows = 1111;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/simple-EVENTS-schema-bigquery.json', true);
	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);

test("simple: users", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/simple-USERS.csv",
		table_name: "test-simple-USERS",
		...commonParams
	};
	const expectedRows = 100;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/simple-USERS-schema-bigquery.json', true);

	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);


test("complex: events", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/complex-EVENTS.csv",
		table_name: "test-complex-EVENTS",
		...commonParams
	};
	const expectedRows = 1111;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/complex-EVENTS-schema-bigquery.json', true);

	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);

test("complex: users", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/complex-USERS.csv",
		table_name: "test-complex-USERS",
		...commonParams
	};
	const expectedRows = 100;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/complex-USERS-schema-bigquery.json', true);

	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);

test("complex: groups", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/complex-GROUP.csv",
		table_name: "test-complex-GROUP",
		...commonParams
	};
	const expectedRows = 350;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/complex-GROUP-schema-bigquery.json', true);

	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);


test("complex: lookups", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/complex-LOOKUP.csv",
		table_name: "test-complex-LOOKUP",
		...commonParams
	};
	const expectedRows = 1000;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/complex-LOOKUP-schema-bigquery.json', true);

	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);


test("complex: scd", async () => {
	/** @type {PARAMS} */
	const PARAMS = {
		csv_file: "./tests/data/complex-SCD.csv",
		table_name: "test-complex-SCD",
		...commonParams
	};
	const expectedRows = 293;
	const job = await main(PARAMS);
	const { totalRows, results } = job;
	expect(totalRows).toBe(expectedRows);
	const result = results[0];
	const { schema, dataset, table, insert } = result;
	expect(dataset).toBe(PARAMS.bigquery_dataset);
	expect(table).toBe(PARAMS.table_name);
	expect(insert.success).toBe(expectedRows);
	expect(insert.failed).toBe(0);
	expect(insert.duration).toBeGreaterThan(0);
	expect(insert.errors.length).toBe(0);

	const expectedSchema = await u.load('./tests/data/complex-SCD-schema-bigquery.json', true);

	expect(schema).toEqual(expectedSchema);
}, TIMEOUT);


