// @ts-nocheck
const path = require('path');
const u = require('ak-tools');
const main = require("../../index.js");
require('dotenv').config();
const { cleanName } = require('../../components/inference.js');

const TIMEOUT = process.env.TIMEOUT || 1000 * 60 * 5; // 5 minutes
const BATCH_SIZE = process.env.BATCH_SIZE || 500;
// Automatically set verbose mode based on debug mode
const isDebugMode = process.env.NODE_OPTIONS?.includes('--inspect') || process.env.NODE_OPTIONS?.includes('--inspect-brk');
const VERBOSE = isDebugMode || process.env.VERBOSE === 'true';

/** @typedef {import('../types').JobConfig} PARAMS */



/** @type {PARAMS} */
const commonParams = {
	bigquery_dataset: "csv_dwh",
	batch_size: BATCH_SIZE,
	warehouse: "bigquery",
	verbose: VERBOSE
};



//MVP CASES
const [arrays, objects, simple, sparse] = [
	'./tests/data/mvp/mvp-arrays.csv',
	'./tests/data/mvp/mvp-objects.csv',
	'./tests/data/mvp/mvp-simple.csv',
	'./tests/data/mvp/mvp-sparse.csv'
].map(p => path.resolve(p));



describe('mvp', () => {
	test("mvp: simple", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: simple,
			table_name: "test-mvp-simple",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { insert } = result;
		const { success, failed, duration, errors } = insert;
		expect(success).toBe(expectedRows);
		expect(failed).toBe(0);
		expect(duration).toBeGreaterThan(0);
		expect(errors.length).toBe(0);
	}, TIMEOUT);


	test("mvp: sparse", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: sparse,
			table_name: "test-mvp-sparse",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { insert } = result;
		const { success, failed, duration, errors } = insert;
		expect(success).toBe(expectedRows);
		expect(failed).toBe(0);
		expect(duration).toBeGreaterThan(0);
		expect(errors.length).toBe(0);
	}, TIMEOUT);

	test("mvp: arrays", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: arrays,
			table_name: "test-mvp-arrays",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { insert } = result;
		const { success, failed, duration, errors } = insert;
		expect(success).toBe(expectedRows);
		expect(failed).toBe(0);
		expect(duration).toBeGreaterThan(0);
		expect(errors.length).toBe(0);
	}, TIMEOUT);

	test("mvp: objects", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: objects,
			table_name: "test-mvp-objects",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { insert } = result;
		const { success, failed, duration, errors } = insert;
		expect(success).toBe(expectedRows);
		expect(failed).toBe(0);
		expect(duration).toBeGreaterThan(0);
		expect(errors.length).toBe(0);
	}, TIMEOUT);


});

describe('csv', () => {
	test("simple: events", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/simple-EVENTS.csv",
			table_name: "test-simple-Events",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/simple-EVENTS-schema-bigquery.json', true);
		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);

	test("simple: users", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/simple-USERS.csv",
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
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/simple-USERS-schema-bigquery.json', true);

		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);

	test("complex: events", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-EVENTS.csv",
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
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/complex-EVENTS-schema-bigquery.json', true);

		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);

	test("complex: users", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-USERS.csv",
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
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/complex-USERS-schema-bigquery.json', true);

		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);

	test("complex: groups", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-GROUP.csv",
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
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/complex-GROUP-schema-bigquery.json', true);

		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);

	test("complex: lookups", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-LOOKUP.csv",
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
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/complex-LOOKUP-schema-bigquery.json', true);

		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);

	test("complex: scd", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-SCD.csv",
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
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);

		const expectedSchema = await u.load('./tests/data/schemas/complex-SCD-schema-bigquery.json', true);

		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);
});


describe('json', () => {
	test("simple: events", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/simple-EVENTS.json",
			table_name: "test-simple-Events-json",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);


	}, TIMEOUT);

	test("simple: users", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/simple-USERS.json",
			table_name: "test-simple-USERS-json",
			...commonParams
		};
		const expectedRows = 100;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);


	}, TIMEOUT);

	test("complex: events", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/complex-EVENTS.json",
			table_name: "test-complex-EVENTS-json",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);


	}, TIMEOUT);

	test("complex: users", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/complex-USERS.json",
			table_name: "test-complex-USERS-json",
			...commonParams
		};
		const expectedRows = 100;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);


	}, TIMEOUT);

	test("complex: groups", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/complex-GROUP.json",
			table_name: "test-complex-GROUP-json",
			...commonParams
		};
		const expectedRows = 350;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);


	}, TIMEOUT);

	test("complex: scd", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/complex-SCD.json",
			table_name: "test-complex-SCD-json",
			...commonParams
		};
		const expectedRows = 285;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
		expect(dataset).toBe(PARAMS.bigquery_dataset);
		expect(table).toBe(cleanName(PARAMS.table_name));
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);


	}, TIMEOUT);

});

