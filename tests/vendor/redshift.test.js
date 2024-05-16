// @ts-nocheck
const u = require('ak-tools');
const path = require('path');
const main = require("../../index.js");
require('dotenv').config();
const { cleanName } = require('../../components/inference.js');
const TIMEOUT = process.env.TIMEOUT || 1000 * 60 * 5; // 5 minutes
let BATCH_SIZE = process.env.BATCH_SIZE || 500;
BATCH_SIZE = 250;

/** @typedef {import('../types').JobConfig} PARAMS */

const { redshift_workgroup,
	redshift_database,
	redshift_access_key_id,
	redshift_secret_access_key,
	redshift_schema_name,
	redshift_region } = process.env;

/** @type {PARAMS} */
const commonParams = {
	redshift_workgroup,
	redshift_database,
	redshift_access_key_id,
	redshift_secret_access_key,
	redshift_schema_name,
	redshift_region,
	batch_size: BATCH_SIZE,
	warehouse: "redshift",
	dry_run: false
};

//MVP CASES
const [arrays, objects, simple, sparse] = [
	'./tests/data/mvp/mvp-arrays.csv',
	'./tests/data/mvp/mvp-objects.csv',
	'./tests/data/mvp/mvp-simple.csv',
	'./tests/data/mvp/mvp-sparse.csv'
].map(p => path.resolve(p));





describe("mvp", ()=>{

	
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


})


describe("csv", ()=>{
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
		const { schema, insert } = result;
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/simple-EVENTS-schema-redshift.json', true);
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
		const { schema, insert } = result;
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/simple-USERS-schema-redshift.json', true);
	
		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);
	
	
	test("complex: events", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-EVENTS.csv",
			table_name: "test-complex-EVENTS",
			...commonParams,
			batch_size: 100
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, insert } = result;
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/complex-EVENTS-schema-redshift.json', true);
	
		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);
	
	test("complex: users", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-USERS.csv",
			table_name: "test-complex-USERS",
			...commonParams,
			batch_size: 25
		};
		const expectedRows = 100;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, insert } = result;
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/complex-USERS-schema-redshift.json', true);
	
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
		const { schema, insert } = result;
	
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/complex-GROUP-schema-redshift.json', true);
	
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
		const { schema, insert } = result;
	
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/complex-LOOKUP-schema-redshift.json', true);
	
		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);
	
	
	test("complex: scd", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			csv_file: "./tests/data/mp_types/complex-SCD.csv",
			table_name: "test-complex-SCD",
			...commonParams
		};
		const expectedRows = 285;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, insert } = result;
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	
		const expectedSchema = await u.load('./tests/data/schemas/complex-SCD-schema-redshift.json', true);
	
		expect(schema).toEqual(expectedSchema);
	}, TIMEOUT);
	
	
})


describe("json", ()=>{
	test("simple: events", async () => {
		/** @type {PARAMS} */
		const PARAMS = {
			json_file: "./tests/data/mp_types/simple-EVENTS.json",
			table_name: "test-simpleEvents-json",
			...commonParams
		};
		const expectedRows = 1111;
		const job = await main(PARAMS);
		const { totalRows, results } = job;
		expect(totalRows).toBe(expectedRows);
		const result = results[0];
		const { schema, dataset, table, insert } = result;
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
		expect(insert.success).toBe(expectedRows);
		expect(insert.failed).toBe(0);
		expect(insert.duration).toBeGreaterThan(0);
		expect(insert.errors.length).toBe(0);
	

	}, TIMEOUT);
	
	
})