// @ts-nocheck
const { execSync } = require("child_process");
const u = require('ak-tools');
const { Readable } = require('stream');
const main = require("../index.js");
const { common } = require("@google-cloud/bigquery");

const timeout = 1000 * 60 * 5; // 5 minutes


describe("bigQuery", () => {
	const commonParams = {
		bigquery_dataset: "csv_dwh",
		batch_size: 2500,
		warehouse: "bigquery"
	};

	test("simple: events", async () => {
		const PARAMS = {
			csv_file: "./testData/simple-EVENTS.csv",
			table_name: "test-simpleEvents",
			...commonParams
		};
		const expectedRows = 5101
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

		const expectedSchema = [
			{
				name: "event",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "_source",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "time",
				type: "TIMESTAMP",
				mode: "NULLABLE",
			},
			{
				name: "_user_id",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "variants",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "flows",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "flags",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "experiment_ids",
				type: "INT64",
				mode: "NULLABLE",
			},
			{
				name: "multiVariate",
				type: "BOOLEAN",
				mode: "NULLABLE",
			},
			{
				name: "platform",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "currentTheme",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "_insert_id",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "videoCategory",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "isFeaturedItem",
				type: "BOOLEAN",
				mode: "NULLABLE",
			},
			{
				name: "watchTimeSec",
				type: "INT64",
				mode: "NULLABLE",
			},
			{
				name: "quality",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "format",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "uploader_id",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "page",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "utm_source",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "itemCategory",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "dateItemListed",
				type: "DATE",
				mode: "NULLABLE",
			},
			{
				name: "itemId",
				type: "INT64",
				mode: "NULLABLE",
			},
			{
				name: "amount",
				type: "INT64",
				mode: "NULLABLE",
			},
			{
				name: "rating",
				type: "INT64",
				mode: "NULLABLE",
			},
			{
				name: "reviews",
				type: "INT64",
				mode: "NULLABLE",
			},
			{
				name: "currency",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "coupon",
				type: "STRING",
				mode: "NULLABLE",
			},
			{
				name: "numItems",
				type: "INT64",
				mode: "NULLABLE",
			},
		];

		expect(schema).toEqual(expectedSchema);
	}, timeout);

	test("simple: users", async () => {
		const PARAMS = {
			csv_file: "./testData/simple-USERS.csv",
			table_name: "test-simpleUsers",
			...commonParams
		};
		const expectedRows = 100
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

		const expectedSchema = [
			{
			  name: "distinct_id",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_name",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_email",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_avatar",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_created",
			  type: "TIMESTAMP",
			  mode: "NULLABLE",
			},
			{
			  name: "anonymousIds",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "sessionIds",
			  type: "STRING",
			  mode: "NULLABLE",
			},
		  ];

		expect(schema).toEqual(expectedSchema);
	}, timeout);


	test("complex: events", async () => {
		const PARAMS = {
			csv_file: "./testData/complex-EVENTS.csv",
			table_name: "test-complexEvents",
			...commonParams
		};
		const expectedRows = 4942
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

		const expectedSchema = [
			{
			  name: "event",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_source",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "time",
			  type: "TIMESTAMP",
			  mode: "NULLABLE",
			},
			{
			  name: "_device_id",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_session_id",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "variant",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "experiment",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "platform",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "company_id",
			  type: "INT64",
			  mode: "NULLABLE",
			},
			{
			  name: "_insert_id",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "_user_id",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "isFeaturedItem",
			  type: "BOOLEAN",
			  mode: "NULLABLE",
			},
			{
			  name: "amount",
			  type: "INT64",
			  mode: "NULLABLE",
			},
			{
			  name: "rating",
			  type: "INT64",
			  mode: "NULLABLE",
			},
			{
			  name: "reviews",
			  type: "INT64",
			  mode: "NULLABLE",
			},
			{
			  name: "product_id",
			  type: "INT64",
			  mode: "NULLABLE",
			},
			{
			  name: "page",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "utm_source",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "colors",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "category",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "hashTags",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "watchTimeSec",
			  type: "INT64",
			  mode: "NULLABLE",
			},
			{
			  name: "quality",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "format",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "uploader_id",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "currency",
			  type: "STRING",
			  mode: "NULLABLE",
			},
			{
			  name: "cart",
			  type: "STRING",
			  mode: "NULLABLE",
			},
		  ];

		expect(schema).toEqual(expectedSchema);
	}, timeout);

	

});