/* cSpell:disable */
import main from "./index.js";
import u from "ak-tools";
import dotenv from "dotenv";
dotenv.config();

const { snowflake_account,
	snowflake_user,
	snowflake_password,
	snowflake_database,
	snowflake_schema,
	snowflake_warehouse,
	snowflake_role } = process.env;

console.log('\n------------------------------\n');

const timerDev = u.timer('cycle');
timerDev.start();
/** @type {import('./types').JobConfig} */
const PARAMS = {
	// demoDataConfig: ecommerce
	// csv_file: "./testData/events-restive-grotto-unfolding.csv",
	// csv_file: "./testData/events-shimmering-skyline-pulsating.csv",
	csv_file: "./testData/simple-EVENTS.csv",
	warehouse: "snowflake",
	table_name: "baby_first_table",
	batch_size: 10,
	snowflake_account,
	snowflake_user,
	snowflake_password,
	snowflake_database,
	snowflake_schema,
	snowflake_warehouse,
	snowflake_role
};
const data = await main(PARAMS);

timerDev.end();
console.log('\n------------------------------\n');
timerDev.report();

debugger;
