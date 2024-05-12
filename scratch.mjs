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
// const PARAMS = {
// 	// demoDataConfig: ecommerce
// 	// csv_file: "./testData/events-restive-grotto-unfolding.csv",
// 	// csv_file: "./testData/events-shimmering-skyline-pulsating.csv",
// 	csv_file: "./testData/simpleForReal-EVENTS.csv",
// 	warehouse: "snowflake",
// 	table_name: "baby_first_table",
// 	batch_size: 2500,
// 	snowflake_account,
// 	snowflake_user,
// 	snowflake_password,
// 	snowflake_database,
// 	snowflake_schema,
// 	snowflake_warehouse,
// 	snowflake_role
// };
// const data = await main(PARAMS);


const commonParams = {
	warehouse: "snowflake",
	bigquery_dataset: "sandbox",
	table_name: "baby_first_table",
	dry_run: true
}

const files = (await u.ls('./testData')).filter(f => f.endsWith('.csv'));

for (const file of files) {
	const PARAMS = { ...commonParams, csv_file: file };
	const data = await main(PARAMS);
	debugger;

}

timerDev.end();
console.log('\n------------------------------\n');
timerDev.report();

debugger;
