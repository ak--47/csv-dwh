/* cSpell:disable */
import main from "./index.js";
import u from "ak-tools";

console.log('\n------------------------------\n');

const timerDev = u.timer('cycle');
timerDev.start();
/** @type {import('./types').JobConfig} */
const PARAMS = {
	// demoDataConfig: ecommerce
	// csv_file: "./testData/events-restive-grotto-unfolding.csv",
	// csv_file: "./testData/events-shimmering-skyline-pulsating.csv",
	csv_file: "./testData/simple-EVENTS.csv",
	warehouse: "bigquery",	
	bigquery_dataset: "csv_dwh",
	table_name: "babys-first-table",
	batch_size: 2500
};
const data = await main(PARAMS);

timerDev.end();
console.log('\n------------------------------\n');
timerDev.report();

debugger;
