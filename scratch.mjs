/* cSpell:disable */
import main from "./index.js";
import u from "ak-tools";
import ecommerce from './models/ecommerce.js';
console.log('\n------------------------------\n');

const timerDev = u.timer('cycle');
timerDev.start();
/** @type {import('./types').JobConfig} */
const PARAMS = {
	// demoDataConfig: ecommerce
	// csv_file: "./testData/events-restive-grotto-unfolding.csv",
	warehouse: "bigquery",
	csv_file: "./testData/events-shimmering-skyline-pulsating.csv",
	bigquery_dataset: "csv_dwh",
	table_name: "babys-first-table"
};
const data = await main(PARAMS);

timerDev.end();
console.log('\n------------------------------\n');
timerDev.report();

debugger;
