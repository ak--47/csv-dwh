import main from "./index.js";
import u from "ak-tools";
console.log('\n------------------------------\n');

const timerDev = u.timer('cycle');
timerDev.start();
const PARAMS = {

}
const data = await main(PARAMS);

timerDev.end();
console.log('\n------------------------------\n');
timerDev.report();

debugger;
