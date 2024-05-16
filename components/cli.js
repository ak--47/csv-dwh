const yargs = require('yargs');
const { version } = require('../package.json');
const u = require('ak-tools');

async function cliParams() {
	// @ts-ignore
	const args = yargs(process.argv.splice(2))
		.scriptName("csv-dwh")
		.usage(`${welcome}\n\nusage:\nnpx $0 [data] [options]

ex:
npx $0 ./myfile.csv --warehouse bigquery

DOCS: https://github.com/ak--47/csv-dwh`)
		.command('$0', 'bulk fetch calls', () => { })
		.option("warehouse", {
			alias: 'w',
			demandOption: true,
			describe: 'warehouse to send data to',
			type: 'string'
		})
		.option("batch_size", {
			alias: 'b',
			demandOption: false,
			describe: '# of records in each insert',
			type: 'number',
			default: 1000
		})
		.option("verbose", {
			alias: 'v',
			demandOption: false,
			default: true,
			describe: 'show progress bar',
			type: 'boolean'
		})		
		.help()
		.wrap(null)
		.argv;
	// @ts-ignore
	if (args._.length === 0) {
		// @ts-ignore
		yargs.showHelp();
		process.exit();
	}

	// // @ts-ignore
	// if (args.headers) args.headers = parse(args.headers);
	// // @ts-ignore
	// if (args.search_params) args.searchParams = parse(args.search_params);
	// // @ts-ignore
	// if (args.body_params) args.bodyParams = parse(args.body_params);
	// // @ts-ignore
	// if (args.payload) args.data = parse(args.payload);
	// // @ts-ignore
	// if (args.dry_run) args.dryRun = 'curl';

	// @ts-ignore
	const file = args._[0];
	if (file) {
		const data = await u.load(file);

		//json
		// @ts-ignore
		if (u.isJSONStr(data)) args.data = JSON.parse(data)

		//jsonl
		// @ts-ignore
		if (data.split('\n').map(u.isJSONStr).every(a => a)) args.data = data.split('\n').map(JSON.parse)
	}

	// @ts-ignore
	if (!args.data) throw new Error('no data provided');
	
	return args;
}

const hero = String.raw`

██████╗███████╗██╗   ██╗    ██████╗ ██╗    ██╗██╗  ██╗
██╔════╝██╔════╝██║   ██║    ██╔══██╗██║    ██║██║  ██║
██║     ███████╗██║   ██║    ██║  ██║██║ █╗ ██║███████║
██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██║███╗██║██╔══██║
╚██████╗███████║ ╚████╔╝     ██████╔╝╚███╔███╔╝██║  ██║
 ╚═════╝╚══════╝  ╚═══╝      ╚═════╝  ╚══╝╚══╝ ╚═╝  ╚═╝

`;

const banner = `... insert CSVs into DWHs ... with little fuss! (v${version || 2})
\tby AK (ak@mixpanel.com)\n\n`;

const welcome = hero.concat('\n').concat(banner);
cliParams.welcome = welcome;


/** 
 * helper to parse values passed in from cli
 * @param {string | string[] | void | any} val - value to parse
 * @param {any} [defaultVal] value if it can't be parsed
 * @return {Object<length, number>}
 */
function parse(val, defaultVal = undefined) {
	if (typeof val === 'string') {
		try {
			val = JSON.parse(val);
		}
		catch (firstError) {
			try {
				if (typeof val === 'string') val = JSON.parse(val?.replace(/'/g, '"'));
			}
			catch (secondError) {
				if (this.verbose) console.log(`error parsing tags: ${val}\ntags must be valid JSON`);
				val = defaultVal; //bad json
			}
		}
	}
	if (Object.keys(val).length === 0) return defaultVal;
	return val;
}


module.exports = cliParams;