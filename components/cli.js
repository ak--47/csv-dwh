const yargs = require('yargs');
const { version } = require('../package.json');
const path = require('path');
const { existsSync } = require('fs');
const u = require('ak-tools');

async function cliParams() {
	// @ts-ignore
	const args = yargs(process.argv.splice(2))
		.scriptName("csv-dwh")
		.usage(`${welcome}\n\n  usage:\nnpx $0 [data] [options]

  ex:
npx $0 ./myfile.csv --warehouse bigquery
npx $0 ./myfile.json --warehouse snowflake --batch_size 1000

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
		.option("dry_run", {
			alias: 'd',
			demandOption: false,
			default: false,
			describe: 'do not actually insert data',
			type: 'boolean'
		})
		.option("table_name", {
			alias: 't',
			demandOption: false,
			describe: 'name of table to insert data into',
			type: 'string'
		})
		.option("bigquery_dataset", {
			alias: "bqd",
			demandOption: false,
			describe: "bigquery dataset to use",
			type: 'string'
		})
		.option("bigquery_project", {
			alias: "bqp",
			demandOption: false,
			describe: "google cloud project id",
			type: 'string'
		})
		.option("bigquery_keyfile", {
			alias: "bqk",
			demandOption: false,
			describe: "bigquery json keyfile path",
			type: 'string'
		})
		.option("bigquery_service_account", {
			alias: "bqu",
			demandOption: false,
			describe: "bigquery service account email",
			type: 'string'
		})
		.option("snowflake_account", {
			alias: "sfa",
			demandOption: false,
			describe: "account id for snowflake access",
			type: 'string'
		})
		.option("snowflake_user", {
			alias: "sfu",
			demandOption: false,
			describe: "user for snowflake access",
			type: 'string'
		})
		.option("snowflake_password", {
			alias: "sfp",
			demandOption: false,
			describe: "password for snowflake user",
			type: 'string'
		})
		.option("snowflake_database", {
			alias: "sfd",
			demandOption: false,
			describe: "database to use in snowflake",
			type: 'string'
		})
		.option("snowflake_schema", {
			alias: "sfs",
			demandOption: false,
			describe: "schema to use in snowflake",
			type: 'string'
		})
		.option("snowflake_warehouse", {
			alias: "sfw",
			demandOption: false,
			describe: "warehouse to use in snowflake",
			type: 'string'
		})
		.option("snowflake_role", {
			alias: "sfr",
			demandOption: false,
			describe: "role to use in snowflake",
			type: 'string'
		})
		.option("snowflake_access_url", {
			alias: "sfurl",
			demandOption: false,
			describe: "access url for snowflake",
			type: 'string'
		})
		.option("redshift_workgroup", {
			alias: "rwg",
			demandOption: false,
			describe: "workgroup name in redshift",
			type: 'string'
		})
		.option("redshift_database", {
			alias: "rdb",
			demandOption: false,
			describe: "database to use in redshift",
			type: 'string'
		})
		.option("redshift_access_key_id", {
			alias: "rak",
			demandOption: false,
			describe: "key id for redshift access",
			type: 'string'
		})
		.option("redshift_secret_access_key", {
			alias: "ras",
			demandOption: false,
			describe: "secret access key for redshift access",
			type: 'string'
		})
		.option("redshift_session_token", {
			alias: "rat",
			demandOption: false,
			describe: "optional: session token for redshift access",
			type: 'string'
		})
		.option("redshift_region", {
			alias: "rsr",
			demandOption: false,
			describe: "redshift region to use",
			type: 'string'
		})
		.option("redshift_schema_name", {
			alias: "rsn",
			demandOption: false,
			describe: "redshift schema to use",
			type: 'string'
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

	const file = args._[0];
	const filePath = path.resolve(file);
	const exists = existsSync(filePath);
	if (!exists) throw new Error(`file not found: ${filePath}`);
	if (filePath.endsWith('.csv')) args.csv_file = filePath;
	if (filePath.endsWith('.json')) args.json_file = filePath;
	if (filePath.endsWith('.jsonl')) args.json_file = filePath;
	if (filePath.endsWith('.ndjson')) args.json_file = filePath;
	if (!args.csv_file && !args.json_file) throw new Error('file must be .csv or .json');
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

const banner = `CSVs (and JSON) files → ☁️ data warehouse\nby AK (ak@mixpanel.com) v${version || 2}`;

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