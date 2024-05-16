# csv-dwh

## 🤨 wat.

**local csv/json file → ☁️ data warehouse table**

schematize and bulk insert local CSV or JSON files to various data warehouses... currently supporting BigQuery, Snowflake, and Redshift!


## 👔 tldr;

this module can be used in _two ways_:

- **as a [CLI](#cli)**, standalone script via:

```bash
npx csv-dwh file.csv --options
```

- **as a [module](#mod)** in code via

```javascript
//for esm:
import csvDwh from "csv-dwh";
//for cjs:
const csvDwh = require("csv-dwh");

const result = await csvDwh({
  warehouse: "bigquery",
  csv_file: "./path/to/data.csv",
});
```

<div id="cli"></div>

### 💻 CLI usage

```bash
npx csv-dwh@latest ./pathToData
```

when running as a CLI, `pathToData` should be a `.csv` or `.json` file.

when using the CLI, supply params as options in the form `--option value`, for example your warehouse configuration:

```bash
npx csv-dwh ./data.csv --warehouse bigquery --bigquery_dataset my_dataset --table_name my_table
```

many other options are available; to see a full list of CLI params, use the `--help` option:

```bash
npx csv-dwh --help
```

alternatively, you may use an [`.env` configuration file](#env) to provide your configuration details.

<div id="mod"></div>

### 🔌 module usage

install `csv-dwh` as a dependency in your project

```bash
npm i csv-dwh --save
```

then use it in code:

```javascript
const csvDwh = require("csv-dwh");

const config = {
  warehouse: "bigquery",
  csv_file: "./path/to/data.csv",
  bigquery_dataset: "my_dataset",
  // other options...
};

const result = await csvDwh(config);

console.log(result);
/*

{
  version: '1.0.0',
  PARAMS: { ... },
  results: [ ... ],
  e2eDuration: 1234,
  clockTime: '00:20',
  recordsPerSec: 500,
  totalRows: 10000,
  intermediateSchema: [ ... ]
}

*/
```

read more about [`config`](#config) below.

<div id="config"></div>

## 🗣️ configuration

when using `csv-dwh`, you will pass in a configuration object. The object should include settings specific to the warehouse you are targeting.

<div id="warehouse"></div>

### 🏢 warehouse

The `warehouse` option specifies the target data warehouse. It can be one of `"bigquery"`, `"snowflake"`, or `"redshift"`.

<div id="auth"></div>

### 🔐 authentication

Each warehouse requires specific authentication details:

#### BigQuery:

```javascript
const config = {
  warehouse: "bigquery",
  // optional: path to your Google Cloud service account key file
  bigquery_keyfile: "/path/to/keyfile.json",
};
```

note: if no `bigquery_keyfile` is provided, the module will attempt to use Application Default Credentials.

#### Snowflake:

```javascript
const config = {
  warehouse: "snowflake",
  snowflake_account: "your_account",
  snowflake_user: "your_user",
  snowflake_password: "your_password",
  snowflake_database: "your_database",
  snowflake_schema: "your_schema",
  snowflake_warehouse: "your_warehouse",
  snowflake_role: "your_role",
};
```

#### Redshift:

```javascript
const config = {
  warehouse: "redshift",
  redshift_workgroup: "your_workgroup",
  redshift_database: "your_database",
  redshift_access_key_id: "your_access_key_id",
  redshift_secret_access_key: "your_secret_access_key",
  redshift_region: "your_region",
  redshift_schema_name: "your_schema",
};
```

<div id="env"></div>

#### 🤖 environment variables:

You can also provide the configuration details using a `.env` file:

```makefile
# bigquery
bigquery_project=my-gcp-project
bigquery_dataset=my_dataset
bigquery_table=my_table
bigquery_keyfile=myfile.json
bigquery_service_account=my-service-acct@foo.com
bigquery_service_account_pass=****

# snowflake
snowflake_account=accountId
snowflake_user=foo
snowflake_password=****
snowflake_database=DEMO
snowflake_schema=PUBLIC
snowflake_warehouse=COMPUTE_WH
snowflake_role=ACCOUNTADMIN


# redshift
redshift_workgroup=my-workgroup
redshift_database=my_db
redshift_access_key_id=my-key
redshift_secret_access_key=my-secret
redshift_schema_name=public
redshift_region=us-east-2
# optional
redshift_session_token=none
```

^ ensure the `.env` file is in the root of your project, and the module will automatically read the configuration details from it; no need to pass them in as options.



<div id="options"></div>

### 🎛 options

Additional options can be provided to customize the behavior of the module:

```javascript
const config = {
  batch_size: 1000, // number of records per batch
  dry_run: false, // if true, does not actually upload data
  verbose: true, // if true, logs detailed information
};
```

### thanks for reading.

found a bug? have an idea? [let me know](https://github.com/your-repo/csv-dwh/issues)
