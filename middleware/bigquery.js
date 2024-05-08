const { BigQuery } = require('@google-cloud/bigquery');
const client = new BigQuery();

async function main(batches, PARAMS) {
	console.log('todo!');
	return { status: 'todo' };
}


async function createDataset(datasetId) {
	if (!datasetId) datasetId = 'my_dataset';	
	const [dataset] = await client.createDataset(datasetId);
	console.log(`Dataset ${dataset.id} created.`);
}

async function createTable(datasetId, tableId, schema) {	
	if (!datasetId) datasetId = 'my_dataset';
	if (!tableId) tableId = 'my_new_table';
	if (!schema) schema = [
		{ name: 'Name', type: 'STRING', mode: 'REQUIRED' },
		{ name: 'Age', type: 'INTEGER' },
		{ name: 'Weight', type: 'FLOAT' },
		{ name: 'IsMagic', type: 'BOOLEAN' },
	];

	/**
	 * TODO(developer): Uncomment the following lines before running the sample.
	 */
	// const datasetId = "my_dataset";
	// const tableId = "my_table";
	// const schema = 'Name:string, Age:integer, Weight:float, IsMagic:boolean';

	// For all options, see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
	const options = {
		schema: schema,
		location: 'US',
	};

	// Create a new table in the dataset
	const [table] = await client
		.dataset(datasetId)
		.createTable(tableId, options);

	console.log(`Table ${table.id} created.`);
}


module.exports = main;