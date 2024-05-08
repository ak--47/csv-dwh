type SimulationConfig = import('make-mp-data').Config

export type JobConfig = {	
	demoDataConfig?: SimulationConfig,
	csv_file: string,

	warehouse : "bigquery",
	batch_size? : number,

	// for csv imports
	table_name? : string,

	// for auto created data
	event_table_name? : string,
	user_table_name? : string,
	scd_table_name? : string,
	lookup_table_name? : string,
	group_table_name? : string,

	// BIGQUERY
	bigquery_dataset? : string
}