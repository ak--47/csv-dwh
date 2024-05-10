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

// TypeScript types for inferred schema
type JSONType = 'ARRAY' | 'OBJECT' | 'JSON';
type NumberType = 'FLOAT' | 'INT';
type DateType = 'DATE' | 'TIMESTAMP';
type BasicType = 'STRING' | 'BOOLEAN' | JSONType | NumberType | DateType;

export interface SchemaField {
  name: string;
  type: BasicType;
}

export type Schema = SchemaField[];

export type Result = { 
	dataset: string; 
	table: string; 
	schema: any; 
	upload: InsertResult[];
}


export type csvRecord = {
	[key: string]: string 
}

export type csvBatch = csvRecord[]



type InsertResult = {
	status: string;
	insertedRows: number;
	failedRows: number;
	duration: number;
	errors?: any;
	errorMessage?: string;
}
