import { Config as SimulationConfig } from "make-mp-data";

// Job configuration interface
export interface JobConfig {
  demoDataConfig?: SimulationConfig;
  csv_file: string;

  warehouse: "bigquery" | "snowflake" | "redshift";
  region: string;
  batch_size?: number;
  dry_run?: boolean;
  verbose?: boolean;

  // for CSV imports
  table_name?: string;

  // for auto-created data
  event_table_name?: string;
  user_table_name?: string;
  scd_table_name?: string;
  lookup_table_name?: string;
  group_table_name?: string;

  // BigQuery specific
  bigquery_dataset?: string;

  // Snowflake specific
  snowflake_account?: string;
  snowflake_user?: string;
  snowflake_password?: string;
  snowflake_database?: string;
  snowflake_schema?: string;
  snowflake_warehouse?: string;
  snowflake_role?: string;

  // Redshift specific
  redshift_workgroup?: string;
  redshift_database?: string;
  redshift_access_key_id?: string;
  redshift_secret_access_key?: string;
  redshift_session_token?: string;
  redshift_region?: string;
  redshift_schema_name?: string;
}

// TypeScript types for inferred schema
type JSONType = "ARRAY" | "OBJECT" | "JSON";
type NumberType = "FLOAT" | "INT";
type DateType = "DATE" | "TIMESTAMP";
type SpecialType = "PRIMARY_KEY" | "FOREIGN_KEY" | "LOOKUP_KEY";
type BasicType = "STRING" | "BOOLEAN" | JSONType | NumberType | DateType | SpecialType;

// Vendor-specific types
type BigQueryTypes = "STRING" | "BOOLEAN" | "INTEGER" | "FLOAT" | "TIMESTAMP";
type SnowflakeTypes = "VARIANT" | "STRING" | "BOOLEAN" | "NUMBER" | "FLOAT" | "TIMESTAMP" | "DATE";
type RedshiftTypes = "SUPER" | "VARCHAR" | "BOOLEAN" | "INTEGER" | "REAL" | "TIMESTAMP" | "DATE";

// Schema field interface
export interface SchemaField {
  name: string;
  type: BasicType | BigQueryTypes | SnowflakeTypes | RedshiftTypes;
}

// Schema type
export type Schema = SchemaField[];

// CSV record and batch types
export type csvRecord = {
  [key: string]: string;
};

export type csvBatch = csvRecord[];

// Warehouse upload result interface
export interface WarehouseUploadResult {
  dataset?: string; // Identifier for the dataset used or created
  database?: string; // Identifier for the database used or created
  table: string; // Identifier for the table used or created
  schema: Schema; // Schema as applied in the warehouse
  upload: InsertResult[]; // Array of results for each batch processed
}

// Insert result type
export type InsertResult = {
  status: "success" | "error"; // Status of the insert operation
  insertedRows?: number; // Number of rows successfully inserted
  failedRows?: number; // Number of rows that failed to insert
  duration: number; // Duration of the insert operation in milliseconds
  errors?: any[]; // Any errors encountered during the operation
  errorMessage?: string; // Error message if the operation failed
  meta?: any; // Additional metadata
};
