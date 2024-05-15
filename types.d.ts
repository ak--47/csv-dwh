type SimulationConfig = import("make-mp-data").Config;

export interface JobConfig {
  demoDataConfig?: SimulationConfig;
  csv_file: string;

  warehouse: "bigquery" | "snowflake";
  region: string;
  batch_size?: number;
  dry_run?: boolean;
  verbose?: boolean;

  // for csv imports
  table_name?: string;

  // for auto created data
  event_table_name?: string;
  user_table_name?: string;
  scd_table_name?: string;
  lookup_table_name?: string;
  group_table_name?: string;

  // BIGQUERY
  bigquery_dataset?: string;

  // SNOWFLAKE
  snowflake_account?: string;
  snowflake_user?: string;
  snowflake_password?: string;
  snowflake_database?: string;
  snowflake_schema?: string;
  snowflake_warehouse?: string;
  snowflake_role?: string;

  // redshift
  redshift_workgroup?: string;
  redshift_database?: string;
  AWS_ACCESS_KEY_ID?: string;
  AWS_SECRET_ACCESS_KEY?: string;
  AWS_SESSION_TOKEN?: string;
  AWS_REGION?: string;
}

// TypeScript types for inferred schema
type JSONType = "ARRAY" | "OBJECT" | "JSON";
type NumberType = "FLOAT" | "INT";
type DateType = "DATE" | "TIMESTAMP";
type SpecialType = "PRIMARY_KEY" | "FOREIGN_KEY" | "LOOKUP_KEY";
type BasicType =
  | "STRING"
  | "BOOLEAN"
  | JSONType
  | NumberType
  | DateType
  | SpecialType;

// vendor types
type SnowflakeTypes = "VARIANT";

export interface SchemaField {
  name: string;
  type: BasicType | SnowflakeTypes;
}

export type Schema = SchemaField[];

export type csvRecord = {
  [key: string]: string;
};

export type csvBatch = csvRecord[];

export interface WarehouseUploadResult {
  dataset?: string; // Identifier for the dataset used or created
  database?: string; // Identifier for the database used or created

  table: string; // Identifier for the table used or created
  schema: Schema; // Schema as applied in the warehouse
  upload: InsertResult[]; // Array of results for each batch processed
}

export type InsertResult = {
  status: "success" | "error"; // Status of the insert operation
  insertedRows?: number; // Number of rows successfully inserted
  failedRows?: number; // Number of rows that failed to insert
  duration: number; // Duration of the insert operation in milliseconds
  errors?: any[]; // Any errors encountered during the operation
  errorMessage?: string; // Error message if the operation failed
};

type InsertResult = {
  status: string;
  insertedRows: number;
  failedRows: number;
  duration: number;
  errors?: any;
  errorMessage?: string;
};
