declare module "pg" {
  export interface ClientConfig { connectionString?: string; connectionTimeoutMillis?: number; statement_timeout?: number }
  export interface QueryResult<Row = Record<string, unknown>> { rows: Row[] }
  export class Client {
    constructor(config?: ClientConfig);
    connect(): Promise<void>;
    end(): Promise<void>;
    query<Row = Record<string, unknown>>(text: string, values?: readonly unknown[]): Promise<QueryResult<Row>>;
  }
}
