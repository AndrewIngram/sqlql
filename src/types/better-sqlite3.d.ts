declare module "better-sqlite3" {
  interface Statement {
    run(...params: unknown[]): unknown;
    all(...params: unknown[]): Array<Record<string, unknown>>;
  }

  type Transaction<TArgs extends unknown[]> = (...args: TArgs) => void;

  export default class Database {
    constructor(filename: string, options?: Record<string, unknown>);
    exec(sql: string): this;
    prepare(sql: string): Statement;
    transaction<TArgs extends unknown[]>(fn: (...args: TArgs) => void): Transaction<TArgs>;
    close(): void;
  }
}
