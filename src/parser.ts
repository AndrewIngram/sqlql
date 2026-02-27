import nodeSqlParser from "node-sql-parser";

export interface SqlAstParser {
  astify(sql: string): unknown;
}

const { Parser } = nodeSqlParser as {
  Parser: new () => {
    astify: (sql: string, options?: { database?: string }) => unknown;
  };
};

class NodeSqlAstParser implements SqlAstParser {
  readonly #parser = new Parser();

  astify(sql: string): unknown {
    return this.#parser.astify(sql);
  }
}

export const defaultSqlAstParser: SqlAstParser = new NodeSqlAstParser();
