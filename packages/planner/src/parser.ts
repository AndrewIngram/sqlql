import { parseSqliteSelectAst } from "./sqlite-parser/parser";

export interface SqlAstParser {
  astify(sql: string): unknown;
}

class SqliteAstParser implements SqlAstParser {
  astify(sql: string): unknown {
    return parseSqliteSelectAst(sql);
  }
}

export const defaultSqlAstParser: SqlAstParser = new SqliteAstParser();
