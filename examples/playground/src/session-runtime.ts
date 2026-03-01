import {
  createArrayTableMethods,
  createQuerySession,
  defaultSqlAstParser,
  defineTableMethods,
  type QueryExecutionPlan,
  type QueryRow,
  type QuerySession,
  type QueryStepEvent,
  type SchemaDefinition,
  type TableMethodsMap,
} from "sqlql";

import { parseRowsText, parseSchemaText } from "./validation";

export interface PlaygroundCompileSuccess {
  ok: true;
  schema: SchemaDefinition;
  rows: Record<string, QueryRow[]>;
  methods: TableMethodsMap<object>;
  sql: string;
}

export interface PlaygroundCompileFailure {
  ok: false;
  issues: string[];
}

export type PlaygroundCompileResult = PlaygroundCompileSuccess | PlaygroundCompileFailure;

export interface SessionSnapshot {
  session: QuerySession;
  plan: QueryExecutionPlan;
  events: QueryStepEvent[];
  result: QueryRow[] | null;
  done: boolean;
}

export function compilePlaygroundInput(
  schemaText: string,
  rowsText: string,
  sqlText: string,
): PlaygroundCompileResult {
  const schemaResult = parseSchemaText(schemaText);
  if (!schemaResult.ok || !schemaResult.schema) {
    return {
      ok: false,
      issues: schemaResult.issues.map((issue) => `${issue.path}: ${issue.message}`),
    };
  }

  const rowsResult = parseRowsText(schemaResult.schema, rowsText);
  const parsedRows = rowsResult.rows;
  if (!rowsResult.ok || !parsedRows) {
    return {
      ok: false,
      issues: rowsResult.issues.map((issue) => `${issue.path}: ${issue.message}`),
    };
  }

  const normalizedSql = sqlText.trim().replace(/;+$/u, "").trim();
  if (normalizedSql.length === 0) {
    return {
      ok: false,
      issues: ["SQL query cannot be empty."],
    };
  }

  try {
    const ast = defaultSqlAstParser.astify(normalizedSql);
    if (Array.isArray(ast)) {
      throw new Error("Only a single SQL statement is supported.");
    }

    const type = (ast as { type?: unknown }).type;
    if (type !== "select") {
      throw new Error("Only SELECT statements are currently supported.");
    }
  } catch (error) {
    return {
      ok: false,
      issues: [error instanceof Error ? error.message : "Invalid SQL query."],
    };
  }

  const methodEntries = Object.keys(schemaResult.schema.tables).map((tableName) => {
    const tableRows = parsedRows[tableName] ?? [];
    return [tableName, createArrayTableMethods(tableRows)] as const;
  });

  const methods = defineTableMethods(schemaResult.schema, Object.fromEntries(methodEntries));

  return {
    ok: true,
    schema: schemaResult.schema,
    rows: parsedRows,
    methods,
    sql: normalizedSql,
  };
}

export function createSession(compiled: PlaygroundCompileSuccess): QuerySession {
  return createQuerySession({
    schema: compiled.schema,
    methods: compiled.methods,
    context: {},
    sql: compiled.sql,
    options: {
      maxConcurrency: 4,
      captureRows: "full",
    },
  });
}

export async function replaySession(
  compiled: PlaygroundCompileSuccess,
  eventCount: number,
): Promise<SessionSnapshot> {
  const session = createSession(compiled);
  const events: QueryStepEvent[] = [];

  while (events.length < eventCount) {
    const next = await session.next();
    if ("done" in next) {
      return {
        session,
        plan: session.getPlan(),
        events,
        result: next.result,
        done: true,
      };
    }

    events.push(next);
  }

  return {
    session,
    plan: session.getPlan(),
    events,
    result: null,
    done: false,
  };
}

export async function runSessionToCompletion(
  session: QuerySession,
  existingEvents: QueryStepEvent[],
): Promise<SessionSnapshot> {
  const events = [...existingEvents];

  while (true) {
    const next = await session.next();
    if ("done" in next) {
      return {
        session,
        plan: session.getPlan(),
        events,
        result: next.result,
        done: true,
      };
    }
    events.push(next);
  }
}
