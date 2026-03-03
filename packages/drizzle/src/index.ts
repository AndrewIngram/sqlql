import {
  type AnyColumn,
  and,
  asc,
  desc,
  eq,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  lt,
  lte,
  ne,
  sql,
  type SQL,
} from "drizzle-orm";
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  QueryRow,
  RelNode,
  ScanFilterClause,
  ScanOrderBy,
  TableScanRequest,
} from "sqlql";

export type DrizzleColumnMap<TColumn extends string = string> = Record<TColumn, AnyColumn>;

export interface DrizzleQueryExecutor {
  select: (...args: unknown[]) => unknown;
}

export interface DrizzleProviderTableConfig<
  TContext,
  TColumn extends string = string,
> {
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  scope?:
    | ((context: TContext) => SQL | SQL[] | undefined | Promise<SQL | SQL[] | undefined>)
    | undefined;
}

export interface CreateDrizzleProviderOptions<TContext> {
  db: DrizzleQueryExecutor;
  tables: Record<string, DrizzleProviderTableConfig<TContext, string>>;
  executeSql?: (sqlText: string, context: TContext) => Promise<QueryRow[]>;
}

export function createDrizzleProvider<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
): ProviderAdapter<TContext> {
  return {
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return !!options.tables[fragment.table];
        case "sql_query": {
          if (!options.executeSql) {
            return {
              supported: false,
              reason: "executeSql is required for sql_query fragments.",
            };
          }

          // Scope predicates are table-level closures; do not bypass them with raw SQL.
          const hasScopedTable = fragment.rel.kind === "sql"
            ? fragment.rel.tables.some((table) => !!options.tables[table]?.scope)
            : false;

          if (hasScopedTable) {
            return {
              supported: false,
              reason: "Raw SQL fragment pushdown is disabled for scoped tables.",
            };
          }

          return true;
        }
        case "rel":
          return options.executeSql && canCompileRel(fragment.rel)
            ? true
            : {
                supported: false,
                reason:
                  "executeSql is required and rel fragment must be in the supported relational subset.",
              };
        default:
          return false;
      }
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      if (fragment.kind === "rel") {
        if (!options.executeSql) {
          throw new Error("Drizzle provider missing executeSql callback for rel fragments.");
        }

        const compiled = compileRelToSql(fragment.rel);
        if (!compiled) {
          throw new Error("Unsupported relational fragment for drizzle provider.");
        }

        return {
          provider: "drizzle",
          kind: fragment.kind,
          payload: {
            sql: compiled.sql,
          },
        };
      }

      return {
        provider: "drizzle",
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      return executeDrizzlePlan(plan, options, context);
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      return lookupManyWithDrizzle(options, request, context);
    },
  };
}

async function executeDrizzlePlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  switch (plan.kind) {
    case "sql_query": {
      const fragment = plan.payload as ProviderFragment;
      if (!options.executeSql) {
        throw new Error("Drizzle provider missing executeSql callback for sql_query fragments.");
      }
      return options.executeSql(fragment.sql, context);
    }
    case "rel": {
      const compiled = plan.payload as { sql: string };
      if (!options.executeSql) {
        throw new Error("Drizzle provider missing executeSql callback for rel fragments.");
      }
      return options.executeSql(compiled.sql, context);
    }
    case "scan": {
      const fragment = plan.payload as ProviderFragment;
      const tableConfig = options.tables[fragment.table];
      if (!tableConfig) {
        throw new Error(`Unknown drizzle table config: ${fragment.table}`);
      }

      const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
      return runDrizzleScan({
        db: options.db,
        tableName: fragment.table,
        table: tableConfig.table,
        columns: tableConfig.columns,
        request: fragment.request,
        scope,
      });
    }
    default:
      throw new Error(`Unsupported drizzle compiled plan kind: ${plan.kind}`);
  }
}

async function lookupManyWithDrizzle<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfig = options.tables[request.table];
  if (!tableConfig) {
    throw new Error(`Unknown drizzle table config: ${request.table}`);
  }

  const where: ScanFilterClause[] = [
    ...(request.where ?? []),
    {
      op: "in",
      column: request.key,
      values: request.keys,
    } as ScanFilterClause,
  ];

  const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
  return runDrizzleScan({
    db: options.db,
    tableName: request.table,
    table: tableConfig.table,
    columns: tableConfig.columns,
    request: {
      table: request.table,
      select: request.select,
      where,
    },
    scope,
  });
}

export interface RunDrizzleScanOptions<TTable extends string, TColumn extends string> {
  db: DrizzleQueryExecutor;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  request: TableScanRequest<TTable, TColumn>;
  scope?: SQL | SQL[];
}

export async function runDrizzleScan<TTable extends string, TColumn extends string>(
  options: RunDrizzleScanOptions<TTable, TColumn>,
): Promise<QueryRow[]> {
  const selection = buildSelection(options.request.select, options.columns, options.tableName);
  const filterConditions = (options.request.where ?? []).map((clause) =>
    toSqlCondition(clause, options.columns, options.tableName),
  );
  const scopeConditions = normalizeScope(options.scope);
  const whereConditions = [...scopeConditions, ...filterConditions];

  let builder = options.db.select(selection).from(options.table as never) as {
    where: (condition: SQL) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    execute: () => Promise<QueryRow[]>;
  };

  const where = and(...whereConditions);
  if (where) {
    builder = builder.where(where) as typeof builder;
  }

  const orderBy = buildOrderBy(options.request.orderBy, options.columns, options.tableName);
  if (orderBy.length > 0) {
    builder = builder.orderBy(...orderBy) as typeof builder;
  }

  if (options.request.limit != null) {
    builder = builder.limit(options.request.limit) as typeof builder;
  }

  if (options.request.offset != null) {
    builder = builder.offset(options.request.offset) as typeof builder;
  }

  return builder.execute();
}

export function impossibleCondition(): SQL {
  return sql`0 = 1`;
}

function normalizeScope(scope: SQL | SQL[] | undefined): SQL[] {
  if (!scope) {
    return [];
  }
  return Array.isArray(scope) ? scope : [scope];
}

function buildSelection<TColumn extends string>(
  selectedColumns: TColumn[],
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): Record<TColumn, AnyColumn> {
  const out = {} as Record<TColumn, AnyColumn>;
  for (const column of selectedColumns) {
    const source = columns[column];
    if (!source) {
      throw new Error(`Unsupported column "${column}" for table "${tableName}".`);
    }
    out[column] = source;
  }
  return out;
}

function buildOrderBy<TColumn extends string>(
  orderBy: ScanOrderBy<TColumn>[] | undefined,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL[] {
  const out: SQL[] = [];
  for (const term of orderBy ?? []) {
    const source = columns[term.column];
    if (!source) {
      throw new Error(`Unsupported ORDER BY column "${term.column}" for table "${tableName}".`);
    }

    out.push(term.direction === "asc" ? asc(source) : desc(source));
  }
  return out;
}

function toSqlCondition<TColumn extends string>(
  clause: ScanFilterClause<TColumn>,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL {
  const source = columns[clause.column as TColumn];
  if (!source) {
    throw new Error(`Unsupported filter column "${clause.column}" for table "${tableName}".`);
  }

  switch (clause.op) {
    case "eq":
      return eq(source, clause.value as never);
    case "neq":
      return ne(source, clause.value as never);
    case "gt":
      return gt(source, clause.value as never);
    case "gte":
      return gte(source, clause.value as never);
    case "lt":
      return lt(source, clause.value as never);
    case "lte":
      return lte(source, clause.value as never);
    case "in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return impossibleCondition();
      }
      return inArray(source, values as never[]);
    }
    case "is_null":
      return isNull(source);
    case "is_not_null":
      return isNotNull(source);
  }
}

interface CompiledRel {
  sql: string;
  output: string[];
}

function canCompileRel(node: RelNode): boolean {
  switch (node.kind) {
    case "scan":
      return true;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return canCompileRel(node.input);
    case "join":
    case "set_op":
      return canCompileRel(node.left) && canCompileRel(node.right);
    case "with":
      return node.ctes.every((cte) => canCompileRel(cte.query)) && canCompileRel(node.body);
    case "sql":
      return false;
  }
}

function compileRelToSql(node: RelNode): CompiledRel | null {
  switch (node.kind) {
    case "scan": {
      const relationAlias = node.alias ?? node.table;
      const selected = node.select.map((column) => ({
        source: `"${relationAlias}"."${escapeIdentifier(column)}"`,
        output: `${relationAlias}.${column}`,
      }));

      let sqlText = `SELECT ${selected
        .map((entry) => `${entry.source} AS "${escapeIdentifier(entry.output)}"`)
        .join(", ")} FROM "${escapeIdentifier(node.table)}" AS "${escapeIdentifier(relationAlias)}"`;

      if (node.where && node.where.length > 0) {
        const clauses = node.where.map((clause) =>
          compileFilterClause(clause, node.select, relationAlias),
        );
        sqlText += ` WHERE ${clauses.join(" AND ")}`;
      }

      if (node.orderBy && node.orderBy.length > 0) {
        sqlText += ` ORDER BY ${node.orderBy
          .map((term) => `"${escapeIdentifier(relationAlias)}"."${escapeIdentifier(term.column)}" ${term.direction.toUpperCase()}`)
          .join(", ")}`;
      }

      if (node.limit != null) {
        sqlText += ` LIMIT ${node.limit}`;
      }

      if (node.offset != null) {
        sqlText += ` OFFSET ${node.offset}`;
      }

      return {
        sql: sqlText,
        output: selected.map((entry) => entry.output),
      };
    }
    case "filter": {
      const input = compileRelToSql(node.input);
      if (!input) {
        return null;
      }
      const alias = "__f";
      const where = node.where
        .map((clause) => compileFilterClause(clause, input.output, alias))
        .join(" AND ");

      return {
        sql: `SELECT * FROM (${input.sql}) AS "${alias}"${where ? ` WHERE ${where}` : ""}`,
        output: [...input.output],
      };
    }
    case "project": {
      const input = compileRelToSql(node.input);
      if (!input) {
        return null;
      }
      const alias = "__p";

      const selectSql = node.columns
        .map((column) => {
          const inputName = resolveOutputColumn(
            `${column.source.alias ?? column.source.table ?? ""}.${column.source.column}`.replace(
              /^\./,
              "",
            ),
            input.output,
          );
          if (!inputName) {
            return null;
          }
          return `"${alias}"."${escapeIdentifier(inputName)}" AS "${escapeIdentifier(column.output)}"`;
        })
        .filter((entry): entry is string => !!entry);

      if (selectSql.length !== node.columns.length) {
        return null;
      }

      return {
        sql: `SELECT ${selectSql.join(", ")} FROM (${input.sql}) AS "${alias}"`,
        output: node.columns.map((column) => column.output),
      };
    }
    case "join": {
      const left = compileRelToSql(node.left);
      const right = compileRelToSql(node.right);
      if (!left || !right) {
        return null;
      }

      const leftAlias = "__l";
      const rightAlias = "__r";
      const leftJoinKey = resolveOutputColumn(
        `${node.leftKey.alias ?? node.leftKey.table ?? ""}.${node.leftKey.column}`.replace(/^\./, ""),
        left.output,
      );
      const rightJoinKey = resolveOutputColumn(
        `${node.rightKey.alias ?? node.rightKey.table ?? ""}.${node.rightKey.column}`.replace(/^\./, ""),
        right.output,
      );
      if (!leftJoinKey || !rightJoinKey) {
        return null;
      }

      const outputColumns = [...left.output, ...right.output];
      const selectSql = [
        ...left.output.map(
          (column) =>
            `"${leftAlias}"."${escapeIdentifier(column)}" AS "${escapeIdentifier(column)}"`,
        ),
        ...right.output.map(
          (column) =>
            `"${rightAlias}"."${escapeIdentifier(column)}" AS "${escapeIdentifier(column)}"`,
        ),
      ];

      const joinKeyword =
        node.joinType === "inner"
          ? "INNER JOIN"
          : node.joinType === "left"
            ? "LEFT JOIN"
            : node.joinType === "right"
              ? "RIGHT JOIN"
              : "FULL JOIN";

      return {
        sql: `SELECT ${selectSql.join(", ")} FROM (${left.sql}) AS "${leftAlias}" ${joinKeyword} (${right.sql}) AS "${rightAlias}" ON "${leftAlias}"."${escapeIdentifier(leftJoinKey)}" = "${rightAlias}"."${escapeIdentifier(rightJoinKey)}"`,
        output: outputColumns,
      };
    }
    case "aggregate": {
      const input = compileRelToSql(node.input);
      if (!input) {
        return null;
      }
      const alias = "__a";
      const selectSql: string[] = [];
      const groupBySql: string[] = [];
      const output: string[] = [];

      for (const groupRef of node.groupBy) {
        const inputName = resolveOutputColumn(
          `${groupRef.alias ?? groupRef.table ?? ""}.${groupRef.column}`.replace(/^\./, ""),
          input.output,
        );
        if (!inputName) {
          return null;
        }
        selectSql.push(
          `"${alias}"."${escapeIdentifier(inputName)}" AS "${escapeIdentifier(groupRef.column)}"`,
        );
        groupBySql.push(`"${alias}"."${escapeIdentifier(inputName)}"`);
        output.push(groupRef.column);
      }

      for (const metric of node.metrics) {
        const fn = metric.fn.toUpperCase();
        if (metric.column) {
          const inputName = resolveOutputColumn(
            `${metric.column.alias ?? metric.column.table ?? ""}.${metric.column.column}`.replace(
              /^\./,
              "",
            ),
            input.output,
          );
          if (!inputName) {
            return null;
          }
          const distinct = metric.distinct ? "DISTINCT " : "";
          selectSql.push(
            `${fn}(${distinct}"${alias}"."${escapeIdentifier(inputName)}") AS "${escapeIdentifier(metric.as)}"`,
          );
        } else {
          selectSql.push(`${fn}(*) AS "${escapeIdentifier(metric.as)}"`);
        }
        output.push(metric.as);
      }

      let sqlText = `SELECT ${selectSql.join(", ")} FROM (${input.sql}) AS "${alias}"`;
      if (groupBySql.length > 0) {
        sqlText += ` GROUP BY ${groupBySql.join(", ")}`;
      }
      return { sql: sqlText, output };
    }
    case "sort": {
      const input = compileRelToSql(node.input);
      if (!input) {
        return null;
      }
      const alias = "__s";
      const orderBySql = node.orderBy
        .map((term) =>
          resolveOutputColumn(
            `${term.source.alias ?? term.source.table ?? ""}.${term.source.column}`.replace(/^\./, ""),
            input.output,
          ),
        )
        .map((column, index) =>
          column
            ? `"${alias}"."${escapeIdentifier(column)}" ${node.orderBy[index]?.direction.toUpperCase()}`
            : null,
        )
        .filter((entry): entry is string => !!entry);

      if (orderBySql.length !== node.orderBy.length) {
        return null;
      }

      return {
        sql: `SELECT * FROM (${input.sql}) AS "${alias}" ORDER BY ${orderBySql.join(", ")}`,
        output: [...input.output],
      };
    }
    case "limit_offset": {
      const input = compileRelToSql(node.input);
      if (!input) {
        return null;
      }
      let sqlText = `SELECT * FROM (${input.sql}) AS "__lo"`;
      if (node.limit != null) {
        sqlText += ` LIMIT ${node.limit}`;
      }
      if (node.offset != null) {
        sqlText += ` OFFSET ${node.offset}`;
      }
      return {
        sql: sqlText,
        output: [...input.output],
      };
    }
    case "set_op": {
      const left = compileRelToSql(node.left);
      const right = compileRelToSql(node.right);
      if (!left || !right) {
        return null;
      }
      const op =
        node.op === "union_all"
          ? "UNION ALL"
          : node.op === "union"
            ? "UNION"
            : node.op === "intersect"
              ? "INTERSECT"
              : "EXCEPT";
      return {
        sql: `(${left.sql}) ${op} (${right.sql})`,
        output: [...node.output.map((column) => column.name)],
      };
    }
    case "with": {
      const ctes = node.ctes
        .map((cte) => {
          const query = compileRelToSql(cte.query);
          if (!query) {
            return null;
          }
          return `"${escapeIdentifier(cte.name)}" AS (${query.sql})`;
        })
        .filter((entry): entry is string => !!entry);
      if (ctes.length !== node.ctes.length) {
        return null;
      }

      const body = compileRelToSql(node.body);
      if (!body) {
        return null;
      }

      return {
        sql: `WITH ${ctes.join(", ")} ${body.sql}`,
        output: [...body.output],
      };
    }
    case "sql":
      return null;
  }
}

function compileFilterClause(
  clause: ScanFilterClause,
  outputs: string[],
  sourceAlias: string,
): string {
  const column = resolveOutputColumn(clause.column, outputs) ?? clause.column;
  const ref = `"${sourceAlias}"."${escapeIdentifier(column)}"`;

  switch (clause.op) {
    case "eq":
      return `${ref} = ${literalSql(clause.value)}`;
    case "neq":
      return `${ref} != ${literalSql(clause.value)}`;
    case "gt":
      return `${ref} > ${literalSql(clause.value)}`;
    case "gte":
      return `${ref} >= ${literalSql(clause.value)}`;
    case "lt":
      return `${ref} < ${literalSql(clause.value)}`;
    case "lte":
      return `${ref} <= ${literalSql(clause.value)}`;
    case "in":
      return clause.values.length > 0
        ? `${ref} IN (${clause.values.map((value) => literalSql(value)).join(", ")})`
        : "1 = 0";
    case "is_null":
      return `${ref} IS NULL`;
    case "is_not_null":
      return `${ref} IS NOT NULL`;
  }
}

function literalSql(value: unknown): string {
  if (value == null) {
    return "NULL";
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }
  return `'${String(value).replaceAll("'", "''")}'`;
}

function resolveOutputColumn(column: string, outputs: string[]): string | null {
  if (outputs.includes(column)) {
    return column;
  }

  const suffix = `.${column}`;
  const matches = outputs.filter((output) => output.endsWith(suffix));
  return matches.length === 1 ? (matches[0] ?? null) : null;
}

function escapeIdentifier(identifier: string): string {
  return identifier.replaceAll('"', '""');
}
