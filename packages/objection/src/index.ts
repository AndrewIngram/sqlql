import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  QueryRow,
  RelNode,
  ScanFilterClause,
  TableScanRequest,
} from "sqlql";

export interface KnexLike {
  table(name: string): any;
  raw(sql: string, params?: unknown[]): Promise<unknown>;
}

export interface ObjectionProviderTableConfig<TContext> {
  scope?: (query: any, context: TContext, table: string) => void | Promise<void>;
}

export interface CreateObjectionProviderOptions<TContext> {
  knex: KnexLike;
  tables?: Record<string, ObjectionProviderTableConfig<TContext>>;
}

export function createObjectionProvider<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
): ProviderAdapter<TContext> {
  return {
    canExecute(fragment): boolean | ProviderCapabilityReport {
      return fragment.kind === "scan" ||
        fragment.kind === "sql_query" ||
        (fragment.kind === "rel" && canCompileRel(fragment.rel));
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      if (fragment.kind === "rel") {
        const compiled = compileRelToSql(fragment.rel);
        if (!compiled) {
          throw new Error("Unsupported relational fragment for objection provider.");
        }
        return {
          provider: "objection",
          kind: "rel",
          payload: {
            sql: compiled.sql,
          },
        };
      }

      return {
        provider: "objection",
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      switch (plan.kind) {
        case "sql_query": {
          const fragment = plan.payload as ProviderFragment;
          return executeRawSql(options.knex, fragment.sql);
        }
        case "rel": {
          const compiled = plan.payload as { sql: string };
          return executeRawSql(options.knex, compiled.sql);
        }
        case "scan": {
          const fragment = plan.payload as ProviderFragment;
          return executeScan(options, fragment.request, context);
        }
        default:
          throw new Error(`Unsupported objection compiled plan kind: ${plan.kind}`);
      }
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      const scanRequest: TableScanRequest = {
        table: request.table,
        select: request.select,
        where: [
          ...(request.where ?? []),
          {
            op: "in",
            column: request.key,
            values: request.keys,
          } as ScanFilterClause,
        ],
      };

      return executeScan(options, scanRequest, context);
    },
  };
}

async function executeScan<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  let qb = options.knex.table(request.table).select(request.select);

  const tableScope = options.tables?.[request.table]?.scope;
  if (tableScope) {
    await tableScope(qb, context, request.table);
  }

  for (const clause of request.where ?? []) {
    qb = applyFilter(qb, clause);
  }

  for (const term of request.orderBy ?? []) {
    qb = qb.orderBy(term.column, term.direction);
  }

  if (request.limit != null) {
    qb = qb.limit(request.limit);
  }

  if (request.offset != null) {
    qb = qb.offset(request.offset);
  }

  const rows = await qb;
  return Array.isArray(rows) ? rows : [];
}

async function executeRawSql(knex: KnexLike, sql: string): Promise<QueryRow[]> {
  const result = await knex.raw(sql);

  if (Array.isArray(result)) {
    const first = result[0];
    return Array.isArray(first) ? (first as QueryRow[]) : [];
  }

  if (result && typeof result === "object" && "rows" in result) {
    const rows = (result as { rows?: unknown }).rows;
    return Array.isArray(rows) ? (rows as QueryRow[]) : [];
  }

  return [];
}

function applyFilter(qb: any, clause: ScanFilterClause): any {
  switch (clause.op) {
    case "eq":
      return qb.where(clause.column, "=", clause.value);
    case "neq":
      return qb.where(clause.column, "!=", clause.value);
    case "gt":
      return qb.where(clause.column, ">", clause.value);
    case "gte":
      return qb.where(clause.column, ">=", clause.value);
    case "lt":
      return qb.where(clause.column, "<", clause.value);
    case "lte":
      return qb.where(clause.column, "<=", clause.value);
    case "in":
      return qb.whereIn(clause.column, clause.values);
    case "is_null":
      return qb.whereNull(clause.column);
    case "is_not_null":
      return qb.whereNotNull(clause.column);
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
