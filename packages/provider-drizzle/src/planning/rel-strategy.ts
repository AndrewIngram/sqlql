import { isRelProjectColumnMapping, type RelExpr, type RelNode } from "@tupl/foundation";
import type { SqlRelationalScanBinding } from "@tupl/provider-kit";
import {
  UnsupportedRelationalPlanError,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  extractRelPipeline,
  isSupportedRelationalPlan,
  resolveRelationalStrategy,
  type RelationalJoinPlan,
  type RelationalJoinStep,
  type RelationalScanBindingBase,
  type RelationalSemiJoinStep,
  type RelationalSingleQueryPlan,
} from "@tupl/provider-kit/shapes";
import { sql, type AnyColumn, type SQL } from "drizzle-orm";

import { resolveColumns } from "../backend/table-columns";
import type { DrizzleColumnMap, ResolvedEntityConfig } from "../types";

export interface DrizzleRelCompiledPlan {
  strategy: DrizzleRelCompileStrategy;
  rel: RelNode;
}

export type DrizzleRelCompileStrategy = "basic" | "set_op" | "with";

export class UnsupportedSingleQueryPlanError extends UnsupportedRelationalPlanError {}

export interface ScanBinding<TContext>
  extends RelationalScanBindingBase, SqlRelationalScanBinding<ResolvedEntityConfig<TContext>> {
  alias: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  tableName: string;
  sourceTable: object;
  scanColumns: DrizzleColumnMap<string>;
  columns: Record<string, AnyColumn | SQL>;
  outputColumns: string[];
  tableConfig: ResolvedEntityConfig<TContext>["config"];
}

export type SemiJoinStep = RelationalSemiJoinStep;
export type JoinStep<TContext> = RelationalJoinStep<ScanBinding<TContext>>;
export type JoinPlan<TContext> = RelationalJoinPlan<ScanBinding<TContext>>;

export interface QualifiedJoinColumnRef {
  alias: string;
  column: string;
}

export type SingleQueryPlan<TContext> = RelationalSingleQueryPlan<ScanBinding<TContext>>;

export function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): { source: { alias?: string; table?: string; column: string }; output: string } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed projections are not supported in Drizzle single-query pushdown.",
    );
  }
  return mapping;
}

export function resolveDrizzleRelCompileStrategy(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<any>>,
): DrizzleRelCompileStrategy | null {
  return resolveRelationalStrategy(node, {
    basicStrategy: "basic",
    setOpStrategy: "set_op",
    withStrategy: "with",
    canCompileBasic: (current) => canCompileBasicRel(current, (table) => !!entityConfigs[table]),
    validateBasic: (current) =>
      isSupportedRelationalPlan(() => {
        buildSingleQueryPlan(current, entityConfigs);
      }),
    canCompileSetOp: (current) =>
      canCompileSetOpRel(
        current,
        (branch) =>
          canCompileBasicRel(branch, (table) => !!entityConfigs[table]) ? "basic" : null,
        requireColumnProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveDrizzleRelCompileStrategy(branch, entityConfigs),
      ),
  });
}

export function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): SingleQueryPlan<TContext> {
  const pipeline = extractRelPipeline(rel);
  const joinPlan = buildJoinPlan(pipeline.base, entityConfigs);

  return {
    joinPlan,
    pipeline,
  };
}

function buildJoinPlan<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): JoinPlan<TContext> {
  if (node.kind === "scan") {
    const root = createScanBinding(node, entityConfigs);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind === "project") {
    const root = createProjectedScanBinding(node, entityConfigs);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind !== "join") {
    throw new UnsupportedSingleQueryPlanError(
      `Expected scan/join base node, received "${node.kind}".`,
    );
  }

  const left = buildJoinPlan(node.left, entityConfigs);
  if (node.joinType === "semi") {
    const leftRef = qualifyJoinColumnRef(node.leftKey, left.aliases);
    const rightAlias = node.rightKey.alias ?? node.rightKey.table;

    return {
      root: left.root,
      joins: [
        ...left.joins,
        {
          joinType: "semi",
          right: node.right,
          leftKey: {
            alias: leftRef.alias,
            column: leftRef.column,
          },
          rightKey: {
            ...(rightAlias ? { alias: rightAlias } : {}),
            column: node.rightKey.column,
          },
        },
      ],
      aliases: new Map(left.aliases),
    };
  }

  const right = buildJoinPlan(node.right, entityConfigs);
  if (
    right.joins.length > 0 &&
    (node.joinType !== "inner" || right.joins.some((join) => join.joinType !== "inner"))
  ) {
    throw new UnsupportedSingleQueryPlanError("Only left-deep join trees are supported.");
  }

  const rightRoot = right.root;
  if (left.aliases.has(rightRoot.alias)) {
    throw new UnsupportedSingleQueryPlanError(`Duplicate alias "${rightRoot.alias}" in join tree.`);
  }

  const seenTables = new Set([...left.aliases.values()].map((binding) => binding.tableName));
  if (seenTables.has(rightRoot.tableName)) {
    throw new UnsupportedSingleQueryPlanError(
      "Joining the same physical table more than once is not supported without aliases.",
    );
  }

  const leftRef = qualifyJoinColumnRef(node.leftKey, left.aliases);
  const rightRef = qualifyJoinColumnRef(node.rightKey, right.aliases);

  const aliases = new Map(left.aliases);
  aliases.set(rightRoot.alias, rightRoot);
  for (const [alias, binding] of right.aliases.entries()) {
    aliases.set(alias, binding);
  }

  return {
    root: left.root,
    joins: [
      ...left.joins,
      {
        joinType: node.joinType,
        right: rightRoot,
        leftKey: {
          alias: leftRef.alias,
          column: leftRef.column,
        },
        rightKey: {
          alias: rightRef.alias,
          column: rightRef.column,
        },
      },
      ...right.joins,
    ],
    aliases,
  };
}

export function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ScanBinding<TContext> {
  const resolved = entityConfigs[scan.table];
  if (!resolved) {
    throw new UnsupportedSingleQueryPlanError(`Missing drizzle table config for "${scan.table}".`);
  }
  const tableConfig = resolved.config;

  return {
    alias: scan.alias ?? scan.table,
    entity: resolved.entity,
    scan,
    tableName: scan.table,
    table: resolved.table,
    sourceTable: tableConfig.table,
    resolved,
    scanColumns: resolveColumns(tableConfig, scan.table),
    columns: resolveColumns(tableConfig, scan.table),
    outputColumns: scan.select,
    tableConfig,
  };
}

function qualifyJoinColumnRef<TContext>(
  ref: { alias?: string; table?: string; column: string },
  aliases: Map<string, ScanBinding<TContext>>,
): QualifiedJoinColumnRef {
  const explicitAlias = ref.alias ?? ref.table;
  if (explicitAlias) {
    return {
      alias: explicitAlias,
      column: ref.column,
    };
  }

  let matchedAlias: string | null = null;
  for (const [alias, binding] of aliases.entries()) {
    if (!(ref.column in binding.columns)) {
      continue;
    }
    if (matchedAlias && matchedAlias !== alias) {
      throw new UnsupportedSingleQueryPlanError(
        `Ambiguous unqualified join key "${ref.column}" in rel fragment.`,
      );
    }
    matchedAlias = alias;
  }

  if (!matchedAlias) {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown unqualified join key "${ref.column}" in rel fragment.`,
    );
  }

  return {
    alias: matchedAlias,
    column: ref.column,
  };
}

function createProjectedScanBinding<TContext>(
  project: Extract<RelNode, { kind: "project" }>,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ScanBinding<TContext> {
  if (project.input.kind !== "scan") {
    throw new UnsupportedSingleQueryPlanError(
      "Projected join inputs must project directly from a scan.",
    );
  }

  const base = createScanBinding(project.input, entityConfigs);
  const aliases = new Map([[base.alias, base]]);
  const columns: Record<string, AnyColumn | SQL> = {};

  for (const rawMapping of project.columns) {
    if (isRelProjectColumnMapping(rawMapping)) {
      if (rawMapping.source.alias && rawMapping.source.alias !== base.alias) {
        throw new UnsupportedSingleQueryPlanError(
          `Projected scan column "${rawMapping.source.alias}.${rawMapping.source.column}" must reference alias "${base.alias}".`,
        );
      }
    }

    columns[rawMapping.output] = resolveProjectedSqlExpression(rawMapping, aliases, true);
  }

  return {
    ...base,
    columns,
    outputColumns: project.columns.map((column) => column.output),
  };
}

export function resolveProjectedSqlExpression<TContext>(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
  aliases: Map<string, ScanBinding<TContext>>,
  allowSourceOnly: boolean,
): SQL | AnyColumn {
  if (isRelProjectColumnMapping(mapping)) {
    const source = resolveColumnRefFromAliasMap(
      aliases,
      toAliasColumnRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
    );
    return allowSourceOnly ? source : sql`${source}`;
  }

  return buildSqlExpressionFromRelExpr(mapping.expr, aliases);
}

export function buildSqlExpressionFromRelExpr<TContext>(
  expr: RelExpr,
  aliases: Map<string, ScanBinding<TContext>>,
): SQL | AnyColumn {
  switch (expr.kind) {
    case "literal":
      return sql`${expr.value}`;
    case "column":
      return resolveColumnRefFromAliasMap(
        aliases,
        toAliasColumnRef(expr.ref.alias ?? expr.ref.table, expr.ref.column),
      );
    case "function": {
      const args = expr.args.map((arg) => buildSqlExpressionFromRelExpr(arg, aliases));
      switch (expr.name) {
        case "eq":
          return sql`${args[0]} = ${args[1]}`;
        case "neq":
          return sql`${args[0]} <> ${args[1]}`;
        case "gt":
          return sql`${args[0]} > ${args[1]}`;
        case "gte":
          return sql`${args[0]} >= ${args[1]}`;
        case "lt":
          return sql`${args[0]} < ${args[1]}`;
        case "lte":
          return sql`${args[0]} <= ${args[1]}`;
        case "add":
          return sql`(${args[0]} + ${args[1]})`;
        case "subtract":
          return sql`(${args[0]} - ${args[1]})`;
        case "multiply":
          return sql`(${args[0]} * ${args[1]})`;
        case "divide":
          return sql`(${args[0]} / ${args[1]})`;
        case "and":
          return sql`(${sql.join(
            args.map((arg) => sql`${arg}`),
            sql` and `,
          )})`;
        case "or":
          return sql`(${sql.join(
            args.map((arg) => sql`${arg}`),
            sql` or `,
          )})`;
        case "not":
          return sql`not (${args[0]})`;
        default:
          throw new UnsupportedSingleQueryPlanError(
            `Unsupported computed projection function "${expr.name}" in Drizzle single-query pushdown.`,
          );
      }
    }
    case "subquery":
      throw new UnsupportedSingleQueryPlanError(
        "Subquery expressions are not supported in Drizzle single-query pushdown.",
      );
  }
}

export function resolveColumnRefFromAliasMap<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): AnyColumn | SQL {
  if (ref.alias) {
    const binding = aliases.get(ref.alias);
    if (!binding) {
      throw new UnsupportedSingleQueryPlanError(`Unknown alias "${ref.alias}" in rel fragment.`);
    }
    const source = binding.columns[ref.column];
    if (!source) {
      throw new UnsupportedSingleQueryPlanError(
        `Unknown column "${ref.column}" on alias "${ref.alias}" in rel fragment.`,
      );
    }
    return source;
  }

  let matched: AnyColumn | SQL | null = null;
  for (const binding of aliases.values()) {
    const source = binding.columns[ref.column];
    if (!source) {
      continue;
    }
    if (matched && matched !== source) {
      throw new UnsupportedSingleQueryPlanError(
        `Ambiguous unqualified column "${ref.column}" in rel fragment.`,
      );
    }
    matched = source;
  }

  if (!matched) {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown unqualified column "${ref.column}" in rel fragment.`,
    );
  }

  return matched;
}

export function resolveJoinKeyColumnRefFromAliasMap<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): AnyColumn {
  const source = resolveColumnRefFromAliasMap(aliases, ref);
  if (typeof source === "object" && source !== null && "name" in source) {
    return source as AnyColumn;
  }
  const qualified = ref.alias ? `${ref.alias}.${ref.column}` : ref.column;
  throw new UnsupportedSingleQueryPlanError(
    `Join keys must resolve to physical columns. "${qualified}" resolved to a computed expression.`,
  );
}

export function toAliasColumnRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  return alias ? { alias, column } : { column };
}
