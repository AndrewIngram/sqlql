import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";

import {
  buildSingleQueryPlan as buildRelationalSingleQueryPlan,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  isSupportedRelationalPlan,
  resolveRelationalStrategy,
  type RelationalSingleQueryPlan,
} from "../../shapes/relational-core";
import type {
  SqlRelationalCompileStrategy,
  SqlRelationalResolvedEntity,
  SqlRelationalScanBinding,
  SqlRelationalSelection,
} from "./types";
import { UnsupportedSqlRelationalPlanError } from "./types";

/**
 * SQL-relational planning helpers own the shared rel-to-single-query analysis used by ordinary
 * SQL-like adapters. They intentionally stop at backend-neutral plans; query syntax remains in the
 * backend translation layer.
 */
export interface SqlRelationalCompileHelpers<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
> {
  buildSingleQueryPlan(rel: RelNode): RelationalSingleQueryPlan<TBinding>;
  resolveStrategy(node: RelNode): SqlRelationalCompileStrategy | null;
}

export function requireSqlRelationalProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): SqlRelationalSelection & { kind: "column" } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSqlRelationalPlanError(
      "Computed projections are not supported in SQL-relational single-query pushdown.",
    );
  }

  return {
    kind: "column",
    output: mapping.output,
    source: mapping.source,
  };
}

export function createSqlRelationalScanBinding<TResolvedEntity extends SqlRelationalResolvedEntity>(
  scan: Extract<RelNode, { kind: "scan" }>,
  resolvedEntities: Record<string, TResolvedEntity>,
): SqlRelationalScanBinding<TResolvedEntity> {
  const resolved = resolvedEntities[scan.table];
  if (!resolved) {
    throw new UnsupportedSqlRelationalPlanError(
      `Missing SQL-relational entity config for "${scan.table}".`,
    );
  }

  return {
    alias: scan.alias ?? resolved.table,
    entity: resolved.entity,
    table: resolved.table,
    scan,
    resolved,
  };
}

export function resolveSqlRelationalCompileStrategy<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  node: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  createScanBinding: (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => TBinding,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
): SqlRelationalCompileStrategy | null {
  return resolveRelationalStrategy(node, {
    basicStrategy: "basic",
    setOpStrategy: "set_op",
    withStrategy: "with",
    canCompileBasic: (current) =>
      canCompileBasicRel(current, (table) => !!resolvedEntities[table], {
        requireColumnProjectMappings: options?.requireColumnProjectMappings ?? true,
      }),
    validateBasic: (current) =>
      isSupportedRelationalPlan(() => {
        buildSqlRelationalSingleQueryPlan(current, resolvedEntities, createScanBinding);
      }),
    canCompileSetOp: (current) =>
      canCompileSetOpRel(
        current,
        (branch) =>
          resolveSqlRelationalCompileStrategy(branch, resolvedEntities, createScanBinding, options),
        requireSqlRelationalProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveSqlRelationalCompileStrategy(branch, resolvedEntities, createScanBinding, options),
      ),
  });
}

export function buildSqlRelationalSingleQueryPlan<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  rel: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  createScanBinding: (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => TBinding,
): RelationalSingleQueryPlan<TBinding> {
  return buildRelationalSingleQueryPlan(rel, (scan) => createScanBinding(scan, resolvedEntities));
}

export function createSqlRelationalCompileHelpers<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  resolvedEntities: Record<string, TResolvedEntity>,
  createScanBinding: (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => TBinding,
  planningHooks: {
    buildSingleQueryPlan?: (
      rel: RelNode,
      resolvedEntities: Record<string, TResolvedEntity>,
    ) => RelationalSingleQueryPlan<TBinding>;
    resolveRelCompileStrategy?: (
      node: RelNode,
      resolvedEntities: Record<string, TResolvedEntity>,
      options?: { requireColumnProjectMappings?: boolean },
    ) => SqlRelationalCompileStrategy | null;
  },
  options?: {
    requireColumnProjectMappings?: boolean;
  },
): SqlRelationalCompileHelpers<TResolvedEntity, TBinding> {
  return {
    buildSingleQueryPlan(rel) {
      return (
        planningHooks.buildSingleQueryPlan?.(rel, resolvedEntities) ??
        buildSqlRelationalSingleQueryPlan(rel, resolvedEntities, createScanBinding)
      );
    },
    resolveStrategy(node) {
      return (
        planningHooks.resolveRelCompileStrategy?.(node, resolvedEntities, options) ??
        resolveSqlRelationalCompileStrategy(node, resolvedEntities, createScanBinding, options)
      );
    },
  };
}
