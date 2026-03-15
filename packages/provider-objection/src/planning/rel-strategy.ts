import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";
import { UnsupportedSqlRelationalPlanError } from "@tupl/provider-kit/relational-sql";
import {
  buildSingleQueryPlan as buildRelationalSingleQueryPlan,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  resolveRelationalStrategy,
  type RelationalScanBindingBase,
  type RelationalSemiJoinStep,
  type RelationalSingleQueryPlan,
} from "@tupl/provider-kit/shapes";

import type { ResolvedEntityConfig } from "../types";

export class UnsupportedSingleQueryPlanError extends UnsupportedSqlRelationalPlanError {}

export type ObjectionRelCompileStrategy = "basic" | "set_op" | "with";

export interface ObjectionRelCompiledPlan {
  strategy: ObjectionRelCompileStrategy;
  rel: RelNode;
}

export interface ScanBinding<TContext> extends RelationalScanBindingBase {
  alias: string;
  entity: string;
  table: string;
  resolved: ResolvedEntityConfig<TContext>;
}

export type SemiJoinStep = RelationalSemiJoinStep;
export type SingleQueryPlan<TContext> = RelationalSingleQueryPlan<ScanBinding<TContext>>;

export function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
) {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed projections are not supported in Objection single-query pushdown.",
    );
  }

  return mapping;
}

export function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): SingleQueryPlan<TContext> {
  return buildRelationalSingleQueryPlan(rel, (scan) => {
    const resolved = entityConfigs[scan.table];
    if (!resolved) {
      throw new UnsupportedSingleQueryPlanError(
        `Missing Objection entity config for "${scan.table}".`,
      );
    }

    return {
      alias: scan.alias ?? resolved.table,
      entity: resolved.entity,
      table: resolved.table,
      scan,
      resolved,
    };
  });
}

export function resolveObjectionRelCompileStrategy<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ObjectionRelCompileStrategy | null {
  const isKnownScan = (table: string) => table in entityConfigs;
  const resolveBranchStrategy = (rel: RelNode): ObjectionRelCompileStrategy | null =>
    resolveObjectionRelCompileStrategy(rel, entityConfigs);

  return resolveRelationalStrategy(node, {
    basicStrategy: "basic" as const,
    setOpStrategy: "set_op" as const,
    withStrategy: "with" as const,
    canCompileBasic: (rel) => canCompileBasicRel(rel, isKnownScan),
    validateBasic: (rel) => canCompileBasicRel(rel, isKnownScan),
    canCompileSetOp: (rel) =>
      canCompileSetOpRel(rel, resolveBranchStrategy, requireColumnProjectMapping),
    canCompileWith: (rel) => canCompileWithRel(rel, resolveBranchStrategy),
  });
}
