import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";
import {
  UnsupportedRelationalPlanError,
  buildSingleQueryPlan as buildRelationalSingleQueryPlan,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  isSupportedRelationalPlan,
  resolveRelationalStrategy,
  type RelationalScanBindingBase,
  type RelationalSemiJoinStep,
  type RelationalSingleQueryPlan,
} from "@tupl/provider-kit/shapes";

import type { ObjectionProviderEntityConfig, ResolvedEntityConfig } from "../types";

export interface ObjectionRelCompiledPlan {
  strategy: ObjectionRelCompileStrategy;
  rel: RelNode;
}

export type ObjectionRelCompileStrategy = "basic" | "set_op" | "with";

export class UnsupportedSingleQueryPlanError extends UnsupportedRelationalPlanError {}

export interface ScanBinding<TContext> extends RelationalScanBindingBase {
  alias: string;
  entity: string;
  table: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  config: ObjectionProviderEntityConfig<TContext>;
}

export type SemiJoinStep = RelationalSemiJoinStep;
export type SingleQueryPlan<TContext> = RelationalSingleQueryPlan<ScanBinding<TContext>>;

export function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): { source: { alias?: string; table?: string; column: string }; output: string } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed projections are not supported in Objection single-query pushdown.",
    );
  }
  return mapping;
}

export function resolveObjectionRelCompileStrategy<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ObjectionRelCompileStrategy | null {
  return resolveRelationalStrategy(node, {
    basicStrategy: "basic",
    setOpStrategy: "set_op",
    withStrategy: "with",
    canCompileBasic: (current) =>
      canCompileBasicRel(current, (table) => !!entityConfigs[table], {
        requireColumnProjectMappings: true,
      }),
    validateBasic: (current) =>
      isSupportedRelationalPlan(() => {
        buildSingleQueryPlan(current, entityConfigs);
      }),
    canCompileSetOp: (current) =>
      canCompileSetOpRel(
        current,
        (branch) => resolveObjectionRelCompileStrategy(branch, entityConfigs),
        requireColumnProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveObjectionRelCompileStrategy(branch, entityConfigs),
      ),
  });
}

export function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): SingleQueryPlan<TContext> {
  return buildRelationalSingleQueryPlan(rel, (scan) => createScanBinding(scan, entityConfigs));
}

export function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ScanBinding<TContext> {
  const binding = entityConfigs[scan.table];
  if (!binding) {
    throw new UnsupportedSingleQueryPlanError(
      `Missing Objection entity config for "${scan.table}".`,
    );
  }

  return {
    alias: scan.alias ?? binding.table,
    entity: binding.entity,
    table: binding.table,
    scan,
    config: binding.config,
  };
}
