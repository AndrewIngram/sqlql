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

import type { KyselyProviderEntityConfig, ResolvedEntityConfig } from "../types";

export interface KyselyRelCompiledPlan {
  strategy: KyselyRelCompileStrategy;
  rel: RelNode;
}

export type KyselyRelCompileStrategy = "basic" | "set_op" | "with";

export class UnsupportedSingleQueryPlanError extends UnsupportedRelationalPlanError {}

export interface ScanBinding<TContext> extends RelationalScanBindingBase {
  alias: string;
  entity: string;
  table: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  config: KyselyProviderEntityConfig<TContext>;
}

export type SemiJoinStep = RelationalSemiJoinStep;
export type SingleQueryPlan<TContext> = RelationalSingleQueryPlan<ScanBinding<TContext>>;

export function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): { source: { alias?: string; table?: string; column: string }; output: string } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed projections are not supported in Kysely single-query pushdown.",
    );
  }
  return mapping;
}

export function resolveKyselyRelCompileStrategy<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): KyselyRelCompileStrategy | null {
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
        (branch) => resolveKyselyRelCompileStrategy(branch, entityConfigs),
        requireColumnProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveKyselyRelCompileStrategy(branch, entityConfigs),
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
    throw new UnsupportedSingleQueryPlanError(`Missing Kysely entity config for "${scan.table}".`);
  }

  return {
    alias: scan.alias ?? binding.table,
    entity: binding.entity,
    table: binding.table,
    scan,
    config: binding.config,
  };
}
