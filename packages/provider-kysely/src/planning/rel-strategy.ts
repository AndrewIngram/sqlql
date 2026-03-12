import type { RelNode } from "@tupl/foundation";
import {
  buildSqlRelationalSingleQueryPlan,
  createSqlRelationalScanBinding,
  resolveSqlRelationalCompileStrategy,
  UnsupportedSqlRelationalPlanError,
  type SqlRelationalCompiledPlan,
  type SqlRelationalCompileStrategy,
  type SqlRelationalScanBinding,
} from "@tupl/provider-kit";

import type { ResolvedEntityConfig } from "../types";

export type KyselyRelCompiledPlan = SqlRelationalCompiledPlan;
export type KyselyRelCompileStrategy = SqlRelationalCompileStrategy;

export class UnsupportedSingleQueryPlanError extends UnsupportedSqlRelationalPlanError {}

export type ScanBinding<TContext> = SqlRelationalScanBinding<ResolvedEntityConfig<TContext>>;

export function resolveKyselyRelCompileStrategy<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): KyselyRelCompileStrategy | null {
  return resolveSqlRelationalCompileStrategy(node, entityConfigs, createScanBinding, {
    requireColumnProjectMappings: true,
  });
}

export function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
) {
  return buildSqlRelationalSingleQueryPlan(rel, entityConfigs, createScanBinding);
}

export function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ScanBinding<TContext> {
  return createSqlRelationalScanBinding(scan, entityConfigs);
}
