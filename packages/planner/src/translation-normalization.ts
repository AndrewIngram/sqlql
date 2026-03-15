import type {
  RelColumnRef,
  RelConvention,
  RelExpr,
  RelNode,
  RelOutputColumn,
  RelProjectMapping,
} from "@tupl/foundation";

import type { PhysicalPlan, PhysicalStep } from "./physical/physical";
import type { ProviderRelTarget } from "./provider-fragments";

interface RelNormalizationState {
  relIds: Map<string, string>;
  relCount: number;
  subqueryIds: Map<string, string>;
  subqueryCount: number;
}

function createRelNormalizationState(): RelNormalizationState {
  return {
    relIds: new Map(),
    relCount: 0,
    subqueryIds: new Map(),
    subqueryCount: 0,
  };
}

function normalizeRelId(state: RelNormalizationState, rel: RelNode): string {
  const existing = state.relIds.get(rel.id);
  if (existing) {
    return existing;
  }

  state.relCount += 1;
  const normalized = `${rel.kind}_${state.relCount}`;
  state.relIds.set(rel.id, normalized);
  return normalized;
}

function normalizeSubqueryId(state: RelNormalizationState, id: string): string {
  const existing = state.subqueryIds.get(id);
  if (existing) {
    return existing;
  }

  state.subqueryCount += 1;
  const normalized = `subquery_${state.subqueryCount}`;
  state.subqueryIds.set(id, normalized);
  return normalized;
}

function normalizeOutput(output: RelOutputColumn[]) {
  return output.map((column) => ({
    name: column.name,
    ...(column.type ? { type: column.type } : {}),
    ...(column.nullable != null ? { nullable: column.nullable } : {}),
  }));
}

function normalizeColumnRef(ref: RelColumnRef) {
  return {
    ...(ref.table ? { table: ref.table } : {}),
    ...(ref.alias ? { alias: ref.alias } : {}),
    column: ref.column,
  };
}

function normalizeExpr(expr: RelExpr, state: RelNormalizationState): unknown {
  switch (expr.kind) {
    case "literal":
      return {
        kind: "literal",
        value: expr.value,
      };
    case "column":
      return {
        kind: "column",
        ref: normalizeColumnRef(expr.ref),
      };
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => normalizeExpr(arg, state)),
      };
    case "subquery":
      return {
        kind: "subquery",
        id: normalizeSubqueryId(state, expr.id),
        mode: expr.mode,
        ...(expr.outputColumn ? { outputColumn: expr.outputColumn } : {}),
        rel: normalizeRelNode(expr.rel, state),
      };
  }
}

function normalizeProjectMapping(mapping: RelProjectMapping, state: RelNormalizationState) {
  if (mapping.kind === "expr") {
    return {
      kind: "expr",
      expr: normalizeExpr(mapping.expr, state),
      output: mapping.output,
    };
  }

  return {
    kind: "column",
    source: normalizeColumnRef(mapping.source),
    output: mapping.output,
  };
}

function withConvention(convention: RelConvention) {
  return { convention };
}

function normalizeRelNode(rel: RelNode, state: RelNormalizationState): unknown {
  const base = {
    id: normalizeRelId(state, rel),
    kind: rel.kind,
    ...withConvention(rel.convention),
    output: normalizeOutput(rel.output),
  };

  switch (rel.kind) {
    case "scan":
      return {
        ...base,
        table: rel.table,
        ...(rel.alias ? { alias: rel.alias } : {}),
        select: [...rel.select],
        ...(rel.where ? { where: rel.where } : {}),
        ...(rel.orderBy ? { orderBy: rel.orderBy } : {}),
        ...(rel.limit != null ? { limit: rel.limit } : {}),
        ...(rel.offset != null ? { offset: rel.offset } : {}),
      };
    case "values":
      return {
        ...base,
        rows: rel.rows.map((row) => [...row]),
      };
    case "cte_ref":
      return {
        ...base,
        name: rel.name,
        ...(rel.alias ? { alias: rel.alias } : {}),
        select: [...rel.select],
        ...(rel.where ? { where: rel.where } : {}),
        ...(rel.orderBy ? { orderBy: rel.orderBy } : {}),
        ...(rel.limit != null ? { limit: rel.limit } : {}),
        ...(rel.offset != null ? { offset: rel.offset } : {}),
      };
    case "filter":
      return {
        ...base,
        input: normalizeRelNode(rel.input, state),
        ...(rel.where ? { where: rel.where } : {}),
        ...(rel.expr ? { expr: normalizeExpr(rel.expr, state) } : {}),
      };
    case "project":
      return {
        ...base,
        input: normalizeRelNode(rel.input, state),
        columns: rel.columns.map((column) => normalizeProjectMapping(column, state)),
      };
    case "join":
      return {
        ...base,
        joinType: rel.joinType,
        leftKey: normalizeColumnRef(rel.leftKey),
        rightKey: normalizeColumnRef(rel.rightKey),
        left: normalizeRelNode(rel.left, state),
        right: normalizeRelNode(rel.right, state),
      };
    case "correlate":
      return {
        ...base,
        correlation: {
          outer: normalizeColumnRef(rel.correlation.outer),
          inner: normalizeColumnRef(rel.correlation.inner),
        },
        apply:
          rel.apply.kind === "scalar_filter"
            ? {
                kind: "scalar_filter",
                comparison: rel.apply.comparison,
                outerCompare: normalizeColumnRef(rel.apply.outerCompare),
                correlationColumn: rel.apply.correlationColumn,
                metricColumn: rel.apply.metricColumn,
              }
            : rel.apply,
        left: normalizeRelNode(rel.left, state),
        right: normalizeRelNode(rel.right, state),
      };
    case "aggregate":
      return {
        ...base,
        input: normalizeRelNode(rel.input, state),
        groupBy: rel.groupBy.map((column) => normalizeColumnRef(column)),
        metrics: rel.metrics.map((metric) => ({
          fn: metric.fn,
          as: metric.as,
          ...(metric.column ? { column: normalizeColumnRef(metric.column) } : {}),
          ...(metric.distinct != null ? { distinct: metric.distinct } : {}),
        })),
      };
    case "window":
      return {
        ...base,
        input: normalizeRelNode(rel.input, state),
        functions: rel.functions.map((fn) => ({
          fn: fn.fn,
          as: fn.as,
          partitionBy: fn.partitionBy.map((column) => normalizeColumnRef(column)),
          orderBy: fn.orderBy.map((order) => ({
            source: normalizeColumnRef(order.source),
            direction: order.direction,
          })),
          ...("column" in fn && fn.column ? { column: normalizeColumnRef(fn.column) } : {}),
          ...("value" in fn ? { value: normalizeExpr(fn.value, state) } : {}),
          ...("offset" in fn && fn.offset != null ? { offset: fn.offset } : {}),
          ...("defaultExpr" in fn && fn.defaultExpr
            ? { defaultExpr: normalizeExpr(fn.defaultExpr, state) }
            : {}),
          ...("distinct" in fn && fn.distinct != null ? { distinct: fn.distinct } : {}),
          ...(fn.frame ? { frame: fn.frame } : {}),
        })),
      };
    case "sort":
      return {
        ...base,
        input: normalizeRelNode(rel.input, state),
        orderBy: rel.orderBy.map((order) => ({
          source: normalizeColumnRef(order.source),
          direction: order.direction,
        })),
      };
    case "limit_offset":
      return {
        ...base,
        input: normalizeRelNode(rel.input, state),
        ...(rel.limit != null ? { limit: rel.limit } : {}),
        ...(rel.offset != null ? { offset: rel.offset } : {}),
      };
    case "set_op":
      return {
        ...base,
        op: rel.op,
        left: normalizeRelNode(rel.left, state),
        right: normalizeRelNode(rel.right, state),
      };
    case "with":
      return {
        ...base,
        ctes: rel.ctes.map((cte) => ({
          name: cte.name,
          query: normalizeRelNode(cte.query, state),
        })),
        body: normalizeRelNode(rel.body, state),
      };
    case "repeat_union":
      return {
        ...base,
        cteName: rel.cteName,
        mode: rel.mode,
        seed: normalizeRelNode(rel.seed, state),
        iterative: normalizeRelNode(rel.iterative, state),
      };
  }
}

export function normalizeRelForSnapshot(rel: RelNode) {
  return normalizeRelNode(rel, createRelNormalizationState());
}

function normalizeProviderFragmentForSnapshot(
  fragment: ProviderRelTarget,
  state: RelNormalizationState,
) {
  return {
    provider: fragment.provider,
    rel: normalizeRelNode(fragment.rel, state),
  };
}

function normalizePhysicalStepForSnapshot(
  step: PhysicalStep,
  stepIds: Map<string, string>,
  relState: RelNormalizationState,
) {
  const base = {
    id: stepIds.get(step.id) ?? step.id,
    kind: step.kind,
    dependsOn: step.dependsOn.map((dependency) => stepIds.get(dependency) ?? dependency),
    summary: step.summary,
  };

  switch (step.kind) {
    case "remote_fragment":
      return {
        ...base,
        provider: step.provider,
        fragment: normalizeProviderFragmentForSnapshot(step.fragment, relState),
      };
    case "lookup_join":
      return {
        ...base,
        leftProvider: step.leftProvider,
        rightProvider: step.rightProvider,
        leftTable: step.leftTable,
        rightTable: step.rightTable,
        leftKey: step.leftKey,
        rightKey: step.rightKey,
        joinType: step.joinType,
      };
    default:
      return base;
  }
}

export function normalizePhysicalPlanForSnapshot(plan: PhysicalPlan) {
  const relState = createRelNormalizationState();
  const stepIds = new Map<string, string>();

  plan.steps.forEach((step, index) => {
    stepIds.set(step.id, `step_${index + 1}`);
  });

  return {
    rel: normalizeRelNode(plan.rel, relState),
    rootStepId: stepIds.get(plan.rootStepId) ?? plan.rootStepId,
    steps: plan.steps.map((step) => normalizePhysicalStepForSnapshot(step, stepIds, relState)),
  };
}
