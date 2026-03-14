import type { QueryExecutionPlanStep, QueryStepState } from "@tupl/runtime/session";

type StepExecutionClass = "domain_call" | "local_over_fetched_rows" | "internal_op";

function readDetails(step: QueryExecutionPlanStep): Record<string, unknown> | undefined {
  return step.operation.details;
}

function readStringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string")
    : [];
}

function readRecordArray(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value)
    ? value.filter(
        (entry): entry is Record<string, unknown> => !!entry && typeof entry === "object",
      )
    : [];
}

function commaList(values: string[], maxItems = 3): string {
  if (values.length === 0) {
    return "";
  }
  const shown = values.slice(0, maxItems);
  return values.length > maxItems
    ? `${shown.join(", ")}, +${values.length - maxItems}`
    : shown.join(", ");
}

function formatMetric(metric: Record<string, unknown>): string {
  const fn = typeof metric.fn === "string" ? metric.fn : "agg";
  const column = typeof metric.column === "string" ? metric.column : "*";
  const output = typeof metric.as === "string" ? metric.as : undefined;
  return output ? `${fn}(${column}) -> ${output}` : `${fn}(${column})`;
}

function formatOrderTerm(term: Record<string, unknown>): string {
  const source = typeof term.source === "string" ? term.source : "?";
  const direction = typeof term.direction === "string" ? term.direction.toUpperCase() : "ASC";
  return `${source} ${direction}`;
}

function formatFunction(fn: Record<string, unknown>): string {
  const name = typeof fn.fn === "string" ? fn.fn : "window";
  const output = typeof fn.as === "string" ? fn.as : undefined;
  return output ? `${name} -> ${output}` : name;
}

function summarizeRequest(step: QueryExecutionPlanStep): string | null {
  const request = step.request;
  if (!request) {
    return null;
  }

  if (step.kind === "scan") {
    const select = readStringArray(request.select);
    const filters = Array.isArray(request.where) ? request.where.length : 0;
    const sortTerms = Array.isArray(request.orderBy) ? request.orderBy.length : 0;
    const parts = [];
    if (select.length > 0) {
      parts.push(`cols ${commaList(select)}`);
    }
    if (filters > 0) {
      parts.push(`${filters} filter${filters === 1 ? "" : "s"}`);
    }
    if (sortTerms > 0) {
      parts.push(`${sortTerms} sort term${sortTerms === 1 ? "" : "s"}`);
    }
    if (typeof request.limit === "number") {
      parts.push(`limit ${request.limit}`);
    }
    if (typeof request.offset === "number") {
      parts.push(`offset ${request.offset}`);
    }
    return parts.join(" | ") || null;
  }

  if (step.kind === "filter") {
    const whereCount = Array.isArray(request.where) ? request.where.length : 0;
    if (whereCount > 0) {
      return `${whereCount} predicate${whereCount === 1 ? "" : "s"}`;
    }
    if ("expr" in request) {
      return "computed predicate";
    }
  }

  return null;
}

function summarizeOperation(step: QueryExecutionPlanStep): string {
  const details = readDetails(step) ?? {};

  if (step.operation.name === "values") {
    const rowCount = typeof details.rowCount === "number" ? details.rowCount : undefined;
    return rowCount != null
      ? `${rowCount} literal row${rowCount === 1 ? "" : "s"}`
      : "literal row materialization";
  }

  switch (step.kind) {
    case "scan": {
      const alias = typeof details.alias === "string" ? details.alias : undefined;
      const table = typeof details.table === "string" ? details.table : step.summary;
      return alias && alias !== table ? `${table} as ${alias}` : table;
    }
    case "filter":
      return summarizeRequest(step) ?? "apply predicates";
    case "projection": {
      const outputs = step.outputs ?? [];
      return outputs.length > 0 ? `emit ${commaList(outputs)}` : "reshape result rows";
    }
    case "aggregate": {
      const groupBy = readStringArray(details.groupBy);
      const metrics = readRecordArray(details.metrics).map(formatMetric);
      const parts = [];
      if (groupBy.length > 0) {
        parts.push(`by ${commaList(groupBy)}`);
      }
      if (metrics.length > 0) {
        parts.push(commaList(metrics, 2));
      }
      return parts.join(" | ") || "grouped aggregation";
    }
    case "window": {
      const functions = readRecordArray(details.functions).map(formatFunction);
      return functions.length > 0 ? commaList(functions, 2) : "window calculation";
    }
    case "order": {
      const terms = readRecordArray(details.orderBy).map(formatOrderTerm);
      return terms.length > 0 ? commaList(terms, 2) : "sort rows";
    }
    case "limit_offset": {
      const limit = typeof details.limit === "number" ? `limit ${details.limit}` : undefined;
      const offset = typeof details.offset === "number" ? `offset ${details.offset}` : undefined;
      return (
        [limit, offset].filter((value): value is string => !!value).join(" | ") || "slice rows"
      );
    }
    case "join":
      return typeof details.on === "string" ? details.on : "combine two inputs";
    case "lookup_join":
      return typeof details.on === "string" ? details.on : "lookup-driven join";
    case "remote_fragment": {
      const provider = typeof details.provider === "string" ? details.provider : undefined;
      const fragment =
        step.request && typeof step.request.fragment === "string"
          ? step.request.fragment
          : "fragment";
      return provider ? `${provider} ${fragment}` : fragment;
    }
    case "cte":
      return typeof details.name === "string" ? details.name : "materialize CTE";
    case "set_op_branch":
      return typeof details.branch === "string" ? `${details.branch} branch` : "set-op branch";
    default:
      return step.summary;
  }
}

function buildFacts(step: QueryExecutionPlanStep, state: QueryStepState | null): string[] {
  const facts: string[] = [];

  if (step.outputs && step.outputs.length > 0) {
    facts.push(`${step.outputs.length} output column${step.outputs.length === 1 ? "" : "s"}`);
  }
  if (step.dependsOn.length > 0) {
    facts.push(`${step.dependsOn.length} input${step.dependsOn.length === 1 ? "" : "s"}`);
  }
  if (state?.outputRowCount != null) {
    facts.push(`${state.outputRowCount} output rows`);
  } else if (state?.rowCount != null) {
    facts.push(`${state.rowCount} rows`);
  }
  if (state?.durationMs != null) {
    facts.push(`${state.durationMs}ms`);
  }

  const requestSummary = summarizeRequest(step);
  if (requestSummary) {
    facts.push(requestSummary);
  }

  return facts.slice(0, 4);
}

function isCteScanStep(step: QueryExecutionPlanStep): boolean {
  if (step.kind !== "scan" || step.operation.name !== "scan") {
    return false;
  }

  const details = step.operation.details;
  if (!details || typeof details !== "object") {
    return false;
  }

  return (details as { isCte?: unknown }).isCte === true;
}

export function classifyStepExecution(
  step: QueryExecutionPlanStep,
  state: QueryStepState | null,
): StepExecutionClass {
  if (step.operation.name === "values") {
    return "internal_op";
  }

  if (step.kind.startsWith("local_")) {
    return "internal_op";
  }

  if (isCteScanStep(step)) {
    return "local_over_fetched_rows";
  }

  if (state?.routeUsed === "local") {
    if (step.kind === "scan" || step.phase === "fetch") {
      return "local_over_fetched_rows";
    }
    return "internal_op";
  }

  if (state?.routeUsed) {
    if (
      ["scan", "lookup", "aggregate", "provider_fragment", "lookup_join"].includes(state.routeUsed)
    ) {
      return "domain_call";
    }
    return "internal_op";
  }

  if (
    step.kind === "aggregate" &&
    Array.isArray((step.pushdown as { routeCandidates?: unknown })?.routeCandidates) &&
    ((step.pushdown as { routeCandidates?: string[] }).routeCandidates ?? []).length === 1 &&
    (step.pushdown as { routeCandidates?: string[] }).routeCandidates?.[0] === "local"
  ) {
    return "local_over_fetched_rows";
  }

  if (
    step.phase === "fetch" ||
    step.kind === "scan" ||
    step.kind === "remote_fragment" ||
    step.kind === "lookup_join"
  ) {
    return "domain_call";
  }

  return "internal_op";
}

function classLabel(stepClass: StepExecutionClass): string {
  switch (stepClass) {
    case "domain_call":
      return "domain call";
    case "local_over_fetched_rows":
      return "local over fetched rows";
    case "internal_op":
      return "internal op";
  }
}

function placementSummary(step: QueryExecutionPlanStep, state: QueryStepState | null): string {
  if (step.kind === "remote_fragment") {
    const provider =
      typeof step.operation.details?.provider === "string"
        ? step.operation.details.provider
        : "provider";
    return `Remote on ${provider}`;
  }
  if (step.kind === "lookup_join") {
    return "Remote lookup + local stitch";
  }
  if (state?.routeUsed === "scan") {
    return "Remote scan";
  }
  if (state?.routeUsed === "aggregate") {
    return "Remote aggregate";
  }
  if (state?.routeUsed === "provider_fragment") {
    return "Remote fragment";
  }
  if (state?.routeUsed === "lookup") {
    return "Remote lookup";
  }
  if (state?.routeUsed === "local") {
    return "Local runtime";
  }

  return classLabel(classifyStepExecution(step, state));
}

function clauseSummary(step: QueryExecutionPlanStep): string {
  return step.sqlOrigin ?? step.phase.toUpperCase();
}

function operatorLabel(step: QueryExecutionPlanStep): string {
  if (step.operation.name === "values") {
    return "Values";
  }

  switch (step.kind) {
    case "scan":
      return "Scan";
    case "filter":
      return "Filter";
    case "projection":
      return "Project";
    case "aggregate":
      return "Aggregate";
    case "window":
      return "Window";
    case "order":
      return "Sort";
    case "limit_offset":
      return "Limit";
    case "join":
      return "Join";
    case "lookup_join":
      return "Lookup Join";
    case "remote_fragment":
      return "Remote Fragment";
    case "cte":
      return "CTE";
    case "set_op_branch":
      return "Set Branch";
    default:
      return step.kind;
  }
}

export interface StepPresentation {
  operator: string;
  clause: string;
  signature: string;
  placement: string;
  facts: string[];
  outputsPreview: string | null;
  executionClass: StepExecutionClass;
  executionLabel: string;
}

export function presentStep(
  step: QueryExecutionPlanStep,
  state: QueryStepState | null,
): StepPresentation {
  const executionClass = classifyStepExecution(step, state);
  return {
    operator: operatorLabel(step),
    clause: clauseSummary(step),
    signature: summarizeOperation(step),
    placement: placementSummary(step, state),
    facts: buildFacts(step, state),
    outputsPreview: step.outputs && step.outputs.length > 0 ? commaList(step.outputs, 4) : null,
    executionClass,
    executionLabel: classLabel(executionClass),
  };
}
