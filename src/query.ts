import {
  getTable,
  resolveColumnType,
  resolveTableColumnDefinition,
  resolveTableQueryBehavior,
  type AggregateFunction,
  type QueryRow,
  type ScanFilterClause,
  type ScanOrderBy,
  type SchemaDefinition,
  type TableAggregateMetric,
  type TableAggregateRequest,
  type TableLookupRequest,
  type TableMethods,
  type TableMethodsMap,
  type TableScanRequest,
} from "./schema";
import { validateTableConstraintRows, type ConstraintValidationOptions } from "./constraints";
import { defaultSqlAstParser } from "./parser";

export interface SqlQuery {
  text: string;
}

export interface PlannedQuery {
  source: string;
  selectAll: boolean;
}

export interface QueryInput<TContext> {
  schema: SchemaDefinition;
  methods: TableMethodsMap<TContext>;
  context: TContext;
  sql: string;
  constraintValidation?: ConstraintValidationOptions;
}

export type QueryStepKind =
  | "cte"
  | "set_op_branch"
  | "scan"
  | "filter"
  | "join"
  | "aggregate"
  | "window"
  | "distinct"
  | "order"
  | "limit_offset"
  | "projection";

export type QueryStepPhase = "logical" | "fetch" | "transform" | "output";
export type QuerySqlOrigin =
  | "SELECT"
  | "FROM"
  | "WHERE"
  | "GROUP BY"
  | "HAVING"
  | "ORDER BY"
  | "WITH"
  | "SET_OP";
export type QueryStepRoute = "scan" | "lookup" | "aggregate" | "local";

export interface QueryStepOperation {
  name: string;
  details?: Record<string, unknown>;
}

export type QueryPlanScopeKind = "root" | "cte" | "subquery" | "set_op_branch";

export interface QueryExecutionPlanScope {
  id: string;
  kind: QueryPlanScopeKind;
  label: string;
  parentId?: string;
}

export interface QueryExecutionPlanStep {
  id: string;
  kind: QueryStepKind;
  dependsOn: string[];
  summary: string;
  phase: QueryStepPhase;
  operation: QueryStepOperation;
  request?: Record<string, unknown>;
  pushdown?: Record<string, unknown>;
  outputs?: string[];
  sqlOrigin?: QuerySqlOrigin;
  scopeId?: string;
}

export interface QueryExecutionPlan {
  steps: QueryExecutionPlanStep[];
  scopes?: QueryExecutionPlanScope[];
}

export type QueryStepStatus = "ready" | "running" | "done" | "failed";

export interface QueryStepState {
  id: string;
  kind: QueryStepKind;
  status: QueryStepStatus;
  summary: string;
  dependsOn: string[];
  executionIndex?: number;
  startedAt?: number;
  endedAt?: number;
  durationMs?: number;
  rowCount?: number;
  inputRowCount?: number;
  outputRowCount?: number;
  rows?: QueryRow[];
  routeUsed?: QueryStepRoute;
  notes?: string[];
  error?: string;
}

export interface QueryStepEvent {
  id: string;
  kind: QueryStepKind;
  status: "done" | "failed";
  summary: string;
  dependsOn: string[];
  executionIndex: number;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  rowCount?: number;
  inputRowCount?: number;
  outputRowCount?: number;
  rows?: QueryRow[];
  routeUsed?: QueryStepRoute;
  notes?: string[];
  error?: string;
}

export interface QuerySessionOptions {
  maxConcurrency?: number;
  eventOrder?: "plan";
  captureRows?: "full";
  onEvent?: (event: QueryStepEvent) => void;
}

export interface QuerySessionInput<TContext> extends QueryInput<TContext> {
  options?: QuerySessionOptions;
}

export interface QuerySession {
  getPlan(): QueryExecutionPlan;
  next(): Promise<QueryStepEvent | { done: true; result: QueryRow[] }>;
  runToCompletion(): Promise<QueryRow[]>;
  getResult(): QueryRow[] | null;
  getStepState(stepId: string): QueryStepState | undefined;
}

interface ExecutionOptions {
  maxConcurrency: number;
  stepController?: StepController;
  subqueryCache?: WeakMap<object, Promise<QueryRow[]>>;
}

interface SelectAst {
  with?: unknown;
  type?: unknown;
  distinct?: unknown;
  set_op?: unknown;
  _next?: unknown;
  from?: unknown;
  where?: unknown;
  having?: unknown;
  columns?: unknown;
  orderby?: unknown;
  limit?: unknown;
  groupby?: unknown;
  window?: unknown;
}

interface TableBinding {
  table: string;
  alias: string;
  index: number;
  isCte: boolean;
}

interface JoinCondition {
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

interface ParsedJoin {
  alias: string;
  join: "inner" | "left" | "right" | "full";
  condition: JoinCondition;
}

interface SelectColumn {
  alias: string;
  column: string;
  output: string;
}

interface AggregateMetric {
  fn: AggregateFunction;
  output: string;
  signature: string;
  hidden: boolean;
  column?: {
    alias: string;
    column: string;
  };
  distinct: boolean;
}

type WindowFunctionName =
  | "row_number"
  | "rank"
  | "dense_rank"
  | "lead"
  | "lag"
  | AggregateFunction;

type WindowFrameMode = "default" | "rows_unbounded_preceding_current_row";

interface WindowFunctionSpec {
  fn: WindowFunctionName;
  output: string;
  partitionBy: Array<{
    alias: string;
    column: string;
  }>;
  orderBy: SourceOrderColumn[];
  column?: {
    alias: string;
    column: string;
  };
  countStar?: boolean;
  offset?: number;
  defaultValue?: unknown;
  frameMode: WindowFrameMode;
}

interface SourceOrderColumn {
  kind: "source";
  alias: string;
  column: string;
  direction: "asc" | "desc";
}

interface OutputOrderColumn {
  kind: "output";
  output: string;
  direction: "asc" | "desc";
}

type OrderColumn = SourceOrderColumn | OutputOrderColumn;
type SelectOrderResolution =
  | {
      kind: "source";
      alias: string;
      column: string;
    }
  | {
      kind: "output";
      output: string;
    };

interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

interface AggregateOutputColumn {
  source: {
    alias: string;
    column: string;
  };
  output: string;
}

interface ParsedSelectQuery {
  bindings: TableBinding[];
  joins: ParsedJoin[];
  joinEdges: JoinCondition[];
  filters: LiteralFilter[];
  where?: unknown;
  whereColumns: Array<{
    alias: string;
    column: string;
  }>;
  wherePushdownSafe: boolean;
  having?: unknown;
  distinct: boolean;
  selectAll: boolean;
  selectColumns: SelectColumn[];
  scalarSelectItems: Array<{
    expr: unknown;
    output: string;
  }>;
  windowFunctions: WindowFunctionSpec[];
  groupBy: Array<{
    alias: string;
    column: string;
  }>;
  aggregateMetrics: AggregateMetric[];
  aggregateOutputColumns: AggregateOutputColumn[];
  isAggregate: boolean;
  orderBy: OrderColumn[];
  limit?: number;
  offset?: number;
}

interface JoinedRowBundle {
  [alias: string]: QueryRow;
}

interface MetricAccumulator {
  count: number;
  sum: number;
  hasValue: boolean;
  min: unknown;
  max: unknown;
  distinctValues?: Set<string>;
}

const DEFAULT_MAX_CONCURRENCY = 4;

interface StepRunMetadata {
  inputRowCount?: number;
  outputRowCount?: number;
  routeUsed?: QueryStepRoute;
  notes?: string[];
}

interface StepRunWithMetadata<T> {
  value: T;
  metadata?: StepRunMetadata;
}

interface StepRuntimeOptions {
  metadata?: StepRunMetadata;
}

interface StepTemplateOptions {
  phase?: QueryStepPhase;
  operation?: QueryStepOperation;
  request?: Record<string, unknown>;
  pushdown?: Record<string, unknown>;
  outputs?: string[];
  sqlOrigin?: QuerySqlOrigin;
  scopeId?: string;
}

interface PrecompiledPlanMatchState {
  byKey: Map<string, string[]>;
}

function stepKey(kind: QueryStepKind, summary: string): string {
  return `${kind}::${summary}`;
}

function createExecutionPlanStep(
  id: string,
  kind: QueryStepKind,
  summary: string,
  dependsOn: string[],
  template: StepTemplateOptions = {},
): QueryExecutionPlanStep {
  return {
    id,
    kind,
    dependsOn: [...dependsOn],
    summary,
    phase: template.phase ?? defaultPhaseForStep(kind),
    operation:
      template.operation ??
      ({
        name: kind,
      } satisfies QueryStepOperation),
    ...(template.request ? { request: template.request } : {}),
    ...(template.pushdown ? { pushdown: template.pushdown } : {}),
    ...(template.outputs ? { outputs: [...template.outputs] } : {}),
    ...(template.sqlOrigin ? { sqlOrigin: template.sqlOrigin } : {}),
    ...(template.scopeId ? { scopeId: template.scopeId } : {}),
  };
}

function defaultPhaseForStep(kind: QueryStepKind): QueryStepPhase {
  switch (kind) {
    case "scan":
    case "aggregate":
      return "fetch";
    case "projection":
      return "output";
    case "cte":
    case "set_op_branch":
      return "logical";
    default:
      return "transform";
  }
}

class StepController {
  readonly #options: QuerySessionOptions;
  readonly #steps: QueryExecutionPlanStep[] = [];
  readonly #scopes: QueryExecutionPlanScope[] = [];
  readonly #states = new Map<string, QueryStepState>();
  readonly #precompiled: PrecompiledPlanMatchState;
  readonly #events: QueryStepEvent[] = [];
  readonly #manual: boolean;
  #permits = 0;
  #pendingTurnResolvers: Array<() => void> = [];
  #eventWaiters: Array<() => void> = [];
  #stepCounter = 0;
  #executionIndex = 0;
  #eventCursor = 0;
  #activeStepDepth = 0;
  #started = false;
  #completed = false;
  #result: QueryRow[] | null = null;
  #error: unknown;
  #executionPromise: Promise<QueryRow[]> | null = null;
  readonly #execute: () => Promise<QueryRow[]>;

  constructor(
    options: QuerySessionOptions,
    execute: () => Promise<QueryRow[]>,
    initialPlan: QueryExecutionPlan = { steps: [] },
    manual = true,
  ) {
    this.#options = options;
    this.#execute = execute;
    this.#manual = manual;
    this.#precompiled = {
      byKey: new Map<string, string[]>(),
    };

    for (const scope of initialPlan.scopes ?? []) {
      this.#scopes.push({ ...scope });
    }

    for (const step of initialPlan.steps) {
      this.#steps.push({ ...step, dependsOn: [...step.dependsOn] });
      this.#states.set(step.id, {
        id: step.id,
        kind: step.kind,
        summary: step.summary,
        dependsOn: [...step.dependsOn],
        status: "ready",
      });
      const key = stepKey(step.kind, step.summary);
      const queue = this.#precompiled.byKey.get(key) ?? [];
      queue.push(step.id);
      this.#precompiled.byKey.set(key, queue);
      const parsedIndex = Number(step.id.replace("step_", ""));
      if (Number.isFinite(parsedIndex)) {
        this.#stepCounter = Math.max(this.#stepCounter, parsedIndex);
      }
    }
  }

  start(): void {
    if (this.#started) {
      return;
    }

    this.#started = true;
    this.#executionPromise = this.#execute()
      .then((rows) => {
        this.#result = rows;
        this.#completed = true;
        this.#notifyEventWaiters();
        return rows;
      })
      .catch((error) => {
        this.#error = error;
        this.#completed = true;
        this.#notifyEventWaiters();
        throw error;
      });
    this.#executionPromise.catch(() => undefined);
  }

  get maxConcurrency(): number {
    return Math.max(1, this.#options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY);
  }

  getPlan(): QueryExecutionPlan {
    return {
      steps: [...this.#steps],
      ...(this.#scopes.length > 0 ? { scopes: [...this.#scopes] } : {}),
    };
  }

  getResult(): QueryRow[] | null {
    return this.#result;
  }

  getStepState(stepId: string): QueryStepState | undefined {
    const state = this.#states.get(stepId);
    return state ? { ...state } : undefined;
  }

  get hasCompleted(): boolean {
    return this.#completed;
  }

  get error(): unknown {
    return this.#error;
  }

  async waitForCompletion(): Promise<QueryRow[]> {
    this.start();
    this.#permits = Number.MAX_SAFE_INTEGER;
    this.#drainPermits();

    const execution = this.#executionPromise;
    if (!execution) {
      throw new Error("Execution did not start.");
    }

    return execution;
  }

  async next(): Promise<QueryStepEvent | { done: true; result: QueryRow[] }> {
    this.start();

    while (true) {
      if (this.#eventCursor < this.#events.length) {
        const next = this.#events[this.#eventCursor];
        this.#eventCursor += 1;
        if (!next) {
          continue;
        }

        return next;
      }

      if (this.#completed) {
        if (this.#error) {
          throw this.#error;
        }

        return {
          done: true,
          result: this.#result ?? [],
        };
      }

      this.#permits += 1;
      this.#drainPermits();
      await this.#waitForNextEvent();
    }
  }

  async runStep<T>(
    kind: QueryStepKind,
    summary: string,
    dependsOn: string[],
    run: () => Promise<T | StepRunWithMetadata<T>>,
    template: StepTemplateOptions = {},
    runtime: StepRuntimeOptions = {},
  ): Promise<T> {
    const stepId = this.#assignStepId(kind, summary, dependsOn, template);
    const state = this.#states.get(stepId);
    if (!state) {
      throw new Error(`Unknown step state for step ${stepId}`);
    }

    if (this.#manual && this.#activeStepDepth === 0) {
      await this.#awaitTurn();
    }

    const startedAt = Date.now();
    state.status = "running";
    state.startedAt = startedAt;
    this.#activeStepDepth += 1;

    try {
      const rawOutput = await run();
      const { value, metadata } = this.#unwrapStepOutput(rawOutput);
      const combinedMetadata = {
        ...runtime.metadata,
        ...metadata,
      };
      const endedAt = Date.now();
      const rowSummary = this.#summarizeRows(value);
      const executionIndex = this.#executionIndex + 1;
      this.#executionIndex = executionIndex;

      state.status = "done";
      state.executionIndex = executionIndex;
      state.endedAt = endedAt;
      state.durationMs = endedAt - startedAt;
      if (rowSummary) {
        state.rowCount = rowSummary.rowCount;
        state.outputRowCount = rowSummary.rowCount;
        if (rowSummary.rows) {
          state.rows = rowSummary.rows;
        }
      }
      if (combinedMetadata.inputRowCount != null) {
        state.inputRowCount = combinedMetadata.inputRowCount;
      }
      if (combinedMetadata.outputRowCount != null) {
        state.outputRowCount = combinedMetadata.outputRowCount;
      }
      if (combinedMetadata.routeUsed) {
        state.routeUsed = combinedMetadata.routeUsed;
      }
      if (combinedMetadata.notes && combinedMetadata.notes.length > 0) {
        state.notes = [...combinedMetadata.notes];
      }

      this.#emitEvent({
        id: stepId,
        kind,
        status: "done",
        summary,
        dependsOn: [...state.dependsOn],
        executionIndex,
        startedAt,
        endedAt,
        durationMs: endedAt - startedAt,
        ...(rowSummary ? { rowCount: rowSummary.rowCount } : {}),
        ...(rowSummary ? { outputRowCount: rowSummary.rowCount } : {}),
        ...(combinedMetadata.inputRowCount != null
          ? { inputRowCount: combinedMetadata.inputRowCount }
          : {}),
        ...(combinedMetadata.outputRowCount != null
          ? { outputRowCount: combinedMetadata.outputRowCount }
          : {}),
        ...(rowSummary?.rows ? { rows: rowSummary.rows } : {}),
        ...(combinedMetadata.routeUsed ? { routeUsed: combinedMetadata.routeUsed } : {}),
        ...(combinedMetadata.notes && combinedMetadata.notes.length > 0
          ? { notes: [...combinedMetadata.notes] }
          : {}),
      });

      return value;
    } catch (error) {
      const endedAt = Date.now();
      const message = error instanceof Error ? error.message : String(error);
      const executionIndex = this.#executionIndex + 1;
      this.#executionIndex = executionIndex;
      state.status = "failed";
      state.executionIndex = executionIndex;
      state.endedAt = endedAt;
      state.durationMs = endedAt - startedAt;
      state.error = message;

      this.#emitEvent({
        id: stepId,
        kind,
        status: "failed",
        summary,
        dependsOn: [...state.dependsOn],
        executionIndex,
        startedAt,
        endedAt,
        durationMs: endedAt - startedAt,
        ...(runtime.metadata?.inputRowCount != null
          ? { inputRowCount: runtime.metadata.inputRowCount }
          : {}),
        ...(runtime.metadata?.outputRowCount != null
          ? { outputRowCount: runtime.metadata.outputRowCount }
          : {}),
        ...(runtime.metadata?.routeUsed ? { routeUsed: runtime.metadata.routeUsed } : {}),
        ...(runtime.metadata?.notes && runtime.metadata.notes.length > 0
          ? { notes: [...runtime.metadata.notes] }
          : {}),
        error: message,
      });
      throw error;
    } finally {
      this.#activeStepDepth -= 1;
    }
  }

  #assignStepId(
    kind: QueryStepKind,
    summary: string,
    dependsOn: string[],
    template: StepTemplateOptions,
  ): string {
    const key = stepKey(kind, summary);
    const queue = this.#precompiled.byKey.get(key);
    const precompiledStepId = queue?.shift();
    if (precompiledStepId) {
      this.#precompiled.byKey.set(key, queue ?? []);
      return precompiledStepId;
    }

    return this.#createStep(kind, summary, dependsOn, template);
  }

  #createStep(
    kind: QueryStepKind,
    summary: string,
    dependsOn: string[],
    template: StepTemplateOptions,
  ): string {
    const stepId = `step_${this.#stepCounter + 1}`;
    this.#stepCounter += 1;
    const step = createExecutionPlanStep(stepId, kind, summary, dependsOn, template);
    this.#steps.push(step);
    this.#states.set(stepId, {
      id: stepId,
      kind,
      summary,
      dependsOn: [...dependsOn],
      status: "ready",
    });
    return stepId;
  }

  async #awaitTurn(): Promise<void> {
    if (this.#permits > 0) {
      this.#permits -= 1;
      return;
    }

    await new Promise<void>((resolve) => {
      this.#pendingTurnResolvers.push(resolve);
    });
  }

  #drainPermits(): void {
    while (this.#permits > 0 && this.#pendingTurnResolvers.length > 0) {
      const resolve = this.#pendingTurnResolvers.shift();
      if (!resolve) {
        continue;
      }
      this.#permits -= 1;
      resolve();
    }
  }

  #emitEvent(event: QueryStepEvent): void {
    this.#events.push(event);
    if (this.#options.onEvent) {
      this.#options.onEvent(event);
    }
    this.#notifyEventWaiters();
  }

  #waitForNextEvent(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.#eventWaiters.push(resolve);
    });
  }

  #notifyEventWaiters(): void {
    while (this.#eventWaiters.length > 0) {
      const waiter = this.#eventWaiters.shift();
      waiter?.();
    }
  }

  #summarizeRows(output: unknown): { rowCount: number; rows?: QueryRow[] } | undefined {
    if (!Array.isArray(output)) {
      return undefined;
    }

    const rowCount = output.length;
    if (this.#options.captureRows === "full") {
      return {
        rowCount,
        rows: output as QueryRow[],
      };
    }

    return { rowCount };
  }

  #unwrapStepOutput<T>(output: T | StepRunWithMetadata<T>): {
    value: T;
    metadata?: StepRunMetadata;
  } {
    if (
      output &&
      typeof output === "object" &&
      "value" in output &&
      Object.prototype.hasOwnProperty.call(output, "value")
    ) {
      const asWithMeta = output as StepRunWithMetadata<T>;
      return {
        value: asWithMeta.value,
        ...(asWithMeta.metadata ? { metadata: asWithMeta.metadata } : {}),
      };
    }

    return { value: output as T };
  }
}

async function runStepWithController<T>(
  options: ExecutionOptions,
  kind: QueryStepKind,
  summary: string,
  dependsOn: string[],
  run: () => Promise<T | StepRunWithMetadata<T>>,
  template: StepTemplateOptions = {},
  runtime: StepRuntimeOptions = {},
): Promise<T> {
  if (!options.stepController) {
    const raw = await run();
    if (raw && typeof raw === "object" && "value" in (raw as Record<string, unknown>)) {
      return (raw as StepRunWithMetadata<T>).value;
    }
    return raw as T;
  }

  return options.stepController.runStep(kind, summary, dependsOn, run, template, runtime);
}

export function parseSql(query: SqlQuery, schema: SchemaDefinition): PlannedQuery {
  const ast = astifySingleSelect(query.text);
  const parsed = parseSelectAst(ast, schema, new Map());
  const source = parsed.bindings[0];
  if (!source) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  return {
    source: source.table,
    selectAll: parsed.selectAll,
  };
}

export async function query<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  return executeQueryInternal(input, {
    maxConcurrency: DEFAULT_MAX_CONCURRENCY,
  });
}

class StaticPlanBuilder {
  #counter = 0;
  #scopeCounter = 0;
  readonly steps: QueryExecutionPlanStep[] = [];
  readonly scopes: QueryExecutionPlanScope[] = [];

  addScope(kind: QueryPlanScopeKind, label: string, parentId?: string): string {
    const id = `scope_${this.#scopeCounter + 1}`;
    this.#scopeCounter += 1;
    this.scopes.push({
      id,
      kind,
      label,
      ...(parentId ? { parentId } : {}),
    });
    return id;
  }

  addStep(
    kind: QueryStepKind,
    summary: string,
    dependsOn: string[],
    template: StepTemplateOptions = {},
  ): string {
    const id = `step_${this.#counter + 1}`;
    this.#counter += 1;
    this.steps.push(createExecutionPlanStep(id, kind, summary, dependsOn, template));
    return id;
  }

  appendStepDependencies(stepId: string, dependencyStepIds: string[]): void {
    if (dependencyStepIds.length === 0) {
      return;
    }
    const step = this.steps.find((candidate) => candidate.id === stepId);
    if (!step) {
      throw new Error(`Unknown static step id: ${stepId}`);
    }
    step.dependsOn = [...new Set([...step.dependsOn, ...dependencyStepIds])];
  }

  result(): QueryExecutionPlan {
    return {
      steps: [...this.steps],
      scopes: [...this.scopes],
    };
  }
}

interface StaticCompileResult {
  terminalStepIds: string[];
  cteStepIdsByName: Map<string, string>;
}

function compileStaticExecutionPlan<TContext>(input: QueryInput<TContext>): QueryExecutionPlan {
  const ast = astifySingleSelect(input.sql);
  const builder = new StaticPlanBuilder();
  const rootScopeId = builder.addScope("root", "Root");
  compileStaticSelectAst(
    ast,
    {
      schema: input.schema,
      methods: input.methods,
    },
    new Set<string>(),
    builder,
    new Map<string, string>(),
    rootScopeId,
  );
  return builder.result();
}

function compileStaticSelectAst<TContext>(
  ast: SelectAst,
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
  },
  parentCteNames: Set<string>,
  builder: StaticPlanBuilder,
  parentCteStepIdsByName: Map<string, string>,
  currentScopeId: string,
): StaticCompileResult {
  if (ast.type !== "select") {
    throw new Error("Only SELECT statements are currently supported.");
  }

  if (ast.set_op != null || ast._next != null) {
    return compileStaticSetOperation(
      ast,
      input,
      parentCteNames,
      builder,
      parentCteStepIdsByName,
      currentScopeId,
    );
  }

  const cteStepIdsByName = new Map<string, string>();
  const rawCtes = Array.isArray(ast.with) ? ast.with : [];
  const cteEntries = rawCtes.map((rawCte, index) => {
    const cteName = readCteName(rawCte);
    const cteStatement = (rawCte as { stmt?: { ast?: unknown } }).stmt?.ast;
    if (!cteStatement || typeof cteStatement !== "object") {
      throw new Error(`Unable to parse CTE statement for: ${cteName}`);
    }
    const cteAst = cteStatement as SelectAst;
    if (cteAst.type !== "select") {
      throw new Error("Only SELECT CTE statements are currently supported.");
    }

    return {
      cteName,
      cteAst,
      index,
    };
  });

  if (cteEntries.length > 0) {
    const cteNameSet = new Set(cteEntries.map((entry) => entry.cteName));
    const cteDependencies = new Map<string, string[]>();
    for (const entry of cteEntries) {
      cteDependencies.set(
        entry.cteName,
        collectCteDependencies(entry.cteAst, cteNameSet, entry.cteName),
      );
    }

    const remaining = new Map(cteEntries.map((entry) => [entry.cteName, entry]));
    while (remaining.size > 0) {
      const ready = [...remaining.values()]
        .filter((entry) =>
          (cteDependencies.get(entry.cteName) ?? []).every((dep) => cteStepIdsByName.has(dep)),
        )
        .sort((left, right) => left.index - right.index);

      if (ready.length === 0) {
        throw new Error("Unable to resolve CTE dependencies.");
      }

      for (const entry of ready) {
        const cteScopeId = builder.addScope("cte", `CTE ${entry.cteName}`, currentScopeId);
        const cteScopeNames = new Set<string>([
          ...parentCteNames,
          ...cteNameSet,
          ...cteStepIdsByName.keys(),
        ]);
        const cteScopeStepIdsByName = new Map<string, string>([
          ...parentCteStepIdsByName,
          ...cteStepIdsByName,
        ]);
        const compiled = compileStaticSelectAst(
          entry.cteAst,
          input,
          cteScopeNames,
          builder,
          cteScopeStepIdsByName,
          cteScopeId,
        );
        const dependencyStepIds = (cteDependencies.get(entry.cteName) ?? [])
          .map((dep) => cteStepIdsByName.get(dep))
          .filter((dep): dep is string => typeof dep === "string");
        const cteStepId = builder.addStep(
          "cte",
          `CTE ${entry.cteName}`,
          [...dependencyStepIds, ...compiled.terminalStepIds],
          {
            phase: "logical",
            operation: {
              name: "cte",
              details: {
                cte: entry.cteName,
                dependencies: cteDependencies.get(entry.cteName) ?? [],
              },
            },
            sqlOrigin: "WITH",
            scopeId: cteScopeId,
          },
        );
        cteStepIdsByName.set(entry.cteName, cteStepId);
        remaining.delete(entry.cteName);
      }
    }
  }

  const cteRows = new Map<string, QueryRow[]>();
  for (const name of parentCteNames) {
    cteRows.set(name, []);
  }
  for (const name of cteStepIdsByName.keys()) {
    cteRows.set(name, []);
  }

  const parsed = parseSelectAst(ast, input.schema, cteRows);
  const scopedCteStepIdsByName = new Map<string, string>([
    ...parentCteStepIdsByName,
    ...cteStepIdsByName,
  ]);
  if (parsed.isAggregate) {
    const aggregate = compileStaticAggregateSelect(
      parsed,
      input,
      cteRows,
      scopedCteStepIdsByName,
      builder,
      currentScopeId,
    );
    return {
      terminalStepIds: aggregate.terminalStepIds,
      cteStepIdsByName,
    };
  }

  const nonAggregate = compileStaticNonAggregateSelect(
    parsed,
    input,
    cteRows,
    scopedCteStepIdsByName,
    builder,
    currentScopeId,
  );
  return {
    terminalStepIds: nonAggregate.terminalStepIds,
    cteStepIdsByName,
  };
}

function compileStaticSetOperation<TContext>(
  ast: SelectAst,
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
  },
  parentCteNames: Set<string>,
  builder: StaticPlanBuilder,
  parentCteStepIdsByName: Map<string, string>,
  currentScopeId: string,
): StaticCompileResult {
  const operation = typeof ast.set_op === "string" ? ast.set_op.toLowerCase() : "";
  const nextRaw = readSetOperationNext(ast._next);
  const next =
    nextRaw && ast.with && !nextRaw.with
      ? {
          ...nextRaw,
          with: ast.with,
        }
      : nextRaw;
  if (!next) {
    throw new Error("Invalid set operation: missing right-hand SELECT.");
  }

  const leftAst = cloneSelectWithoutSetOperation(ast);
  const leftScopeId = builder.addScope("set_op_branch", "Set operation left branch", currentScopeId);
  const leftCompiled = compileStaticSelectAst(
    leftAst,
    input,
    parentCteNames,
    builder,
    parentCteStepIdsByName,
    leftScopeId,
  );
  const leftStepId = builder.addStep(
    "set_op_branch",
    "Set operation left branch",
    leftCompiled.terminalStepIds,
    {
      phase: "logical",
      operation: {
        name: "set_op_branch",
        details: {
          side: "left",
          operation,
        },
      },
      sqlOrigin: "SET_OP",
      scopeId: leftScopeId,
    },
  );

  const rightScopeId = builder.addScope(
    "set_op_branch",
    "Set operation right branch",
    currentScopeId,
  );
  const rightCompiled = compileStaticSelectAst(
    next,
    input,
    parentCteNames,
    builder,
    parentCteStepIdsByName,
    rightScopeId,
  );
  const rightStepId = builder.addStep(
    "set_op_branch",
    "Set operation right branch",
    rightCompiled.terminalStepIds,
    {
      phase: "logical",
      operation: {
        name: "set_op_branch",
        details: {
          side: "right",
          operation,
        },
      },
      sqlOrigin: "SET_OP",
      scopeId: rightScopeId,
    },
  );

  return {
    terminalStepIds: [leftStepId, rightStepId],
    cteStepIdsByName: new Map(),
  };
}

interface StaticSubquerySite {
  label: string;
  ast: SelectAst;
}

function collectSubqueryAstsFromExpression(rawExpr: unknown): SelectAst[] {
  const out: SelectAst[] = [];
  const visit = (node: unknown): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    const subquery = toSubqueryAst(node);
    if (subquery) {
      out.push(subquery);
      return;
    }

    if (Array.isArray(node)) {
      for (const entry of node) {
        visit(entry);
      }
      return;
    }

    const entries = Object.entries(node as Record<string, unknown>).sort(([left], [right]) =>
      left.localeCompare(right),
    );
    for (const [, value] of entries) {
      visit(value);
    }
  };

  visit(rawExpr);
  return out;
}

function compileStaticSubquerySites<TContext>(
  sites: StaticSubquerySite[],
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
  },
  parentCteNames: Set<string>,
  builder: StaticPlanBuilder,
  cteStepIdsByName: Map<string, string>,
  currentScopeId: string,
): string[] {
  const terminalStepIds: string[] = [];
  for (const site of sites) {
    const scopeId = builder.addScope("subquery", site.label, currentScopeId);
    const compiled = compileStaticSelectAst(
      site.ast,
      input,
      parentCteNames,
      builder,
      cteStepIdsByName,
      scopeId,
    );
    terminalStepIds.push(...compiled.terminalStepIds);
  }
  return [...new Set(terminalStepIds)];
}

function compileStaticNonAggregateSelect<TContext>(
  parsed: ParsedSelectQuery,
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
  },
  cteRows: Map<string, QueryRow[]>,
  cteStepIdsByName: Map<string, string>,
  builder: StaticPlanBuilder,
  currentScopeId: string,
): { terminalStepIds: string[] } {
  const parentCteNames = new Set<string>(cteRows.keys());
  const { orderedScanStepIds } = compileStaticScanSteps(
    parsed,
    input,
    cteRows,
    cteStepIdsByName,
    builder,
    currentScopeId,
  );

  const joinStepId = builder.addStep("join", "Join source bindings", orderedScanStepIds, {
    phase: "transform",
    operation: {
      name: "join",
      details: {
        joinCount: parsed.joins.length,
        joins: parsed.joins.map((join) => ({
          alias: join.alias,
          join: join.join,
          condition: `${join.condition.leftAlias}.${join.condition.leftColumn} = ${join.condition.rightAlias}.${join.condition.rightColumn}`,
        })),
      },
    },
    sqlOrigin: "FROM",
    scopeId: currentScopeId,
  });

  const filterStepId = builder.addStep(
    "filter",
    "Apply WHERE filter",
    [joinStepId],
    {
      phase: "transform",
      operation: {
        name: "filter",
        details: {
          wherePushdownSafe: parsed.wherePushdownSafe,
        },
      },
      request: {
        whereColumns: parsed.whereColumns.map((column) => `${column.alias}.${column.column}`),
      },
      sqlOrigin: "WHERE",
      scopeId: currentScopeId,
    },
  );
  const whereSubquerySites = collectSubqueryAstsFromExpression(parsed.where).map((ast, index) => ({
    label: `Subquery WHERE #${index + 1}`,
    ast,
  }));
  const whereSubqueryTerminalStepIds = compileStaticSubquerySites(
    whereSubquerySites,
    input,
    parentCteNames,
    builder,
    cteStepIdsByName,
    currentScopeId,
  );
  builder.appendStepDependencies(filterStepId, whereSubqueryTerminalStepIds);

  let previousStepId = filterStepId;
  if (parsed.windowFunctions.length > 0) {
    const windowStepId = builder.addStep("window", "Compute window functions", [previousStepId], {
      phase: "transform",
      operation: {
        name: "window",
        details: {
          functions: parsed.windowFunctions.map((fn) => fn.output),
        },
      },
      request: {
        functions: parsed.windowFunctions.map((fn) => ({
          fn: fn.fn,
          output: fn.output,
          partitionBy: fn.partitionBy.map((entry) => `${entry.alias}.${entry.column}`),
          orderBy: fn.orderBy.map((entry) => `${entry.alias}.${entry.column}:${entry.direction}`),
        })),
      },
      outputs: parsed.windowFunctions.map((fn) => fn.output),
      sqlOrigin: "SELECT",
      scopeId: currentScopeId,
    });
    previousStepId = windowStepId;
  }

  const projectedOutputs = parsed.selectAll
    ? deriveSelectAllOutputs(parsed, input.schema, cteRows)
    : [
        ...parsed.selectColumns.map((column) => column.output),
        ...parsed.scalarSelectItems.map((item) => item.output),
        ...parsed.windowFunctions.map((fn) => fn.output),
      ];

  const projectionStepId = builder.addStep(
    "projection",
    "Project result rows",
    [previousStepId],
    {
      phase: "output",
      operation: {
        name: "projection",
      },
      outputs: projectedOutputs,
      sqlOrigin: "SELECT",
      scopeId: currentScopeId,
    },
  );
  const selectSubquerySites = parsed.scalarSelectItems
    .flatMap((item) => collectSubqueryAstsFromExpression(item.expr))
    .map((ast, index) => ({
      label: `Subquery SELECT #${index + 1}`,
      ast,
    }));
  const selectSubqueryTerminalStepIds = compileStaticSubquerySites(
    selectSubquerySites,
    input,
    parentCteNames,
    builder,
    cteStepIdsByName,
    currentScopeId,
  );
  builder.appendStepDependencies(projectionStepId, selectSubqueryTerminalStepIds);

  const requiresProjectedOrderLimit = parsed.orderBy.some((column) => column.kind === "output");

  if (parsed.distinct) {
    const distinctStepId = builder.addStep("distinct", "Apply DISTINCT", [projectionStepId], {
      phase: "transform",
      operation: {
        name: "distinct",
      },
      sqlOrigin: "SELECT",
      scopeId: currentScopeId,
    });
    const orderStepId = builder.addStep(
      "order",
      "Apply ORDER/LIMIT/OFFSET on projected rows",
      [distinctStepId],
      {
        phase: "output",
        operation: {
          name: "order_limit_offset",
        },
        request: {
          orderBy: parsed.orderBy.map((column) => {
            if (column.kind === "source") {
              return `${column.alias}.${column.column}:${column.direction}`;
            }
            return `${column.output}:${column.direction}`;
          }),
          ...(parsed.limit != null ? { limit: parsed.limit } : {}),
          ...(parsed.offset != null ? { offset: parsed.offset } : {}),
        },
        sqlOrigin: "ORDER BY",
        scopeId: currentScopeId,
      },
    );
    return { terminalStepIds: [orderStepId] };
  }

  if (requiresProjectedOrderLimit) {
    const orderStepId = builder.addStep(
      "order",
      "Apply ORDER/LIMIT/OFFSET on projected rows",
      [projectionStepId],
      {
        phase: "output",
        operation: {
          name: "order_limit_offset",
        },
        request: {
          orderBy: parsed.orderBy.map((column) => {
            if (column.kind === "source") {
              return `${column.alias}.${column.column}:${column.direction}`;
            }
            return `${column.output}:${column.direction}`;
          }),
          ...(parsed.limit != null ? { limit: parsed.limit } : {}),
          ...(parsed.offset != null ? { offset: parsed.offset } : {}),
        },
        sqlOrigin: "ORDER BY",
        scopeId: currentScopeId,
      },
    );
    return { terminalStepIds: [orderStepId] };
  }

  return {
    terminalStepIds: [projectionStepId],
  };
}

function compileStaticAggregateSelect<TContext>(
  parsed: ParsedSelectQuery,
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
  },
  cteRows: Map<string, QueryRow[]>,
  cteStepIdsByName: Map<string, string>,
  builder: StaticPlanBuilder,
  currentScopeId: string,
): { terminalStepIds: string[] } {
  const parentCteNames = new Set<string>(cteRows.keys());
  const { orderedScanStepIds } = compileStaticScanSteps(
    parsed,
    input,
    cteRows,
    cteStepIdsByName,
    builder,
    currentScopeId,
  );
  const rootBinding = parsed.bindings[0];
  const aggregateRoutePossible =
    parsed.bindings.length === 1 &&
    parsed.joins.length === 0 &&
    !parsed.having &&
    parsed.wherePushdownSafe &&
    !!rootBinding &&
    !rootBinding.isCte &&
    !!input.methods[rootBinding.table]?.aggregate;
  const aggregateTablePolicy =
    rootBinding && !rootBinding.isCte
      ? resolveTableQueryBehavior(input.schema, rootBinding.table)
      : null;

  let previousStepId = builder.addStep(
    "aggregate",
    "Run aggregate route",
    orderedScanStepIds,
    {
      phase: "fetch",
      operation: {
        name: "aggregate",
      },
      request: {
        groupBy: parsed.groupBy.map((column) => `${column.alias}.${column.column}`),
        metrics: parsed.aggregateMetrics.map((metric) => ({
          fn: metric.fn,
          output: metric.output,
          ...(metric.column
            ? {
                column: `${metric.column.alias}.${metric.column.column}`,
              }
            : {}),
          distinct: metric.distinct,
        })),
      },
      pushdown: {
        routeCandidates: aggregateRoutePossible ? ["aggregate", "local"] : ["local"],
        aggregateRoutePossible,
        ...(aggregateTablePolicy
          ? {
              schemaPolicy: {
                reject: aggregateTablePolicy.reject,
                fallback: aggregateTablePolicy.fallback,
              },
            }
          : {}),
      },
      outputs: [
        ...parsed.aggregateOutputColumns.map((column) => column.output),
        ...parsed.aggregateMetrics.filter((metric) => !metric.hidden).map((metric) => metric.output),
      ],
      sqlOrigin: "SELECT",
      scopeId: currentScopeId,
    },
  );
  const whereSubquerySites = collectSubqueryAstsFromExpression(parsed.where).map((ast, index) => ({
    label: `Subquery WHERE #${index + 1}`,
    ast,
  }));
  const whereSubqueryTerminalStepIds = compileStaticSubquerySites(
    whereSubquerySites,
    input,
    parentCteNames,
    builder,
    cteStepIdsByName,
    currentScopeId,
  );
  builder.appendStepDependencies(previousStepId, whereSubqueryTerminalStepIds);

  if (parsed.having) {
    const havingFilterStepId = builder.addStep(
      "filter",
      "Apply HAVING filter",
      [previousStepId],
      {
        phase: "transform",
        operation: {
          name: "having_filter",
        },
        sqlOrigin: "HAVING",
        scopeId: currentScopeId,
      },
    );
    const havingSubquerySites = collectSubqueryAstsFromExpression(parsed.having).map((ast, index) => ({
      label: `Subquery HAVING #${index + 1}`,
      ast,
    }));
    const havingSubqueryTerminalStepIds = compileStaticSubquerySites(
      havingSubquerySites,
      input,
      parentCteNames,
      builder,
      cteStepIdsByName,
      currentScopeId,
    );
    builder.appendStepDependencies(havingFilterStepId, havingSubqueryTerminalStepIds);
    previousStepId = havingFilterStepId;
  }

  if (parsed.distinct) {
    previousStepId = builder.addStep("distinct", "Apply DISTINCT", [previousStepId], {
      phase: "transform",
      operation: {
        name: "distinct",
      },
      sqlOrigin: "SELECT",
      scopeId: currentScopeId,
    });
  }

  if (parsed.orderBy.length > 0) {
    previousStepId = builder.addStep("order", "Apply ORDER BY", [previousStepId], {
      phase: "output",
      operation: {
        name: "order",
      },
      request: {
        orderBy: parsed.orderBy.map((column) => {
          if (column.kind === "source") {
            return `${column.alias}.${column.column}:${column.direction}`;
          }
          return `${column.output}:${column.direction}`;
        }),
      },
      sqlOrigin: "ORDER BY",
      scopeId: currentScopeId,
    });
  }

  if (parsed.offset != null || parsed.limit != null) {
    previousStepId = builder.addStep("limit_offset", "Apply LIMIT/OFFSET", [previousStepId], {
      phase: "output",
      operation: {
        name: "limit_offset",
      },
      request: {
        ...(parsed.limit != null ? { limit: parsed.limit } : {}),
        ...(parsed.offset != null ? { offset: parsed.offset } : {}),
      },
      sqlOrigin: "ORDER BY",
      scopeId: currentScopeId,
    });
  }

  const projectionStepId = builder.addStep(
    "projection",
    "Project aggregate output",
    [previousStepId],
    {
      phase: "output",
      operation: {
        name: "projection",
      },
      outputs: [
        ...parsed.aggregateOutputColumns.map((column) => column.output),
        ...parsed.aggregateMetrics
          .filter((metric) => !metric.hidden)
          .map((metric) => metric.output),
      ],
      sqlOrigin: "SELECT",
      scopeId: currentScopeId,
    },
  );
  const selectSubquerySites = parsed.scalarSelectItems
    .flatMap((item) => collectSubqueryAstsFromExpression(item.expr))
    .map((ast, index) => ({
      label: `Subquery SELECT #${index + 1}`,
      ast,
    }));
  const selectSubqueryTerminalStepIds = compileStaticSubquerySites(
    selectSubquerySites,
    input,
    parentCteNames,
    builder,
    cteStepIdsByName,
    currentScopeId,
  );
  builder.appendStepDependencies(projectionStepId, selectSubqueryTerminalStepIds);

  return {
    terminalStepIds: [projectionStepId],
  };
}

function compileStaticScanSteps<TContext>(
  parsed: ParsedSelectQuery,
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
  },
  cteRows: Map<string, QueryRow[]>,
  cteStepIdsByName: Map<string, string>,
  builder: StaticPlanBuilder,
  currentScopeId: string,
): { scanStepIdsByAlias: Map<string, string>; orderedScanStepIds: string[] } {
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const projectionByAlias = buildProjection(parsed, input.schema, cteRows);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const executionOrder = buildExecutionOrder(parsed.bindings, parsed.joinEdges, filtersByAlias);
  const aliasDependencies = buildAliasPrerequisites(parsed.joins);
  const scanStepIdsByAlias = new Map<string, string>();
  const orderedScanStepIds: string[] = [];

  const canPushFinalSortAndLimitAll =
    !parsed.distinct &&
    parsed.bindings.length === 1 &&
    parsed.orderBy.every((term) => term.kind === "source" && term.alias === rootBinding.alias);

  for (const alias of executionOrder) {
    const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
    if (!binding) {
      continue;
    }

    const dependencyAliases = aliasDependencies.get(alias) ?? [];
    const dependsOn = dependencyAliases
      .map((dependencyAlias) => scanStepIdsByAlias.get(dependencyAlias))
      .filter((stepId): stepId is string => typeof stepId === "string");
    if (binding.isCte) {
      const cteStepId = cteStepIdsByName.get(binding.table);
      if (typeof cteStepId === "string" && !dependsOn.includes(cteStepId)) {
        dependsOn.push(cteStepId);
      }
    }

    const localFilters = filtersByAlias.get(alias) ?? [];
    const dependencyFilters = deriveStaticDependencyFilters(alias, parsed.joinEdges);
    const canPushFinalSortAndLimit = canPushFinalSortAndLimitAll && alias === rootBinding.alias;
    const requestOrderBy: ScanOrderBy[] | undefined = canPushFinalSortAndLimit
      ? parsed.orderBy
          .filter((term): term is SourceOrderColumn => term.kind === "source")
          .map((term) => ({
            column: term.column,
            direction: term.direction,
          }))
      : undefined;

    const projection = projectionByAlias.get(alias) ?? new Set<string>();
    const request: Record<string, unknown> = {
      table: binding.table,
      alias,
      select: [...projection],
    };

    const requestWhere = [...localFilters, ...dependencyFilters];
    if (requestWhere.length > 0) {
      request.where = requestWhere;
    }
    if (requestOrderBy && requestOrderBy.length > 0) {
      request.orderBy = requestOrderBy;
    }
    if (canPushFinalSortAndLimit && parsed.limit != null) {
      request.limit = parsed.limit;
    }
    if (canPushFinalSortAndLimit && parsed.offset != null) {
      request.offset = parsed.offset;
    }

    const tableMethods = input.methods[binding.table];
    const tablePolicy = !binding.isCte ? resolveTableQueryBehavior(input.schema, binding.table) : null;
    const lookupCandidate =
      !binding.isCte &&
      !!tableMethods?.lookup &&
      requestWhere.filter((clause) => clause.op === "in").length === 1 &&
      requestOrderBy == null &&
      parsed.limit == null &&
      parsed.offset == null;

    const stepId = builder.addStep("scan", `Scan ${alias} (${binding.table})`, dependsOn, {
      phase: "fetch",
      operation: {
        name: "scan",
        details: {
          alias,
          table: binding.table,
          isCte: binding.isCte,
        },
      },
      request,
      pushdown: {
        where: parsed.wherePushdownSafe ? "pushed" : "local",
        orderBy: canPushFinalSortAndLimit ? "pushed" : "local",
        limit: canPushFinalSortAndLimit ? "pushed" : "local",
        routeCandidates: lookupCandidate ? ["lookup", "scan"] : ["scan"],
        ...(tablePolicy
          ? {
              schemaPolicy: {
                reject: tablePolicy.reject,
                fallback: tablePolicy.fallback,
              },
            }
          : {}),
      },
      outputs: [...projection],
      sqlOrigin: "FROM",
      scopeId: currentScopeId,
    });

    scanStepIdsByAlias.set(alias, stepId);
    orderedScanStepIds.push(stepId);
  }

  return { scanStepIdsByAlias, orderedScanStepIds };
}

function deriveSelectAllOutputs(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): string[] {
  const base = parsed.bindings[0];
  if (!base) {
    return [];
  }

  return getBindingColumns(base, schema, cteRows);
}

function deriveStaticDependencyFilters(
  alias: string,
  joinEdges: JoinCondition[],
): ScanFilterClause[] {
  const clauses: ScanFilterClause[] = [];
  for (const edge of joinEdges) {
    if (edge.leftAlias === alias) {
      clauses.push({
        op: "in",
        column: edge.leftColumn,
        values: [`<from ${edge.rightAlias}.${edge.rightColumn}>`],
      });
    } else if (edge.rightAlias === alias) {
      clauses.push({
        op: "in",
        column: edge.rightColumn,
        values: [`<from ${edge.leftAlias}.${edge.leftColumn}>`],
      });
    }
  }
  return dedupeInClauses(clauses);
}

export function createQuerySession<TContext>(input: QuerySessionInput<TContext>): QuerySession {
  const options = input.options ?? {};
  let precompiledPlan: QueryExecutionPlan = { steps: [] };
  let precompileError: unknown;
  try {
    precompiledPlan = compileStaticExecutionPlan(input);
  } catch (error) {
    precompileError = error;
  }
  let stepController: StepController;
  stepController = new StepController(
    options,
    (): Promise<QueryRow[]> => {
      if (precompileError) {
        throw precompileError;
      }

      return executeQueryInternal(input, {
        maxConcurrency: Math.max(1, options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY),
        stepController,
      });
    },
    precompiledPlan,
  );

  return {
    getPlan: () => stepController.getPlan(),
    next: () => stepController.next(),
    runToCompletion: () => stepController.waitForCompletion(),
    getResult: () => stepController.getResult(),
    getStepState: (stepId: string) => stepController.getStepState(stepId),
  };
}

async function executeQueryInternal<TContext>(
  input: QueryInput<TContext>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  const ast = astifySingleSelect(input.sql);
  const executionOptions: ExecutionOptions = {
    ...options,
    subqueryCache: options.subqueryCache ?? new WeakMap<object, Promise<QueryRow[]>>(),
  };
  return executeSelectAst(ast, input, new Map(), executionOptions);
}

async function executeSelectAst<TContext>(
  ast: SelectAst,
  input: QueryInput<TContext>,
  parentCtes: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  if (ast.type !== "select") {
    throw new Error("Only SELECT statements are currently supported.");
  }

  if (ast.set_op != null || ast._next != null) {
    return executeSetOperation(ast, input, parentCtes, options);
  }

  const cteRows = new Map(parentCtes);
  const rawCtes = Array.isArray(ast.with) ? ast.with : [];
  if (rawCtes.length > 0) {
    const cteEntries = rawCtes.map((rawCte, index) => {
      if ((rawCte as { recursive?: unknown }).recursive === true) {
        throw new Error("Recursive CTEs are not yet supported.");
      }

      const cteName = readCteName(rawCte);
      const cteStatement = (rawCte as { stmt?: { ast?: unknown } }).stmt?.ast;
      if (!cteStatement || typeof cteStatement !== "object") {
        throw new Error(`Unable to parse CTE statement for: ${cteName}`);
      }

      const cteAst = cteStatement as SelectAst;
      if (cteAst.type !== "select") {
        throw new Error("Only SELECT CTE statements are currently supported.");
      }

      return {
        rawCte,
        cteName,
        cteAst,
        index,
      };
    });

    const cteNameSet = new Set(cteEntries.map((entry) => entry.cteName));
    const cteDependencies = new Map<string, string[]>();
    for (const entry of cteEntries) {
      cteDependencies.set(
        entry.cteName,
        collectCteDependencies(entry.cteAst, cteNameSet, entry.cteName),
      );
    }

    const remaining = new Map(cteEntries.map((entry) => [entry.cteName, entry]));
    const resolved = new Map<string, QueryRow[]>();

    while (remaining.size > 0) {
      const ready = [...remaining.values()]
        .filter((entry) => {
          const deps = cteDependencies.get(entry.cteName) ?? [];
          return deps.every((dependency) => resolved.has(dependency) || cteRows.has(dependency));
        })
        .sort((left, right) => left.index - right.index);

      if (ready.length === 0) {
        throw new Error("Unable to resolve CTE dependencies.");
      }

      for (let index = 0; index < ready.length; index += options.maxConcurrency) {
        const batch = ready.slice(index, index + options.maxConcurrency);
        const batchRows = await Promise.all(
          batch.map(async (entry) => {
            const deps = cteDependencies.get(entry.cteName) ?? [];
            const availableCtes = new Map(parentCtes);
            for (const dependency of deps) {
              const rows = resolved.get(dependency) ?? cteRows.get(dependency);
              if (rows) {
                availableCtes.set(dependency, rows);
              }
            }

            const rows = await runStepWithController(
              options,
              "cte",
              `CTE ${entry.cteName}`,
              deps,
              () => executeSelectAst(entry.cteAst, input, availableCtes, options),
            );

            return {
              name: entry.cteName,
              rows,
            };
          }),
        );

        for (const rowSet of batchRows) {
          resolved.set(rowSet.name, rowSet.rows);
          cteRows.set(rowSet.name, rowSet.rows);
          remaining.delete(rowSet.name);
        }
      }
    }
  }

  const parsed = parseSelectAst(ast, input.schema, cteRows);
  return executeParsedSelect(parsed, input, cteRows, options);
}

async function executeSetOperation<TContext>(
  ast: SelectAst,
  input: QueryInput<TContext>,
  parentCtes: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  const operation = typeof ast.set_op === "string" ? ast.set_op.toLowerCase() : "";
  const nextRaw = readSetOperationNext(ast._next);
  const next =
    nextRaw && ast.with && !nextRaw.with
      ? {
          ...nextRaw,
          with: ast.with,
        }
      : nextRaw;
  if (!next) {
    throw new Error("Invalid set operation: missing right-hand SELECT.");
  }

  const leftAst = cloneSelectWithoutSetOperation(ast);
  const [leftRows, rightRows] = await Promise.all([
    runStepWithController(options, "set_op_branch", "Set operation left branch", [], () =>
      executeSelectAst(leftAst, input, parentCtes, options),
    ),
    runStepWithController(options, "set_op_branch", "Set operation right branch", [], () =>
      executeSelectAst(next, input, parentCtes, options),
    ),
  ]);

  switch (operation) {
    case "union all":
      return [...leftRows, ...rightRows];
    case "union":
      return dedupeRows([...leftRows, ...rightRows]);
    case "intersect":
      return intersectRows(leftRows, rightRows);
    case "except":
    case "minus":
      return exceptRows(leftRows, rightRows);
    default:
      throw new Error(`Unsupported set operation: ${String(ast.set_op)}`);
  }
}

async function executeParsedSelect<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  for (const binding of parsed.bindings) {
    if (binding.isCte) {
      continue;
    }

    getTable(input.schema, binding.table);
    if (!input.methods[binding.table]) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
    }
  }

  if (parsed.isAggregate) {
    return executeAggregateSelect(parsed, input, cteRows, options);
  }

  const requiresProjectedSortLimit =
    parsed.distinct || parsed.orderBy.some((term) => term.kind === "output");

  const joinedRows = await runStepWithController(options, "join", "Join source bindings", [], () =>
    executeJoinedRows(parsed, input, cteRows, options, {
      applyFinalSortAndLimit: !requiresProjectedSortLimit,
    }),
  );
  const whereFiltered = await runStepWithController(
    options,
    "filter",
    "Apply WHERE filter",
    [],
    () => applyWhereFilter(joinedRows, parsed, input, cteRows, options),
  );
  let windowed = whereFiltered;
  if (parsed.windowFunctions.length > 0) {
    windowed = await runStepWithController(options, "window", "Compute window functions", [], () =>
      applyWindowFunctions(whereFiltered, parsed),
    );
  }

  let projected = await runStepWithController(
    options,
    "projection",
    "Project result rows",
    [],
    () => projectResultRows(windowed, parsed, input, cteRows, options),
  );

  if (parsed.distinct) {
    projected = await runStepWithController(options, "distinct", "Apply DISTINCT", [], async () =>
      dedupeRows(projected),
    );
  }

  if (requiresProjectedSortLimit) {
    projected = await runStepWithController(
      options,
      "order",
      "Apply ORDER/LIMIT/OFFSET on projected rows",
      [],
      async () => applyProjectedSortLimit(projected, parsed),
    );
  }

  return projected;
}

async function executeAggregateSelect<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  if (parsed.windowFunctions.length > 0) {
    throw new Error("Window functions are not supported in grouped aggregate queries yet.");
  }

  let aggregateRows = await runStepWithController(
    options,
    "aggregate",
    "Run aggregate route",
    [],
    async () => {
      const routed = await tryRunAggregateRoute(parsed, input);
      if (routed.rows) {
        return {
          value: routed.rows,
          metadata: {
            routeUsed: "aggregate",
            outputRowCount: routed.rows.length,
            notes: ["Aggregate handler route was used.", ...(routed.notes ?? [])],
          },
        };
      }

      const rootBinding = parsed.bindings[0];
      if (rootBinding && !rootBinding.isCte) {
        const tableBehavior = resolveTableQueryBehavior(input.schema, rootBinding.table);
        if (tableBehavior.fallback.aggregates === "require_pushdown") {
          throw new Error(
            `Query rejected by fallback policy for table ${rootBinding.table}: aggregate route is required.`,
          );
        }
      }

      const local = await runLocalAggregate(parsed, input, cteRows, options);
      return {
        value: local,
        metadata: {
          routeUsed: "local",
          outputRowCount: local.length,
          notes: ["Fell back to local aggregate execution.", ...(routed.notes ?? [])],
        },
      };
    },
  );

  if (parsed.having) {
    aggregateRows = await runStepWithController(options, "filter", "Apply HAVING filter", [], () =>
      applyHavingFilter(aggregateRows, parsed, input, cteRows, options),
    );
  }

  if (parsed.distinct) {
    aggregateRows = await runStepWithController(
      options,
      "distinct",
      "Apply DISTINCT",
      [],
      async () => dedupeRows(aggregateRows),
    );
  }

  let out = aggregateRows;
  if (parsed.orderBy.length > 0) {
    out = await runStepWithController(options, "order", "Apply ORDER BY", [], async () =>
      applyOutputSort(out, parsed.orderBy),
    );
  }

  if (parsed.offset != null || parsed.limit != null) {
    out = await runStepWithController(
      options,
      "limit_offset",
      "Apply LIMIT/OFFSET",
      [],
      async () => {
        let limited = out;
        if (parsed.offset != null) {
          limited = limited.slice(parsed.offset);
        }
        if (parsed.limit != null) {
          limited = limited.slice(0, parsed.limit);
        }
        return limited;
      },
    );
  }

  return runStepWithController(options, "projection", "Project aggregate output", [], async () =>
    out.map((row) => projectAggregateOutputRow(row, parsed)),
  );
}

async function tryRunAggregateRoute<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
): Promise<{ rows: QueryRow[] | null; notes?: string[] }> {
  if (!canUseAggregateRoute(parsed, input)) {
    return { rows: null };
  }

  const binding = parsed.bindings[0];
  if (!binding || binding.isCte) {
    return { rows: null };
  }

  const method = input.methods[binding.table];
  if (!method?.aggregate) {
    return { rows: null };
  }

  const tableBehavior = resolveTableQueryBehavior(input.schema, binding.table);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const where = filtersByAlias.get(binding.alias) ?? [];
  const whereTerms = withFilterTermIds(where, "aggregate_where");
  const metricTerms = parsed.aggregateMetrics.map((metric, index) => ({
    id: `aggregate_metric_${index + 1}`,
    metric: {
      fn: metric.fn,
      as: metric.output,
      ...(metric.column ? { column: metric.column.column } : {}),
      ...(metric.distinct ? { distinct: true } : {}),
    } satisfies TableAggregateMetric,
  }));

  const groupBy = parsed.groupBy.map((column) => column.column);
  const baseRequest = {
    table: binding.table,
    alias: binding.alias,
    ...(whereTerms.length > 0 ? { where: whereTerms.map((term) => term.clause) } : {}),
    ...(groupBy.length > 0 ? { groupBy } : {}),
    metrics: metricTerms.map((term) => term.metric),
    ...(parsed.orderBy.length === 0 && parsed.offset == null && parsed.limit != null
      ? { limit: parsed.limit }
      : {}),
  } satisfies TableAggregateRequest;

  const planningRequest = {
    table: binding.table,
    alias: binding.alias,
    ...(whereTerms.length > 0 ? { where: whereTerms } : {}),
    ...(groupBy.length > 0 ? { groupBy } : {}),
    metrics: metricTerms,
    ...(baseRequest.limit != null ? { limit: baseRequest.limit } : {}),
  } as const;

  const decision = method.planAggregate?.(planningRequest, input.context);
  if (decision?.reject) {
    throw new Error(decision.reject.message);
  }

  if (decision?.mode === "remote_residual") {
    const hasResidual =
      (decision.residual?.where?.length ?? 0) > 0 ||
      (decision.residual?.groupBy?.length ?? 0) > 0 ||
      (decision.residual?.metrics?.length ?? 0) > 0 ||
      decision.residual?.limit != null;
    if (hasResidual) {
      if (tableBehavior.fallback.aggregates === "require_pushdown") {
        throw new Error(
          `Query rejected by fallback policy for table ${binding.table}: aggregate residuals require local execution.`,
        );
      }
      return {
        rows: null,
        ...(decision.notes ? { notes: decision.notes } : {}),
      };
    }

    const remoteRequest: TableAggregateRequest = {
      table: binding.table,
      alias: binding.alias,
      metrics: decision.remote?.metrics ?? baseRequest.metrics,
      ...(decision.remote?.where ? { where: decision.remote.where } : {}),
      ...(decision.remote?.groupBy ? { groupBy: decision.remote.groupBy } : {}),
      ...(decision.remote?.limit != null ? { limit: decision.remote.limit } : {}),
    };

    const rows = await method.aggregate(remoteRequest, input.context);
    const normalizedRows = normalizeRowsForBinding(binding.table, rows, input.schema);
    validateRowsForBinding(binding.table, normalizedRows, input);
    return {
      rows: normalizedRows.map((row) => normalizeAggregateRowFromRoute(row, parsed)),
      ...(decision.notes ? { notes: decision.notes } : {}),
    };
  }

  const whereIds =
    decision?.whereIds != null ? new Set(decision.whereIds) : new Set(whereTerms.map((term) => term.id));
  const metricIds =
    decision?.metricIds != null
      ? new Set(decision.metricIds)
      : new Set(metricTerms.map((term) => term.id));
  const pushGroupBy = decision?.groupBy !== "residual";
  const pushLimit = decision?.limit !== "residual";

  const residualNeeded =
    whereTerms.some((term) => !whereIds.has(term.id)) ||
    metricTerms.some((term) => !metricIds.has(term.id)) ||
    (!pushGroupBy && groupBy.length > 0) ||
    (!pushLimit && baseRequest.limit != null);

  if (residualNeeded) {
    if (tableBehavior.fallback.aggregates === "require_pushdown") {
      throw new Error(
        `Query rejected by fallback policy for table ${binding.table}: aggregate residuals require local execution.`,
      );
    }
    return { rows: null, ...(decision?.notes ? { notes: decision.notes } : {}) };
  }

  const request: TableAggregateRequest = {
    table: binding.table,
    alias: binding.alias,
    metrics: metricTerms.filter((term) => metricIds.has(term.id)).map((term) => term.metric),
    ...(whereTerms.length > 0
      ? { where: whereTerms.filter((term) => whereIds.has(term.id)).map((term) => term.clause) }
      : {}),
    ...(pushGroupBy && groupBy.length > 0 ? { groupBy } : {}),
    ...(pushLimit && baseRequest.limit != null ? { limit: baseRequest.limit } : {}),
  };

  const rows = await method.aggregate(request, input.context);
  const normalizedRows = normalizeRowsForBinding(binding.table, rows, input.schema);
  validateRowsForBinding(binding.table, normalizedRows, input);
  return {
    rows: normalizedRows.map((row) => normalizeAggregateRowFromRoute(row, parsed)),
    ...(decision?.notes ? { notes: decision.notes } : {}),
  };
}

function canUseAggregateRoute<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
): boolean {
  if (parsed.bindings.length !== 1 || parsed.joins.length > 0) {
    return false;
  }

  if (parsed.having) {
    return false;
  }

  if (!parsed.wherePushdownSafe) {
    return false;
  }

  const binding = parsed.bindings[0];
  if (!binding || binding.isCte) {
    return false;
  }

  const method = input.methods[binding.table];
  if (!method?.aggregate) {
    return false;
  }

  if (parsed.groupBy.some((column) => column.alias !== binding.alias)) {
    return false;
  }

  if (
    parsed.aggregateMetrics.some((metric) => metric.column && metric.column.alias !== binding.alias)
  ) {
    return false;
  }

  return true;
}

function normalizeAggregateRowFromRoute(row: QueryRow, parsed: ParsedSelectQuery): QueryRow {
  const out: QueryRow = {};

  for (const column of parsed.aggregateOutputColumns) {
    const direct = row[column.output];
    out[column.output] = direct ?? row[column.source.column] ?? null;
  }

  for (const metric of parsed.aggregateMetrics) {
    if (metric.hidden) {
      continue;
    }
    out[metric.output] = row[metric.output] ?? null;
  }

  return out;
}

async function runLocalAggregate<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  const joinedRows = await executeJoinedRows(parsed, input, cteRows, options, {
    applyFinalSortAndLimit: false,
  });
  const whereFiltered = await applyWhereFilter(joinedRows, parsed, input, cteRows, options);

  return aggregateJoinedRows(whereFiltered, parsed);
}

function aggregateJoinedRows(rows: JoinedRowBundle[], parsed: ParsedSelectQuery): QueryRow[] {
  const groupSourceKeys = parsed.groupBy.map((column) =>
    sourceColumnKey(column.alias, column.column),
  );

  const groups = new Map<
    string,
    {
      groupValues: Map<string, unknown>;
      metricState: MetricAccumulator[];
    }
  >();

  for (const bundle of rows) {
    const groupValues = groupSourceKeys.map((key) => {
      const [alias, column] = key.split(".");
      if (!alias || !column) {
        return null;
      }
      return bundle[alias]?.[column] ?? null;
    });

    const groupKey = JSON.stringify(groupValues);
    let state = groups.get(groupKey);

    if (!state) {
      const stateGroupValues = new Map<string, unknown>();
      parsed.groupBy.forEach((column, index) => {
        stateGroupValues.set(
          sourceColumnKey(column.alias, column.column),
          groupValues[index] ?? null,
        );
      });

      state = {
        groupValues: stateGroupValues,
        metricState: parsed.aggregateMetrics.map((metric) => createMetricAccumulator(metric)),
      };

      groups.set(groupKey, state);
    }

    parsed.aggregateMetrics.forEach((metric, index) => {
      const accumulator = state.metricState[index];
      if (!accumulator) {
        return;
      }

      const value = metric.column
        ? (bundle[metric.column.alias]?.[metric.column.column] ?? null)
        : null;

      applyMetricValue(accumulator, metric, value);
    });
  }

  if (groups.size === 0 && parsed.groupBy.length === 0 && parsed.aggregateMetrics.length > 0) {
    const state = {
      groupValues: new Map<string, unknown>(),
      metricState: parsed.aggregateMetrics.map((metric) => createMetricAccumulator(metric)),
    };
    groups.set("__all__", state);
  }

  const out: QueryRow[] = [];

  for (const state of groups.values()) {
    const row: QueryRow = {};

    for (const column of parsed.aggregateOutputColumns) {
      const sourceKey = sourceColumnKey(column.source.alias, column.source.column);
      row[column.output] = state.groupValues.get(sourceKey) ?? null;
    }

    parsed.aggregateMetrics.forEach((metric, index) => {
      const accumulator = state.metricState[index];
      row[metric.output] = accumulator ? finalizeMetricValue(metric, accumulator) : null;
    });

    out.push(row);
  }

  return out;
}

function createMetricAccumulator(metric: AggregateMetric): MetricAccumulator {
  const accumulator: MetricAccumulator = {
    count: 0,
    sum: 0,
    hasValue: false,
    min: null,
    max: null,
  };

  if (metric.distinct) {
    accumulator.distinctValues = new Set<string>();
  }

  return accumulator;
}

function applyMetricValue(
  accumulator: MetricAccumulator,
  metric: AggregateMetric,
  value: unknown,
): void {
  if (metric.distinct) {
    const distinctKey = JSON.stringify(value);
    if (accumulator.distinctValues?.has(distinctKey)) {
      return;
    }
    accumulator.distinctValues?.add(distinctKey);
  }

  switch (metric.fn) {
    case "count": {
      if (!metric.column) {
        accumulator.count += 1;
      } else if (value != null) {
        accumulator.count += 1;
      }
      return;
    }
    case "sum": {
      if (value == null) {
        return;
      }
      accumulator.sum += toFiniteNumber(value, "SUM");
      accumulator.hasValue = true;
      return;
    }
    case "avg": {
      if (value == null) {
        return;
      }
      accumulator.sum += toFiniteNumber(value, "AVG");
      accumulator.count += 1;
      accumulator.hasValue = true;
      return;
    }
    case "min": {
      if (value == null) {
        return;
      }
      if (!accumulator.hasValue || compareNullableValues(value, accumulator.min) < 0) {
        accumulator.min = value;
      }
      accumulator.hasValue = true;
      return;
    }
    case "max": {
      if (value == null) {
        return;
      }
      if (!accumulator.hasValue || compareNullableValues(value, accumulator.max) > 0) {
        accumulator.max = value;
      }
      accumulator.hasValue = true;
      return;
    }
  }
}

function finalizeMetricValue(metric: AggregateMetric, accumulator: MetricAccumulator): unknown {
  switch (metric.fn) {
    case "count":
      return accumulator.count;
    case "sum":
      return accumulator.hasValue ? accumulator.sum : null;
    case "avg":
      return accumulator.count > 0 ? accumulator.sum / accumulator.count : null;
    case "min":
      return accumulator.hasValue ? accumulator.min : null;
    case "max":
      return accumulator.hasValue ? accumulator.max : null;
  }
}

function toFiniteNumber(value: unknown, functionName: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }

  return parsed;
}

async function executeJoinedRows<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  executionOptions: ExecutionOptions,
  options: {
    applyFinalSortAndLimit: boolean;
  },
): Promise<JoinedRowBundle[]> {
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const canPushFinalSortAndLimitAll =
    options.applyFinalSortAndLimit &&
    parsed.bindings.length === 1 &&
    parsed.orderBy.every((term) => term.kind === "source" && term.alias === rootBinding.alias);

  const projectionByAlias = buildProjection(parsed, input.schema, cteRows);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const executionOrder = buildExecutionOrder(parsed.bindings, parsed.joinEdges, filtersByAlias);
  const rowsByAlias = new Map<string, QueryRow[]>();
  const aliasDependencies = buildAliasPrerequisites(parsed.joins);
  const unresolved = new Set(executionOrder);
  const resolved = new Set<string>();

  while (unresolved.size > 0) {
    let ready = executionOrder.filter((alias) => {
      if (!unresolved.has(alias)) {
        return false;
      }
      const dependencies = aliasDependencies.get(alias) ?? [];
      return dependencies.every((dependency) => resolved.has(dependency));
    });

    if (ready.length === 0) {
      const fallback = executionOrder.find((alias) => unresolved.has(alias));
      if (!fallback) {
        throw new Error("Unable to determine next scan task.");
      }
      ready = [fallback];
    }

    for (let index = 0; index < ready.length; index += executionOptions.maxConcurrency) {
      const batch = ready.slice(index, index + executionOptions.maxConcurrency);
      const scanResults = await Promise.all(
        batch.map(async (alias) => {
          const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
          if (!binding) {
            throw new Error(`Unknown alias in execution order: ${alias}`);
          }

          const dependencyFilters = buildDependencyFilters(
            alias,
            parsed.joins,
            parsed.joinEdges,
            rowsByAlias,
          );
          const localFilters = filtersByAlias.get(alias) ?? [];

          if (
            dependencyFilters.some((filter) => filter.op === "in" && filter.values.length === 0)
          ) {
            return {
              alias,
              rows: [] as QueryRow[],
            };
          }

          const requestWhere: ScanFilterClause[] = [...localFilters, ...dependencyFilters];

          const canPushFinalSortAndLimit =
            canPushFinalSortAndLimitAll && alias === rootBinding.alias;
          const tableBehavior = !binding.isCte
            ? resolveTableQueryBehavior(input.schema, binding.table)
            : undefined;

          if (
            tableBehavior &&
            tableBehavior.fallback.sorting === "require_pushdown" &&
            !canPushFinalSortAndLimit &&
            parsed.orderBy.some((term) => term.kind === "source" && term.alias === alias)
          ) {
            throw new Error(
              `Query rejected by fallback policy for table ${binding.table}: sorting must be fully pushed down.`,
            );
          }
          if (
            tableBehavior &&
            tableBehavior.fallback.limitOffset === "require_pushdown" &&
            !canPushFinalSortAndLimit &&
            (parsed.limit != null || parsed.offset != null) &&
            alias === rootBinding.alias
          ) {
            throw new Error(
              `Query rejected by fallback policy for table ${binding.table}: LIMIT/OFFSET must be fully pushed down.`,
            );
          }

          const requestOrderBy: ScanOrderBy[] | undefined = canPushFinalSortAndLimit
            ? parsed.orderBy
                .filter((term): term is SourceOrderColumn => term.kind === "source")
                .map((term) => ({
                  column: term.column,
                  direction: term.direction,
                }))
            : undefined;

          let requestLimit = canPushFinalSortAndLimit ? parsed.limit : undefined;
          const requestOffset = canPushFinalSortAndLimit ? parsed.offset : undefined;

          if (tableBehavior) {
            const defaultMaxRows = tableBehavior.maxRows;

            if (requestLimit == null && defaultMaxRows != null) {
              requestLimit = defaultMaxRows;
            }

            if (requestLimit != null && defaultMaxRows != null && requestLimit > defaultMaxRows) {
              throw new Error(
                `Requested limit ${requestLimit} exceeds maxRows ${defaultMaxRows} for table ${binding.table}`,
              );
            }
          }

          const projection = projectionByAlias.get(alias);
          if (!projection) {
            throw new Error(`Unable to resolve projection columns for alias: ${alias}`);
          }

          const request: TableScanRequest = {
            table: binding.table,
            alias,
            select: [...projection],
          };

          if (requestWhere.length > 0) {
            request.where = requestWhere;
          }

          if (requestOrderBy && requestOrderBy.length > 0) {
            request.orderBy = requestOrderBy;
          }

          if (requestLimit != null) {
            request.limit = requestLimit;
          }

          if (requestOffset != null) {
            request.offset = requestOffset;
          }

          const rows = await runStepWithController(
            executionOptions,
            "scan",
            `Scan ${alias} (${binding.table})`,
            aliasDependencies.get(alias) ?? [],
            async () => runSourceScan(binding, request, cteRows, input, tableBehavior),
          );

          const normalizedRows = !binding.isCte
            ? normalizeRowsForBinding(binding.table, rows, input.schema)
            : rows;

          if (!binding.isCte) {
            validateRowsForBinding(binding.table, normalizedRows, input);
          }

          return {
            alias,
            rows: normalizedRows,
          };
        }),
      );

      for (const result of scanResults) {
        rowsByAlias.set(result.alias, result.rows);
        unresolved.delete(result.alias);
        resolved.add(result.alias);
      }
    }
  }

  let joinedRows = initializeJoinedRows(rowsByAlias, rootBinding.alias);
  for (const join of parsed.joins) {
    switch (join.join) {
      case "left":
        joinedRows = applyLeftJoin(joinedRows, join, rowsByAlias);
        break;
      case "right":
        joinedRows = applyRightJoin(joinedRows, join, rowsByAlias);
        break;
      case "full":
        joinedRows = applyFullJoin(joinedRows, join, rowsByAlias);
        break;
      default:
        joinedRows = applyInnerJoin(joinedRows, join, rowsByAlias);
    }
  }

  if (options.applyFinalSortAndLimit && !canPushFinalSortAndLimitAll) {
    if (parsed.orderBy.length > 0) {
      joinedRows = applyFinalSort(joinedRows, parsed.orderBy);
    }

    if (parsed.offset != null) {
      joinedRows = joinedRows.slice(parsed.offset);
    }

    if (parsed.limit != null) {
      joinedRows = joinedRows.slice(0, parsed.limit);
    }
  }

  return joinedRows;
}

async function runSourceScan<TContext>(
  binding: TableBinding,
  request: TableScanRequest,
  cteRows: Map<string, QueryRow[]>,
  input: QueryInput<TContext>,
  tableBehavior?: ReturnType<typeof resolveTableQueryBehavior>,
): Promise<StepRunWithMetadata<QueryRow[]>> {
  if (binding.isCte) {
    const rows = cteRows.get(binding.table) ?? [];
    const scanned = scanRows(rows, request);
    return {
      value: scanned,
      metadata: {
        routeUsed: "local",
        outputRowCount: scanned.length,
        notes: ["Resolved from CTE materialization."],
      },
    };
  }

  const method = input.methods[binding.table];
  if (!method) {
    throw new Error(`No table methods registered for table: ${binding.table}`);
  }

  return runScan(
    method,
    request,
    input.context,
    tableBehavior ?? resolveTableQueryBehavior(input.schema, binding.table),
  );
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = applyScanFilters(rows, request.where ?? []);

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(
          left[term.column] ?? null,
          right[term.column] ?? null,
        );
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

function applyScanFilters(rows: QueryRow[], clauses: ScanFilterClause[]): QueryRow[] {
  let out = [...rows];

  for (const clause of clauses) {
    switch (clause.op) {
      case "eq":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && value === clause.value;
        });
        break;
      case "neq":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && value !== clause.value;
        });
        break;
      case "gt":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) > 0;
        });
        break;
      case "gte":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) >= 0;
        });
        break;
      case "lt":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) < 0;
        });
        break;
      case "lte":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) <= 0;
        });
        break;
      case "in": {
        const set = new Set<unknown>(clause.values.filter((value) => value != null));
        out = out.filter((row) => {
          const value = row[clause.column];
          return value != null && set.has(value as unknown);
        });
        break;
      }
      case "is_null":
        out = out.filter((row) => row[clause.column] == null);
        break;
      case "is_not_null":
        out = out.filter((row) => row[clause.column] != null);
        break;
    }
  }

  return out;
}

async function runScan<TContext>(
  method: TableMethods<TContext>,
  request: TableScanRequest,
  context: TContext,
  tableBehavior: ReturnType<typeof resolveTableQueryBehavior>,
): Promise<StepRunWithMetadata<QueryRow[]>> {
  const whereClauses = request.where ?? [];
  const dependencyFilters = whereClauses.filter((clause) => clause.op === "in");
  const lookupCandidate =
    dependencyFilters.length === 1 &&
    method.lookup &&
    dependencyFilters[0] &&
    dependencyFilters[0].values.length > 0 &&
    request.orderBy == null &&
    request.limit == null &&
    request.offset == null;

  if (lookupCandidate) {
    const lookup = dependencyFilters[0];
    if (lookup) {
      return runLookupRoute(method, request, lookup, context, tableBehavior);
    }
  }

  const plannedWhere = withFilterTermIds(request.where ?? [], "where");
  const plannedOrderBy = withOrderTermIds(request.orderBy ?? [], "order");
  const plannedRequest = {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    select: request.select,
    ...(plannedWhere.length > 0 ? { where: plannedWhere } : {}),
    ...(plannedOrderBy.length > 0 ? { orderBy: plannedOrderBy } : {}),
    ...(request.limit != null ? { limit: request.limit } : {}),
    ...(request.offset != null ? { offset: request.offset } : {}),
  } as const;

  const decision = method.planScan?.(plannedRequest, context);
  if (decision?.reject) {
    throw new Error(decision.reject.message);
  }

  const partitioned = partitionScanPlanDecision(decision, request, plannedWhere, plannedOrderBy);
  if (partitioned.residual.where.length > 0 && tableBehavior.fallback.filters === "require_pushdown") {
    throw new Error(
      `Query rejected by fallback policy for table ${request.table}: filter residuals require local execution.`,
    );
  }
  if (
    partitioned.residual.orderBy.length > 0 &&
    tableBehavior.fallback.sorting === "require_pushdown"
  ) {
    throw new Error(
      `Query rejected by fallback policy for table ${request.table}: sorting residuals require local execution.`,
    );
  }
  if (
    (partitioned.residual.limit != null || partitioned.residual.offset != null) &&
    tableBehavior.fallback.limitOffset === "require_pushdown"
  ) {
    throw new Error(
      `Query rejected by fallback policy for table ${request.table}: limit/offset residuals require local execution.`,
    );
  }

  const remoteRequest: TableScanRequest = {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    select: request.select,
    ...(partitioned.remote.where.length > 0 ? { where: partitioned.remote.where } : {}),
    ...(partitioned.remote.orderBy.length > 0 ? { orderBy: partitioned.remote.orderBy } : {}),
    ...(partitioned.remote.limit != null ? { limit: partitioned.remote.limit } : {}),
    ...(partitioned.remote.offset != null ? { offset: partitioned.remote.offset } : {}),
  };

  const scanned = await method.scan(remoteRequest, context);
  const hasResidualWork =
    partitioned.residual.where.length > 0 ||
    partitioned.residual.orderBy.length > 0 ||
    partitioned.residual.limit != null ||
    partitioned.residual.offset != null;

  if (!hasResidualWork) {
    return {
      value: scanned,
      metadata: {
        routeUsed: "scan",
        outputRowCount: scanned.length,
        ...(decision?.notes && decision.notes.length > 0 ? { notes: decision.notes } : {}),
      },
    };
  }

  const local = scanRows(scanned, {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    select: request.select,
    ...(partitioned.residual.where.length > 0 ? { where: partitioned.residual.where } : {}),
    ...(partitioned.residual.orderBy.length > 0 ? { orderBy: partitioned.residual.orderBy } : {}),
    ...(partitioned.residual.limit != null ? { limit: partitioned.residual.limit } : {}),
    ...(partitioned.residual.offset != null ? { offset: partitioned.residual.offset } : {}),
  });

  const notes = [
    "Applied residual scan operations locally.",
    ...(decision?.notes ?? []),
  ];

  return {
    value: local,
    metadata: {
      routeUsed: "scan",
      outputRowCount: local.length,
      notes,
    },
  };
}

async function runLookupRoute<TContext>(
  method: TableMethods<TContext>,
  request: TableScanRequest,
  lookup: Extract<ScanFilterClause, { op: "in" }>,
  context: TContext,
  tableBehavior: ReturnType<typeof resolveTableQueryBehavior>,
): Promise<StepRunWithMetadata<QueryRow[]>> {
  const remainingWhere = (request.where ?? []).filter((clause) => clause !== lookup);
  const plannedRequest = {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    key: lookup.column,
    values: lookup.values,
    select: request.select,
    ...(remainingWhere.length > 0 ? { where: withFilterTermIds(remainingWhere, "lookup_where") } : {}),
  } as const;

  const decision = method.planLookup?.(plannedRequest, context);
  if (decision?.reject) {
    throw new Error(decision.reject.message);
  }

  const partitioned = partitionLookupPlanDecision(decision, remainingWhere);
  if (partitioned.residual.length > 0 && tableBehavior.fallback.filters === "require_pushdown") {
    throw new Error(
      `Query rejected by fallback policy for table ${request.table}: lookup residual filters require local execution.`,
    );
  }

  const lookupRequest: TableLookupRequest = {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    key: lookup.column,
    values: lookup.values,
    select: request.select,
    ...(partitioned.remote.length > 0 ? { where: partitioned.remote } : {}),
  };
  const lookedUp = await method.lookup!(lookupRequest, context);

  const local =
    partitioned.residual.length > 0
      ? applyScanFilters(lookedUp, partitioned.residual)
      : lookedUp;

  const notes = [
    "Lookup route selected due a single IN dependency filter.",
    ...(partitioned.residual.length > 0 ? ["Applied residual lookup filters locally."] : []),
    ...(decision?.notes ?? []),
  ];

  return {
    value: local,
    metadata: {
      routeUsed: "lookup",
      outputRowCount: local.length,
      notes,
    },
  };
}

function withFilterTermIds(clauses: ScanFilterClause[], prefix: string): Array<{
  id: string;
  clause: ScanFilterClause;
}> {
  return clauses.map((clause, index) => ({
    id: clause.id ?? `${prefix}_${index + 1}`,
    clause,
  }));
}

function withOrderTermIds(terms: ScanOrderBy[], prefix: string): Array<{
  id: string;
  term: ScanOrderBy;
}> {
  return terms.map((term, index) => ({
    id: term.id ?? `${prefix}_${index + 1}`,
    term,
  }));
}

function partitionScanPlanDecision(
  decision: ReturnType<NonNullable<TableMethods["planScan"]>> | undefined,
  request: TableScanRequest,
  whereTerms: Array<{ id: string; clause: ScanFilterClause }>,
  orderTerms: Array<{ id: string; term: ScanOrderBy }>,
): {
  remote: {
    where: ScanFilterClause[];
    orderBy: ScanOrderBy[];
    limit?: number;
    offset?: number;
  };
  residual: {
    where: ScanFilterClause[];
    orderBy: ScanOrderBy[];
    limit?: number;
    offset?: number;
  };
} {
  if (decision?.mode === "remote_residual") {
    return {
      remote: {
        where: [...(decision.remote?.where ?? [])],
        orderBy: [...(decision.remote?.orderBy ?? [])],
        ...(decision.remote?.limit != null ? { limit: decision.remote.limit } : {}),
        ...(decision.remote?.offset != null ? { offset: decision.remote.offset } : {}),
      },
      residual: {
        where: [...(decision.residual?.where ?? [])],
        orderBy: [...(decision.residual?.orderBy ?? [])],
        ...(decision.residual?.limit != null ? { limit: decision.residual.limit } : {}),
        ...(decision.residual?.offset != null ? { offset: decision.residual.offset } : {}),
      },
    };
  }

  const whereIds =
    decision?.whereIds != null ? new Set(decision.whereIds) : new Set(whereTerms.map((term) => term.id));
  const orderIds =
    decision?.orderByIds != null
      ? new Set(decision.orderByIds)
      : new Set(orderTerms.map((term) => term.id));
  const pushLimitOffset = decision?.limitOffset !== "residual";

  return {
    remote: {
      where: whereTerms.filter((term) => whereIds.has(term.id)).map((term) => term.clause),
      orderBy: orderTerms.filter((term) => orderIds.has(term.id)).map((term) => term.term),
      ...(pushLimitOffset && request.limit != null ? { limit: request.limit } : {}),
      ...(pushLimitOffset && request.offset != null ? { offset: request.offset } : {}),
    },
    residual: {
      where: whereTerms.filter((term) => !whereIds.has(term.id)).map((term) => term.clause),
      orderBy: orderTerms.filter((term) => !orderIds.has(term.id)).map((term) => term.term),
      ...(!pushLimitOffset && request.limit != null ? { limit: request.limit } : {}),
      ...(!pushLimitOffset && request.offset != null ? { offset: request.offset } : {}),
    },
  };
}

function partitionLookupPlanDecision(
  decision: ReturnType<NonNullable<TableMethods["planLookup"]>> | undefined,
  where: ScanFilterClause[],
): { remote: ScanFilterClause[]; residual: ScanFilterClause[] } {
  if (decision?.mode === "remote_residual") {
    return {
      remote: [...(decision.remote?.where ?? [])],
      residual: [...(decision.residual?.where ?? [])],
    };
  }

  const whereTerms = withFilterTermIds(where, "lookup_where");
  const whereIds =
    decision?.whereIds != null ? new Set(decision.whereIds) : new Set(whereTerms.map((term) => term.id));
  return {
    remote: whereTerms.filter((term) => whereIds.has(term.id)).map((term) => term.clause),
    residual: whereTerms.filter((term) => !whereIds.has(term.id)).map((term) => term.clause),
  };
}

function parseSelectAst(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): ParsedSelectQuery {
  if (ast.type !== "select") {
    throw new Error("Only SELECT statements are currently supported.");
  }
  const windowDefinitions = parseWindowDefinitions((ast as { window?: unknown }).window);

  const rawFrom: unknown[] = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (rawFrom.length === 0) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const bindings = rawFrom.map((entry: unknown, index: number) => {
    if (!entry || typeof entry !== "object" || !("table" in entry)) {
      throw new Error("Unsupported FROM clause entry.");
    }

    const table = (entry as { table?: unknown }).table;
    const alias = (entry as { as?: unknown }).as;
    if (typeof table !== "string" || table.length === 0) {
      throw new Error("Unable to resolve table name from query.");
    }

    return {
      table,
      alias: typeof alias === "string" && alias.length > 0 ? alias : table,
      index,
      isCte: cteRows.has(table),
    } as TableBinding;
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));

  const joins: ParsedJoin[] = [];
  const joinEdges: JoinCondition[] = [];

  for (let i = 1; i < rawFrom.length; i += 1) {
    const entry = rawFrom[i] as { join?: unknown; on?: unknown; as?: unknown; table?: unknown };
    const joinType = typeof entry.join === "string" ? entry.join.toUpperCase() : "";
    if (
      joinType !== "INNER JOIN" &&
      joinType !== "JOIN" &&
      joinType !== "LEFT JOIN" &&
      joinType !== "LEFT OUTER JOIN" &&
      joinType !== "RIGHT JOIN" &&
      joinType !== "RIGHT OUTER JOIN" &&
      joinType !== "FULL JOIN" &&
      joinType !== "FULL OUTER JOIN"
    ) {
      throw new Error(`Unsupported join type: ${String(entry.join ?? "unknown")}`);
    }

    const parsedJoin = parseJoinCondition(entry.on, bindings, aliasToBinding);
    const joinedAlias =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : String(entry.table);

    joins.push({
      alias: joinedAlias,
      join:
        joinType === "LEFT JOIN" || joinType === "LEFT OUTER JOIN"
          ? "left"
          : joinType === "RIGHT JOIN" || joinType === "RIGHT OUTER JOIN"
            ? "right"
            : joinType === "FULL JOIN" || joinType === "FULL OUTER JOIN"
              ? "full"
              : "inner",
      condition: parsedJoin,
    });

    joinEdges.push(parsedJoin);
  }
  const where = ast.where;
  const whereColumns = collectColumnReferences(where, bindings, aliasToBinding);
  const filters: LiteralFilter[] = [];
  const wherePushdown = tryParseConjunctivePushdownFilters(where, bindings, aliasToBinding);
  if (wherePushdown) {
    filters.push(...wherePushdown.filters);
    joinEdges.push(...wherePushdown.joinEdges);
  }
  const wherePushdownSafe = where == null || wherePushdown != null;

  const groupBy = parseGroupBy(ast.groupby, bindings, aliasToBinding);

  const selectColumnsRaw: unknown = ast.columns;
  const selectAll =
    selectColumnsRaw === "*" ||
    (Array.isArray(selectColumnsRaw) &&
      selectColumnsRaw.length === 1 &&
      isStarColumn(selectColumnsRaw[0] as { expr?: unknown }));

  const selectColumns: SelectColumn[] = [];
  const scalarSelectItems: Array<{ expr: unknown; output: string }> = [];
  const aggregateMetrics: AggregateMetric[] = [];
  const aggregateOutputColumns: AggregateOutputColumn[] = [];
  const windowFunctions: WindowFunctionSpec[] = [];

  if (!selectAll) {
    if (!Array.isArray(selectColumnsRaw)) {
      throw new Error("Unsupported SELECT clause.");
    }

    for (const item of selectColumnsRaw) {
      if (!item || typeof item !== "object") {
        throw new Error("Unsupported SELECT item.");
      }

      const expr = (item as { expr?: unknown }).expr;
      const as = (item as { as?: unknown }).as;
      const explicitOutput = typeof as === "string" && as.length > 0 ? as : undefined;

      const windowFunction = parseWindowFunction(
        expr,
        explicitOutput,
        bindings,
        aliasToBinding,
        schema,
        windowDefinitions,
      );
      if (windowFunction) {
        if (windowFunctions.some((existing) => existing.output === windowFunction.output)) {
          throw new Error(`Duplicate window output alias: ${windowFunction.output}`);
        }
        windowFunctions.push(windowFunction);
        continue;
      }

      const aggregateMetric = parseAggregateMetric(
        expr,
        explicitOutput,
        bindings,
        aliasToBinding,
        schema,
      );
      if (aggregateMetric) {
        if (aggregateMetrics.some((existing) => existing.output === aggregateMetric.output)) {
          throw new Error(`Duplicate aggregate output alias: ${aggregateMetric.output}`);
        }
        aggregateMetrics.push(aggregateMetric);
        continue;
      }

      const colRef = resolveColumnRef(expr, bindings, aliasToBinding);
      if (colRef) {
        const output =
          explicitOutput ??
          (selectColumns.some((existing) => existing.output === colRef.column)
            ? `${colRef.alias}.${colRef.column}`
            : colRef.column);

        selectColumns.push({
          alias: colRef.alias,
          column: colRef.column,
          output,
        });
        continue;
      }

      if (toSubqueryAst(expr)) {
        const output =
          explicitOutput ?? `expr_${selectColumns.length + scalarSelectItems.length + 1}`;
        scalarSelectItems.push({
          expr,
          output,
        });
        continue;
      }

      throw new Error(
        "Only direct column references, scalar subqueries, aggregate functions, and supported window functions are currently supported in SELECT.",
      );
    }
  }

  const having = ast.having;
  const havingMetrics = collectHavingAggregateMetrics(
    having,
    bindings,
    aliasToBinding,
    schema,
    aggregateMetrics,
  );
  aggregateMetrics.push(...havingMetrics);

  const isAggregate = groupBy.length > 0 || aggregateMetrics.length > 0;

  if (selectAll && bindings.length > 1) {
    throw new Error("SELECT * is only supported for single-table queries.");
  }

  if (isAggregate && selectAll) {
    throw new Error("SELECT * is not supported for aggregate queries.");
  }

  if (windowFunctions.length > 0 && (isAggregate || groupBy.length > 0 || having != null)) {
    throw new Error("Window functions cannot be mixed with GROUP BY/HAVING.");
  }

  const groupByKeys = new Set(
    groupBy.map((column) => sourceColumnKey(column.alias, column.column)),
  );

  if (isAggregate) {
    for (const column of selectColumns) {
      const key = sourceColumnKey(column.alias, column.column);
      if (!groupByKeys.has(key)) {
        throw new Error(
          `Column ${column.alias}.${column.column} must appear in GROUP BY or be aggregated.`,
        );
      }
    }

    for (const column of selectColumns) {
      aggregateOutputColumns.push({
        source: {
          alias: column.alias,
          column: column.column,
        },
        output: column.output,
      });
    }
  }

  const selectOrderByOutputByName = new Map<string, SelectOrderResolution>();
  for (const column of selectColumns) {
    selectOrderByOutputByName.set(column.output, {
      kind: "source",
      alias: column.alias,
      column: column.column,
    });
  }
  for (const item of scalarSelectItems) {
    selectOrderByOutputByName.set(item.output, {
      kind: "output",
      output: item.output,
    });
  }
  for (const windowFunction of windowFunctions) {
    selectOrderByOutputByName.set(windowFunction.output, {
      kind: "output",
      output: windowFunction.output,
    });
  }

  const aggregateOutputNames = new Set<string>();
  for (const column of aggregateOutputColumns) {
    aggregateOutputNames.add(column.output);
  }
  for (const metric of aggregateMetrics) {
    aggregateOutputNames.add(metric.output);
  }

  const groupOutputBySource = new Map<string, string>();
  for (const outputColumn of aggregateOutputColumns) {
    groupOutputBySource.set(
      sourceColumnKey(outputColumn.source.alias, outputColumn.source.column),
      outputColumn.output,
    );
  }

  const orderBy = parseOrderBy(
    ast.orderby,
    bindings,
    aliasToBinding,
    isAggregate,
    selectOrderByOutputByName,
    aggregateOutputNames,
    groupOutputBySource,
  );

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  const parsedQuery: ParsedSelectQuery = {
    bindings,
    joins,
    joinEdges: uniqueJoinEdges(joinEdges),
    filters,
    whereColumns,
    wherePushdownSafe,
    ...(where != null ? { where } : {}),
    ...(having != null ? { having } : {}),
    distinct: ast.distinct != null,
    selectAll,
    selectColumns,
    scalarSelectItems,
    windowFunctions,
    groupBy,
    aggregateMetrics,
    aggregateOutputColumns,
    isAggregate,
    orderBy,
    ...(limit != null ? { limit } : {}),
    ...(offset != null ? { offset } : {}),
  };

  validateParsedSelectPolicy(parsedQuery, schema, aliasToBinding);
  validateEnumLiteralFilters(parsedQuery.where, bindings, aliasToBinding, schema);

  return parsedQuery;
}

function validateParsedSelectPolicy(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
  aliasToBinding: Map<string, TableBinding>,
): void {
  for (const whereColumn of parsed.whereColumns) {
    const binding = aliasToBinding.get(whereColumn.alias);
    if (!binding || binding.isCte || !schema.tables[binding.table]) {
      continue;
    }

    const column = resolveTableColumnDefinition(schema, binding.table, whereColumn.column);
    if (!column.filterable) {
      throw new Error(
        `Filtering on ${binding.table}.${whereColumn.column} is not supported by schema policy.`,
      );
    }
  }

  for (const orderBy of parsed.orderBy) {
    if (orderBy.kind !== "source") {
      continue;
    }

    const binding = aliasToBinding.get(orderBy.alias);
    if (!binding || binding.isCte || !schema.tables[binding.table]) {
      continue;
    }

    const column = resolveTableColumnDefinition(schema, binding.table, orderBy.column);
    if (!column.sortable) {
      throw new Error(
        `Sorting by ${binding.table}.${orderBy.column} is not supported by schema policy.`,
      );
    }
  }

  for (const binding of parsed.bindings) {
    if (binding.isCte) {
      continue;
    }
    if (!schema.tables[binding.table]) {
      continue;
    }

    const tableBehavior = resolveTableQueryBehavior(schema, binding.table);
    if (tableBehavior.reject.requiresLimit && parsed.limit == null) {
      throw new Error(
        `Query rejected by schema policy for table ${binding.table}: LIMIT is required.`,
      );
    }

    const whereColumnsForAlias = parsed.whereColumns.filter((column) => column.alias === binding.alias);
    if (tableBehavior.reject.forbidFullScan && whereColumnsForAlias.length === 0) {
      throw new Error(
        `Query rejected by schema policy for table ${binding.table}: full scans are forbidden.`,
      );
    }

    if (tableBehavior.reject.requireAnyFilterOn.length > 0) {
      const hasRequiredFilter = whereColumnsForAlias.some((column) =>
        tableBehavior.reject.requireAnyFilterOn.includes(column.column),
      );
      if (!hasRequiredFilter) {
        throw new Error(
          `Query rejected by schema policy for table ${binding.table}: expected a WHERE filter on one of [${tableBehavior.reject.requireAnyFilterOn.join(", ")}].`,
        );
      }
    }

    if (
      tableBehavior.fallback.filters === "require_pushdown" &&
      parsed.where &&
      !parsed.wherePushdownSafe &&
      whereColumnsForAlias.length > 0
    ) {
      throw new Error(
        `Query rejected by fallback policy for table ${binding.table}: non-pushdown WHERE predicates are not allowed.`,
      );
    }
  }
}

function validateEnumLiteralFilters(
  expr: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
): void {
  if (!expr || typeof expr !== "object") {
    return;
  }

  if (toSubqueryAst(expr)) {
    return;
  }

  const node = expr as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    args?: { value?: unknown };
  };

  if (node.type === "binary_expr") {
    const operator = typeof node.operator === "string" ? node.operator.toUpperCase() : "";
    if (operator === "=") {
      const leftColumn = tryResolveColumnRef(node.left, bindings, aliasToBinding);
      const rightColumn = tryResolveColumnRef(node.right, bindings, aliasToBinding);
      const leftLiteral = parseLiteral(node.left);
      const rightLiteral = parseLiteral(node.right);

      if (leftColumn && !rightColumn && rightLiteral !== undefined) {
        validateEnumLiteralValue(schema, aliasToBinding, leftColumn, rightLiteral);
      } else if (rightColumn && !leftColumn && leftLiteral !== undefined) {
        validateEnumLiteralValue(schema, aliasToBinding, rightColumn, leftLiteral);
      }
    } else if (operator === "IN") {
      const leftColumn = tryResolveColumnRef(node.left, bindings, aliasToBinding);
      const rightColumn = tryResolveColumnRef(node.right, bindings, aliasToBinding);
      if (leftColumn && !rightColumn) {
        const values = tryParseLiteralExpressionList(node.right);
        if (values) {
          for (const value of values) {
            validateEnumLiteralValue(schema, aliasToBinding, leftColumn, value);
          }
        }
      } else if (rightColumn && !leftColumn) {
        const values = tryParseLiteralExpressionList(node.left);
        if (values) {
          for (const value of values) {
            validateEnumLiteralValue(schema, aliasToBinding, rightColumn, value);
          }
        }
      }
    }

    validateEnumLiteralFilters(node.left, bindings, aliasToBinding, schema);
    validateEnumLiteralFilters(node.right, bindings, aliasToBinding, schema);
    return;
  }

  const args = node.args?.value;
  if (Array.isArray(args)) {
    for (const arg of args) {
      validateEnumLiteralFilters(arg, bindings, aliasToBinding, schema);
    }
  } else if (args) {
    validateEnumLiteralFilters(args, bindings, aliasToBinding, schema);
  }
}

function validateEnumLiteralValue(
  schema: SchemaDefinition,
  aliasToBinding: Map<string, TableBinding>,
  columnRef: { alias: string; column: string },
  value: unknown,
): void {
  if (value == null) {
    return;
  }

  const binding = aliasToBinding.get(columnRef.alias);
  if (!binding || binding.isCte || !schema.tables[binding.table]) {
    return;
  }

  const column = resolveTableColumnDefinition(schema, binding.table, columnRef.column);
  if (!column.enum) {
    return;
  }

  if (typeof value !== "string" || !column.enum.includes(value)) {
    throw new Error(
      `Invalid enum value for ${binding.table}.${columnRef.column}: ${JSON.stringify(value)}. Allowed values: ${column.enum.join(", ")}`,
    );
  }
}

function tryResolveColumnRef(
  expr: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): { alias: string; column: string } | null {
  try {
    return resolveColumnRef(expr, bindings, aliasToBinding) ?? null;
  } catch {
    return null;
  }
}

function parseGroupBy(
  rawGroupBy: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): Array<{ alias: string; column: string }> {
  if (!rawGroupBy || typeof rawGroupBy !== "object") {
    return [];
  }

  const columns = (rawGroupBy as { columns?: unknown }).columns;
  if (!Array.isArray(columns)) {
    return [];
  }

  return columns.map((columnExpr) => {
    const column = resolveColumnRef(columnExpr, bindings, aliasToBinding);
    if (!column) {
      throw new Error("GROUP BY currently supports only direct column references.");
    }
    return column;
  });
}

function parseWindowFunction(
  expr: unknown,
  explicitOutput: string | undefined,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
  windowDefinitions: Map<string, unknown>,
): WindowFunctionSpec | null {
  if (!expr || typeof expr !== "object") {
    return null;
  }

  const asWindow = (expr as { over?: unknown }).over;
  if (!asWindow) {
    return null;
  }

  const over = asWindow as { as_window_specification?: unknown };
  const spec = over.as_window_specification;
  if (!spec) {
    throw new Error("Window function OVER clause must include a window specification.");
  }

  const windowSpecification = resolveWindowSpecification(spec, windowDefinitions, new Set());
  if (!windowSpecification || typeof windowSpecification !== "object") {
    throw new Error("Unsupported window specification.");
  }

  const frameClause = (windowSpecification as { window_frame_clause?: unknown })
    .window_frame_clause;
  const frameMode = parseWindowFrameMode(frameClause);

  const partitionByRaw = (windowSpecification as { partitionby?: unknown }).partitionby;
  const partitionByEntries = Array.isArray(partitionByRaw) ? partitionByRaw : [];
  const partitionBy = partitionByEntries.map((entry) => {
    const parsed = resolveColumnRef((entry as { expr?: unknown }).expr, bindings, aliasToBinding);
    if (!parsed) {
      throw new Error("PARTITION BY currently supports only direct column references.");
    }
    return parsed;
  });

  const orderByRaw = (windowSpecification as { orderby?: unknown }).orderby;
  const orderByEntries = Array.isArray(orderByRaw) ? orderByRaw : [];
  const orderBy = orderByEntries.map((entry) => {
    const column = resolveColumnRef((entry as { expr?: unknown }).expr, bindings, aliasToBinding);
    if (!column) {
      throw new Error("Window ORDER BY currently supports only direct column references.");
    }

    const direction = (entry as { type?: unknown }).type === "DESC" ? "desc" : "asc";
    return {
      kind: "source",
      alias: column.alias,
      column: column.column,
      direction,
    } as SourceOrderColumn;
  });

  const functionExpr = expr as {
    type?: unknown;
    name?: unknown;
    args?: { expr?: unknown; distinct?: unknown } | { value?: unknown[] };
  };

  if (functionExpr.type === "function") {
    const fnName = readFunctionName(functionExpr.name);
    const rankingFn = mapRankingWindowFunction(fnName);
    if (rankingFn) {
      const output = explicitOutput ?? rankingFn;
      return {
        fn: rankingFn,
        output,
        partitionBy,
        orderBy,
        frameMode,
      };
    }

    if (fnName !== "LEAD" && fnName !== "LAG") {
      throw new Error(`Unsupported window function: ${fnName || "unknown"}`);
    }

    const argsRaw = (functionExpr.args as { value?: unknown } | undefined)?.value;
    const args = Array.isArray(argsRaw) ? argsRaw : argsRaw != null ? [argsRaw] : [];
    const firstArg = args[0];
    const column = resolveColumnRef(firstArg, bindings, aliasToBinding);
    if (!column) {
      throw new Error(`${fnName} window function must reference a column as the first argument.`);
    }

    let offset = 1;
    if (args[1] != null) {
      const parsedOffset = parseLiteral(args[1]);
      if (
        typeof parsedOffset !== "number" ||
        !Number.isInteger(parsedOffset) ||
        parsedOffset < 0
      ) {
        throw new Error(`${fnName} offset must be a non-negative integer literal.`);
      }
      offset = parsedOffset;
    }

    let defaultValue: unknown = null;
    if (args[2] != null) {
      const parsedDefault = parseLiteral(args[2]);
      if (parsedDefault === undefined) {
        throw new Error(`${fnName} default value must be a literal.`);
      }
      defaultValue = parsedDefault;
    }

    if (args.length > 3) {
      throw new Error(`${fnName} supports at most three arguments.`);
    }

    const fn = fnName.toLowerCase() as "lead" | "lag";
    const output = explicitOutput ?? `${fn}_${column.column}`;
    return {
      fn,
      output,
      partitionBy,
      orderBy,
      column,
      offset,
      defaultValue,
      frameMode,
    };
  }

  if (functionExpr.type !== "aggr_func") {
    throw new Error("Unsupported window expression.");
  }

  const rawName = typeof functionExpr.name === "string" ? functionExpr.name.toUpperCase() : "";
  const fn = mapAggregateFunction(rawName);
  if (!fn) {
    throw new Error(`Unsupported window aggregate function: ${String(functionExpr.name)}`);
  }

  const args = functionExpr.args as { expr?: unknown; distinct?: unknown } | undefined;
  const distinct = args?.distinct === "DISTINCT";
  const argExpr = args?.expr;

  let column: { alias: string; column: string } | undefined;
  let countStar = false;
  if (isStarExpr(argExpr)) {
    if (fn !== "count") {
      throw new Error(`${rawName}(*) is not supported as a window function.`);
    }
    countStar = true;
  } else {
    column = resolveColumnRef(argExpr, bindings, aliasToBinding);
    if (!column) {
      throw new Error(`${rawName} window function must reference a column or *.`);
    }
    if ((fn === "sum" || fn === "avg") && !isNumericColumn(column, aliasToBinding, schema)) {
      throw new Error(`${rawName} requires a numeric column.`);
    }
  }

  if (distinct && fn !== "count") {
    throw new Error(`DISTINCT is currently only supported for COUNT window functions.`);
  }

  const output =
    explicitOutput ??
    (column ? `${fn}_${column.column}` : fn === "count" ? "count" : `${fn}_value`);

  return {
    fn,
    output,
    partitionBy,
    orderBy,
    frameMode,
    ...(column ? { column } : {}),
    ...(countStar ? { countStar: true } : {}),
  };
}

function parseWindowDefinitions(rawWindow: unknown): Map<string, unknown> {
  const map = new Map<string, unknown>();
  const entries = Array.isArray(rawWindow) ? rawWindow : [];

  for (const entry of entries) {
    if (!entry || typeof entry !== "object") {
      throw new Error("Unsupported WINDOW clause.");
    }

    const rawName = (entry as { name?: unknown }).name;
    const name = typeof rawName === "string" && rawName.length > 0 ? rawName : undefined;
    const rawSpec = (entry as { as_window_specification?: { window_specification?: unknown } })
      .as_window_specification?.window_specification;
    if (!name || !rawSpec || typeof rawSpec !== "object") {
      throw new Error("Unsupported WINDOW clause.");
    }

    if (map.has(name)) {
      throw new Error(`Duplicate WINDOW definition: ${name}`);
    }
    map.set(name, rawSpec);
  }

  return map;
}

function resolveWindowSpecification(
  spec: unknown,
  windowDefinitions: Map<string, unknown>,
  resolving: Set<string>,
): unknown {
  if (typeof spec === "string") {
    const resolved = windowDefinitions.get(spec);
    if (!resolved) {
      throw new Error(`Unknown WINDOW reference: ${spec}`);
    }

    if (resolving.has(spec)) {
      throw new Error(`Cyclic WINDOW reference: ${spec}`);
    }
    resolving.add(spec);

    const merged = resolveWindowSpecification(
      { window_specification: resolved },
      windowDefinitions,
      resolving,
    );

    resolving.delete(spec);
    return merged;
  }

  const windowSpecification = (spec as { window_specification?: unknown }).window_specification;
  if (!windowSpecification || typeof windowSpecification !== "object") {
    return undefined;
  }

  const baseName =
    typeof (windowSpecification as { name?: unknown }).name === "string"
      ? ((windowSpecification as { name?: string }).name ?? "")
      : "";
  if (!baseName) {
    return windowSpecification;
  }

  const baseRaw = windowDefinitions.get(baseName);
  if (!baseRaw) {
    throw new Error(`Unknown WINDOW reference: ${baseName}`);
  }
  if (resolving.has(baseName)) {
    throw new Error(`Cyclic WINDOW reference: ${baseName}`);
  }
  resolving.add(baseName);
  const resolvedBase = resolveWindowSpecification(
    { window_specification: baseRaw },
    windowDefinitions,
    resolving,
  ) as {
    partitionby?: unknown;
    orderby?: unknown;
    window_frame_clause?: unknown;
  };
  resolving.delete(baseName);

  const derived = windowSpecification as {
    partitionby?: unknown;
    orderby?: unknown;
    window_frame_clause?: unknown;
  };

  return {
    ...resolvedBase,
    ...(derived.partitionby != null ? { partitionby: derived.partitionby } : {}),
    ...(derived.orderby != null ? { orderby: derived.orderby } : {}),
    ...(derived.window_frame_clause != null
      ? { window_frame_clause: derived.window_frame_clause }
      : {}),
  };
}

function parseWindowFrameMode(rawFrameClause: unknown): WindowFrameMode {
  if (rawFrameClause == null) {
    return "default";
  }

  const raw = (rawFrameClause as { raw?: unknown }).raw;
  if (typeof raw !== "string") {
    throw new Error("Unsupported window frame clause.");
  }

  const normalized = raw.replace(/\s+/g, " ").trim().toUpperCase();
  if (
    normalized === "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" ||
    normalized === "ROWS UNBOUNDED PRECEDING"
  ) {
    return "rows_unbounded_preceding_current_row";
  }

  throw new Error("Only ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is supported.");
}

function mapRankingWindowFunction(
  rawName: string,
): Exclude<WindowFunctionName, AggregateFunction> | null {
  switch (rawName) {
    case "ROW_NUMBER":
      return "row_number";
    case "RANK":
      return "rank";
    case "DENSE_RANK":
      return "dense_rank";
    default:
      return null;
  }
}

function parseAggregateMetric(
  expr: unknown,
  explicitOutput: string | undefined,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
): AggregateMetric | null {
  const aggregateExpr = expr as {
    type?: unknown;
    name?: unknown;
    over?: unknown;
    args?: {
      expr?: unknown;
      distinct?: unknown;
    };
  };

  if (aggregateExpr.type !== "aggr_func") {
    return null;
  }

  if (aggregateExpr.over != null) {
    return null;
  }

  const rawName = typeof aggregateExpr.name === "string" ? aggregateExpr.name.toUpperCase() : "";
  const fn = mapAggregateFunction(rawName);
  if (!fn) {
    throw new Error(`Unsupported aggregate function: ${String(aggregateExpr.name)}`);
  }

  const args = aggregateExpr.args;
  const distinct = args?.distinct === "DISTINCT";

  const argExpr = args?.expr;
  let column: { alias: string; column: string } | undefined;

  if (isStarExpr(argExpr)) {
    if (fn !== "count") {
      throw new Error(`${rawName}(*) is not supported.`);
    }

    if (distinct) {
      throw new Error("COUNT(DISTINCT *) is not supported.");
    }
  } else {
    column = resolveColumnRef(argExpr, bindings, aliasToBinding);
    if (!column) {
      throw new Error(`${rawName} must reference a column or *.`);
    }

    if ((fn === "sum" || fn === "avg") && !isNumericColumn(column, aliasToBinding, schema)) {
      throw new Error(`${rawName} requires a numeric column.`);
    }
  }

  if (distinct && fn !== "count") {
    throw new Error(`DISTINCT is currently only supported for COUNT.`);
  }

  const output =
    explicitOutput ??
    (column ? `${fn}_${column.column}` : fn === "count" ? "count" : `${fn}_value`);

  const signature = buildAggregateMetricSignature({
    fn,
    distinct,
    ...(column ? { column } : {}),
  });

  return {
    fn,
    output,
    signature,
    hidden: false,
    ...(column ? { column } : {}),
    distinct,
  };
}

function isNumericColumn(
  column: { alias: string; column: string },
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
): boolean {
  const binding = aliasToBinding.get(column.alias);
  if (!binding || binding.isCte) {
    return true;
  }

  const table = getTable(schema, binding.table);
  const columnDefinition = table.columns[column.column];
  if (!columnDefinition) {
    return false;
  }

  const columnType = resolveColumnType(columnDefinition);
  return columnType === "integer";
}

function parseOrderBy(
  rawOrderBy: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  isAggregate: boolean,
  selectOrderByOutputByName: Map<string, SelectOrderResolution>,
  aggregateOutputNames: Set<string>,
  aggregateGroupOutputBySource: Map<string, string>,
): OrderColumn[] {
  if (!Array.isArray(rawOrderBy)) {
    return [];
  }

  const out: OrderColumn[] = [];

  for (const item of rawOrderBy) {
    const expr = (item as { expr?: unknown }).expr;
    const rawType = (item as { type?: unknown }).type;
    const direction: "asc" | "desc" = rawType === "DESC" ? "desc" : "asc";

    const rawColumnRef = toRawColumnRef(expr);
    if (!rawColumnRef) {
      throw new Error("Only column references are currently supported in ORDER BY.");
    }

    if (isAggregate) {
      if (!rawColumnRef.table && aggregateOutputNames.has(rawColumnRef.column)) {
        out.push({
          kind: "output",
          output: rawColumnRef.column,
          direction,
        });
        continue;
      }

      const source = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!source) {
        throw new Error("Unable to resolve ORDER BY column.");
      }

      const groupOutput = aggregateGroupOutputBySource.get(
        sourceColumnKey(source.alias, source.column),
      );
      if (!groupOutput) {
        throw new Error(
          `Aggregate ORDER BY on ${source.alias}.${source.column} must reference a grouped selected column or output alias.`,
        );
      }

      out.push({
        kind: "output",
        output: groupOutput,
        direction,
      });
      continue;
    }

    if (!rawColumnRef.table) {
      const selectOutput = selectOrderByOutputByName.get(rawColumnRef.column);
      if (selectOutput) {
        if (selectOutput.kind === "source") {
          out.push({
            kind: "source",
            alias: selectOutput.alias,
            column: selectOutput.column,
            direction,
          });
        } else {
          out.push({
            kind: "output",
            output: selectOutput.output,
            direction,
          });
        }
        continue;
      }
    }

    const source = resolveColumnRef(expr, bindings, aliasToBinding);
    if (!source) {
      throw new Error("Unable to resolve ORDER BY column.");
    }

    out.push({
      kind: "source",
      alias: source.alias,
      column: source.column,
      direction,
    });
  }

  return out;
}

function mapAggregateFunction(raw: string): AggregateFunction | null {
  switch (raw) {
    case "COUNT":
      return "count";
    case "SUM":
      return "sum";
    case "AVG":
      return "avg";
    case "MIN":
      return "min";
    case "MAX":
      return "max";
    default:
      return null;
  }
}

function parseLimitAndOffset(rawLimit: unknown): { limit?: number; offset?: number } {
  if (!rawLimit || typeof rawLimit !== "object") {
    return {};
  }

  const limitNode = rawLimit as {
    value?: Array<{ value?: unknown }>;
    seperator?: unknown;
  };

  if (!Array.isArray(limitNode.value) || limitNode.value.length === 0) {
    return {};
  }

  const first = parseNumericLiteral(limitNode.value[0]?.value);
  const second = parseNumericLiteral(limitNode.value[1]?.value);
  const separator = limitNode.seperator;

  if (first == null) {
    throw new Error("Unable to parse LIMIT value.");
  }

  if (separator === "offset") {
    const out: { limit?: number; offset?: number } = { limit: first };
    if (second != null) {
      out.offset = second;
    }
    return out;
  }

  if (separator === ",") {
    const out: { limit?: number; offset?: number } = { offset: first };
    if (second != null) {
      out.limit = second;
    }
    return out;
  }

  return {
    limit: first,
  };
}

function parseNumericLiteral(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

interface ParsedPushdownWhere {
  filters: LiteralFilter[];
  joinEdges: JoinCondition[];
}

function tryParseConjunctivePushdownFilters(
  where: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): ParsedPushdownWhere | null {
  if (!where) {
    return {
      filters: [],
      joinEdges: [],
    };
  }

  const whereParts = flattenConjunctiveWhere(where);
  if (!whereParts) {
    return null;
  }

  const filters: LiteralFilter[] = [];
  const joinEdges: JoinCondition[] = [];

  for (const part of whereParts) {
    if (!part || typeof part !== "object") {
      return null;
    }

    const binary = part as { type?: unknown; operator?: unknown; left?: unknown; right?: unknown };
    if (binary.type !== "binary_expr") {
      return null;
    }

    const operator = tryNormalizeBinaryOperator(binary.operator);
    if (!operator) {
      return null;
    }
    if (operator === "in") {
      const colRef = resolveColumnRef(binary.left, bindings, aliasToBinding);
      if (!colRef) {
        return null;
      }

      const values = tryParseLiteralExpressionList(binary.right);
      if (!values) {
        return null;
      }

      filters.push({
        alias: colRef.alias,
        clause: {
          op: "in",
          column: colRef.column,
          values: values as unknown[],
        } as ScanFilterClause,
      });
      continue;
    }

    const leftCol = resolveColumnRef(binary.left, bindings, aliasToBinding);
    const rightCol = resolveColumnRef(binary.right, bindings, aliasToBinding);
    const leftLiteral = parseLiteral(binary.left);
    const rightLiteral = parseLiteral(binary.right);

    if (operator === "is_null" || operator === "is_not_null") {
      if (leftCol && rightLiteral === null) {
        filters.push({
          alias: leftCol.alias,
          clause: {
            op: operator,
            column: leftCol.column,
          },
        });
        continue;
      }

      if (rightCol && leftLiteral === null) {
        filters.push({
          alias: rightCol.alias,
          clause: {
            op: operator,
            column: rightCol.column,
          },
        });
        continue;
      }

      return null;
    }

    if (operator === "eq" && leftCol && rightCol) {
      joinEdges.push({
        leftAlias: leftCol.alias,
        leftColumn: leftCol.column,
        rightAlias: rightCol.alias,
        rightColumn: rightCol.column,
      });
      continue;
    }

    if (leftCol && rightLiteral !== undefined) {
      filters.push({
        alias: leftCol.alias,
        clause: {
          op: operator,
          column: leftCol.column,
          value: rightLiteral,
        } as ScanFilterClause,
      });
      continue;
    }

    if (rightCol && leftLiteral !== undefined) {
      filters.push({
        alias: rightCol.alias,
        clause: {
          op: invertOperator(operator),
          column: rightCol.column,
          value: leftLiteral,
        } as ScanFilterClause,
      });
      continue;
    }

    return null;
  }

  return {
    filters,
    joinEdges,
  };
}

function collectHavingAggregateMetrics(
  having: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
  existingMetrics: AggregateMetric[],
): AggregateMetric[] {
  if (!having) {
    return [];
  }

  const seen = new Set(existingMetrics.map((metric) => metric.signature));
  const out: AggregateMetric[] = [];
  let counter = 1;

  const visit = (node: unknown): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    const aggregateMetric = parseAggregateMetric(
      node,
      `__having_${counter}`,
      bindings,
      aliasToBinding,
      schema,
    );
    if (aggregateMetric) {
      if (!seen.has(aggregateMetric.signature)) {
        aggregateMetric.hidden = true;
        out.push(aggregateMetric);
        seen.add(aggregateMetric.signature);
        counter += 1;
      }
      return;
    }

    const expr = node as {
      type?: unknown;
      left?: unknown;
      right?: unknown;
      args?: { value?: unknown };
    };

    if (expr.type === "binary_expr") {
      visit(expr.left);
      visit(expr.right);
      return;
    }

    if (expr.type === "function") {
      const args = expr.args?.value;
      if (Array.isArray(args)) {
        for (const arg of args) {
          visit(arg);
        }
      } else {
        visit(args);
      }
    }
  };

  visit(having);
  return out;
}

function collectColumnReferences(
  raw: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): Array<{ alias: string; column: string }> {
  if (!raw) {
    return [];
  }

  const out = new Map<string, { alias: string; column: string }>();

  const visit = (node: unknown): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (toSubqueryAst(node)) {
      return;
    }

    const ref = toRawColumnRef(node);
    if (ref) {
      try {
        const resolved = resolveColumnRef(node, bindings, aliasToBinding);
        if (resolved) {
          out.set(sourceColumnKey(resolved.alias, resolved.column), resolved);
        }
      } catch {
        // Ignore columns that do not bind in this query scope.
      }
    }

    const expr = node as {
      type?: unknown;
      left?: unknown;
      right?: unknown;
      args?: { expr?: unknown; value?: unknown };
    };

    if (expr.type === "binary_expr") {
      visit(expr.left);
      visit(expr.right);
      return;
    }

    if (expr.type === "aggr_func") {
      visit(expr.args?.expr);
      return;
    }

    if (expr.type === "function") {
      const values = expr.args?.value;
      if (Array.isArray(values)) {
        for (const value of values) {
          visit(value);
        }
      } else {
        visit(values);
      }
    }
  };

  visit(raw);
  return [...out.values()];
}

function buildAggregateMetricSignature(metric: {
  fn: AggregateFunction;
  distinct: boolean;
  column?: {
    alias: string;
    column: string;
  };
}): string {
  const source = metric.column ? `${metric.column.alias}.${metric.column.column}` : "*";
  return `${metric.fn}|${metric.distinct ? "distinct" : "all"}|${source}`;
}

function buildProjection(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): Map<string, Set<string>> {
  const projections = new Map<string, Set<string>>();
  for (const binding of parsed.bindings) {
    projections.set(binding.alias, new Set());
  }

  if (parsed.selectAll) {
    const base = parsed.bindings[0];
    if (!base) {
      throw new Error("SELECT queries must include a FROM clause.");
    }

    for (const column of getBindingColumns(base, schema, cteRows)) {
      projections.get(base.alias)?.add(column);
    }
  } else {
    for (const item of parsed.selectColumns) {
      projections.get(item.alias)?.add(item.column);
    }

    for (const groupColumn of parsed.groupBy) {
      projections.get(groupColumn.alias)?.add(groupColumn.column);
    }

    for (const metric of parsed.aggregateMetrics) {
      if (metric.column) {
        projections.get(metric.column.alias)?.add(metric.column.column);
      }
    }

    for (const windowFunction of parsed.windowFunctions) {
      for (const partition of windowFunction.partitionBy) {
        projections.get(partition.alias)?.add(partition.column);
      }
      for (const orderTerm of windowFunction.orderBy) {
        projections.get(orderTerm.alias)?.add(orderTerm.column);
      }
      if (windowFunction.column) {
        projections.get(windowFunction.column.alias)?.add(windowFunction.column.column);
      }
    }
  }

  for (const join of parsed.joinEdges) {
    projections.get(join.leftAlias)?.add(join.leftColumn);
    projections.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const filter of parsed.filters) {
    projections.get(filter.alias)?.add(filter.clause.column);
  }

  for (const reference of parsed.whereColumns) {
    projections.get(reference.alias)?.add(reference.column);
  }

  for (const term of parsed.orderBy) {
    if (term.kind === "source") {
      projections.get(term.alias)?.add(term.column);
    }
  }

  for (const [alias, columns] of projections) {
    if (columns.size > 0) {
      continue;
    }

    const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
    if (!binding) {
      continue;
    }

    const bindingColumns = getBindingColumns(binding, schema, cteRows);
    const firstColumn = bindingColumns[0];
    if (firstColumn) {
      columns.add(firstColumn);
    }
  }

  return projections;
}

function getBindingColumns(
  binding: TableBinding,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): string[] {
  if (binding.isCte) {
    const rows = cteRows.get(binding.table) ?? [];
    const first = rows[0];
    return first ? Object.keys(first) : [];
  }

  return Object.keys(getTable(schema, binding.table).columns);
}

function groupFiltersByAlias(filters: LiteralFilter[]): Map<string, ScanFilterClause[]> {
  const grouped = new Map<string, ScanFilterClause[]>();
  for (const filter of filters) {
    const existing = grouped.get(filter.alias) ?? [];
    existing.push(filter.clause);
    grouped.set(filter.alias, existing);
  }
  return grouped;
}

function buildExecutionOrder(
  bindings: TableBinding[],
  joinEdges: JoinCondition[],
  filtersByAlias: Map<string, ScanFilterClause[]>,
): string[] {
  const score = new Map<string, number>();
  for (const binding of bindings) {
    score.set(binding.alias, filtersByAlias.get(binding.alias)?.length ?? 0);
  }

  const unvisited = new Set(bindings.map((binding) => binding.alias));
  const visited = new Set<string>();
  const order: string[] = [];

  while (unvisited.size > 0) {
    const candidates = [...unvisited].filter((alias) => {
      if (visited.size === 0) {
        return true;
      }
      return joinEdges.some(
        (edge) =>
          (edge.leftAlias === alias && visited.has(edge.rightAlias)) ||
          (edge.rightAlias === alias && visited.has(edge.leftAlias)),
      );
    });

    const pool = candidates.length > 0 ? candidates : [...unvisited];
    pool.sort((a, b) => {
      const aScore = score.get(a) ?? 0;
      const bScore = score.get(b) ?? 0;
      if (aScore !== bScore) {
        return bScore - aScore;
      }

      const aIndex = bindings.find((binding) => binding.alias === a)?.index ?? 0;
      const bIndex = bindings.find((binding) => binding.alias === b)?.index ?? 0;
      return bIndex - aIndex;
    });

    const next = pool[0];
    if (!next) {
      break;
    }

    order.push(next);
    visited.add(next);
    unvisited.delete(next);
  }

  return order;
}

function buildDependencyFilters(
  alias: string,
  joins: ParsedJoin[],
  joinEdges: JoinCondition[],
  rowsByAlias: Map<string, QueryRow[]>,
): ScanFilterClause[] {
  const clauses: ScanFilterClause[] = [];
  for (const edge of joinEdges) {
    const join = joins.find(
      (candidate) =>
        (candidate.condition.leftAlias === edge.leftAlias &&
          candidate.condition.leftColumn === edge.leftColumn &&
          candidate.condition.rightAlias === edge.rightAlias &&
          candidate.condition.rightColumn === edge.rightColumn) ||
        (candidate.condition.leftAlias === edge.rightAlias &&
          candidate.condition.leftColumn === edge.rightColumn &&
          candidate.condition.rightAlias === edge.leftAlias &&
          candidate.condition.rightColumn === edge.leftColumn),
    );
    const preservedAliases = join ? getPreservedAliases(join) : [];

    if (edge.leftAlias === alias && rowsByAlias.has(edge.rightAlias)) {
      if (preservedAliases.includes(alias)) {
        continue;
      }
      clauses.push({
        op: "in",
        column: edge.leftColumn,
        values: uniqueValues(rowsByAlias.get(edge.rightAlias) ?? [], edge.rightColumn) as unknown[],
      } as ScanFilterClause);
      continue;
    }

    if (edge.rightAlias === alias && rowsByAlias.has(edge.leftAlias)) {
      if (preservedAliases.includes(alias)) {
        continue;
      }
      clauses.push({
        op: "in",
        column: edge.rightColumn,
        values: uniqueValues(rowsByAlias.get(edge.leftAlias) ?? [], edge.leftColumn) as unknown[],
      } as ScanFilterClause);
    }
  }

  return dedupeInClauses(clauses);
}

function buildAliasPrerequisites(joins: ParsedJoin[]): Map<string, string[]> {
  const prerequisites = new Map<string, Set<string>>();

  for (const join of joins) {
    const existingAlias = getExistingJoinAlias(join);
    const preservedAliases = getPreservedAliases(join);

    if (join.join === "inner") {
      const set = prerequisites.get(join.alias) ?? new Set<string>();
      set.add(existingAlias);
      prerequisites.set(join.alias, set);
      continue;
    }

    if (join.join === "full") {
      continue;
    }

    if (!preservedAliases.includes(join.alias)) {
      const set = prerequisites.get(join.alias) ?? new Set<string>();
      set.add(existingAlias);
      prerequisites.set(join.alias, set);
      continue;
    }

    if (!preservedAliases.includes(existingAlias)) {
      const set = prerequisites.get(existingAlias) ?? new Set<string>();
      set.add(join.alias);
      prerequisites.set(existingAlias, set);
    }
  }

  return new Map([...prerequisites.entries()].map(([alias, values]) => [alias, [...values]]));
}

function getExistingJoinAlias(join: ParsedJoin): string {
  return join.condition.leftAlias === join.alias
    ? join.condition.rightAlias
    : join.condition.leftAlias;
}

function getJoinAliasColumn(join: ParsedJoin): string {
  return join.condition.leftAlias === join.alias
    ? join.condition.leftColumn
    : join.condition.rightColumn;
}

function getExistingJoinColumn(join: ParsedJoin): string {
  return join.condition.leftAlias === join.alias
    ? join.condition.rightColumn
    : join.condition.leftColumn;
}

function getPreservedAliases(join: ParsedJoin): string[] {
  const existingAlias = getExistingJoinAlias(join);
  switch (join.join) {
    case "left":
      return [existingAlias];
    case "right":
      return [join.alias];
    case "full":
      return [existingAlias, join.alias];
    default:
      return [];
  }
}

function initializeJoinedRows(
  rowsByAlias: Map<string, QueryRow[]>,
  baseAlias: string,
): Array<Record<string, QueryRow>> {
  const baseRows = rowsByAlias.get(baseAlias) ?? [];
  return baseRows.map((row) => ({
    [baseAlias]: row,
  }));
}

function applyInnerJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, QueryRow[]>();
  for (const row of rightRows) {
    const key = row[joinAliasColumn];
    if (key == null) {
      continue;
    }
    const bucket = index.get(key) ?? [];
    bucket.push(row);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    if (!leftRow) {
      continue;
    }

    const key = leftRow[existingColumn];
    if (key == null) {
      continue;
    }
    const matches = index.get(key) ?? [];
    for (const match of matches) {
      joined.push({
        ...bundle,
        [join.alias]: match,
      });
    }
  }

  return joined;
}

function applyLeftJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, QueryRow[]>();
  for (const row of rightRows) {
    const key = row[joinAliasColumn];
    const bucket = index.get(key) ?? [];
    bucket.push(row);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    if (!leftRow) {
      joined.push({
        ...bundle,
        [join.alias]: {},
      });
      continue;
    }

    const key = leftRow[existingColumn];
    const matches = key == null ? [] : (index.get(key) ?? []);
    if (matches.length === 0) {
      joined.push({
        ...bundle,
        [join.alias]: {},
      });
      continue;
    }

    for (const match of matches) {
      joined.push({
        ...bundle,
        [join.alias]: match,
      });
    }
  }

  return joined;
}

function applyRightJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, Array<Record<string, QueryRow>>>();
  for (const bundle of existing) {
    const row = bundle[existingAlias];
    if (!row) {
      continue;
    }

    const key = row[existingColumn];
    if (key == null) {
      continue;
    }

    const bucket = index.get(key) ?? [];
    bucket.push(bundle);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const rightRow of rightRows) {
    const key = rightRow[joinAliasColumn];
    const matches = key == null ? [] : (index.get(key) ?? []);
    if (matches.length === 0) {
      joined.push({
        [join.alias]: rightRow,
      });
      continue;
    }

    for (const bundle of matches) {
      joined.push({
        ...bundle,
        [join.alias]: rightRow,
      });
    }
  }

  return joined;
}

function applyFullJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, number[]>();
  rightRows.forEach((row, idx) => {
    const key = row[joinAliasColumn];
    if (key == null) {
      return;
    }

    const bucket = index.get(key) ?? [];
    bucket.push(idx);
    index.set(key, bucket);
  });

  const matchedRight = new Set<number>();
  const joined: Array<Record<string, QueryRow>> = [];

  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    const key = leftRow?.[existingColumn];
    const matchIndexes = key == null ? [] : (index.get(key) ?? []);

    if (matchIndexes.length === 0) {
      joined.push({
        ...bundle,
        [join.alias]: {},
      });
      continue;
    }

    for (const idx of matchIndexes) {
      matchedRight.add(idx);
      const rightRow = rightRows[idx];
      joined.push({
        ...bundle,
        [join.alias]: rightRow ?? {},
      });
    }
  }

  rightRows.forEach((rightRow, idx) => {
    if (matchedRight.has(idx)) {
      return;
    }

    joined.push({
      [join.alias]: rightRow,
    });
  });

  return joined;
}

function applyFinalSort(
  rows: Array<Record<string, QueryRow>>,
  orderBy: OrderColumn[],
): Array<Record<string, QueryRow>> {
  const sourceTerms = orderBy.filter((term): term is SourceOrderColumn => term.kind === "source");
  if (sourceTerms.length === 0) {
    return rows;
  }

  const sorted = [...rows];
  sorted.sort((left, right) => {
    for (const term of sourceTerms) {
      const leftValue = left[term.alias]?.[term.column] as
        | string
        | number
        | boolean
        | null
        | undefined;
      const rightValue = right[term.alias]?.[term.column] as
        | string
        | number
        | boolean
        | null
        | undefined;
      if (leftValue === rightValue) {
        continue;
      }

      const leftNorm = leftValue ?? null;
      const rightNorm = rightValue ?? null;
      const comparison = compareNullableValues(leftNorm, rightNorm);
      return term.direction === "asc" ? comparison : -comparison;
    }

    return 0;
  });

  return sorted;
}

function applyOutputSort(rows: QueryRow[], orderBy: OrderColumn[]): QueryRow[] {
  const sorted = [...rows];

  sorted.sort((left, right) => {
    for (const term of orderBy) {
      const key = term.kind === "output" ? term.output : term.column;
      const leftValue = left[key] as string | number | boolean | null | undefined;
      const rightValue = right[key] as string | number | boolean | null | undefined;

      if (leftValue === rightValue) {
        continue;
      }

      const comparison = compareNullableValues(leftValue ?? null, rightValue ?? null);
      return term.direction === "asc" ? comparison : -comparison;
    }

    return 0;
  });

  return sorted;
}

async function applyWhereFilter<TContext>(
  rows: JoinedRowBundle[],
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<JoinedRowBundle[]> {
  if (!parsed.where) {
    return rows;
  }

  const out: JoinedRowBundle[] = [];
  for (const bundle of rows) {
    const truth = await evaluatePredicateTruth(parsed.where, {
      parsed,
      input,
      cteRows,
      options,
      bundle,
    });
    if (truth === true) {
      out.push(bundle);
    }
  }

  return out;
}

async function applyHavingFilter<TContext>(
  rows: QueryRow[],
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  if (!parsed.having) {
    return rows;
  }

  const out: QueryRow[] = [];
  for (const row of rows) {
    const truth = await evaluatePredicateTruth(parsed.having, {
      parsed,
      input,
      cteRows,
      options,
      aggregateRow: row,
    });
    if (truth === true) {
      out.push(row);
    }
  }

  return out;
}

function applyProjectedSortLimit(rows: QueryRow[], parsed: ParsedSelectQuery): QueryRow[] {
  let out = rows;

  if (parsed.orderBy.length > 0) {
    const rootBinding = parsed.bindings[0];
    const mapped = parsed.orderBy.map((term) => {
      if (term.kind === "output") {
        return {
          key: term.output,
          direction: term.direction,
        };
      }

      const selected = parsed.selectColumns.find(
        (candidate) => candidate.alias === term.alias && candidate.column === term.column,
      );
      if (!selected) {
        if (parsed.selectAll && rootBinding && term.alias === rootBinding.alias) {
          return {
            key: term.column,
            direction: term.direction,
          };
        }
        throw new Error(
          `ORDER BY ${term.alias}.${term.column} must reference a selected output when post-projection ordering is required.`,
        );
      }

      return {
        key: selected.output,
        direction: term.direction,
      };
    });

    out = [...out].sort((left, right) => {
      for (const term of mapped) {
        const leftValue = left[term.key] as string | number | boolean | null | undefined;
        const rightValue = right[term.key] as string | number | boolean | null | undefined;
        const comparison = compareNullableValues(leftValue ?? null, rightValue ?? null);
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (parsed.offset != null) {
    out = out.slice(parsed.offset);
  }

  if (parsed.limit != null) {
    out = out.slice(0, parsed.limit);
  }

  return out;
}

interface PredicateEvalScope<TContext> {
  parsed: ParsedSelectQuery;
  input: QueryInput<TContext>;
  cteRows: Map<string, QueryRow[]>;
  options: ExecutionOptions;
  bundle?: JoinedRowBundle;
  aggregateRow?: QueryRow;
}

type SqlTruth = true | false | null;
const WINDOW_OUTPUT_ALIAS = "__window__";

async function applyWindowFunctions(
  rows: JoinedRowBundle[],
  parsed: ParsedSelectQuery,
): Promise<JoinedRowBundle[]> {
  if (parsed.windowFunctions.length === 0) {
    return rows;
  }

  const out = rows.map((bundle) => ({
    ...bundle,
    [WINDOW_OUTPUT_ALIAS]: {} as QueryRow,
  })) as JoinedRowBundle[];

  for (const windowSpec of parsed.windowFunctions) {
    const partitions = new Map<string, number[]>();

    for (let index = 0; index < out.length; index += 1) {
      const bundle = out[index];
      if (!bundle) {
        continue;
      }
      const key = JSON.stringify(
        windowSpec.partitionBy.map(
          (partition) => bundle[partition.alias]?.[partition.column] ?? null,
        ),
      );
      const entries = partitions.get(key) ?? [];
      entries.push(index);
      partitions.set(key, entries);
    }

    for (const partitionRows of partitions.values()) {
      const ordered = [...partitionRows];
      if (windowSpec.orderBy.length > 0) {
        ordered.sort((leftIndex, rightIndex) => {
          const left = out[leftIndex];
          const right = out[rightIndex];
          if (!left || !right) {
            return 0;
          }

          for (const term of windowSpec.orderBy) {
            const comparison = compareNullableValues(
              left[term.alias]?.[term.column] ?? null,
              right[term.alias]?.[term.column] ?? null,
            );
            if (comparison !== 0) {
              return term.direction === "asc" ? comparison : -comparison;
            }
          }
          return 0;
        });
      }

      switch (windowSpec.fn) {
        case "row_number":
          for (let index = 0; index < ordered.length; index += 1) {
            const rowIndex = ordered[index];
            if (rowIndex == null) {
              continue;
            }
            const row = out[rowIndex];
            if (!row) {
              continue;
            }
            (row[WINDOW_OUTPUT_ALIAS] as QueryRow)[windowSpec.output] = index + 1;
          }
          break;
        case "rank": {
          let rank = 1;
          for (let index = 0; index < ordered.length; index += 1) {
            if (
              index > 0 &&
              !isSameWindowPeer(out, ordered[index - 1], ordered[index], windowSpec)
            ) {
              rank = index + 1;
            }

            const rowIndex = ordered[index];
            if (rowIndex == null) {
              continue;
            }
            const row = out[rowIndex];
            if (!row) {
              continue;
            }
            (row[WINDOW_OUTPUT_ALIAS] as QueryRow)[windowSpec.output] = rank;
          }
          break;
        }
        case "dense_rank": {
          let denseRank = 1;
          for (let index = 0; index < ordered.length; index += 1) {
            if (
              index > 0 &&
              !isSameWindowPeer(out, ordered[index - 1], ordered[index], windowSpec)
            ) {
              denseRank += 1;
            }

            const rowIndex = ordered[index];
            if (rowIndex == null) {
              continue;
            }
            const row = out[rowIndex];
            if (!row) {
              continue;
            }
            (row[WINDOW_OUTPUT_ALIAS] as QueryRow)[windowSpec.output] = denseRank;
          }
          break;
        }
        case "lead":
        case "lag": {
          const offset = windowSpec.offset ?? 1;
          const defaultValue = windowSpec.defaultValue ?? null;
          for (let index = 0; index < ordered.length; index += 1) {
            const rowIndex = ordered[index];
            if (rowIndex == null) {
              continue;
            }
            const row = out[rowIndex];
            if (!row) {
              continue;
            }

            const targetIndex =
              windowSpec.fn === "lead" ? index + offset : index - offset;
            const targetRow = targetIndex >= 0 ? out[ordered[targetIndex] ?? -1] : undefined;
            const value =
              targetRow && windowSpec.column
                ? (targetRow[windowSpec.column.alias]?.[windowSpec.column.column] ?? null)
                : defaultValue;

            (row[WINDOW_OUTPUT_ALIAS] as QueryRow)[windowSpec.output] =
              value ?? defaultValue ?? null;
          }
          break;
        }
        default: {
          const isRunning =
            windowSpec.orderBy.length > 0 ||
            windowSpec.frameMode === "rows_unbounded_preceding_current_row";
          if (!isRunning) {
            const aggregateValue = computeWindowAggregate(out, ordered, windowSpec, ordered.length);
            for (const rowIndex of ordered) {
              if (rowIndex == null) {
                continue;
              }
              const row = out[rowIndex];
              if (!row) {
                continue;
              }
              (row[WINDOW_OUTPUT_ALIAS] as QueryRow)[windowSpec.output] = aggregateValue;
            }
            break;
          }

          for (let index = 0; index < ordered.length; index += 1) {
            const aggregateValue = computeWindowAggregate(out, ordered, windowSpec, index + 1);
            const rowIndex = ordered[index];
            if (rowIndex == null) {
              continue;
            }
            const row = out[rowIndex];
            if (!row) {
              continue;
            }
            (row[WINDOW_OUTPUT_ALIAS] as QueryRow)[windowSpec.output] = aggregateValue;
          }
          break;
        }
      }
    }
  }

  return out;
}

function isSameWindowPeer(
  rows: JoinedRowBundle[],
  leftIndex: number | undefined,
  rightIndex: number | undefined,
  spec: WindowFunctionSpec,
): boolean {
  if (leftIndex == null || rightIndex == null) {
    return false;
  }

  const left = rows[leftIndex];
  const right = rows[rightIndex];
  if (!left || !right) {
    return false;
  }

  return spec.orderBy.every(
    (term) =>
      compareNullableValues(
        left[term.alias]?.[term.column] ?? null,
        right[term.alias]?.[term.column] ?? null,
      ) === 0,
  );
}

function computeWindowAggregate(
  rows: JoinedRowBundle[],
  orderedIndices: number[],
  spec: WindowFunctionSpec,
  count: number,
): unknown {
  let numericSum = 0;
  let numericCount = 0;
  let hasValue = false;
  let minValue: unknown = null;
  let maxValue: unknown = null;
  let countValue = 0;

  for (let index = 0; index < count; index += 1) {
    const rowIndex = orderedIndices[index];
    if (rowIndex == null) {
      continue;
    }

    const row = rows[rowIndex];
    if (!row) {
      continue;
    }

    const value = spec.column ? (row[spec.column.alias]?.[spec.column.column] ?? null) : null;

    switch (spec.fn) {
      case "count":
        if (spec.countStar || value != null) {
          countValue += 1;
        }
        break;
      case "sum":
        if (value != null) {
          numericSum += toFiniteNumber(value, "SUM");
          hasValue = true;
        }
        break;
      case "avg":
        if (value != null) {
          numericSum += toFiniteNumber(value, "AVG");
          numericCount += 1;
          hasValue = true;
        }
        break;
      case "min":
        if (value != null) {
          if (!hasValue || compareNullableValues(value, minValue) < 0) {
            minValue = value;
          }
          hasValue = true;
        }
        break;
      case "max":
        if (value != null) {
          if (!hasValue || compareNullableValues(value, maxValue) > 0) {
            maxValue = value;
          }
          hasValue = true;
        }
        break;
      default:
        throw new Error(`Unsupported window aggregate function: ${spec.fn}`);
    }
  }

  switch (spec.fn) {
    case "count":
      return countValue;
    case "sum":
      return hasValue ? numericSum : null;
    case "avg":
      return numericCount > 0 ? numericSum / numericCount : null;
    case "min":
      return hasValue ? minValue : null;
    case "max":
      return hasValue ? maxValue : null;
    default:
      return null;
  }
}

async function evaluatePredicateTruth<TContext>(
  expr: unknown,
  scope: PredicateEvalScope<TContext>,
): Promise<SqlTruth> {
  if (!expr || typeof expr !== "object") {
    return null;
  }

  const node = expr as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown };
  };

  if (node.type === "function") {
    const fn = readFunctionName(node.name);
    const args = readFunctionArgs(node.args);

    if (fn === "NOT") {
      const arg = args[0];
      const truth = await evaluatePredicateTruth(arg, scope);
      if (truth === null) {
        return null;
      }
      return !truth;
    }

    if (fn === "EXISTS") {
      const subquery = toSubqueryAst(args[0]);
      if (!subquery) {
        throw new Error("EXISTS requires a subquery argument.");
      }

      const rows = await executeSubquery(subquery, scope);
      return rows.length > 0;
    }

    throw new Error(`Unsupported predicate function: ${fn}`);
  }

  if (node.type !== "binary_expr") {
    throw new Error("Only binary predicates are currently supported in WHERE/HAVING clauses.");
  }

  const operator = typeof node.operator === "string" ? node.operator.toUpperCase() : "";
  switch (operator) {
    case "AND": {
      const left = await evaluatePredicateTruth(node.left, scope);
      const right = await evaluatePredicateTruth(node.right, scope);
      if (left === false || right === false) {
        return false;
      }
      if (left === null || right === null) {
        return null;
      }
      return true;
    }
    case "OR": {
      const left = await evaluatePredicateTruth(node.left, scope);
      const right = await evaluatePredicateTruth(node.right, scope);
      if (left === true || right === true) {
        return true;
      }
      if (left === null || right === null) {
        return null;
      }
      return false;
    }
    case "IN": {
      const left = await evaluateExpressionValue(node.left, scope);
      if (left == null) {
        return null;
      }

      const candidates = await evaluateInCandidates(node.right, scope);
      let sawNull = false;
      for (const candidate of candidates) {
        if (candidate == null) {
          sawNull = true;
          continue;
        }
        if (candidate === left) {
          return true;
        }
      }

      return sawNull ? null : false;
    }
    case "IS":
    case "IS NOT": {
      const left = await evaluateExpressionValue(node.left, scope);
      const right = await evaluateExpressionValue(node.right, scope);
      const isNull = right == null;
      if (!isNull) {
        throw new Error("IS/IS NOT currently only support NULL checks.");
      }
      return operator === "IS" ? left == null : left != null;
    }
    case "=":
    case "!=":
    case "<>":
    case ">":
    case ">=":
    case "<":
    case "<=": {
      const left = await evaluateExpressionValue(node.left, scope);
      const right = await evaluateExpressionValue(node.right, scope);
      return compareSqlValues(left, right, operator);
    }
    case "BETWEEN": {
      const left = await evaluateExpressionValue(node.left, scope);
      const range = node.right as { value?: unknown } | undefined;
      const values = Array.isArray(range?.value) ? range.value : [];
      const low = await evaluateExpressionValue(values[0], scope);
      const high = await evaluateExpressionValue(values[1], scope);
      if (left == null || low == null || high == null) {
        return null;
      }
      return compareNonNull(left, low) >= 0 && compareNonNull(left, high) <= 0;
    }
    default:
      throw new Error(`Unsupported predicate operator: ${String(node.operator)}`);
  }
}

async function evaluateExpressionValue<TContext>(
  expr: unknown,
  scope: PredicateEvalScope<TContext>,
): Promise<unknown> {
  const literal = parseLiteral(expr);
  if (literal !== undefined) {
    return literal;
  }

  const columnRef = toRawColumnRef(expr);
  if (columnRef) {
    return evaluateColumnReference(columnRef, scope);
  }

  const aggregateExpr = expr as {
    type?: unknown;
    name?: unknown;
    args?: { expr?: unknown; distinct?: unknown };
  };
  if (aggregateExpr.type === "aggr_func") {
    if (!scope.aggregateRow) {
      throw new Error("Aggregate expressions are only valid in aggregate contexts.");
    }

    const metric = parseAggregateMetric(
      expr,
      undefined,
      scope.parsed.bindings,
      new Map(scope.parsed.bindings.map((binding) => [binding.alias, binding])),
      scope.input.schema,
    );
    if (!metric) {
      throw new Error("Unable to resolve aggregate expression.");
    }

    const existing = scope.parsed.aggregateMetrics.find(
      (candidate) => candidate.signature === metric.signature,
    );
    if (!existing) {
      throw new Error("HAVING references an aggregate that is not available.");
    }

    return scope.aggregateRow[existing.output] ?? null;
  }

  const subquery = toSubqueryAst(expr);
  if (subquery) {
    const rows = await executeSubquery(subquery, scope);
    if (rows.length === 0) {
      return null;
    }
    if (rows.length > 1) {
      throw new Error("Scalar subquery returned more than one row.");
    }

    const row = rows[0];
    if (!row) {
      return null;
    }

    const firstKey = Object.keys(row)[0];
    return firstKey ? (row[firstKey] ?? null) : null;
  }

  const functionExpr = expr as { type?: unknown; name?: unknown; args?: { value?: unknown } };
  if (functionExpr.type === "function") {
    const name = readFunctionName(functionExpr.name);
    if (name === "NOT" || name === "EXISTS") {
      const truth = await evaluatePredicateTruth(expr, scope);
      return truth === null ? null : truth;
    }
  }

  throw new Error("Unsupported expression.");
}

function evaluateColumnReference<TContext>(
  ref: { table: string | null; column: string },
  scope: PredicateEvalScope<TContext>,
): unknown {
  if (scope.aggregateRow) {
    if (!ref.table) {
      const direct = scope.aggregateRow[ref.column];
      if (direct !== undefined) {
        return direct ?? null;
      }

      const fromGroup = scope.parsed.aggregateOutputColumns.find(
        (column) => column.source.column === ref.column,
      );
      return fromGroup ? (scope.aggregateRow[fromGroup.output] ?? null) : null;
    }

    const fromGroup = scope.parsed.aggregateOutputColumns.find(
      (column) => column.source.alias === ref.table && column.source.column === ref.column,
    );
    return fromGroup ? (scope.aggregateRow[fromGroup.output] ?? null) : null;
  }

  const bundle = scope.bundle;
  if (!bundle) {
    return null;
  }

  if (ref.table) {
    return bundle[ref.table]?.[ref.column] ?? null;
  }

  if (scope.parsed.bindings.length === 1) {
    const alias = scope.parsed.bindings[0]?.alias;
    return alias ? (bundle[alias]?.[ref.column] ?? null) : null;
  }

  const matches = scope.parsed.bindings.filter(
    (binding) => bundle[binding.alias] && ref.column in (bundle[binding.alias] ?? {}),
  );
  if (matches.length === 1) {
    const alias = matches[0]?.alias;
    return alias ? (bundle[alias]?.[ref.column] ?? null) : null;
  }
  if (matches.length > 1) {
    throw new Error(`Ambiguous unqualified column reference: ${ref.column}`);
  }

  return null;
}

async function evaluateInCandidates<TContext>(
  raw: unknown,
  scope: PredicateEvalScope<TContext>,
): Promise<unknown[]> {
  const subquery = toSubqueryAst(raw);
  if (subquery) {
    const rows = await executeSubquery(subquery, scope);
    return rows.map((row) => {
      const key = Object.keys(row)[0];
      return key ? (row[key] ?? null) : null;
    });
  }

  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    throw new Error("IN predicates must use literal lists or subqueries.");
  }

  if (expr.value.length === 1) {
    const directSubquery = toSubqueryAst(expr.value[0]);
    if (directSubquery) {
      const rows = await executeSubquery(directSubquery, scope);
      return rows.map((row) => {
        const key = Object.keys(row)[0];
        return key ? (row[key] ?? null) : null;
      });
    }
  }

  const values: unknown[] = [];
  for (const item of expr.value) {
    values.push(await evaluateExpressionValue(item, scope));
  }
  return values;
}

async function executeSubquery<TContext>(
  subquery: SelectAst,
  scope: PredicateEvalScope<TContext>,
): Promise<QueryRow[]> {
  const key = subquery as object;
  const cache = scope.options.subqueryCache;
  const cached = cache?.get(key);
  if (cached) {
    return cached;
  }

  const pending = (async () => {
    try {
      return await executeSelectAst(subquery, scope.input, scope.cteRows, scope.options);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.startsWith("Unknown table alias:")) {
        throw new Error("Correlated subqueries are not yet supported.");
      }
      throw error;
    }
  })();

  if (cache) {
    cache.set(key, pending);
  }

  try {
    return await pending;
  } catch (error) {
    if (cache) {
      cache.delete(key);
    }
    throw error;
  }
}

function readFunctionName(raw: unknown): string {
  const node = raw as { name?: unknown } | undefined;
  const nameParts = Array.isArray(node?.name) ? node?.name : [];
  const first = nameParts[0] as { value?: unknown } | undefined;
  return typeof first?.value === "string" ? first.value.toUpperCase() : "";
}

function readFunctionArgs(raw: { value?: unknown } | undefined): unknown[] {
  const value = raw?.value;
  if (Array.isArray(value)) {
    return value;
  }
  return value != null ? [value] : [];
}

function compareSqlValues(left: unknown, right: unknown, operator: string): SqlTruth {
  if (left == null || right == null) {
    return null;
  }

  switch (operator) {
    case "=":
      return left === right;
    case "!=":
    case "<>":
      return left !== right;
    case ">":
      return compareNonNull(left, right) > 0;
    case ">=":
      return compareNonNull(left, right) >= 0;
    case "<":
      return compareNonNull(left, right) < 0;
    case "<=":
      return compareNonNull(left, right) <= 0;
    default:
      throw new Error(`Unsupported comparison operator: ${operator}`);
  }
}

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNum = Number(left);
    const rightNum = Number(right);
    return leftNum === rightNum ? 0 : leftNum < rightNum ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}

async function projectResultRows<TContext>(
  rows: JoinedRowBundle[],
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: ExecutionOptions,
): Promise<QueryRow[]> {
  if (parsed.selectAll) {
    const baseAlias = parsed.bindings[0]?.alias;
    if (!baseAlias) {
      return [];
    }

    return rows.map((row) => {
      const baseRow = row[baseAlias];
      return baseRow ? { ...baseRow } : {};
    });
  }

  return Promise.all(
    rows.map(async (bundle) => {
      const out: QueryRow = {};
      for (const item of parsed.selectColumns) {
        out[item.output] = bundle[item.alias]?.[item.column] ?? null;
      }
      for (const windowFunction of parsed.windowFunctions) {
        out[windowFunction.output] = bundle[WINDOW_OUTPUT_ALIAS]?.[windowFunction.output] ?? null;
      }
      for (const item of parsed.scalarSelectItems) {
        out[item.output] = await evaluateExpressionValue(item.expr, {
          parsed,
          input,
          cteRows,
          options,
          bundle,
        });
      }
      return out;
    }),
  );
}

function projectAggregateOutputRow(row: QueryRow, parsed: ParsedSelectQuery): QueryRow {
  const out: QueryRow = {};

  for (const column of parsed.aggregateOutputColumns) {
    out[column.output] = row[column.output] ?? null;
  }

  for (const metric of parsed.aggregateMetrics) {
    if (metric.hidden) {
      continue;
    }
    out[metric.output] = row[metric.output] ?? null;
  }

  return out;
}

function parseJoinCondition(
  raw: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): JoinCondition {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr?.type !== "binary_expr" || expr.operator !== "=") {
    throw new Error("Only equality join conditions are currently supported.");
  }

  const left = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const right = resolveColumnRef(expr.right, bindings, aliasToBinding);
  if (!left || !right) {
    throw new Error("JOIN conditions must compare two columns.");
  }

  return {
    leftAlias: left.alias,
    leftColumn: left.column,
    rightAlias: right.alias,
    rightColumn: right.column,
  };
}

function flattenConjunctiveWhere(where: unknown): unknown[] | null {
  if (!where) {
    return [];
  }

  const expr = where as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type === "binary_expr" && expr.operator === "AND") {
    const left = flattenConjunctiveWhere(expr.left);
    const right = flattenConjunctiveWhere(expr.right);
    if (!left || !right) {
      return null;
    }

    return [...left, ...right];
  }

  if (expr.type === "binary_expr" && expr.operator === "OR") {
    return null;
  }

  if (expr.type === "function") {
    return null;
  }

  return [expr];
}

function tryNormalizeBinaryOperator(raw: unknown): Exclude<ScanFilterClause["op"], never> | null {
  switch (raw) {
    case "=":
      return "eq";
    case "!=":
    case "<>":
      return "neq";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case "IN":
      return "in";
    case "IS":
      return "is_null";
    case "IS NOT":
      return "is_not_null";
    default:
      return null;
  }
}

function invertOperator(
  op: Exclude<ScanFilterClause["op"], "in" | "is_null" | "is_not_null">,
): Exclude<ScanFilterClause["op"], "in" | "is_null" | "is_not_null"> {
  switch (op) {
    case "eq":
      return "eq";
    case "neq":
      return "neq";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
    case "lt":
      return "gt";
    case "lte":
      return "gte";
  }
}

function toRawColumnRef(raw: unknown): { table: string | null; column: string } | undefined {
  const expr = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (expr?.type !== "column_ref") {
    return undefined;
  }

  if (typeof expr.column !== "string" || expr.column.length === 0) {
    return undefined;
  }

  const table = typeof expr.table === "string" && expr.table.length > 0 ? expr.table : null;
  return {
    table,
    column: expr.column,
  };
}

function resolveColumnRef(
  raw: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): { alias: string; column: string } | undefined {
  const rawRef = toRawColumnRef(raw);
  if (!rawRef) {
    return undefined;
  }

  if (rawRef.table) {
    if (!aliasToBinding.has(rawRef.table)) {
      throw new Error(`Unknown table alias: ${rawRef.table}`);
    }

    return {
      alias: rawRef.table,
      column: rawRef.column,
    };
  }

  if (bindings.length === 1) {
    return {
      alias: bindings[0]?.alias ?? "",
      column: rawRef.column,
    };
  }

  throw new Error(`Ambiguous unqualified column reference: ${rawRef.column}`);
}

function isStarColumn(raw: { expr?: unknown }): boolean {
  const expr = raw.expr as { type?: unknown; column?: unknown } | undefined;
  return expr?.type === "column_ref" && expr.column === "*";
}

function isStarExpr(raw: unknown): boolean {
  const expr = raw as { type?: unknown; value?: unknown } | undefined;
  return expr?.type === "star" && expr.value === "*";
}

function parseLiteral(raw: unknown): unknown | undefined {
  const expr = raw as { type?: unknown; value?: unknown };

  switch (expr?.type) {
    case "single_quote_string":
    case "double_quote_string":
    case "string":
      return String(expr.value ?? "");
    case "number": {
      const value = expr.value;
      if (typeof value === "number") {
        return value;
      }
      if (typeof value === "string") {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
      }
      return undefined;
    }
    case "bool":
      return Boolean(expr.value);
    case "null":
      return null;
    default:
      return undefined;
  }
}

function tryParseLiteralExpressionList(raw: unknown): unknown[] | undefined {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return undefined;
  }

  const values = expr.value.map((entry) => parseLiteral(entry));
  if (values.some((value) => value === undefined)) {
    return undefined;
  }

  return values;
}

function uniqueJoinEdges(edges: JoinCondition[]): JoinCondition[] {
  const seen = new Set<string>();
  const out: JoinCondition[] = [];

  for (const edge of edges) {
    const key = `${edge.leftAlias}.${edge.leftColumn}=${edge.rightAlias}.${edge.rightColumn}`;
    const reverseKey = `${edge.rightAlias}.${edge.rightColumn}=${edge.leftAlias}.${edge.leftColumn}`;
    if (seen.has(key) || seen.has(reverseKey)) {
      continue;
    }
    seen.add(key);
    out.push(edge);
  }

  return out;
}

function uniqueValues(rows: QueryRow[], column: string): unknown[] {
  const seen = new Set<unknown>();
  const out: unknown[] = [];
  for (const row of rows) {
    const value = row[column] ?? null;
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    out.push(value);
  }
  return out;
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  return leftString < rightString ? -1 : 1;
}

function dedupeInClauses(clauses: ScanFilterClause[]): ScanFilterClause[] {
  const out: ScanFilterClause[] = [];
  const seen = new Set<string>();

  for (const clause of clauses) {
    if (clause.op !== "in") {
      out.push(clause);
      continue;
    }

    const key = `${clause.column}:${JSON.stringify(clause.values)}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    out.push(clause);
  }

  return out;
}

function cloneSelectWithoutSetOperation(ast: SelectAst): SelectAst {
  const clone = {
    ...ast,
  };
  delete clone.set_op;
  delete clone._next;
  return clone;
}

function readSetOperationNext(raw: unknown): SelectAst | undefined {
  if (!raw || typeof raw !== "object") {
    return undefined;
  }

  const next = raw as SelectAst;
  return next.type === "select" ? next : undefined;
}

function dedupeRows(rows: QueryRow[]): QueryRow[] {
  const seen = new Set<string>();
  const out: QueryRow[] = [];
  for (const row of rows) {
    const key = rowSignature(row);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(row);
  }
  return out;
}

function intersectRows(left: QueryRow[], right: QueryRow[]): QueryRow[] {
  const rightSet = new Set(right.map((row) => rowSignature(row)));
  return dedupeRows(left).filter((row) => rightSet.has(rowSignature(row)));
}

function exceptRows(left: QueryRow[], right: QueryRow[]): QueryRow[] {
  const rightSet = new Set(right.map((row) => rowSignature(row)));
  return dedupeRows(left).filter((row) => !rightSet.has(rowSignature(row)));
}

function rowSignature(row: QueryRow): string {
  const keys = Object.keys(row).sort();
  const payload = keys.map((key) => [key, row[key] ?? null]);
  return JSON.stringify(payload);
}

function toSubqueryAst(raw: unknown): SelectAst | undefined {
  if (!raw || typeof raw !== "object") {
    return undefined;
  }

  const wrapped = raw as { ast?: unknown; type?: unknown };
  if (wrapped.type === "select") {
    return wrapped as SelectAst;
  }

  if (!wrapped.ast || typeof wrapped.ast !== "object") {
    return undefined;
  }

  const ast = wrapped.ast as SelectAst;
  return ast.type === "select" ? ast : undefined;
}

function sourceColumnKey(alias: string, column: string): string {
  return `${alias}.${column}`;
}

function validateRowsForBinding<TContext>(
  tableName: string,
  rows: QueryRow[],
  input: QueryInput<TContext>,
): void {
  const payload = {
    schema: input.schema,
    tableName,
    rows,
  } as const;

  if (input.constraintValidation) {
    validateTableConstraintRows({
      ...payload,
      options: input.constraintValidation,
    });
    return;
  }

  validateTableConstraintRows(payload);
}

function normalizeRowsForBinding(
  tableName: string,
  rows: QueryRow[],
  schema: SchemaDefinition,
): QueryRow[] {
  const table = schema.tables[tableName];
  if (!table) {
    return rows;
  }

  const timestampColumns = Object.entries(table.columns)
    .filter(([, definition]) => resolveColumnType(definition) === "timestamp")
    .map(([column]) => column);

  if (timestampColumns.length === 0) {
    return rows;
  }

  return rows.map((row) => {
    let changed = false;
    const next: QueryRow = { ...row };

    for (const column of timestampColumns) {
      const value = next[column];
      if (value instanceof Date) {
        next[column] = value.toISOString();
        changed = true;
      }
    }

    return changed ? next : row;
  });
}

function collectCteDependencies(ast: SelectAst, cteNames: Set<string>, selfName: string): string[] {
  const out = new Set<string>();

  const visit = (node: unknown): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    const entry = node as {
      from?: unknown;
      table?: unknown;
      stmt?: { ast?: unknown };
      with?: unknown;
      _next?: unknown;
      left?: unknown;
      right?: unknown;
      args?: { value?: unknown };
      expr?: unknown;
      value?: unknown;
    };

    if (typeof entry.table === "string" && cteNames.has(entry.table) && entry.table !== selfName) {
      out.add(entry.table);
    }

    if (entry.from) {
      visit(entry.from);
    }

    if (entry.stmt?.ast) {
      visit(entry.stmt.ast);
    }

    if (entry.with) {
      visit(entry.with);
    }

    if (entry._next) {
      visit(entry._next);
    }

    if (entry.left) {
      visit(entry.left);
    }

    if (entry.right) {
      visit(entry.right);
    }

    if (entry.expr) {
      visit(entry.expr);
    }

    if (entry.args?.value) {
      visit(entry.args.value);
    }

    if (entry.value) {
      visit(entry.value);
    }

    for (const value of Object.values(entry)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return [...out];
}

function astifySingleSelect(sql: string): SelectAst {
  const astRaw = defaultSqlAstParser.astify(sql);
  if (Array.isArray(astRaw)) {
    throw new Error("Only a single SQL statement is supported.");
  }

  if (!astRaw || typeof astRaw !== "object") {
    throw new Error("Unable to parse SQL statement.");
  }

  return astRaw as SelectAst;
}

function readCteName(rawCte: unknown): string {
  const nameNode = (rawCte as { name?: { value?: unknown } | unknown }).name;

  if (typeof nameNode === "string" && nameNode.length > 0) {
    return nameNode;
  }

  if (
    nameNode &&
    typeof nameNode === "object" &&
    "value" in nameNode &&
    typeof (nameNode as { value?: unknown }).value === "string"
  ) {
    return (nameNode as { value: string }).value;
  }

  throw new Error("Unable to parse CTE name.");
}
