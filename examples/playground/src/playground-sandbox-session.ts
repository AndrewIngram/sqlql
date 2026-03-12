import type {
  QueryExecutionPlan,
  QuerySession,
  QueryStepEvent,
  QueryStepState,
} from "@tupl/runtime/session";
import type { QueryRow } from "@tupl/schema";

import { requestSandboxWorker } from "./playground-sandbox-client";

const SANDBOX_SESSION_PROXY = Symbol("playgroundSandboxSessionProxy");

interface SandboxSessionState {
  sessionId: string;
  plan: QueryExecutionPlan;
  stepStates: Map<string, QueryStepState>;
  result: QueryRow[] | null;
  done: boolean;
  events: QueryStepEvent[];
}

type SandboxQuerySession = QuerySession & {
  [SANDBOX_SESSION_PROXY]: SandboxSessionState;
};

function createInitialStepStates(plan: QueryExecutionPlan): Map<string, QueryStepState> {
  return new Map(
    plan.steps.map((step) => [
      step.id,
      {
        id: step.id,
        kind: step.kind,
        status: "ready",
        summary: step.summary,
        dependsOn: step.dependsOn,
        ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
      } satisfies QueryStepState,
    ]),
  );
}

function applyStepEvent(state: SandboxSessionState, event: QueryStepEvent): void {
  state.events.push(event);
  state.stepStates.set(event.id, {
    id: event.id,
    kind: event.kind,
    status: event.status === "failed" ? "failed" : "done",
    summary: event.summary,
    dependsOn: event.dependsOn,
    executionIndex: event.executionIndex,
    startedAt: event.startedAt,
    endedAt: event.endedAt,
    durationMs: event.durationMs,
    ...(typeof event.rowCount === "number" ? { rowCount: event.rowCount } : {}),
    ...(typeof event.inputRowCount === "number" ? { inputRowCount: event.inputRowCount } : {}),
    ...(typeof event.outputRowCount === "number" ? { outputRowCount: event.outputRowCount } : {}),
    ...(event.rows ? { rows: event.rows } : {}),
    ...(event.routeUsed ? { routeUsed: event.routeUsed } : {}),
    ...(event.notes ? { notes: event.notes } : {}),
    ...(event.error ? { error: event.error } : {}),
    ...(event.diagnostics ? { diagnostics: event.diagnostics } : {}),
  });
}

export function isSandboxQuerySession(session: QuerySession): session is SandboxQuerySession {
  return SANDBOX_SESSION_PROXY in session;
}

export function createSandboxQuerySession(
  sessionId: string,
  plan: QueryExecutionPlan,
  initialEvents: QueryStepEvent[] = [],
  initialResult: QueryRow[] | null = null,
  initialDone = false,
): QuerySession {
  const state: SandboxSessionState = {
    sessionId,
    plan,
    stepStates: createInitialStepStates(plan),
    result: initialResult,
    done: initialDone,
    events: [],
  };

  for (const event of initialEvents) {
    applyStepEvent(state, event);
  }

  const session: SandboxQuerySession = {
    [SANDBOX_SESSION_PROXY]: state,
    getPlan() {
      return state.plan;
    },
    async next() {
      if (state.done) {
        return {
          done: true,
          result: state.result ?? [],
        };
      }

      const next = await requestSandboxWorker("session_next", {
        sessionId: state.sessionId,
      });
      if ("done" in next) {
        state.done = true;
        state.result = next.result;
        return next;
      }

      applyStepEvent(state, next);
      return next;
    },
    async runToCompletion() {
      const snapshot = await requestSandboxWorker("session_run_to_completion", {
        sessionId: state.sessionId,
      });
      for (const event of snapshot.events) {
        applyStepEvent(state, event);
      }
      state.done = snapshot.done;
      state.result = snapshot.result;
      return snapshot.result ?? [];
    },
    getResult() {
      return state.result;
    },
    getStepState(stepId: string) {
      return state.stepStates.get(stepId);
    },
  };

  return session;
}

export function readSandboxSessionId(session: QuerySession): string {
  return (session as SandboxQuerySession)[SANDBOX_SESSION_PROXY].sessionId;
}

export function applySandboxCompletionSnapshot(
  session: QuerySession,
  snapshot: {
    events: QueryStepEvent[];
    result: QueryRow[] | null;
    done: boolean;
  },
): void {
  const state = (session as SandboxQuerySession)[SANDBOX_SESSION_PROXY];
  for (const event of snapshot.events) {
    applyStepEvent(state, event);
  }
  state.done = snapshot.done;
  state.result = snapshot.result;
}
