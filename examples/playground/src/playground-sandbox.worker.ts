/// <reference lib="webworker" />

import {
  createSandboxSession,
  disposeSandboxSession,
  nextSandboxSessionEvent,
  replaySandboxSession,
  runSandboxSessionToCompletion,
  validateSchemaInSandbox,
  type SandboxRpcRequestMap,
  type SandboxRpcResponseMap,
} from "./playground-sandbox";

type SandboxRpcRequest = {
  [K in keyof SandboxRpcRequestMap]: {
    id: number;
    kind: K;
    payload: SandboxRpcRequestMap[K];
  };
}[keyof SandboxRpcRequestMap];

type SandboxRpcPayload = SandboxRpcResponseMap[keyof SandboxRpcResponseMap];

type SandboxRpcResponse =
  | {
      id: number;
      ok: true;
      payload: SandboxRpcPayload;
    }
  | {
      id: number;
      ok: false;
      error: string;
    };

declare const self: DedicatedWorkerGlobalScope;

function formatWorkerError(error: unknown): string {
  if (error instanceof Error && error.message.length > 0) {
    return error.message;
  }

  if (
    error &&
    typeof error === "object" &&
    "message" in error &&
    typeof (error as { message?: unknown }).message === "string" &&
    (error as { message: string }).message.length > 0
  ) {
    return (error as { message: string }).message;
  }

  if (typeof error === "string" && error.length > 0) {
    return error;
  }

  return "Sandbox worker request failed.";
}

function createJsonSanitizer() {
  const seen = new WeakSet<object>();

  return (_key: string, value: unknown): unknown => {
    if (typeof value === "bigint") {
      return value.toString();
    }

    if (typeof value === "function") {
      return `[Function ${value.name || "anonymous"}]`;
    }

    if (typeof value === "symbol") {
      return String(value);
    }

    if (value instanceof Error) {
      return {
        name: value.name,
        message: value.message,
        ...(value.stack ? { stack: value.stack } : {}),
      };
    }

    if (value instanceof Map) {
      return Object.fromEntries(value.entries());
    }

    if (value instanceof Set) {
      return [...value];
    }

    if (value && typeof value === "object") {
      if (seen.has(value)) {
        return "[Circular]";
      }
      seen.add(value);
    }

    return value;
  };
}

function toCloneablePayload<T>(value: T): T {
  try {
    return structuredClone(value);
  } catch {
    return JSON.parse(JSON.stringify(value, createJsonSanitizer())) as T;
  }
}

async function dispatch(request: SandboxRpcRequest): Promise<SandboxRpcPayload> {
  switch (request.kind) {
    case "validate_schema":
      return validateSchemaInSandbox(request.payload.schemaCode, request.payload.options);
    case "create_session":
      return createSandboxSession(
        request.payload.compiled,
        request.payload.context,
        request.payload.options,
      );
    case "session_next":
      return nextSandboxSessionEvent(request.payload.sessionId);
    case "session_run_to_completion":
      return runSandboxSessionToCompletion(request.payload.sessionId);
    case "replay_session":
      return replaySandboxSession(
        request.payload.compiled,
        request.payload.context,
        request.payload.eventCount,
        request.payload.options,
      );
    case "dispose_session":
      disposeSandboxSession(request.payload.sessionId);
      return null;
  }
}

self.onmessage = (event: MessageEvent<SandboxRpcRequest>) => {
  void dispatch(event.data)
    .then((payload) => {
      const response: SandboxRpcResponse = {
        id: event.data.id,
        ok: true,
        payload: toCloneablePayload(payload),
      };
      self.postMessage(response);
    })
    .catch((error: unknown) => {
      const response: SandboxRpcResponse = {
        id: event.data.id,
        ok: false,
        error: formatWorkerError(error),
      };
      self.postMessage(response);
    });
};

export {};
