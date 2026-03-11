import type { SandboxRpcRequestMap, SandboxRpcResponseMap } from "./playground-sandbox";
import {
  createSandboxSession,
  disposeSandboxSession,
  nextSandboxSessionEvent,
  replaySandboxSession,
  runSandboxSessionToCompletion,
  validateSchemaInSandbox,
} from "./playground-sandbox";

type SandboxRpcRequest = {
  [K in keyof SandboxRpcRequestMap]: {
    id: number;
    kind: K;
    payload: SandboxRpcRequestMap[K];
  };
}[keyof SandboxRpcRequestMap];

type SandboxRpcResponse =
  | {
      id: number;
      ok: true;
      payload: SandboxRpcResponseMap[keyof SandboxRpcResponseMap];
    }
  | {
      id: number;
      ok: false;
      error: string;
    };

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}

class SandboxWorkerTransportError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SandboxWorkerTransportError";
  }
}

let sandboxWorker: Worker | null = null;
let nextRequestId = 1;
const pendingRequests = new Map<number, PendingRequest>();
const SANDBOX_WORKER_ENABLED = true;

function resetSandboxWorker(): void {
  sandboxWorker?.terminate();
  sandboxWorker = null;
}

function rejectPendingRequests(error: Error): void {
  for (const request of pendingRequests.values()) {
    request.reject(error);
  }
  pendingRequests.clear();
}

function formatSandboxWorkerError(event: ErrorEvent): string {
  if (event.error instanceof Error && event.error.message.length > 0) {
    return event.error.message;
  }

  const location =
    typeof event.filename === "string" && event.filename.length > 0
      ? `${event.filename}${event.lineno ? `:${event.lineno}` : ""}${
          event.colno ? `:${event.colno}` : ""
        }`
      : null;
  const baseMessage = event.message || "Sandbox worker crashed.";
  return location ? `${baseMessage} (${location})` : baseMessage;
}

function getSandboxWorker(): Worker {
  sandboxWorker ??= new Worker(new URL("./playground-sandbox.worker.ts", import.meta.url), {
    type: "module",
  });

  sandboxWorker.onmessage = (event: MessageEvent<SandboxRpcResponse>) => {
    const request = pendingRequests.get(event.data.id);
    if (!request) {
      return;
    }

    pendingRequests.delete(event.data.id);
    if (event.data.ok) {
      request.resolve(event.data.payload);
      return;
    }

    console.error("[playground-sandbox] worker request failed", event.data.error);
    request.reject(new Error(event.data.error));
  };

  sandboxWorker.onerror = (event) => {
    rejectPendingRequests(new SandboxWorkerTransportError(formatSandboxWorkerError(event)));
    resetSandboxWorker();
  };

  sandboxWorker.onmessageerror = () => {
    rejectPendingRequests(
      new SandboxWorkerTransportError(
        "Sandbox worker returned a response that could not be transferred back to the app.",
      ),
    );
    resetSandboxWorker();
  };

  return sandboxWorker;
}

async function requestSandboxViaWorker<K extends keyof SandboxRpcRequestMap>(
  kind: K,
  payload: SandboxRpcRequestMap[K],
): Promise<SandboxRpcResponseMap[K]> {
  const worker = getSandboxWorker();
  const id = nextRequestId;
  nextRequestId += 1;

  const request = {
    id,
    kind,
    payload,
  } as SandboxRpcRequest;

  return new Promise<SandboxRpcResponseMap[K]>((resolve, reject) => {
    pendingRequests.set(id, {
      resolve,
      reject,
    });

    try {
      worker.postMessage(request);
    } catch (error) {
      pendingRequests.delete(id);
      reject(
        new SandboxWorkerTransportError(
          error instanceof Error ? error.message : "Sandbox worker request could not be sent.",
        ),
      );
    }
  });
}

async function requestSandboxInProcess<K extends keyof SandboxRpcRequestMap>(
  kind: K,
  payload: SandboxRpcRequestMap[K],
): Promise<SandboxRpcResponseMap[K]> {
  switch (kind) {
    case "validate_schema": {
      const input = payload as SandboxRpcRequestMap["validate_schema"];
      return validateSchemaInSandbox(input.schemaCode, input.options) as Promise<
        SandboxRpcResponseMap[K]
      >;
    }
    case "create_session": {
      const input = payload as SandboxRpcRequestMap["create_session"];
      return createSandboxSession(input.compiled, input.context, input.options) as Promise<
        SandboxRpcResponseMap[K]
      >;
    }
    case "session_next": {
      const input = payload as SandboxRpcRequestMap["session_next"];
      return nextSandboxSessionEvent(input.sessionId) as Promise<SandboxRpcResponseMap[K]>;
    }
    case "session_run_to_completion": {
      const input = payload as SandboxRpcRequestMap["session_run_to_completion"];
      return runSandboxSessionToCompletion(input.sessionId) as Promise<SandboxRpcResponseMap[K]>;
    }
    case "replay_session": {
      const input = payload as SandboxRpcRequestMap["replay_session"];
      return replaySandboxSession(
        input.compiled,
        input.context,
        input.eventCount,
        input.options,
      ) as Promise<SandboxRpcResponseMap[K]>;
    }
    case "dispose_session": {
      const input = payload as SandboxRpcRequestMap["dispose_session"];
      disposeSandboxSession(input.sessionId);
      return Promise.resolve(null as SandboxRpcResponseMap[K]);
    }
  }
}

export function requestSandboxWorker<K extends keyof SandboxRpcRequestMap>(
  kind: K,
  payload: SandboxRpcRequestMap[K],
): Promise<SandboxRpcResponseMap[K]> {
  if (!SANDBOX_WORKER_ENABLED || typeof Worker === "undefined") {
    return requestSandboxInProcess(kind, payload);
  }

  return requestSandboxViaWorker(kind, payload).catch(async (error: unknown) => {
    if (!(error instanceof SandboxWorkerTransportError)) {
      throw error;
    }

    resetSandboxWorker();
    return requestSandboxInProcess(kind, payload);
  });
}
