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

type SandboxRpcRequest =
  {
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

async function dispatch(request: SandboxRpcRequest): Promise<SandboxRpcPayload> {
  switch (request.kind) {
    case "validate_schema":
      return validateSchemaInSandbox(request.payload.schemaCode, request.payload.options);
    case "create_session":
      return createSandboxSession(request.payload.compiled, request.payload.context, request.payload.options);
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
        payload,
      };
      self.postMessage(response);
    })
    .catch((error: unknown) => {
      const response: SandboxRpcResponse = {
        id: event.data.id,
        ok: false,
        error: error instanceof Error ? error.message : "Sandbox worker request failed.",
      };
      self.postMessage(response);
    });
};

export {};
