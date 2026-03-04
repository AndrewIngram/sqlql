/// <reference lib="webworker" />

import {
  evaluateSchemaCodeInProcess,
  type SchemaCodeEvaluationResult,
} from "./schema-code-runtime";

interface WorkerRequest {
  code: string;
  modules?: Record<string, string>;
}

declare const self: DedicatedWorkerGlobalScope;

self.onmessage = (event: MessageEvent<WorkerRequest>) => {
  const code = typeof event.data?.code === "string" ? event.data.code : "";
  const result: SchemaCodeEvaluationResult = evaluateSchemaCodeInProcess(code, event.data?.modules
    ? { modules: event.data.modules }
    : undefined);
  self.postMessage(result);
};

export {};
