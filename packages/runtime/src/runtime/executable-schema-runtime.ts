import { Result } from "better-result";

import { TuplRuntimeError, type TuplResult } from "@tupl/foundation";

import type { ExecutableSchema } from "./contracts";
import type { ExecutableSchemaSessionInput, QuerySession } from "./session/contracts";

/**
 * Executable-schema runtime storage owns the private closure-based hooks advanced subpaths use to
 * access bound runtime behavior without widening the public executable-schema interface.
 */
export interface ExecutableSchemaSessionAccess<TContext> {
  createSessionResult(input: ExecutableSchemaSessionInput<TContext>): TuplResult<QuerySession>;
}

const EXECUTABLE_SCHEMA_SESSION_ACCESS = Symbol.for(
  "@tupl/runtime/executable-schema-session-access",
);

type ExecutableSchemaSessionCarrier<TContext> = ExecutableSchema<TContext> & {
  [EXECUTABLE_SCHEMA_SESSION_ACCESS]?: ExecutableSchemaSessionAccess<TContext>;
};

export function bindExecutableSchemaSessionAccess<TContext>(
  executableSchema: ExecutableSchema<TContext>,
  access: ExecutableSchemaSessionAccess<TContext>,
): void {
  Object.defineProperty(
    executableSchema as ExecutableSchemaSessionCarrier<TContext>,
    EXECUTABLE_SCHEMA_SESSION_ACCESS,
    {
      value: access,
      enumerable: false,
      configurable: false,
      writable: false,
    },
  );
}

export function readExecutableSchemaSessionAccessResult<TContext>(
  executableSchema: ExecutableSchema<TContext>,
): TuplResult<ExecutableSchemaSessionAccess<TContext>> {
  const access = (executableSchema as ExecutableSchemaSessionCarrier<TContext>)[
    EXECUTABLE_SCHEMA_SESSION_ACCESS
  ];
  if (!access) {
    return Result.err(
      new TuplRuntimeError({
        operation: "read executable schema session access",
        message:
          "Executable schema session access is missing. This usually means the schema object was not created by createExecutableSchema(...).",
      }),
    );
  }

  return Result.ok(access);
}
