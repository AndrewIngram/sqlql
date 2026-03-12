import type { Result as BetterResult } from "better-result";

import { createExecutableSchemaResult, type ExecutableSchema, type TuplResult } from "@tupl/schema";
import { AdapterResult, type ProviderOperationResult } from "@tupl/provider-kit";
import type { TuplProviderBindingError } from "@tupl/runtime";
import { createExecutableSchemaSessionResult, type QuerySession } from "@tupl/runtime/session";
import { createSchemaBuilder, type QueryRow, type SchemaDefinition } from "@tupl/schema";
import { resolveTableProviderResult } from "@tupl/schema-model";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

const builder = createSchemaBuilder<Record<string, never>>();
const createExecutableSchemaResultValue = createExecutableSchemaResult(builder);

type _createExecutableSchemaResultStaysExplicit = Expect<
  Equal<
    typeof createExecutableSchemaResultValue,
    TuplResult<ExecutableSchema<Record<string, never>, SchemaDefinition>>
  >
>;

declare const executableSchema: ExecutableSchema<Record<string, never>, SchemaDefinition>;
declare const sessionInput: {
  context: Record<string, never>;
  sql: string;
};

type _queryResultStaysExplicit = Expect<
  Equal<ReturnType<typeof executableSchema.queryResult>, Promise<TuplResult<QueryRow[]>>>
>;

const createExecutableSchemaSessionResultValue = createExecutableSchemaSessionResult(
  executableSchema,
  sessionInput,
);

type _createSessionResultStaysExplicit = Expect<
  Equal<typeof createExecutableSchemaSessionResultValue, TuplResult<QuerySession>>
>;

declare const resolveTableProviderResultValue: ReturnType<typeof resolveTableProviderResult>;
const _resolveTableProviderResultNarrows: BetterResult<string, TuplProviderBindingError> =
  resolveTableProviderResultValue;

const adapterResultOkValue = AdapterResult.ok(1);

const _adapterResultSurfaceMatchesProviderOperationResult: ProviderOperationResult<number, never> =
  adapterResultOkValue;
