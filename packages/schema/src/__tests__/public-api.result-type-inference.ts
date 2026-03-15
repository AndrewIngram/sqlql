import type { Result as BetterResult } from "better-result";

import { createExecutableSchema, type ExecutableSchema, type TuplResult } from "@tupl/schema";
import { AdapterResult, type ProviderOperationResult } from "@tupl/provider-kit";
import type { TuplProviderBindingError } from "@tupl/runtime";
import { createExecutableSchemaSession, type QuerySession } from "@tupl/runtime/session";
import { createSchemaBuilder, type QueryRow, type SchemaDefinition } from "@tupl/schema";
import { resolveTableProvider } from "@tupl/schema-model/normalization";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

const builder = createSchemaBuilder<Record<string, never>>();
const createExecutableSchemaResultValue = createExecutableSchema(builder);

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
  Equal<ReturnType<typeof executableSchema.query>, Promise<TuplResult<QueryRow[]>>>
>;

const createExecutableSchemaSessionResultValue = createExecutableSchemaSession(
  executableSchema,
  sessionInput,
);

type _createSessionResultStaysExplicit = Expect<
  Equal<typeof createExecutableSchemaSessionResultValue, TuplResult<QuerySession>>
>;

declare const resolveTableProviderResultValue: ReturnType<typeof resolveTableProvider>;
const _resolveTableProviderResultNarrows: BetterResult<string, TuplProviderBindingError> =
  resolveTableProviderResultValue;

const adapterResultOkValue = AdapterResult.ok(1);

const _adapterResultSurfaceMatchesProviderOperationResult: ProviderOperationResult<number, never> =
  adapterResultOkValue;
