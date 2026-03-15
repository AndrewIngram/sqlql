import { buildScanUnsupportedReport } from "@tupl/provider-kit/shapes";
import { createSchemaBuilder, createExecutableSchema, type QueryGuardrails } from "@tupl/schema";
import { getNormalizedTableBinding } from "@tupl/schema-model/normalization";
import type {
  PlannedAggregateRequest,
  PlannedLookupRequest,
  PlannedScanRequest,
} from "@tupl/schema-model/planning";

// @ts-expect-error schema facade should not expose normalization helpers
import { getNormalizedTableBinding as _schemaFacadeBinding } from "@tupl/schema";

// @ts-expect-error schema facade should not expose low-level schema resolution helpers
import { resolveTableColumnDefinition as _schemaFacadeResolveTableColumnDefinition } from "@tupl/schema";

// @ts-expect-error schema facade should not expose planning split types
import type { PlannedScanRequest as _schemaFacadePlannedScanRequest } from "@tupl/schema";

// @ts-expect-error schema facade should not expose planning split types
import type { PlannedLookupRequest as _schemaFacadePlannedLookupRequest } from "@tupl/schema";

// @ts-expect-error schema facade should not expose planning split types
import type { PlannedAggregateRequest as _schemaFacadePlannedAggregateRequest } from "@tupl/schema";

// @ts-expect-error schema facade should not expose plan/session graph types
import type { QueryExecutionPlan as _schemaFacadeQueryExecutionPlan } from "@tupl/schema";

const builder = createSchemaBuilder<Record<string, never>>();
const executable = createExecutableSchema(builder);

const _schemaGuardrails: QueryGuardrails | undefined = undefined;
const _schemaModelScanPlan: PlannedScanRequest | undefined = undefined;
const _schemaModelLookupPlan: PlannedLookupRequest | undefined = undefined;
const _schemaModelAggregatePlan: PlannedAggregateRequest | undefined = undefined;
const _schemaBindingReader = getNormalizedTableBinding;
const _providerShapesSurface = buildScanUnsupportedReport;
const _executableRuntime = executable;

const facadeBoundaryCompileCheck = [
  _schemaGuardrails,
  _schemaModelScanPlan,
  _schemaModelLookupPlan,
  _schemaModelAggregatePlan,
  _schemaBindingReader,
  _providerShapesSurface,
  _executableRuntime,
] as const;

void facadeBoundaryCompileCheck;
