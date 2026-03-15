import { describe, expect, it } from "vitest";

import * as foundation from "@tupl/foundation";
import * as providerKit from "@tupl/provider-kit";
import * as providerKitRelationalSql from "@tupl/provider-kit/relational-sql";
import * as providerKitShapes from "@tupl/provider-kit/shapes";
import * as providerKitTesting from "@tupl/provider-kit/testing";
import * as providerDrizzle from "@tupl/provider-drizzle";
import * as providerIoredis from "@tupl/provider-ioredis";
import * as providerKysely from "@tupl/provider-kysely";
import * as providerObjection from "@tupl/provider-objection";
import * as planner from "@tupl/planner";
import * as runtime from "@tupl/runtime";
import * as runtimeExecutor from "@tupl/runtime/executor";
import * as runtimeSession from "@tupl/runtime/session";
import * as schema from "@tupl/schema";
import * as schemaModel from "@tupl/schema-model";
import * as schemaModelDefinition from "@tupl/schema-model/definition";
import * as schemaModelEnums from "@tupl/schema-model/enums";
import * as schemaModelMapping from "@tupl/schema-model/mapping";
import * as schemaModelNormalized from "@tupl/schema-model/normalized";
import * as schemaModelNormalization from "@tupl/schema-model/normalization";
import * as schemaModelPlanning from "@tupl/schema-model/planning";
import type {
  QueryRow as ProviderQueryRow,
  RelationalProviderOptions as ProviderRelationalProviderOptions,
  RelationalProviderCapabilityContext as ProviderRelationalProviderCapabilityContext,
  RelationalProviderEntityConfig as ProviderRelationalProviderEntityConfig,
  RelationalProviderRelCompileStrategy as ProviderRelationalProviderRelCompileStrategy,
  ScanFilterClause as ProviderScanFilterClause,
  ScanOrderBy as ProviderScanOrderBy,
  TableAggregateMetric as ProviderTableAggregateMetric,
  TableAggregateRequest as ProviderTableAggregateRequest,
  TableLookupRequest as ProviderTableLookupRequest,
  TableScanRequest as ProviderTableScanRequest,
} from "@tupl/provider-kit";
import type { NormalizedColumnBinding as SchemaModelNormalizedColumnBinding } from "@tupl/schema-model/normalized";
import type { PlannedScanRequest as SchemaModelPlannedScanRequest } from "@tupl/schema-model/planning";

declare const providerQueryRow: ProviderQueryRow;
declare const providerRelationalProviderOptions: ProviderRelationalProviderOptions<
  unknown,
  Record<string, ProviderRelationalProviderEntityConfig>,
  ProviderRelationalProviderRelCompileStrategy
>;
declare const providerRelationalProviderCapabilityContext: ProviderRelationalProviderCapabilityContext<
  unknown,
  Record<string, ProviderRelationalProviderEntityConfig>,
  ProviderRelationalProviderRelCompileStrategy
>;
declare const providerScanFilter: ProviderScanFilterClause;
declare const providerScanOrderBy: ProviderScanOrderBy;
declare const providerTableScanRequest: ProviderTableScanRequest;
declare const providerTableLookupRequest: ProviderTableLookupRequest;
declare const providerTableAggregateMetric: ProviderTableAggregateMetric;
declare const providerTableAggregateRequest: ProviderTableAggregateRequest;

void providerQueryRow;
void providerRelationalProviderOptions;
void providerRelationalProviderCapabilityContext;
void providerScanFilter;
void providerScanOrderBy;
void providerTableScanRequest;
void providerTableLookupRequest;
void providerTableAggregateMetric;
void providerTableAggregateRequest;
declare const schemaModelNormalizedColumnBinding: SchemaModelNormalizedColumnBinding;
declare const schemaModelPlannedScanRequest: SchemaModelPlannedScanRequest;

void schemaModelNormalizedColumnBinding;
void schemaModelPlannedScanRequest;

describe("public package imports", () => {
  it("exposes the canonical schema surface", () => {
    expect(typeof schema.createSchemaBuilder).toBe("function");
    expect(typeof schema.createExecutableSchema).toBe("function");
    expect("resolveTableProviderResult" in schema).toBe(false);
    expect("resolveTableColumnDefinition" in schema).toBe(false);
    expect("getNormalizedTableBinding" in schema).toBe(false);
    expect("isNormalizedSourceColumnBinding" in schema).toBe(false);
    expect("mapProviderRowsToLogical" in schema).toBe(false);
    expect("resolveSchemaLinkedEnums" in schema).toBe(false);
    expect("validateProviderBindingsResult" in schema).toBe(false);
    expect("QueryExecutionPlan" in schema).toBe(false);
    expect("QueryExecutionPlanScope" in schema).toBe(false);
    expect("QueryExecutionPlanStep" in schema).toBe(false);
    expect("QuerySession" in schema).toBe(false);
    expect("QuerySessionOptions" in schema).toBe(false);
    expect("QueryStepEvent" in schema).toBe(false);
    expect("QueryStepState" in schema).toBe(false);
    expect("validateTableConstraintRows" in schema).toBe(false);
    expect("PlannedScanRequest" in schema).toBe(false);
    expect("PlannedLookupRequest" in schema).toBe(false);
    expect("PlannedAggregateRequest" in schema).toBe(false);
    expect("ScanPlanDecision" in schema).toBe(false);
    expect("LookupPlanDecision" in schema).toBe(false);
    expect("AggregatePlanDecision" in schema).toBe(false);
  });

  it("keeps schema-model root focused on schema authoring contracts", () => {
    expect(typeof schemaModel.createSchemaBuilder).toBe("function");
    expect(typeof schemaModel.defineTableMethods).toBe("function");
    expect(typeof schemaModel.toSqlDDL).toBe("function");
    expect("getNormalizedTableBinding" in schemaModel).toBe(false);
    expect("NormalizedColumnBinding" in schemaModel).toBe(false);
    expect("NormalizedTableBinding" in schemaModel).toBe(false);
    expect("TableName" in schemaModel).toBe(false);
    expect("PlannedScanRequest" in schemaModel).toBe(false);
    expect("PlannedLookupRequest" in schemaModel).toBe(false);
    expect("PlannedAggregateRequest" in schemaModel).toBe(false);
    expect("ScanPlanDecision" in schemaModel).toBe(false);
    expect("LookupPlanDecision" in schemaModel).toBe(false);
    expect("AggregatePlanDecision" in schemaModel).toBe(false);
    expect("mapProviderRowsToLogical" in schemaModel).toBe(false);
    expect("resolveSchemaLinkedEnums" in schemaModel).toBe(false);
    expect("resolveTableColumnDefinition" in schemaModel).toBe(false);
  });

  it("exposes adapter-authoring contracts from provider-kit", () => {
    expect(typeof providerKit.createDataEntityHandle).toBe("function");
    expect(typeof providerKit.createRelationalProviderAdapter).toBe("function");
    expect(typeof providerKit.createSqlRelationalProviderAdapter).toBe("function");
    expect("UnsupportedSqlRelationalPlanError" in providerKit).toBe(false);
    expect(typeof providerKit.AdapterResult.ok).toBe("function");
  });

  it("resolves canonical public subpaths directly", () => {
    expect(typeof providerKitShapes.buildScanUnsupportedReport).toBe("function");
    expect(typeof providerKitRelationalSql.UnsupportedSqlRelationalPlanError).toBe("function");
    expect("createSqlRelationalProviderAdapter" in providerKitRelationalSql).toBe(false);
    expect("SqlRelationalProviderOptions" in providerKitRelationalSql).toBe(false);
    expect("hasSqlNode" in providerKitShapes).toBe(false);
    expect(typeof runtimeExecutor.executeRelWithProvidersResult).toBe("function");
    expect(typeof runtimeSession.createExecutableSchemaSession).toBe("function");
    expect(typeof providerKitTesting.createProviderConformanceCases).toBe("function");
    expect(typeof schemaModelNormalization.getNormalizedTableBinding).toBe("function");
    expect("createSchemaBuilder" in schemaModelNormalized).toBe(false);
    expect("PlannedScanRequest" in schemaModelNormalized).toBe(false);
    expect("QueryRow" in schemaModelNormalized).toBe(false);
    expect("createSchemaBuilder" in schemaModelPlanning).toBe(false);
    expect("NormalizedColumnBinding" in schemaModelPlanning).toBe(false);
    expect("QueryRow" in schemaModelPlanning).toBe(false);
    expect(typeof schemaModelMapping.mapProviderRowsToLogical).toBe("function");
    expect(typeof schemaModelEnums.resolveSchemaLinkedEnums).toBe("function");
    expect(typeof schemaModelDefinition.resolveTableColumnDefinition).toBe("function");
  });

  it("keeps session observation off the runtime root surface", () => {
    expect("QueryExecutionPlan" in runtime).toBe(false);
    expect("QueryExecutionPlanScope" in runtime).toBe(false);
    expect("QueryExecutionPlanStep" in runtime).toBe(false);
    expect("QuerySession" in runtime).toBe(false);
    expect("QuerySessionOptions" in runtime).toBe(false);
    expect("QueryStepEvent" in runtime).toBe(false);
    expect("QueryStepState" in runtime).toBe(false);
  });

  it("keeps the planner root surface stable", () => {
    expect(typeof planner.lowerSqlToRelResult).toBe("function");
    expect(typeof planner.expandRelViewsResult).toBe("function");
    expect(typeof planner.planPhysicalQueryResult).toBe("function");
    expect(typeof planner.buildLogicalQueryPlanResult).toBe("function");
    expect(typeof planner.buildPhysicalQueryPlanResult).toBe("function");
  });

  it("resolves the Drizzle provider package", () => {
    expect(typeof providerDrizzle.createDrizzleProvider).toBe("function");
  });

  it("resolves the ioredis provider package", () => {
    expect(typeof providerIoredis.createIoredisProvider).toBe("function");
  });

  it("resolves the Kysely provider package", () => {
    expect(typeof providerKysely.createKyselyProvider).toBe("function");
  });

  it("resolves the Objection provider package", () => {
    expect(typeof providerObjection.createObjectionProvider).toBe("function");
  });

  it("keeps foundational helpers available to adapter implementations", () => {
    expect(typeof foundation.stringifyUnknownValue).toBe("function");
  });
});
