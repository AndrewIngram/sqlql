import { describe, expect, it } from "vitest";

import * as foundation from "@tupl/foundation";
import * as providerKit from "@tupl/provider-kit";
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
import type {
  QueryRow as ProviderQueryRow,
  RelationalProviderAdapterOptions as ProviderRelationalProviderAdapterOptions,
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

declare const providerQueryRow: ProviderQueryRow;
declare const providerRelationalProviderAdapterOptions: ProviderRelationalProviderAdapterOptions<
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
void providerRelationalProviderAdapterOptions;
void providerRelationalProviderCapabilityContext;
void providerScanFilter;
void providerScanOrderBy;
void providerTableScanRequest;
void providerTableLookupRequest;
void providerTableAggregateMetric;
void providerTableAggregateRequest;

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

  it("exposes adapter-authoring contracts from provider-kit", () => {
    expect(typeof providerKit.createDataEntityHandle).toBe("function");
    expect(typeof providerKit.createRelationalProviderAdapter).toBe("function");
    expect(typeof providerKit.AdapterResult.ok).toBe("function");
  });

  it("resolves canonical public subpaths directly", () => {
    expect(typeof providerKitShapes.buildScanUnsupportedReport).toBe("function");
    expect("hasSqlNode" in providerKitShapes).toBe(false);
    expect(typeof runtimeExecutor.executeRelWithProvidersResult).toBe("function");
    expect(typeof runtimeSession.createExecutableSchemaSession).toBe("function");
    expect(typeof providerKitTesting.createProviderConformanceCases).toBe("function");
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
    expect(typeof planner.lowerSqlToRel).toBe("function");
    expect(typeof planner.expandRelViews).toBe("function");
    expect(typeof planner.planPhysicalQuery).toBe("function");
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
