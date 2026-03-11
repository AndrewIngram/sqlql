import { describe, expect, it } from "vitest";

import * as foundation from "@tupl/foundation";
import * as providerKit from "@tupl/provider-kit";
import * as providerKitShapes from "@tupl/provider-kit/shapes";
import * as providerKitTesting from "@tupl/provider-kit/testing";
import * as providerDrizzle from "@tupl/provider-drizzle";
import * as providerIoredis from "@tupl/provider-ioredis";
import * as providerKysely from "@tupl/provider-kysely";
import * as providerObjection from "@tupl/provider-objection";
import * as runtimeExecutor from "@tupl/runtime/executor";
import * as schema from "@tupl/schema";

describe("public package imports", () => {
  it("exposes the canonical schema surface", () => {
    expect(typeof schema.createSchemaBuilder).toBe("function");
    expect(typeof schema.createExecutableSchema).toBe("function");
  });

  it("exposes adapter-authoring contracts from provider-kit", () => {
    expect(typeof providerKit.createDataEntityHandle).toBe("function");
    expect(typeof providerKit.AdapterResult.ok).toBe("function");
  });

  it("resolves canonical public subpaths directly", () => {
    expect(typeof providerKitShapes.buildScanUnsupportedReport).toBe("function");
    expect(typeof runtimeExecutor.executeRelWithProvidersResult).toBe("function");
    expect(typeof providerKitTesting.createProviderConformanceCases).toBe("function");
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
