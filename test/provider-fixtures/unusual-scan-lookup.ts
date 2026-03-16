import {
  AdapterResult,
  createRelationalProviderAdapter,
  type QueryRow,
  type RelationalProviderEntityConfig,
} from "@tupl/provider-kit";
import { extractSimpleRelScanRequest } from "@tupl/provider-kit/shapes";

import { applyScanRequest, projectRows } from "./row-ops";

type FixtureEntityConfig = RelationalProviderEntityConfig & {
  rows: QueryRow[];
};

const entities = {
  orders: {
    shape: {
      id: "text",
      customer_id: "text",
      total_cents: "integer",
    },
    rows: [
      { id: "o1", customer_id: "c1", total_cents: 900 },
      { id: "o2", customer_id: "c1", total_cents: 1800 },
      { id: "o3", customer_id: "c2", total_cents: 2600 },
    ],
  },
} satisfies Record<string, FixtureEntityConfig>;

export function createUnusualScanLookupFixtureProvider() {
  return createRelationalProviderAdapter({
    name: "fixture_scan_lookup",
    entities,
    resolveRelCompileStrategy({ rel }) {
      return extractSimpleRelScanRequest(rel) ? "simple_scan" : null;
    },
    buildRelPlanPayload({ rel }) {
      const request = extractSimpleRelScanRequest(rel);
      if (!request) {
        throw new Error("Fixture scan/lookup provider expected a simple scan request.");
      }

      return {
        strategy: "simple_scan",
        request,
      };
    },
    async executeCompiledPlan({ plan, entities }) {
      const payload = plan.payload as { request?: unknown } | null;
      const request =
        payload && typeof payload === "object" && payload.request ? payload.request : null;
      if (!request || typeof request !== "object" || !("table" in request)) {
        return AdapterResult.err(
          new Error("Fixture scan/lookup provider only executes simple scan rel plans."),
        );
      }

      const scanRequest = request as {
        table: string;
        select: string[];
        where?: unknown;
        orderBy?: unknown;
        limit?: unknown;
        offset?: unknown;
      };
      const config = entities[scanRequest.table as keyof typeof entities];
      if (!config) {
        return AdapterResult.err(new Error(`Unknown fixture entity: ${scanRequest.table}`));
      }

      return AdapterResult.ok(
        applyScanRequest(config.rows, {
          table: scanRequest.table,
          select: scanRequest.select,
          ...(Array.isArray(scanRequest.where) ? { where: scanRequest.where } : {}),
          ...(Array.isArray(scanRequest.orderBy) ? { orderBy: scanRequest.orderBy } : {}),
          ...(typeof scanRequest.limit === "number" ? { limit: scanRequest.limit } : {}),
          ...(typeof scanRequest.offset === "number" ? { offset: scanRequest.offset } : {}),
        }),
      );
    },
    async lookupMany({ request, entities }) {
      const config = entities[request.table as keyof typeof entities];
      if (!config) {
        return AdapterResult.err(new Error(`Unknown fixture entity: ${request.table}`));
      }

      const matched = config.rows.filter((row) =>
        request.keys.includes(row[request.key as keyof typeof row]),
      );
      return AdapterResult.ok(projectRows(matched, request.select));
    },
  });
}
