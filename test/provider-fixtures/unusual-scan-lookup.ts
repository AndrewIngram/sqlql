import {
  AdapterResult,
  createRelationalProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type RelationalProviderEntityConfig,
} from "@tupl/provider-kit";

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
    declaredAtoms: [
      "scan.project",
      "scan.filter.basic",
      "scan.filter.set_membership",
      "scan.sort",
      "scan.limit_offset",
      "lookup.bulk",
    ] as const,
    entities,
    resolveRelCompileStrategy() {
      return null;
    },
    async executeCompiledPlan({ plan, entities }) {
      const fragment = plan.payload as ProviderFragment;
      if (fragment.kind !== "scan") {
        return AdapterResult.err(
          new Error("Fixture scan/lookup provider only executes scan fragments."),
        );
      }

      const config = entities[fragment.table];
      if (!config) {
        return AdapterResult.err(new Error(`Unknown fixture entity: ${fragment.table}`));
      }

      return AdapterResult.ok(applyScanRequest(config.rows, fragment.request));
    },
    async lookupMany({ request, entities }) {
      const config = entities[request.table];
      if (!config) {
        return AdapterResult.err(new Error(`Unknown fixture entity: ${request.table}`));
      }

      const matched = config.rows.filter((row) => request.keys.includes(row[request.key]));
      return AdapterResult.ok(projectRows(matched, request.select));
    },
  });
}
