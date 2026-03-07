import { describe, expect, it } from "vitest";

import {
  createDataEntityHandle,
  defineSchema,
  resolveTableColumnDefinition,
  toSqlDDL,
} from "../../src";

describe("query/schema dsl", () => {
  it("keeps foreignKey metadata on source-based table lens columns", () => {
    const ordersEntity = createDataEntityHandle<"id" | "vendor_id">({
      entity: "orders_raw",
      provider: "warehouse",
    });
    const vendorsEntity = createDataEntityHandle<"id">({
      entity: "vendors_raw",
      provider: "warehouse",
    });

    const schema = defineSchema(({ table }) => ({
      tables: {
        my_vendors: table(vendorsEntity, {
          columns: () => ({
            id: { source: "id", type: "text", nullable: false, primaryKey: true },
          }),
        }),
        my_orders: table(ordersEntity, {
          columns: () => ({
            id: { source: "id", type: "text", nullable: false, primaryKey: true },
            vendor_id: {
              source: "vendor_id",
              type: "text",
              nullable: false,
              foreignKey: {
                table: "my_vendors",
                column: "id",
              },
            },
          }),
        }),
      },
    }));

    const resolved = resolveTableColumnDefinition(schema, "my_orders", "vendor_id");
    expect(resolved.foreignKey).toEqual({
      table: "my_vendors",
      column: "id",
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain('FOREIGN KEY ("vendor_id") REFERENCES "my_vendors" ("id")');
  });
});
