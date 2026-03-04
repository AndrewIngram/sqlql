import { describe, expect, it } from "vitest";

import { FACADE_SCHEMA } from "../src/examples";
import {
  buildSchemaGraphLayout,
  buildSchemaGraphModel,
  schemaHandleId,
} from "../src/schema-graph-model";

describe("playground/schema-graph-model", () => {
  it("builds graph edges from foreign keys only", () => {
    const schema = FACADE_SCHEMA;
    if (!schema) {
      throw new Error("Expected default example schema.");
    }

    const layout = buildSchemaGraphLayout(schema);

    expect(layout.tableOrder).toEqual([
      "my_orders",
      "my_order_items",
      "vendors_for_org",
      "products_for_org",
      "product_access_for_user",
      "active_products",
      "my_order_lines",
      "product_view_counts",
    ]);
    expect(layout.edges).toHaveLength(7);
    expect(
      layout.edges
        .map(
          (edge) =>
            `${edge.sourceTable}.${edge.sourceColumn}->${edge.targetTable}.${edge.targetColumn}`,
        )
        .sort(),
    ).toEqual([
      "my_order_items.order_id->my_orders.id",
      "my_order_items.product_id->active_products.id",
      "my_order_lines.order_id->my_orders.id",
      "my_order_lines.product_id->active_products.id",
      "my_orders.vendor_id->vendors_for_org.id",
      "product_access_for_user.product_id->products_for_org.id",
      "product_view_counts.product_id->active_products.id",
    ]);
  });

  it("is deterministic across repeated runs", () => {
    const schema = FACADE_SCHEMA;
    if (!schema) {
      throw new Error("Expected facade schema.");
    }

    const first = buildSchemaGraphLayout(schema);
    const second = buildSchemaGraphLayout(schema);

    expect(first.tableOrder).toEqual(second.tableOrder);
    expect(first.edges).toEqual(second.edges);

    for (const tableName of first.tableOrder) {
      expect(first.positionsById.get(tableName)).toEqual(second.positionsById.get(tableName));
    }
  });

  it("handles schemas with no foreign keys", () => {
    const schema = {
      tables: {
        one: { columns: { id: { type: "text" as const, nullable: false } } },
        two: { columns: { id: { type: "text" as const, nullable: false } } },
      },
    };

    const layout = buildSchemaGraphLayout(schema);
    const model = buildSchemaGraphModel(schema, layout, null);

    expect(layout.edges).toHaveLength(0);
    expect(model.nodes).toHaveLength(2);
    expect(model.edges).toHaveLength(0);
  });

  it("maps relation edges to column handles", () => {
    const schema = FACADE_SCHEMA;
    if (!schema) {
      throw new Error("Expected default example schema.");
    }

    const layout = buildSchemaGraphLayout(schema);
    const model = buildSchemaGraphModel(schema, layout, "my_orders");

    const relation = model.edges.find(
      (edge) => edge.source === "my_orders" && edge.target === "vendors_for_org",
    );

    expect(relation?.sourceHandle).toBe(schemaHandleId("out", "vendor_id"));
    expect(relation?.targetHandle).toBe(schemaHandleId("in", "id"));

    const ordersNode = model.nodes.find((node) => node.id === "my_orders");
    expect(ordersNode?.data.columns.map((column) => column.name)).toEqual([
      "id",
      "vendor_id",
      "status",
      "total_cents",
      "created_at",
    ]);
  });
});
