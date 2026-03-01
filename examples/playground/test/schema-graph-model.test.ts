import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS } from "../src/examples";
import {
  buildSchemaGraphLayout,
  buildSchemaGraphModel,
  schemaHandleId,
} from "../src/schema-graph-model";

describe("playground/schema-graph-model", () => {
  it("builds graph edges from foreign keys only", () => {
    const schema = EXAMPLE_PACKS[0]?.schema;
    if (!schema) {
      throw new Error("Expected default example schema.");
    }

    const layout = buildSchemaGraphLayout(schema);

    expect(layout.tableOrder).toEqual(["customers", "products", "orders", "order_items"]);
    expect(layout.edges).toHaveLength(3);
    expect(
      layout.edges
        .map(
          (edge) =>
            `${edge.sourceTable}.${edge.sourceColumn}->${edge.targetTable}.${edge.targetColumn}`,
        )
        .sort(),
    ).toEqual([
      "order_items.order_id->orders.id",
      "order_items.product_id->products.id",
      "orders.customer_id->customers.id",
    ]);
  });

  it("is deterministic across repeated runs", () => {
    const schema = EXAMPLE_PACKS[1]?.schema;
    if (!schema) {
      throw new Error("Expected finance schema.");
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
    const schema = EXAMPLE_PACKS[0]?.schema;
    if (!schema) {
      throw new Error("Expected default example schema.");
    }

    const layout = buildSchemaGraphLayout(schema);
    const model = buildSchemaGraphModel(schema, layout, "orders");

    const relation = model.edges.find(
      (edge) => edge.source === "orders" && edge.target === "customers",
    );

    expect(relation?.sourceHandle).toBe(schemaHandleId("out", "customer_id"));
    expect(relation?.targetHandle).toBe(schemaHandleId("in", "id"));

    const ordersNode = model.nodes.find((node) => node.id === "orders");
    expect(ordersNode?.data.columns.map((column) => column.name)).toEqual([
      "id",
      "customer_id",
      "status",
      "total_cents",
      "ordered_at",
    ]);
  });
});
