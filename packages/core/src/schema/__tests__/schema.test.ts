import { describe, expect, it } from "vitest";

import { createDataEntityHandle } from "@tupl/core";
import {
  asIso8601Timestamp,
  createSchemaBuilder,
  defineTableMethods,
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  resolveSchemaLinkedEnums,
  resolveTableColumnDefinition,
  toSqlDDL,
} from "@tupl/core/schema";
import { buildSchema, buildEntitySchema } from "../../testing/schema-builder";

describe("createSchemaBuilder", () => {
  it("supports source-neutral physical entity lens mappings", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "regional",
    });

    const schema = buildSchema((builder) => {
      builder.table("my_orders", ordersEntity, {
        columns: {
          id: { source: "id", type: "text", nullable: false },
          total_cents: { source: "total_cents", type: "integer", nullable: false },
        },
      });
    });

    const binding = getNormalizedTableBinding(schema, "my_orders");
    expect(binding).toMatchObject({
      kind: "physical",
      provider: "regional",
      entity: "orders_raw",
      columnBindings: {
        id: { kind: "source", source: "id", definition: { type: "text", nullable: false } },
        total_cents: {
          kind: "source",
          source: "total_cents",
          definition: { type: "integer", nullable: false },
        },
      },
      columnToSource: {
        id: "id",
        total_cents: "total_cents",
      },
    });
  });

  it("supports view table bindings in schema DSL", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "regional",
    });

    const schema = buildSchema((builder) => {
      builder.table("my_orders", ordersEntity, {
        columns: {
          id: { source: "id", type: "text", nullable: false },
          total_cents: { source: "total_cents", type: "integer", nullable: false },
        },
      });
      builder.view(
        "my_order_stats",
        ({ scan, aggregate, col, agg }) =>
          aggregate({
            from: scan("my_orders"),
            groupBy: { order_id: col("my_orders.id") },
            measures: {
              spend: agg.sum(col("my_orders.total_cents")),
              rows: agg.count(),
            },
          }),
        {
          columns: ({ col }) => ({
            order_id: col.string("order_id", { nullable: false }),
            spend: col.integer("spend"),
          }),
        },
      );
    });

    const binding = getNormalizedTableBinding(schema, "my_order_stats");
    expect(binding?.kind).toBe("view");
    expect(binding?.columnBindings).toEqual({
      order_id: {
        kind: "source",
        source: "order_id",
        definition: { type: "text", nullable: false },
      },
      spend: { kind: "source", source: "spend", definition: { type: "integer" } },
    });
    expect(binding?.columnToSource).toEqual({
      order_id: "order_id",
      spend: "spend",
    });
  });

  it("supports typed table tokens in col(...) and scan(...)", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
    });
    const vendorsEntity = createDataEntityHandle({
      entity: "vendors_raw",
      provider: "warehouse",
    });

    const schema = buildSchema((builder) => {
      const myOrders = builder.table("myOrders", ordersEntity, {
        columns: {
          id: { source: "id", type: "text", nullable: false },
          vendorId: { source: "vendor_id", type: "text", nullable: false },
          totalCents: { source: "total_cents", type: "integer", nullable: false },
        },
      });

      const vendorsForOrg = builder.table("vendorsForOrg", vendorsEntity, {
        columns: {
          id: { source: "id", type: "text", nullable: false },
          name: { source: "name", type: "text", nullable: false },
        },
      });

      builder.view(
        "spendByVendor",
        ({ scan, join, aggregate, col, expr, agg }) =>
          aggregate({
            from: join({
              left: scan(myOrders),
              right: scan(vendorsForOrg),
              on: expr.eq(col(myOrders, "vendorId"), col(vendorsForOrg, "id")),
            }),
            groupBy: {
              vendor_id: col(vendorsForOrg, "id"),
              vendor_name: col(vendorsForOrg, "name"),
            },
            measures: {
              spend: agg.sum(col(myOrders, "totalCents")),
            },
          }),
        {
          columns: ({ col }) => ({
            vendor_id: col.string("vendor_id", { nullable: false }),
            vendor_name: col.string("vendor_name", { nullable: false }),
            spend: col.integer("spend"),
          }),
        },
      );
    });

    const binding = getNormalizedTableBinding(schema, "spendByVendor");
    expect(binding?.kind).toBe("view");
    expect(binding?.columnBindings).toEqual({
      vendor_id: {
        kind: "source",
        source: "vendor_id",
        definition: { type: "text", nullable: false },
      },
      vendor_name: {
        kind: "source",
        source: "vendor_name",
        definition: { type: "text", nullable: false },
      },
      spend: { kind: "source", source: "spend", definition: { type: "integer" } },
    });
    expect(binding?.columnToSource).toEqual({
      vendor_id: "vendor_id",
      vendor_name: "vendor_name",
      spend: "spend",
    });

    const viewRel = binding && binding.kind === "view" ? binding.rel({}) : null;
    expect(viewRel).toEqual({
      kind: "aggregate",
      from: {
        kind: "join",
        left: { kind: "scan", table: "myOrders" },
        right: { kind: "scan", table: "vendorsForOrg" },
        on: {
          kind: "eq",
          left: { kind: "dsl_col_ref", ref: "myOrders.vendorId" },
          right: { kind: "dsl_col_ref", ref: "vendorsForOrg.id" },
        },
        type: "inner",
      },
      groupBy: {
        vendor_id: { kind: "dsl_col_ref", ref: "vendorsForOrg.id" },
        vendor_name: { kind: "dsl_col_ref", ref: "vendorsForOrg.name" },
      },
      measures: {
        spend: {
          kind: "metric",
          fn: "sum",
          column: { kind: "dsl_col_ref", ref: "myOrders.totalCents" },
        },
      },
    });
  });

  it("supports direct source mappings in table lenses", () => {
    const ordersEntity = createDataEntityHandle<"id" | "status">({
      entity: "orders_raw",
      provider: "warehouse",
    });

    const schema = buildSchema((builder) => {
      builder.table("myOrders", ordersEntity, {
        columns: {
          id: { source: "id" },
          status: { source: "status" },
        },
      });
    });

    const binding = getNormalizedTableBinding(schema, "myOrders");
    expect(binding).toMatchObject({
      kind: "physical",
      provider: "warehouse",
      entity: "orders_raw",
      columnBindings: {
        id: { kind: "source", source: "id", definition: { type: "text" } },
        status: { kind: "source", source: "status", definition: { type: "text" } },
      },
      columnToSource: {
        id: "id",
        status: "status",
      },
    });
  });

  it("supports typed column builders backed by entity metadata", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        vendorId: { source: "vendor_id", type: "text", nullable: false },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
        createdAt: { source: "created_at", type: "timestamp", nullable: false },
      },
    });

    const schema = buildSchema((builder) => {
      builder.table("myOrders", ordersEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          vendorId: col.string("vendorId"),
          totalCents: col.integer("totalCents"),
          createdAt: col.string("createdAt", { coerce: "isoTimestamp" }),
        }),
      });
    });

    const binding = getNormalizedTableBinding(schema, "myOrders");
    expect(binding).toMatchObject({
      kind: "physical",
      provider: "warehouse",
      entity: "orders_raw",
      columnBindings: {
        id: { kind: "source", source: "id", definition: { type: "text" } },
        vendorId: { kind: "source", source: "vendor_id", definition: { type: "text" } },
        totalCents: { kind: "source", source: "total_cents", definition: { type: "integer" } },
        createdAt: {
          kind: "source",
          source: "created_at",
          definition: { type: "text" },
          coerce: "isoTimestamp",
        },
      },
      columnToSource: {
        id: "id",
        vendorId: "vendor_id",
        totalCents: "total_cents",
        createdAt: "created_at",
      },
    });
  });

  it("normalizes virtual columns on physical tables as expr bindings", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
      },
    });

    const schema = buildSchema((builder) => {
      builder.table("myOrders", ordersEntity, {
        columns: ({ col, expr }) => ({
          id: col.id("id"),
          totalCents: col.integer("totalCents"),
          totalDollars: col.real(expr.divide(col("totalCents"), expr.literal(100)), {
            nullable: false,
          }),
          isLargeOrder: col.boolean(expr.gte(col("totalCents"), expr.literal(3000)), {
            nullable: false,
          }),
        }),
      });
    });

    const binding = getNormalizedTableBinding(schema, "myOrders");
    expect(binding?.kind).toBe("physical");
    if (!binding || binding.kind !== "physical") {
      throw new Error("Expected a physical table binding.");
    }

    expect(binding.columnBindings.id).toEqual({
      kind: "source",
      source: "id",
      definition: { type: "text" },
    });
    expect(binding.columnBindings.totalCents).toEqual({
      kind: "source",
      source: "total_cents",
      definition: { type: "integer" },
    });
    expect(binding.columnBindings.totalDollars).toMatchObject({
      kind: "expr",
      definition: { type: "real", nullable: false },
      expr: {
        kind: "function",
        name: "divide",
        args: [
          { kind: "column", ref: { column: "totalCents" } },
          { kind: "literal", value: 100 },
        ],
      },
    });
    expect(binding.columnBindings.isLargeOrder).toMatchObject({
      kind: "expr",
      definition: { type: "boolean", nullable: false },
      expr: {
        kind: "function",
        name: "gte",
        args: [
          { kind: "column", ref: { column: "totalCents" } },
          { kind: "literal", value: 3000 },
        ],
      },
    });
    expect(binding.columnToSource).toEqual({
      id: "id",
      totalCents: "total_cents",
    });
  });

  it("fails fast when entity metadata and typed column declarations are incompatible", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        createdAt: { source: "created_at", type: "timestamp", nullable: false },
      },
    });

    expect(() =>
      buildSchema((builder) => {
        builder.table("myOrders", ordersEntity, {
          columns: ({ col }) => ({
            createdAt: col.string("createdAt"),
          }),
        });
      }),
    ).toThrow(
      "Column orders_raw.created_at is exposed as timestamp, but the schema declared text. Add a coerce function or align the declared type.",
    );
  });

  it("validates coerced provider rows against richer schema scalar types", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        totalCents: { source: "total_cents", type: "text", nullable: false },
        payload: { source: "payload", type: "text", nullable: true },
      },
    });

    const schema = buildSchema((builder) => {
      builder.table("myOrders", ordersEntity, {
        columns: ({ col }) => ({
          totalCents: col.integer("totalCents", { coerce: (value) => Number(value) }),
          payload: col.json("payload", { coerce: (value) => JSON.parse(String(value)) }),
        }),
      });
    });

    const binding = getNormalizedTableBinding(schema, "myOrders");
    expect(binding?.kind).toBe("physical");

    const rows = mapProviderRowsToLogical(
      [{ total_cents: "42", payload: '{"ok":true}' }],
      ["totalCents", "payload"],
      binding?.kind === "physical" ? binding : null,
      schema.tables.myOrders,
    );
    expect(rows).toEqual([{ totalCents: 42, payload: { ok: true } }]);

    expect(() =>
      mapProviderRowsToLogical(
        [{ total_cents: "nope", payload: '{"ok":true}' }],
        ["totalCents", "payload"],
        binding?.kind === "physical" ? binding : null,
        schema.tables.myOrders,
      ),
    ).toThrow("must be an integer");
  });

  it("supports the full aggregate helper surface in view DSL", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        vendorId: { source: "vendor_id", type: "text", nullable: false },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
      },
    });

    const schema = buildSchema((builder) => {
      const myOrders = builder.table("myOrders", ordersEntity, {
        columns: ({ col }) => ({
          vendorId: col.string("vendorId"),
          totalCents: col.integer("totalCents"),
        }),
      });

      builder.view(
        "orderStats",
        ({ scan, aggregate, col, agg }) =>
          aggregate({
            from: scan(myOrders),
            groupBy: {
              vendorId: col(myOrders, "vendorId"),
            },
            measures: {
              rowCount: agg.count(),
              distinctVendorCount: agg.countDistinct(col(myOrders, "vendorId")),
              totalSpend: agg.sum(col(myOrders, "totalCents")),
              totalSpendDistinct: agg.sumDistinct(col(myOrders, "totalCents")),
              avgSpend: agg.avg(col(myOrders, "totalCents")),
              avgSpendDistinct: agg.avgDistinct(col(myOrders, "totalCents")),
              minSpend: agg.min(col(myOrders, "totalCents")),
              maxSpend: agg.max(col(myOrders, "totalCents")),
            },
          }),
        {
          columns: ({ col }) => ({
            vendorId: col.string("vendorId"),
            rowCount: col.integer("rowCount"),
            distinctVendorCount: col.integer("distinctVendorCount"),
            totalSpend: col.integer("totalSpend"),
            totalSpendDistinct: col.integer("totalSpendDistinct"),
            avgSpend: col.real("avgSpend"),
            avgSpendDistinct: col.real("avgSpendDistinct"),
            minSpend: col.integer("minSpend"),
            maxSpend: col.integer("maxSpend"),
          }),
        },
      );
    });

    const binding = getNormalizedTableBinding(schema, "orderStats");
    expect(binding?.kind).toBe("view");
    const rel = binding?.kind === "view" ? binding.rel({}) : null;
    expect(rel).toMatchObject({
      kind: "aggregate",
      measures: {
        rowCount: { fn: "count" },
        distinctVendorCount: { fn: "count", distinct: true },
        totalSpend: { fn: "sum" },
        totalSpendDistinct: { fn: "sum", distinct: true },
        avgSpend: { fn: "avg" },
        avgSpendDistinct: { fn: "avg", distinct: true },
        minSpend: { fn: "min" },
        maxSpend: { fn: "max" },
      },
    });
  });

  it("materializes enumFrom links and enforces strict enumMap coverage", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "db",
        columns: {
          status: { type: "text", enum: ["pending", "paid", "shipped"] as const },
        },
      },
      my_orders: {
        provider: "db",
        columns: {
          status: {
            type: "text",
            enumFrom: "orders.status",
            enumMap: {
              pending: "open",
              paid: "settled",
              shipped: "settled",
            },
            enum: ["open", "settled"] as const,
          },
        },
      },
    });

    const resolved = resolveSchemaLinkedEnums(schema);
    const status = resolveTableColumnDefinition(resolved, "my_orders", "status");
    expect(status.enum).toEqual(["open", "settled"]);
  });

  it("rejects enumMap with unmapped upstream values by default", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "db",
        columns: {
          status: { type: "text", enum: ["pending", "paid"] as const },
        },
      },
      my_orders: {
        provider: "db",
        columns: {
          status: {
            type: "text",
            enumFrom: "orders.status",
            enumMap: {
              pending: "open",
            },
            enum: ["open"] as const,
          },
        },
      },
    });

    expect(() => resolveSchemaLinkedEnums(schema)).toThrow("Unmapped enumFrom value");
  });

  it("generates DDL with metadata comments for timestamp/description fields", () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: { type: "text", nullable: false, description: "Order id" },
          status: {
            type: "text",
            nullable: false,
            enum: ["draft", "paid", "void"] as const,
          },
          created_at: "timestamp",
        },
      },
    });

    const ddl = toSqlDDL(schema, { ifNotExists: true });
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS "orders"');
    expect(ddl).toContain('"id" TEXT NOT NULL /* tupl: description:"Order id" */');
    expect(ddl).toContain('"status" TEXT NOT NULL');
    expect(ddl).toContain('"created_at" TEXT /* tupl: format:iso8601 */');
    expect(ddl).toContain("CHECK (\"status\" IN ('draft', 'paid', 'void'))");
  });

  it("generates explicit CHECK constraints", () => {
    const schema = buildEntitySchema({
      invoices: {
        columns: {
          id: { type: "text", nullable: false },
          amount_due: { type: "integer", nullable: false },
        },
        constraints: {
          checks: [
            {
              name: "invoices_amount_due_allowed",
              kind: "in",
              column: "amount_due",
              values: [0, 1000, 2000],
            },
          ],
        },
      },
    });

    expect(toSqlDDL(schema)).toContain(
      'CONSTRAINT "invoices_amount_due_allowed" CHECK ("amount_due" IN (0, 1000, 2000))',
    );
  });

  it("supports field-level foreignKey declarations and emits FOREIGN KEY in DDL", () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: { type: "text", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
        },
      },
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          user_id: {
            type: "text",
            nullable: false,
            foreignKey: {
              table: "users",
              column: "id",
              onDelete: "CASCADE",
            },
          },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
        },
      },
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain('FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE');
  });

  it("supports field-level primaryKey/unique and emits constraints in DDL", () => {
    const schema = buildEntitySchema({
      products: {
        columns: {
          id: { type: "text", nullable: false, primaryKey: true },
          sku: { type: "text", nullable: false, unique: true },
          name: { type: "text", nullable: false },
        },
      },
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain('PRIMARY KEY ("id")');
    expect(ddl).toContain('UNIQUE ("sku")');
    expect(ddl).toContain('"id" TEXT NOT NULL');
    expect(ddl).toContain('"sku" TEXT NOT NULL');
  });

  it("rejects invalid enum/check declarations", () => {
    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            status: { type: "integer", enum: ["active"] },
          },
        },
      }),
    ).toThrow("enum is only supported on text columns");

    expect(() =>
      buildEntitySchema({
        invoices: {
          columns: {
            amount_due: "integer",
          },
          constraints: {
            checks: [
              {
                kind: "in",
                column: "amount_due",
                values: ["not_a_number"],
              },
            ],
          },
        },
      }),
    ).toThrow("does not match column type integer");
  });

  it("rejects conflicting field-level key declarations", () => {
    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            id: { type: "text", nullable: false, primaryKey: true, unique: true } as any,
          },
        },
      }),
    ).toThrow("primaryKey and unique cannot both be true");

    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            id: { type: "text", primaryKey: true },
          },
        },
      }),
    ).toThrow("primaryKey columns must be nullable: false");
  });

  it("rejects multiple column-level primary keys; uses table-level for composite keys", () => {
    expect(() =>
      buildEntitySchema({
        memberships: {
          columns: {
            org_id: { type: "text", nullable: false, primaryKey: true },
            user_id: { type: "text", nullable: false, primaryKey: true },
          },
        },
      }),
    ).toThrow("Use table.constraints.primaryKey for composite keys");

    const schema = buildEntitySchema({
      memberships: {
        columns: {
          org_id: { type: "text", nullable: false },
          user_id: { type: "text", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["org_id", "user_id"] },
        },
      },
    });

    expect(toSqlDDL(schema)).toContain('PRIMARY KEY ("org_id", "user_id")');
  });

  it("rejects constraints that reference unknown columns/tables or mismatched arity", () => {
    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            id: "text",
          },
          constraints: {
            primaryKey: {
              columns: ["missing_column"],
            },
          },
        },
      }),
    ).toThrow('column "missing_column" does not exist');

    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            id: "text",
          },
        },
        projects: {
          columns: {
            id: "text",
            owner_id: "text",
          },
          constraints: {
            foreignKeys: [
              {
                columns: ["owner_id"],
                references: {
                  table: "missing_table",
                  columns: ["id"],
                },
              },
            ],
          },
        },
      }),
    ).toThrow('referenced table "missing_table" does not exist');

    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            id: "text",
            email: "text",
          },
        },
        projects: {
          columns: {
            id: "text",
            owner_id: "text",
          },
          constraints: {
            foreignKeys: [
              {
                columns: ["id", "owner_id"],
                references: {
                  table: "users",
                  columns: ["id"],
                },
              },
            ],
          },
        },
      }),
    ).toThrow("must have the same length");
  });

  it("rejects field-level foreign keys with missing references", () => {
    expect(() =>
      buildEntitySchema({
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            user_id: {
              type: "text",
              nullable: false,
              foreignKey: {
                table: "users",
                column: "",
              },
            },
          },
        },
      }),
    ).toThrow("foreignKey.column cannot be empty");

    expect(() =>
      buildEntitySchema({
        users: {
          columns: {
            id: { type: "text", nullable: false },
          },
        },
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            user_id: {
              type: "text",
              nullable: false,
              foreignKey: {
                table: "users",
                column: "missing",
              },
            },
          },
        },
      }),
    ).toThrow('referenced column "missing" does not exist');
  });

  it("infers schema-typed request columns and enum values", () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          org_id: "text",
          status: { type: "text", enum: ["draft", "paid"] as const },
          total_cents: "integer",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: {
        async scan(request) {
          request.select.push("id");
          request.where?.push({ op: "eq", column: "status", value: "paid" });
          // @ts-expect-error invalid enum literal
          request.where?.push({ op: "eq", column: "status", value: "refunded" });
          // @ts-expect-error not a valid orders column
          request.select.push("email");
          return [];
        },
        async aggregate(request) {
          request.groupBy?.push("org_id");
          request.metrics.push({ fn: "sum", column: "total_cents", as: "total" });
          // @ts-expect-error not a valid orders column
          request.groupBy?.push("email");
          return [];
        },
      },
    });

    expect(methods.orders).toBeDefined();
  });

  it("provides a timestamp helper for ISO-8601 values", () => {
    expect(asIso8601Timestamp("2026-02-01T10:00:00.000Z")).toBe("2026-02-01T10:00:00.000Z");
    expect(asIso8601Timestamp(new Date("2026-02-01T10:00:00.000Z"))).toBe(
      "2026-02-01T10:00:00.000Z",
    );
  });

  it("keeps foreignKey metadata on source-based table lens columns", () => {
    const ordersEntity = createDataEntityHandle<"id" | "vendor_id">({
      entity: "orders_raw",
      provider: "warehouse",
    });
    const vendorsEntity = createDataEntityHandle<"id">({
      entity: "vendors_raw",
      provider: "warehouse",
    });

    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("my_vendors", vendorsEntity, {
      columns: () => ({
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
      }),
    });
    builder.table("my_orders", ordersEntity, {
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
    });
    const schema = builder.build();

    const resolved = resolveTableColumnDefinition(schema, "my_orders", "vendor_id");
    expect(resolved.foreignKey).toEqual({
      table: "my_vendors",
      column: "id",
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain('FOREIGN KEY ("vendor_id") REFERENCES "my_vendors" ("id")');
  });
});
