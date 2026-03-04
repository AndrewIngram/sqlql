import { describe, expect, it } from "vitest";

import {
  asIso8601Timestamp,
  createDataEntityHandle,
  defineSchema,
  defineTableMethods,
  getNormalizedTableBinding,
  resolveSchemaLinkedEnums,
  resolveTableColumnDefinition,
  resolveTableQueryBehavior,
  toSqlDDL,
} from "../src";

describe("defineSchema", () => {
  it("supports source-neutral physical entity lens mappings", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "regional",
    });

    const schema = defineSchema(({ table }) => ({
      tables: {
        my_orders: table({
          from: ordersEntity,
          columns: {
            id: { source: "id", type: "text", nullable: false },
            total_cents: { source: "total_cents", type: "integer", nullable: false },
          },
        }),
      },
    }));

    const binding = getNormalizedTableBinding(schema, "my_orders");
    expect(binding).toEqual({
      kind: "physical",
      provider: "regional",
      entity: "orders_raw",
      columnBindings: {
        id: { source: "id" },
        total_cents: { source: "total_cents" },
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

    const schema = defineSchema(({ table, view, rel, col, expr: _expr, agg }) => ({
      tables: {
        my_orders: table({
          from: ordersEntity,
          columns: {
            id: { source: "id", type: "text", nullable: false },
            total_cents: { source: "total_cents", type: "integer", nullable: false },
          },
        }),
        my_order_stats: view({
          rel: () =>
            rel.aggregate({
              from: rel.scan("my_orders"),
              groupBy: [col("my_orders.id")],
              measures: {
                spend: agg.sum(col("my_orders.total_cents")),
                rows: agg.count(),
              },
            }),
          columns: {
            order_id: col("id"),
            spend: col("spend"),
          },
        }),
      },
    }));

    const binding = getNormalizedTableBinding(schema, "my_order_stats");
    expect(binding?.kind).toBe("view");
    expect(binding?.columnBindings).toEqual({
      order_id: { source: "id" },
      spend: { source: "spend" },
    });
    expect(binding?.columnToSource).toEqual({
      order_id: "id",
      spend: "spend",
    });
  });

  it("supports typed table tokens in col(...) and rel.scan(...)", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
    });
    const vendorsEntity = createDataEntityHandle({
      entity: "vendors_raw",
      provider: "warehouse",
    });

    const schema = defineSchema(({ table, view, rel, col, expr, agg }) => {
      const myOrders = table({
        from: ordersEntity,
        columns: {
          id: { source: "id", type: "text", nullable: false },
          vendorId: { source: "vendor_id", type: "text", nullable: false },
          totalCents: { source: "total_cents", type: "integer", nullable: false },
        },
      });

      const vendorsForOrg = table({
        from: vendorsEntity,
        columns: {
          id: { source: "id", type: "text", nullable: false },
          name: { source: "name", type: "text", nullable: false },
        },
      });

      return {
        tables: {
          myOrders,
          vendorsForOrg,
          spendByVendor: view({
            rel: () =>
              rel.aggregate({
                from: rel.join({
                  left: rel.scan(myOrders),
                  right: rel.scan(vendorsForOrg),
                  on: expr.eq(col(myOrders, "vendorId"), col(vendorsForOrg, "id")),
                }),
                groupBy: [col(vendorsForOrg, "id"), col(vendorsForOrg, "name")],
                measures: {
                  spend: agg.sum(col(myOrders, "totalCents")),
                },
              }),
            columns: {
              vendor_id: col("id"),
              vendor_name: col("name"),
              spend: col("spend"),
            },
          }),
        },
      };
    });

    const binding = getNormalizedTableBinding(schema, "spendByVendor");
    expect(binding?.kind).toBe("view");
    expect(binding?.columnBindings).toEqual({
      vendor_id: { source: "id" },
      vendor_name: { source: "name" },
      spend: { source: "spend" },
    });
    expect(binding?.columnToSource).toEqual({
      vendor_id: "id",
      vendor_name: "name",
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
      groupBy: [
        { kind: "dsl_col_ref", ref: "vendorsForOrg.id" },
        { kind: "dsl_col_ref", ref: "vendorsForOrg.name" },
      ],
      measures: {
        spend: {
          kind: "metric",
          fn: "sum",
          column: { kind: "dsl_col_ref", ref: "myOrders.totalCents" },
        },
      },
    });
  });

  it("supports col(dataEntityHandle, column) typed references in table lenses", () => {
    const ordersEntity = createDataEntityHandle<"id" | "status">({
      entity: "orders_raw",
      provider: "warehouse",
    });

    const schema = defineSchema(({ table, col }) => ({
      tables: {
        myOrders: table({
          from: ordersEntity,
          columns: {
            id: col(ordersEntity, "id"),
            status: col(ordersEntity, "status"),
          },
        }),
      },
    }));

    const binding = getNormalizedTableBinding(schema, "myOrders");
    expect(binding).toEqual({
      kind: "physical",
      provider: "warehouse",
      entity: "orders_raw",
      columnBindings: {
        id: { source: "id" },
        status: { source: "status" },
      },
      columnToSource: {
        id: "id",
        status: "status",
      },
    });
  });

  it("materializes enumFrom links and enforces strict enumMap coverage", () => {
    const schema = defineSchema({
      tables: {
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
      },
    });

    const resolved = resolveSchemaLinkedEnums(schema);
    const status = resolveTableColumnDefinition(resolved, "my_orders", "status");
    expect(status.enum).toEqual(["open", "settled"]);
  });

  it("rejects enumMap with unmapped upstream values by default", () => {
    const schema = defineSchema({
      tables: {
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
      },
    });

    expect(() => resolveSchemaLinkedEnums(schema)).toThrow("Unmapped enumFrom value");
  });

  it("applies default non-column query policy", () => {
    const schema = defineSchema({
      tables: {
        agent_events: {
          columns: {
            event_id: "text",
            org_id: "text",
            created_at: "timestamp",
          },
        },
      },
    });

    expect(resolveTableQueryBehavior(schema, "agent_events")).toEqual({
      maxRows: null,
      reject: {
        requiresLimit: false,
        forbidFullScan: false,
        requireAnyFilterOn: [],
      },
      fallback: {
        filters: "allow_local",
        sorting: "allow_local",
        aggregates: "allow_local",
        limitOffset: "allow_local",
      },
    });
  });

  it("supports table-level reject/fallback policy overrides", () => {
    const schema = defineSchema({
      defaults: {
        query: {
          maxRows: 5_000,
          reject: {
            requiresLimit: true,
          },
          fallback: {
            filters: "require_pushdown",
          },
        },
      },
      tables: {
        agent_events: {
          columns: {
            event_id: "text",
            org_id: "text",
          },
          query: {
            maxRows: 100,
            reject: {
              forbidFullScan: true,
            },
            fallback: {
              sorting: "require_pushdown",
            },
          },
        },
      },
    });

    expect(resolveTableQueryBehavior(schema, "agent_events")).toEqual({
      maxRows: 100,
      reject: {
        requiresLimit: true,
        forbidFullScan: true,
        requireAnyFilterOn: [],
      },
      fallback: {
        filters: "require_pushdown",
        sorting: "require_pushdown",
        aggregates: "allow_local",
        limitOffset: "allow_local",
      },
    });
  });

  it("generates DDL with column metadata comments on every column and table metadata", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: { type: "text", nullable: false, description: "Order id" },
            status: {
              type: "text",
              nullable: false,
              enum: ["draft", "paid", "void"] as const,
              sortable: false,
            },
            created_at: "timestamp",
          },
          query: {
            reject: {
              requiresLimit: true,
            },
          },
        },
      },
    });

    const ddl = toSqlDDL(schema, { ifNotExists: true });
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS "orders"');
    expect(ddl).toContain('"id" TEXT NOT NULL /* sqlql: filterable:true sortable:true description:"Order id" */');
    expect(ddl).toContain('"status" TEXT NOT NULL /* sqlql: filterable:true sortable:false */');
    expect(ddl).toContain('"created_at" TEXT /* sqlql: filterable:true sortable:true format:iso8601 */');
    expect(ddl).toContain('CHECK ("status" IN (\'draft\', \'paid\', \'void\'))');
    expect(ddl).toContain('/* sqlql: query:{"maxRows":null,"reject":{"requiresLimit":true');
  });

  it("generates explicit CHECK constraints", () => {
    const schema = defineSchema({
      tables: {
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
      },
    });

    expect(toSqlDDL(schema)).toContain(
      'CONSTRAINT "invoices_amount_due_allowed" CHECK ("amount_due" IN (0, 1000, 2000))',
    );
  });

  it("supports field-level foreignKey declarations and emits FOREIGN KEY in DDL", () => {
    const schema = defineSchema({
      tables: {
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
      },
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain(
      'FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE',
    );
  });

  it("supports field-level primaryKey/unique and emits constraints in DDL", () => {
    const schema = defineSchema({
      tables: {
        products: {
          columns: {
            id: { type: "text", nullable: false, primaryKey: true },
            sku: { type: "text", nullable: false, unique: true },
            name: { type: "text", nullable: false },
          },
        },
      },
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain('PRIMARY KEY ("id")');
    expect(ddl).toContain('UNIQUE ("sku")');
    expect(ddl).toContain('"id" TEXT NOT NULL /* sqlql: filterable:true sortable:true */');
    expect(ddl).toContain('"sku" TEXT NOT NULL /* sqlql: filterable:true sortable:true */');
  });

  it("rejects invalid enum/check declarations", () => {
    expect(() =>
      defineSchema({
        tables: {
          users: {
            columns: {
              status: { type: "integer", enum: ["active"] },
            },
          },
        },
      }),
    ).toThrow("enum is only supported on text columns");

    expect(() =>
      defineSchema({
        tables: {
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
        },
      }),
    ).toThrow("does not match column type integer");
  });

  it("rejects conflicting field-level key declarations", () => {
    expect(() =>
      defineSchema({
        tables: {
          users: {
            columns: {
              id: { type: "text", nullable: false, primaryKey: true, unique: true } as any,
            },
          },
        },
      }),
    ).toThrow("primaryKey and unique cannot both be true");

    expect(() =>
      defineSchema({
        tables: {
          users: {
            columns: {
              id: { type: "text", primaryKey: true },
            },
          },
        },
      }),
    ).toThrow("primaryKey columns must be nullable: false");
  });

  it("rejects multiple column-level primary keys; uses table-level for composite keys", () => {
    expect(() =>
      defineSchema({
        tables: {
          memberships: {
            columns: {
              org_id: { type: "text", nullable: false, primaryKey: true },
              user_id: { type: "text", nullable: false, primaryKey: true },
            },
          },
        },
      }),
    ).toThrow("Use table.constraints.primaryKey for composite keys");

    const schema = defineSchema({
      tables: {
        memberships: {
          columns: {
            org_id: { type: "text", nullable: false },
            user_id: { type: "text", nullable: false },
          },
          constraints: {
            primaryKey: { columns: ["org_id", "user_id"] },
          },
        },
      },
    });

    expect(toSqlDDL(schema)).toContain('PRIMARY KEY ("org_id", "user_id")');
  });

  it("rejects constraints that reference unknown columns/tables or mismatched arity", () => {
    expect(() =>
      defineSchema({
        tables: {
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
        },
      }),
    ).toThrow('column "missing_column" does not exist');

    expect(() =>
      defineSchema({
        tables: {
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
        },
      }),
    ).toThrow('referenced table "missing_table" does not exist');

    expect(() =>
      defineSchema({
        tables: {
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
        },
      }),
    ).toThrow("must have the same length");
  });

  it("rejects field-level foreign keys with missing references", () => {
    expect(() =>
      defineSchema({
        tables: {
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
        },
      }),
    ).toThrow("foreignKey.column cannot be empty");

    expect(() =>
      defineSchema({
        tables: {
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
        },
      }),
    ).toThrow('referenced column "missing" does not exist');
  });

  it("infers schema-typed request columns and enum values", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: "text",
            org_id: "text",
            status: { type: "text", enum: ["draft", "paid"] as const },
            total_cents: "integer",
          },
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
});
