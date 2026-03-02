import { describe, expect, it, vi } from "vitest";

import {
  asIso8601Timestamp,
  defineSchema,
  defineTableMethods,
  resolveTableColumnDefinition,
  resolveTableQueryBehavior,
  toSqlDDL,
} from "../src";

describe("defineSchema", () => {
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

  it("accepts legacy query.filterable/sortable and maps them to column metadata", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: "text",
            email: "text",
            admin_notes: { type: "text", filterable: false },
          },
          query: {
            filterable: ["id", "email"],
            sortable: ["id"],
          },
        },
      },
    });

    expect(resolveTableColumnDefinition(schema, "users", "id").filterable).toBe(true);
    expect(resolveTableColumnDefinition(schema, "users", "email").filterable).toBe(true);
    expect(resolveTableColumnDefinition(schema, "users", "email").sortable).toBe(false);
    expect(resolveTableColumnDefinition(schema, "users", "admin_notes").filterable).toBe(false);
    expect(warn).toHaveBeenCalledTimes(1);

    warn.mockRestore();
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
