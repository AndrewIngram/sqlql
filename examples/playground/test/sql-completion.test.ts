import { describe, expect, it } from "vitest";

import { FACADE_SCHEMA } from "../src/examples";
import { getSqlSuggestionLabels } from "../src/sql-completion";

describe("playground/sql-completion", () => {
  const schema = FACADE_SCHEMA;

  it("suggests table names after FROM", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT * FROM ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("table");
    expect(suggestions.labels).toContain("my_orders");
    expect(suggestions.labels).toContain("vendors_for_org");
  });

  it("suggests columns for alias-qualified prefixes", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT o. FROM my_orders o";
    const suggestions = getSqlSuggestionLabels(sql, "SELECT o.".length, schema);

    expect(suggestions.context).toBe("alias_column");
    expect(suggestions.labels).toContain("id");
    expect(suggestions.labels).toContain("total_cents");
    expect(suggestions.labels).toContain("total_dollars");
  });

  it("includes SQL functions in general context", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.labels).toContain("SUM");
    expect(suggestions.labels).toContain("ROW_NUMBER");
  });

  it("suggests enum literals for enum-typed predicates", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT * FROM my_orders WHERE status = ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("enum_value");
    expect(suggestions.labels).toContain("'pending'");
    expect(suggestions.labels).toContain("'paid'");
    expect(suggestions.labels).toContain("'shipped'");
  });

  it("keeps enum suggestions active while typing inside an open string literal", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT * FROM my_orders WHERE status = 'pa";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("enum_value");
    expect(suggestions.labels).toContain("'paid'");
  });

  it("suggests enum literals when cursor is between empty quotes", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = [
      "SELECT o.id, v.name, o.total_cents",
      "FROM my_orders o",
      "JOIN vendors_for_org v ON o.vendor_id = v.id",
      "WHERE o.status = ''",
      "ORDER BY o.created_at DESC",
      "LIMIT 10;",
    ].join("\n");
    const quoteIndex = sql.indexOf("''");
    const cursorOffset = quoteIndex >= 0 ? quoteIndex + 1 : sql.length;
    const suggestions = getSqlSuggestionLabels(sql, cursorOffset, schema);

    expect(suggestions.context).toBe("enum_value");
    expect(suggestions.labels).toContain("'pending'");
    expect(suggestions.labels).toContain("'paid'");
  });

  it("includes declared columns in WHERE suggestions", () => {
    const schemaForSuggestions = {
      tables: {
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            status: { type: "text", nullable: false },
            created_at: { type: "timestamp", nullable: false },
          },
        },
      },
    } as const;

    const aliasSql = "SELECT * FROM orders o WHERE o.";
    const aliasSuggestions = getSqlSuggestionLabels(
      aliasSql,
      aliasSql.length,
      schemaForSuggestions,
    );
    expect(aliasSuggestions.context).toBe("alias_column");
    expect(aliasSuggestions.labels).toContain("id");
    expect(aliasSuggestions.labels).toContain("created_at");
    expect(aliasSuggestions.labels).toContain("status");

    const generalSql = "SELECT * FROM orders WHERE ";
    const generalSuggestions = getSqlSuggestionLabels(
      generalSql,
      generalSql.length,
      schemaForSuggestions,
    );
    expect(generalSuggestions.labels).toContain("id");
    expect(generalSuggestions.labels).toContain("created_at");
    expect(generalSuggestions.labels).toContain("status");
    expect(generalSuggestions.labels).toContain("orders.status");
  });

  it("includes declared columns in ORDER BY suggestions", () => {
    const schemaForSuggestions = {
      tables: {
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            status: { type: "text", nullable: false },
            created_at: { type: "timestamp", nullable: false },
          },
        },
      },
    } as const;

    const aliasSql = "SELECT * FROM orders o ORDER BY o.";
    const aliasSuggestions = getSqlSuggestionLabels(
      aliasSql,
      aliasSql.length,
      schemaForSuggestions,
    );
    expect(aliasSuggestions.context).toBe("alias_column");
    expect(aliasSuggestions.labels).toContain("created_at");
    expect(aliasSuggestions.labels).toContain("id");
    expect(aliasSuggestions.labels).toContain("status");

    const generalSql = "SELECT * FROM orders ORDER BY ";
    const generalSuggestions = getSqlSuggestionLabels(
      generalSql,
      generalSql.length,
      schemaForSuggestions,
    );
    expect(generalSuggestions.labels).toContain("created_at");
    expect(generalSuggestions.labels).toContain("id");
    expect(generalSuggestions.labels).toContain("status");
    expect(generalSuggestions.labels).toContain("orders.status");
  });
});
