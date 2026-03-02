import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS } from "../src/examples";
import { getSqlSuggestionLabels } from "../src/sql-completion";

describe("playground/sql-completion", () => {
  const schema = EXAMPLE_PACKS[0]?.schema;

  it("suggests table names after FROM", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT * FROM ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("table");
    expect(suggestions.labels).toContain("orders");
    expect(suggestions.labels).toContain("customers");
  });

  it("suggests columns for alias-qualified prefixes", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT o. FROM orders o";
    const suggestions = getSqlSuggestionLabels(sql, "SELECT o.".length, schema);

    expect(suggestions.context).toBe("alias_column");
    expect(suggestions.labels).toContain("id");
    expect(suggestions.labels).toContain("total_cents");
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

    const sql = "SELECT * FROM orders WHERE status = ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("enum_value");
    expect(suggestions.labels).toContain("'pending'");
    expect(suggestions.labels).toContain("'paid'");
  });

  it("keeps enum suggestions active while typing inside an open string literal", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT * FROM orders WHERE status = 'pa";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("enum_value");
    expect(suggestions.labels).toContain("'paid'");
  });

  it("suggests enum literals when cursor is between empty quotes", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = [
      "SELECT o.id, c.full_name, o.total_cents",
      "FROM orders o",
      "JOIN customers c ON o.customer_id = c.id",
      "WHERE o.status = ''",
      "ORDER BY o.ordered_at DESC",
      "LIMIT 10;",
    ].join("\n");
    const quoteIndex = sql.indexOf("''");
    const cursorOffset = quoteIndex >= 0 ? quoteIndex + 1 : sql.length;
    const suggestions = getSqlSuggestionLabels(sql, cursorOffset, schema);

    expect(suggestions.context).toBe("enum_value");
    expect(suggestions.labels).toContain("'pending'");
    expect(suggestions.labels).toContain("'paid'");
  });

  it("omits non-filterable columns from WHERE suggestions", () => {
    const schemaForPolicies = {
      tables: {
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            status: { type: "text", nullable: false, filterable: false },
            created_at: { type: "timestamp", nullable: false },
          },
        },
      },
    } as const;

    const aliasSql = "SELECT * FROM orders o WHERE o.";
    const aliasSuggestions = getSqlSuggestionLabels(aliasSql, aliasSql.length, schemaForPolicies);
    expect(aliasSuggestions.context).toBe("alias_column");
    expect(aliasSuggestions.labels).toContain("id");
    expect(aliasSuggestions.labels).toContain("created_at");
    expect(aliasSuggestions.labels).not.toContain("status");

    const generalSql = "SELECT * FROM orders WHERE ";
    const generalSuggestions = getSqlSuggestionLabels(
      generalSql,
      generalSql.length,
      schemaForPolicies,
    );
    expect(generalSuggestions.labels).toContain("id");
    expect(generalSuggestions.labels).toContain("created_at");
    expect(generalSuggestions.labels).not.toContain("status");
    expect(generalSuggestions.labels).not.toContain("orders.status");
  });

  it("omits non-sortable columns from ORDER BY suggestions", () => {
    const schemaForPolicies = {
      tables: {
        orders: {
          columns: {
            id: { type: "text", nullable: false, sortable: false },
            status: { type: "text", nullable: false, sortable: false },
            created_at: { type: "timestamp", nullable: false, sortable: true },
          },
        },
      },
    } as const;

    const aliasSql = "SELECT * FROM orders o ORDER BY o.";
    const aliasSuggestions = getSqlSuggestionLabels(aliasSql, aliasSql.length, schemaForPolicies);
    expect(aliasSuggestions.context).toBe("alias_column");
    expect(aliasSuggestions.labels).toContain("created_at");
    expect(aliasSuggestions.labels).not.toContain("id");
    expect(aliasSuggestions.labels).not.toContain("status");

    const generalSql = "SELECT * FROM orders ORDER BY ";
    const generalSuggestions = getSqlSuggestionLabels(
      generalSql,
      generalSql.length,
      schemaForPolicies,
    );
    expect(generalSuggestions.labels).toContain("created_at");
    expect(generalSuggestions.labels).not.toContain("id");
    expect(generalSuggestions.labels).not.toContain("status");
    expect(generalSuggestions.labels).not.toContain("orders.status");
  });
});
