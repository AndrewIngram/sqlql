import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import {
  parseSqliteSelectAst,
  parseSqliteSelectAstResult,
} from "../../../../../planner/src/planner/sqlite-parser/parser";

describe("sqlite-parser", () => {
  it("parses boolean precedence as OR over AND", () => {
    const ast = parseSqliteSelectAst(
      "SELECT id FROM orders WHERE org_id = 'org_1' OR status = 'paid' AND total_cents > 5000",
    );

    const where = ast.where as { operator?: unknown; right?: { operator?: unknown } };
    expect(where.operator).toBe("OR");
    expect(where.right?.operator).toBe("AND");
  });

  it("preserves explicit parentheses on grouped boolean expressions", () => {
    const ast = parseSqliteSelectAst(
      "SELECT id FROM orders WHERE (org_id = 'org_1' OR status = 'paid') AND total_cents > 1000",
    );

    const where = ast.where as {
      operator?: unknown;
      left?: { operator?: unknown; parentheses?: unknown };
    };

    expect(where.operator).toBe("AND");
    expect(where.left?.operator).toBe("OR");
    expect(where.left?.parentheses).toBe(true);
  });

  it("parses CTEs and set operations", () => {
    const ast = parseSqliteSelectAst(`
      WITH scoped AS (
        SELECT id FROM orders
      )
      SELECT id FROM scoped
      UNION ALL
      SELECT id FROM users
    `);

    expect(ast.with).toHaveLength(1);
    expect(ast.set_op).toBe("UNION ALL");
    expect(ast._next?.type).toBe("select");
  });

  it("parses window specifications with partition/order", () => {
    const ast = parseSqliteSelectAst(`
      SELECT ROW_NUMBER() OVER (
        PARTITION BY org_id
        ORDER BY created_at DESC
      ) AS rn
      FROM orders
    `);

    const columns = ast.columns as Array<{
      expr?: { over?: { as_window_specification?: unknown } };
    }>;
    const over = columns[0]?.expr?.over?.as_window_specification as {
      window_specification?: {
        partitionby?: unknown[];
        orderby?: unknown[];
      };
    };

    expect(over.window_specification?.partitionby).toHaveLength(1);
    expect(over.window_specification?.orderby).toHaveLength(1);
  });

  it("parses named window references in OVER", () => {
    const ast = parseSqliteSelectAst(`
      SELECT SUM(total_cents) OVER w AS running_total
      FROM orders
    `);

    const columns = ast.columns as Array<{
      expr?: { over?: { as_window_specification?: unknown } };
    }>;
    expect(columns[0]?.expr?.over?.as_window_specification).toBe("w");
  });

  it("captures top-level WINDOW clauses", () => {
    const ast = parseSqliteSelectAst(`
      SELECT SUM(total_cents) OVER w AS running_total
      FROM orders
      WINDOW w AS (PARTITION BY org_id ORDER BY created_at)
    `);

    expect(ast.window).toBeDefined();
    expect(ast.window?.[0]?.name).toBe("w");
  });

  it("parses RIGHT/FULL joins", () => {
    const right = parseSqliteSelectAst(`
      SELECT o.id, u.id AS user_id
      FROM orders o
      RIGHT JOIN users u ON o.user_id = u.id
    `);
    const rightFrom = right.from as Array<{ join?: string }>;
    expect(rightFrom[1]?.join).toBe("RIGHT JOIN");

    const full = parseSqliteSelectAst(`
      SELECT o.id, u.id AS user_id
      FROM orders o
      FULL JOIN users u ON o.user_id = u.id
    `);
    const fullFrom = full.from as Array<{ join?: string }>;
    expect(fullFrom[1]?.join).toBe("FULL JOIN");
  });

  it("rejects non-select statements", () => {
    expect(() => parseSqliteSelectAst("UPDATE orders SET status = 'refunded'")).toThrow(
      "Only SELECT statements are currently supported.",
    );
  });

  it("rejects multiple SQL statements", () => {
    expect(() => parseSqliteSelectAst("SELECT id FROM orders; SELECT id FROM users")).toThrow(
      "Only a single SQL statement is supported.",
    );
  });

  it("returns tagged parse errors from the result API", () => {
    const result = parseSqliteSelectAstResult("UPDATE orders SET status = 'refunded'");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected parse result to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplParseError",
      name: "TuplParseError",
      message: "Only SELECT statements are currently supported.",
    });
  });
});
