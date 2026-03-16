import { describe, expect, it } from "vite-plus/test";

import {
  addEmptyRow,
  buildEmptyRow,
  coerceCellInput,
  deleteRow,
  mergeTableRows,
  updateRowCell,
} from "../src/data-editing";

describe("playground/data-editing", () => {
  it("coerces values by column type", () => {
    expect(coerceCellInput({ type: "integer", nullable: false }, "42")).toEqual({
      ok: true,
      value: 42,
    });

    expect(coerceCellInput({ type: "boolean", nullable: false }, "true")).toEqual({
      ok: true,
      value: true,
    });

    expect(coerceCellInput({ type: "boolean", nullable: false }, "0")).toEqual({
      ok: true,
      value: false,
    });

    expect(coerceCellInput({ type: "text", nullable: false }, "hello")).toEqual({
      ok: true,
      value: "hello",
    });

    expect(coerceCellInput({ type: "timestamp", nullable: true }, "")).toEqual({
      ok: true,
      value: null,
    });

    expect(
      coerceCellInput(
        { type: "text", nullable: false, enum: ["pending", "paid"] as const },
        "paid",
      ),
    ).toEqual({
      ok: true,
      value: "paid",
    });
  });

  it("rejects invalid coercions", () => {
    const invalidInteger = coerceCellInput({ type: "integer", nullable: false }, "1.25");
    expect(invalidInteger.ok).toBe(false);

    const invalidBoolean = coerceCellInput({ type: "boolean", nullable: false }, "yes");
    expect(invalidBoolean.ok).toBe(false);

    const missingRequired = coerceCellInput({ type: "text", nullable: false }, "");
    expect(missingRequired.ok).toBe(false);

    const invalidEnum = coerceCellInput(
      { type: "text", nullable: false, enum: ["pending", "paid"] as const },
      "void",
    );
    expect(invalidEnum.ok).toBe(false);
  });

  it("supports row CRUD helpers", () => {
    const table = {
      columns: {
        id: { type: "text" as const, nullable: false },
        status: {
          type: "text" as const,
          nullable: false,
          enum: ["pending", "paid"] as const,
        },
        score: { type: "integer" as const, nullable: false },
        active: { type: "boolean" as const, nullable: false },
      },
    };

    const empty = buildEmptyRow(table);
    expect(empty).toEqual({ id: "", status: "pending", score: 0, active: false });

    const withRow = addEmptyRow([], table);
    expect(withRow).toHaveLength(1);

    const updated = updateRowCell(withRow, 0, "id", "row_1");
    expect(updated[0]?.id).toBe("row_1");

    const deleted = deleteRow(updated, 0);
    expect(deleted).toEqual([]);
  });

  it("merges table rows without mutating other tables", () => {
    const allRows = {
      users: [{ id: "u1" }],
      orders: [{ id: "o1" }],
    };

    const merged = mergeTableRows(allRows, "orders", [{ id: "o2" }]);
    expect(merged.users).toEqual([{ id: "u1" }]);
    expect(merged.orders).toEqual([{ id: "o2" }]);
  });
});
