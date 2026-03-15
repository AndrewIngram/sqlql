import { describe, expect, it } from "vitest";

import { Result } from "better-result";

import {
  checkSimpleRelScanCapability,
  collectSimpleRelScanReferencedColumns,
  extractSimpleRelScanRequest,
  validateSimpleRelScanRequest,
} from "@tupl/provider-kit/shapes";
import type { RelNode } from "@tupl/foundation";

describe("simple scan request extraction", () => {
  it("extracts a simple scan pipeline with project, filter, sort, and limit", () => {
    const rel: RelNode = {
      id: "limit_users",
      kind: "limit_offset",
      convention: "provider:memory",
      limit: 5,
      offset: 2,
      output: [{ name: "id" }, { name: "email" }],
      input: {
        id: "sort_users",
        kind: "sort",
        convention: "provider:memory",
        orderBy: [{ source: { column: "email" }, direction: "asc" }],
        output: [{ name: "id" }, { name: "email" }],
        input: {
          id: "project_users",
          kind: "project",
          convention: "provider:memory",
          columns: [
            {
              kind: "column",
              source: { column: "id" },
              output: "id",
            },
            {
              kind: "column",
              source: { column: "email" },
              output: "email",
            },
          ],
          output: [{ name: "id" }, { name: "email" }],
          input: {
            id: "filter_users",
            kind: "filter",
            convention: "provider:memory",
            where: [{ op: "eq", column: "org_id", value: "org_1" }],
            output: [{ name: "u.id" }, { name: "u.email" }, { name: "u.org_id" }],
            input: {
              id: "scan_users",
              kind: "scan",
              convention: "provider:memory",
              table: "users",
              alias: "u",
              select: ["id", "email", "org_id"],
              output: [{ name: "u.id" }, { name: "u.email" }, { name: "u.org_id" }],
            },
          },
        },
      },
    };

    expect(extractSimpleRelScanRequest(rel)).toEqual({
      table: "users",
      alias: "u",
      select: ["id", "email"],
      where: [{ op: "eq", column: "org_id", value: "org_1" }],
      orderBy: [{ column: "email", direction: "asc" }],
      limit: 5,
      offset: 2,
    });
  });

  it("rejects non-simple shapes", () => {
    const rel: RelNode = {
      id: "project_users",
      kind: "project",
      convention: "provider:memory",
      columns: [
        {
          kind: "expr",
          expr: {
            kind: "function",
            name: "upper",
            args: [{ kind: "column", ref: { column: "email" } }],
          },
          output: "email_upper",
        },
      ],
      output: [{ name: "email_upper" }],
      input: {
        id: "scan_users",
        kind: "scan",
        convention: "provider:memory",
        table: "users",
        select: ["email"],
        output: [{ name: "email" }],
      },
    };

    expect(extractSimpleRelScanRequest(rel)).toBeNull();
  });

  it("validates field-sensitive simple scan support", () => {
    const validation = validateSimpleRelScanRequest(
      {
        table: "users",
        select: ["id", "email"],
        where: [{ op: "eq", column: "org_id", value: "org_1" }],
        orderBy: [{ column: "email", direction: "asc" }],
      },
      {
        supportsSelectColumn(column) {
          return column === "id" || column === "email";
        },
        supportsFilterClause(clause) {
          return clause.column === "org_id" && clause.op === "eq";
        },
        supportsSortTerm(term) {
          return term.column === "email";
        },
      },
    );

    expect(Result.isOk(validation)).toBe(true);
  });

  it("reports unsupported filter and sort fields", () => {
    const invalidFilter = validateSimpleRelScanRequest(
      {
        table: "users",
        select: ["id"],
        where: [{ op: "eq", column: "email", value: "ada@example.com" }],
      },
      {
        supportsFilterClause(clause) {
          return clause.column === "org_id" && clause.op === "eq";
        },
      },
    );

    expect(Result.isError(invalidFilter)).toBe(true);
    expect(Result.isError(invalidFilter) ? invalidFilter.error.message : "").toContain(
      "Unsupported filter clause for users: email eq",
    );

    const invalidSort = validateSimpleRelScanRequest(
      {
        table: "users",
        select: ["id"],
        orderBy: [{ column: "created_at", direction: "desc" }],
      },
      {
        supportsSortTerm(term) {
          return term.column === "email";
        },
      },
    );

    expect(Result.isError(invalidSort)).toBe(true);
    expect(Result.isError(invalidSort) ? invalidSort.error.message : "").toContain(
      "Unsupported sort column for users: created_at",
    );
  });

  it("checks simple scan capability in one pass", () => {
    const rel: RelNode = {
      id: "scan_users",
      kind: "scan",
      convention: "provider:memory",
      table: "users",
      select: ["id"],
      where: [{ op: "eq", column: "email", value: "ada@example.com" }],
      output: [{ name: "id" }],
    };

    const capability = checkSimpleRelScanCapability(rel, {
      policy: {
        supportsSelectColumn(column) {
          return column === "id";
        },
        supportsFilterClause(clause) {
          return clause.column === "org_id";
        },
      },
    });

    expect(Result.isError(capability)).toBe(true);
    expect(Result.isError(capability) ? capability.error : null).toMatchObject({
      supported: false,
      routeFamily: "scan",
      reason: "Unsupported filter clause for users: email eq",
    });
  });

  it("collects referenced scan columns once", () => {
    expect(
      collectSimpleRelScanReferencedColumns(
        {
          table: "users",
          select: ["id", "email"],
          where: [{ op: "eq", column: "org_id", value: "org_1" }],
          orderBy: [{ column: "created_at", direction: "desc" }],
        },
        ["tenant_id"],
      ),
    ).toEqual(["id", "email", "org_id", "created_at", "tenant_id"]);
  });
});
