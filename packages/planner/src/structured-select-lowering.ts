import { Result, type Result as BetterResult } from "better-result";

import { type RelLoweringError, type RelNode } from "@tupl/foundation";
import type { CteAst, FromEntryAst, SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import { nextRelId } from "./physical/planner-ids";
import { collectTablesFromSelectAst } from "./sql-expr-lowering";
import { toRelLoweringError } from "./planner-errors";
import { tryLowerSimpleSelect } from "./simple-select-lowering";
import { parseRelColumnRef } from "./select/select-from-lowering";
import { parseSetOp } from "./select/set-op-lowering";

/**
 * Structured select lowering owns select/set-op/CTE lowering into relational nodes.
 */
export function tryLowerStructuredSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): BetterResult<RelNode | null, RelLoweringError> {
  return Result.gen(function* () {
    const normalizedAst = rewriteDerivedTables(ast);
    const scopedCteNames = new Set(cteNames);
    const loweredCtes: Array<{ name: string; query: RelNode }> = [];
    const withClauses = Array.isArray(normalizedAst.with) ? normalizedAst.with : [];

    for (const clause of withClauses) {
      const rawName = (clause as { name?: unknown }).name;
      const cteName =
        typeof rawName === "string"
          ? rawName
          : rawName &&
              typeof rawName === "object" &&
              typeof (rawName as { value?: unknown }).value === "string"
            ? (rawName as { value: string }).value
            : null;
      const cteAst = (clause as { stmt?: { ast?: unknown } }).stmt?.ast;
      if (!cteName || !cteAst || typeof cteAst !== "object") {
        return Result.ok(null);
      }
      const visibleCteNames = new Set(scopedCteNames);
      if (clause.recursive) {
        visibleCteNames.add(cteName);
      }
      const loweredCte = yield* clause.recursive && isRecursiveCteBody(cteAst as SelectAst, cteName)
        ? lowerRecursiveCte(cteName, cteAst as SelectAst, schema, visibleCteNames)
        : tryLowerStructuredSelect(cteAst as SelectAst, schema, visibleCteNames);
      if (!loweredCte) {
        return Result.ok(null);
      }
      loweredCtes.push({ name: cteName, query: loweredCte });
      scopedCteNames.add(cteName);
    }

    const hasSetOp = typeof normalizedAst.set_op === "string" && !!normalizedAst._next;
    if (!hasSetOp) {
      const { with: _ignoredWith, ...withoutWith } = normalizedAst;
      const simple = yield* tryLowerSimpleSelectWithinStructuredLowering(
        withoutWith as SelectAst,
        schema,
        scopedCteNames,
      );
      if (!simple) {
        return Result.ok(null);
      }

      if (loweredCtes.length === 0) {
        return Result.ok(simple);
      }

      const withNode: RelNode = {
        id: nextRelId("with"),
        kind: "with",
        convention: "local",
        ctes: loweredCtes,
        body: simple,
        output: simple.output,
      };

      return Result.ok(withNode);
    }

    const { with: _ignoredWith, ...withoutWith } = normalizedAst;
    let currentAst: SelectAst = withoutWith as SelectAst;
    const { set_op: _ignoredSetOp, _next: _ignoredNext, ...currentBaseAst } = currentAst;
    let currentNode = yield* tryLowerSimpleSelectWithinStructuredLowering(
      currentBaseAst as SelectAst,
      schema,
      scopedCteNames,
    );
    if (!currentNode) {
      return Result.ok(null);
    }

    while (typeof currentAst.set_op === "string" && currentAst._next) {
      const op = parseSetOp(currentAst.set_op);
      if (!op) {
        return Result.ok(null);
      }

      const {
        with: _ignoredRightWith,
        set_op: _ignoredRightSetOp,
        _next: _ignoredRightNext,
        ...rightBaseAst
      } = currentAst._next;
      const aliasedRightBaseAst = applyOutputAliases(
        rightBaseAst as SelectAst,
        currentNode.output.map((column) => column.name),
      );
      const rightBase = yield* tryLowerSimpleSelectWithinStructuredLowering(
        aliasedRightBaseAst,
        schema,
        scopedCteNames,
      );
      if (!rightBase) {
        return Result.ok(null);
      }
      const alignedRightBase = alignRelOutputShape(rightBase, currentNode.output);

      currentNode = {
        id: nextRelId("set_op"),
        kind: "set_op",
        convention: "local",
        op,
        left: currentNode,
        right: alignedRightBase,
        output: currentNode.output,
      };

      currentAst = currentAst._next;
    }

    if (loweredCtes.length === 0) {
      return Result.ok(currentNode);
    }

    const withNode: RelNode = {
      id: nextRelId("with"),
      kind: "with",
      convention: "local",
      ctes: loweredCtes,
      body: currentNode,
      output: currentNode.output,
    };

    return Result.ok(withNode);
  });
}

export { collectTablesFromSelectAst };

function lowerRecursiveCte(
  cteName: string,
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): BetterResult<RelNode | null, RelLoweringError> {
  return Result.gen(function* () {
    if (!ast.set_op || !ast._next) {
      return Result.ok(null);
    }

    const op = parseSetOp(ast.set_op);
    if (op !== "union" && op !== "union_all") {
      return Result.ok(null);
    }

    const { with: _ignoredWith, set_op: _ignoredSetOp, _next: _ignoredNext, ...seedAst } = ast;
    const seed = yield* tryLowerSimpleSelectWithinStructuredLowering(
      seedAst as SelectAst,
      schema,
      cteNames,
    );
    if (!seed) {
      return Result.ok(null);
    }

    const recursiveAst = applyOutputAliases(
      rewriteDerivedTables(ast._next) as SelectAst,
      seed.output.map((column) => column.name),
    );
    const recursiveTerm = yield* tryLowerSimpleSelectWithinStructuredLowering(
      recursiveAst,
      schema,
      cteNames,
    );
    if (!recursiveTerm) {
      return Result.ok(null);
    }
    const alignedRecursiveTerm = alignRelOutputShape(recursiveTerm, seed.output);

    const repeatUnionNode: RelNode = {
      id: nextRelId("repeat_union"),
      kind: "repeat_union",
      convention: "logical",
      cteName,
      mode: op,
      seed,
      iterative: alignedRecursiveTerm,
      output: seed.output,
    };

    return Result.ok(repeatUnionNode);
  });
}

function tryLowerSimpleSelectWithinStructuredLowering(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): BetterResult<RelNode | null, RelLoweringError> {
  return Result.try({
    try: () => {
      const result = tryLowerSimpleSelect(ast, schema, cteNames, (subqueryAst) => {
        const subqueryResult = tryLowerStructuredSelect(subqueryAst, schema, cteNames);
        if (Result.isError(subqueryResult)) {
          throw subqueryResult.error;
        }
        return subqueryResult.value;
      });
      if (Result.isError(result)) {
        throw result.error;
      }
      return result.value;
    },
    catch: (error) => toRelLoweringError(error, "lower structured SELECT"),
  });
}

function alignRelOutputShape(rel: RelNode, output: RelNode["output"]): RelNode {
  if (
    rel.output.length === output.length &&
    rel.output.every((column, index) => column.name === output[index]?.name)
  ) {
    return rel;
  }

  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: output.map((column, index) => ({
      kind: "column" as const,
      source: parseRelColumnRef(rel.output[index]?.name ?? column.name),
      output: column.name,
    })),
    output,
  };
}

function isRecursiveCteBody(ast: SelectAst, cteName: string): boolean {
  const visit = (value: unknown): boolean => {
    if (!value || typeof value !== "object") {
      return false;
    }
    if (Array.isArray(value)) {
      return value.some(visit);
    }

    const record = value as Record<string, unknown>;
    if (typeof record.table === "string" && record.table === cteName) {
      return true;
    }

    return Object.values(record).some(visit);
  };

  return visit(ast._next);
}

function applyOutputAliases(ast: SelectAst, outputNames: string[]): SelectAst {
  if (!Array.isArray(ast.columns) || ast.columns.length !== outputNames.length) {
    return ast;
  }

  return {
    ...ast,
    columns: ast.columns.map((column, index) => {
      const alias = outputNames[index];
      return alias ? { ...column, as: alias } : { ...column };
    }),
  };
}

function rewriteDerivedTables(ast: SelectAst): SelectAst {
  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (!from.some((entry) => !!entry.stmt)) {
    return ast;
  }

  const existingCteNames = new Set(
    (ast.with ?? []).flatMap((clause) => {
      const name = clause.name;
      if (typeof name === "string") {
        return [name];
      }
      if (name && typeof name === "object" && typeof name.value === "string") {
        return [name.value];
      }
      return [];
    }),
  );

  const syntheticCtes: CteAst[] = [];
  const rewrittenFrom: FromEntryAst[] = from.map((entry, index) => {
    if (!entry.stmt) {
      return entry;
    }

    const alias =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : `derived_${index + 1}`;
    let syntheticName = `__tupl_derived_${index + 1}`;
    while (existingCteNames.has(syntheticName)) {
      syntheticName = `${syntheticName}_next`;
    }
    existingCteNames.add(syntheticName);
    syntheticCtes.push({
      name: { value: syntheticName },
      stmt: {
        ast: rewriteDerivedTables(entry.stmt.ast),
      },
    });

    return {
      table: syntheticName,
      as: alias,
      ...(entry.join ? { join: entry.join } : {}),
      ...(entry.on ? { on: entry.on } : {}),
    };
  });

  return {
    ...ast,
    from: rewrittenFrom,
    with: [...(ast.with ?? []), ...syntheticCtes],
  };
}
