import type { RelNode } from "@tupl/foundation";
import type { SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import { nextRelId } from "./planner-ids";
import { collectTablesFromSelectAst } from "./sql-expr-lowering";
import { tryLowerSimpleSelect } from "./simple-select-lowering";
import { parseSetOp } from "./select/set-op-lowering";

/**
 * Structured select lowering owns select/set-op/CTE lowering into relational nodes.
 */
export function tryLowerStructuredSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelNode | null {
  const scopedCteNames = new Set(cteNames);
  const loweredCtes: Array<{ name: string; query: RelNode }> = [];
  const withClauses = Array.isArray(ast.with) ? ast.with : [];

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
    if (!cteName) {
      return null;
    }
    scopedCteNames.add(cteName);
  }

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
      return null;
    }
    const loweredCte = tryLowerStructuredSelect(cteAst as SelectAst, schema, scopedCteNames);
    if (!loweredCte) {
      return null;
    }
    loweredCtes.push({ name: cteName, query: loweredCte });
  }

  const hasSetOp = typeof ast.set_op === "string" && !!ast._next;
  if (!hasSetOp) {
    const { with: _ignoredWith, ...withoutWith } = ast;
    const simple = tryLowerSimpleSelect(
      withoutWith as SelectAst,
      schema,
      scopedCteNames,
      (subqueryAst) => tryLowerStructuredSelect(subqueryAst, schema, scopedCteNames),
    );
    if (!simple) {
      return null;
    }

    if (loweredCtes.length === 0) {
      return simple;
    }

    return {
      id: nextRelId("with"),
      kind: "with",
      convention: "local",
      ctes: loweredCtes,
      body: simple,
      output: simple.output,
    };
  }

  const { with: _ignoredWith, ...withoutWith } = ast;
  let currentAst: SelectAst = withoutWith as SelectAst;
  const { set_op: _ignoredSetOp, _next: _ignoredNext, ...currentBaseAst } = currentAst;
  let currentNode = tryLowerSimpleSelect(
    currentBaseAst as SelectAst,
    schema,
    scopedCteNames,
    (subqueryAst) => tryLowerStructuredSelect(subqueryAst, schema, scopedCteNames),
  );
  if (!currentNode) {
    return null;
  }

  while (typeof currentAst.set_op === "string" && currentAst._next) {
    const op = parseSetOp(currentAst.set_op);
    if (!op) {
      return null;
    }

    const {
      with: _ignoredRightWith,
      set_op: _ignoredRightSetOp,
      _next: _ignoredRightNext,
      ...rightBaseAst
    } = currentAst._next;
    const rightBase = tryLowerSimpleSelect(
      rightBaseAst as SelectAst,
      schema,
      scopedCteNames,
      (subqueryAst) => tryLowerStructuredSelect(subqueryAst, schema, scopedCteNames),
    );
    if (!rightBase) {
      return null;
    }

    currentNode = {
      id: nextRelId("set_op"),
      kind: "set_op",
      convention: "local",
      op,
      left: currentNode,
      right: rightBase,
      output: currentNode.output,
    };

    currentAst = currentAst._next;
  }

  if (loweredCtes.length === 0) {
    return currentNode;
  }

  return {
    id: nextRelId("with"),
    kind: "with",
    convention: "local",
    ctes: loweredCtes,
    body: currentNode,
    output: currentNode.output,
  };
}

export { collectTablesFromSelectAst };
