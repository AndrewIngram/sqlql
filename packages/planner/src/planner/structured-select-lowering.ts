import type { RelNode } from "@tupl/foundation";
import type { CteAst, FromEntryAst, SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import { nextRelId } from "./physical/planner-ids";
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
    const loweredCte =
      clause.recursive && isRecursiveCteBody(cteAst as SelectAst, cteName)
        ? lowerRecursiveCte(cteName, cteAst as SelectAst, schema, scopedCteNames)
        : tryLowerStructuredSelect(cteAst as SelectAst, schema, scopedCteNames);
    if (!loweredCte) {
      return null;
    }
    loweredCtes.push({ name: cteName, query: loweredCte });
  }

  const hasSetOp = typeof normalizedAst.set_op === "string" && !!normalizedAst._next;
  if (!hasSetOp) {
    const { with: _ignoredWith, ...withoutWith } = normalizedAst;
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

  const { with: _ignoredWith, ...withoutWith } = normalizedAst;
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

function lowerRecursiveCte(
  cteName: string,
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelNode | null {
  if (!ast.set_op || !ast._next) {
    return null;
  }

  const op = parseSetOp(ast.set_op);
  if (op !== "union" && op !== "union_all") {
    return null;
  }

  const { with: _ignoredWith, set_op: _ignoredSetOp, _next: _ignoredNext, ...seedAst } = ast;
  const seed = tryLowerSimpleSelect(seedAst as SelectAst, schema, cteNames, (subqueryAst) =>
    tryLowerStructuredSelect(subqueryAst, schema, cteNames),
  );
  if (!seed) {
    return null;
  }

  const recursiveTerm = tryLowerSimpleSelect(
    rewriteDerivedTables(ast._next) as SelectAst,
    schema,
    cteNames,
    (subqueryAst) => tryLowerStructuredSelect(subqueryAst, schema, cteNames),
  );
  if (!recursiveTerm) {
    return null;
  }

  return {
    id: nextRelId("repeat_union"),
    kind: "repeat_union",
    convention: "logical",
    cteName,
    mode: op,
    seed,
    iterative: recursiveTerm,
    output: seed.output,
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
