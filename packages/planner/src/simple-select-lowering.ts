import { Result, type Result as BetterResult } from "better-result";

import { type RelLoweringError, type RelNode } from "@tupl/foundation";
import type { SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import { toRelLoweringError } from "./planner-errors";
import { buildSimpleSelectJoinTree } from "./select/select-join-tree";
import { finalizeSimpleSelectRel } from "./select/select-project";
import { prepareSimpleSelectLowering } from "./select/select-shape";

/**
 * Simple-select lowering owns lowering a single SELECT core into relational nodes.
 */
export function tryLowerSimpleSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
  tryLowerSelect: (ast: SelectAst) => RelNode | null,
): BetterResult<RelNode | null, RelLoweringError> {
  return Result.gen(function* () {
    const shape = yield* Result.try({
      try: () => {
        const result = prepareSimpleSelectLowering(ast, schema, cteNames, tryLowerSelect);
        if (Result.isError(result)) {
          throw result.error;
        }
        return result.value;
      },
      catch: (error) => toRelLoweringError(error, "prepare simple select lowering"),
    });
    if (!shape) {
      return Result.ok(null);
    }

    const current = yield* Result.try({
      try: () => buildSimpleSelectJoinTree(shape, schema, tryLowerSelect),
      catch: (error) => toRelLoweringError(error, "build simple select join tree"),
    });
    if (!current) {
      return Result.ok(null);
    }

    const finalized = yield* Result.try({
      try: () => finalizeSimpleSelectRel(current, shape),
      catch: (error) => toRelLoweringError(error, "finalize simple select lowering"),
    });

    return Result.ok(finalized);
  });
}
