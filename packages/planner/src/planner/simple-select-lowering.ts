import type { RelNode } from "@tupl/foundation";
import type { SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
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
): RelNode | null {
  const shape = prepareSimpleSelectLowering(ast, schema, cteNames, tryLowerSelect);
  if (!shape) {
    return null;
  }

  const current = buildSimpleSelectJoinTree(shape, schema, tryLowerSelect);
  if (!current) {
    return null;
  }

  return finalizeSimpleSelectRel(current, shape);
}
