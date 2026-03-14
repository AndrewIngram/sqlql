import type { RelNode } from "@tupl/foundation";

import { inferAggregateMetricDefinition, inferRelExprDefinition } from "./rel-expr-definition";
import { inferScanOutputDefinitions } from "./scan-output-definitions";
import {
  applyJoinNullability,
  buildInferredColumnDefinition,
  buildRelOutputCoercion,
  resolveRelRefOutputDefinition,
} from "./output-definition-utils";
import type { SchemaDefinition, TableColumnDefinition } from "../types";

/**
 * Output inference owns column-definition inference for relational output trees.
 */
export function inferRelOutputDefinitions(
  rel: RelNode,
  schema: SchemaDefinition,
  cteDefinitions: Map<string, Record<string, TableColumnDefinition | undefined>> = new Map(),
): Record<string, TableColumnDefinition | undefined> {
  switch (rel.kind) {
    case "scan":
      return inferScanOutputDefinitions(rel, schema, cteDefinitions);
    case "values":
      return Object.fromEntries(rel.output.map((column) => [column.name, undefined]));
    case "filter":
    case "sort":
    case "limit_offset":
      return inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
    case "project": {
      const inputDefinitions = inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
      return Object.fromEntries(
        rel.columns.map((mapping) => [
          mapping.output,
          mapping.kind !== "expr"
            ? resolveRelRefOutputDefinition(inputDefinitions, mapping.source)
            : inferRelExprDefinition(mapping.expr, inputDefinitions),
        ]),
      );
    }
    case "join": {
      const leftDefinitions = inferRelOutputDefinitions(rel.left, schema, cteDefinitions);
      const rightDefinitions = inferRelOutputDefinitions(rel.right, schema, cteDefinitions);
      return {
        ...applyJoinNullability(
          leftDefinitions,
          rel.joinType === "right" || rel.joinType === "full",
        ),
        ...applyJoinNullability(
          rightDefinitions,
          rel.joinType === "left" || rel.joinType === "full",
        ),
      };
    }
    case "aggregate": {
      const inputDefinitions = inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
      const out: Record<string, TableColumnDefinition | undefined> = {};

      for (let index = 0; index < rel.groupBy.length; index += 1) {
        const groupRef = rel.groupBy[index];
        const output = rel.output[index];
        if (!groupRef || !output) {
          continue;
        }
        out[output.name] = resolveRelRefOutputDefinition(inputDefinitions, groupRef);
      }

      for (let index = 0; index < rel.metrics.length; index += 1) {
        const metric = rel.metrics[index];
        const output = rel.output[rel.groupBy.length + index];
        if (!metric || !output) {
          continue;
        }
        out[output.name] = inferAggregateMetricDefinition(metric, inputDefinitions);
      }

      return out;
    }
    case "window": {
      const out = {
        ...inferRelOutputDefinitions(rel.input, schema, cteDefinitions),
      };
      for (const fn of rel.functions) {
        out[fn.as] = buildInferredColumnDefinition("integer", false);
      }
      return out;
    }
    case "set_op": {
      const leftDefinitions = inferRelOutputDefinitions(rel.left, schema, cteDefinitions);
      const rightDefinitions = inferRelOutputDefinitions(rel.right, schema, cteDefinitions);
      const out: Record<string, TableColumnDefinition | undefined> = {};
      for (let index = 0; index < rel.output.length; index += 1) {
        const output = rel.output[index];
        const leftOutput = rel.left.output[index];
        const rightOutput = rel.right.output[index];
        if (!output) {
          continue;
        }
        out[output.name] =
          (leftOutput && leftDefinitions[leftOutput.name]) ||
          (rightOutput && rightDefinitions[rightOutput.name]);
      }
      return out;
    }
    case "with": {
      const nextCtes = new Map(cteDefinitions);
      for (const cte of rel.ctes) {
        nextCtes.set(cte.name, inferRelOutputDefinitions(cte.query, schema, nextCtes));
      }
      return inferRelOutputDefinitions(rel.body, schema, nextCtes);
    }
    case "repeat_union":
      return inferRelOutputDefinitions(rel.seed, schema, cteDefinitions);
    case "sql":
      return {};
  }
}

export { buildRelOutputCoercion };
