import type { Result as BetterResult } from "better-result";

import type { RelNode, RelRewriteError } from "@tupl/foundation";

import type { ViewAliasColumnMap } from "../planner-types";

export interface ViewExpansionResult {
  node: RelNode;
  aliases: Map<string, ViewAliasColumnMap>;
}

export type ViewExpansionResultValue = BetterResult<ViewExpansionResult, RelRewriteError>;
