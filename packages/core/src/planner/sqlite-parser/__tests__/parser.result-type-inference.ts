import type { Result as BetterResult } from "better-result";

import type { TuplParseError } from "@tupl/core";

import type { SelectAst } from "../../../../../internal-planner/src/planner/sqlite-parser/ast";
import { parseSqliteSelectAstResult } from "../../../../../internal-planner/src/planner/sqlite-parser/parser";

declare const parseSqliteSelectAstResultValue: ReturnType<typeof parseSqliteSelectAstResult>;

const _parseSqliteSelectAstResultNarrows: BetterResult<SelectAst, TuplParseError> =
  parseSqliteSelectAstResultValue;
