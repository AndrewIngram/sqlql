import type { Result as BetterResult } from "better-result";

import type { TuplParseError } from "@tupl/foundation";

import type { SelectAst } from "../ast";
import { parseSqliteSelectAstResult } from "../parser";

declare const parseSqliteSelectAstResultValue: ReturnType<typeof parseSqliteSelectAstResult>;

const _parseSqliteSelectAstResultNarrows: BetterResult<SelectAst, TuplParseError> =
  parseSqliteSelectAstResultValue;
