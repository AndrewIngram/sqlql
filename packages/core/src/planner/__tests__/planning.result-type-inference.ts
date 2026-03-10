import type { Result as BetterResult } from "better-result";

import { lowerSqlToRelResult, type RelLoweringResult } from "../index";
import type { TuplParseError, TuplPlanningError } from "../../model/errors";

declare const lowerSqlToRelResultValue: ReturnType<typeof lowerSqlToRelResult>;

const _lowerSqlToRelResultNarrows: BetterResult<
  RelLoweringResult,
  TuplParseError | TuplPlanningError
> = lowerSqlToRelResultValue;
