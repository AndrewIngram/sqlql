import type { Result as BetterResult } from "better-result";

import { lowerSqlToRelResult, type RelLoweringResult } from "../index";
import type {
  RelLoweringError,
  TuplParseError,
  UnsupportedQueryShapeError,
} from "@tupl/foundation";

declare const lowerSqlToRelResultValue: ReturnType<typeof lowerSqlToRelResult>;

const _lowerSqlToRelResultNarrows: BetterResult<
  RelLoweringResult,
  TuplParseError | UnsupportedQueryShapeError | RelLoweringError
> = lowerSqlToRelResultValue;
