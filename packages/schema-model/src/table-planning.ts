/**
 * Table-planning contracts expose the lower-level planning-hook extension seam for runtimes and
 * advanced tests. Ordinary schema consumers should stay on the root TableMethods surface instead.
 */
export type {
  TablePlanningMethods,
  TablePlanningMethodsForSchema,
  TablePlanningMethodsMap,
} from "./contracts/table-planning-contracts";
