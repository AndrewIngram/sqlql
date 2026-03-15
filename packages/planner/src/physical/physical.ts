import type { RelNode } from "@tupl/foundation";
import type { ProviderRelTarget } from "../provider-fragments";

export type PhysicalStepKind =
  | "remote_fragment"
  | "lookup_join"
  | "local_hash_join"
  | "local_set_op"
  | "local_with"
  | "local_filter"
  | "local_project"
  | "local_aggregate"
  | "local_window"
  | "local_sort"
  | "local_limit_offset";

export interface PhysicalStepBase {
  id: string;
  kind: PhysicalStepKind;
  dependsOn: string[];
  summary: string;
}

export interface RemoteFragmentPhysicalStep extends PhysicalStepBase {
  kind: "remote_fragment";
  provider: string;
  fragment: ProviderRelTarget;
}

export interface LookupJoinPhysicalStep extends PhysicalStepBase {
  kind: "lookup_join";
  leftProvider: string;
  rightProvider: string;
  leftTable: string;
  rightTable: string;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
}

export interface LocalPhysicalStep extends PhysicalStepBase {
  kind:
    | "local_hash_join"
    | "local_set_op"
    | "local_with"
    | "local_filter"
    | "local_project"
    | "local_aggregate"
    | "local_window"
    | "local_sort"
    | "local_limit_offset";
}

export type PhysicalStep = RemoteFragmentPhysicalStep | LookupJoinPhysicalStep | LocalPhysicalStep;

export interface PhysicalPlan {
  rel: RelNode;
  rootStepId: string;
  steps: PhysicalStep[];
}
