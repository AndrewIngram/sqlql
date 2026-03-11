import type { QueryStepKind, QueryStepRoute } from "../contracts";

/**
 * Step routing owns the public-facing categorization of runtime plan steps.
 */
export function formatColumnRef(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
}

export function routeForStepKind(kind: QueryStepKind): QueryStepRoute | null {
  switch (kind) {
    case "scan":
      return "scan";
    case "lookup_join":
      return "lookup_join";
    case "remote_fragment":
      return "provider_fragment";
    default:
      return "local";
  }
}
