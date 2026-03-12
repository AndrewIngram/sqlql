/**
 * Provider kit owns provider contracts, adapter-authoring helpers, entity binding,
 * and reusable shape helpers.
 * Callers can build providers against this surface without depending on schema
 * construction or query execution internals.
 */
export * from "./provider";
export * from "./provider/shapes";
