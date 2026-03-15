/**
 * Advanced SQL-relational adapter types live on their own subpath so the package root can keep
 * the ordinary provider-authoring path small. Adapter authors that need backend translation hooks
 * or custom SQL-relational strategy helpers can opt into this surface explicitly.
 */
export * from "./provider/relational/sql-relational-provider";
