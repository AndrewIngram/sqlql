import type {
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
  ProviderRuntimeBinding,
} from "@tupl/provider-kit";
import type { SqlRelationalScanBinding } from "@tupl/provider-kit/relational-sql";

export type KnexLikeQueryBuilder = {
  clone?: (...args: any[]) => KnexLikeQueryBuilder;
  as?: (...args: any[]) => KnexLikeQueryBuilder;
  clearSelect?: (...args: any[]) => KnexLikeQueryBuilder;
  select: (...args: any[]) => KnexLikeQueryBuilder;
  from: (...args: any[]) => KnexLikeQueryBuilder;
  innerJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  leftJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  rightJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  fullJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  with?: (...args: any[]) => KnexLikeQueryBuilder;
  union?: (...args: any[]) => KnexLikeQueryBuilder;
  unionAll?: (...args: any[]) => KnexLikeQueryBuilder;
  intersect?: (...args: any[]) => KnexLikeQueryBuilder;
  except?: (...args: any[]) => KnexLikeQueryBuilder;
  where: (...args: any[]) => KnexLikeQueryBuilder;
  whereIn: (...args: any[]) => KnexLikeQueryBuilder;
  whereNull: (...args: any[]) => KnexLikeQueryBuilder;
  whereNotNull: (...args: any[]) => KnexLikeQueryBuilder;
  groupBy: (...args: any[]) => KnexLikeQueryBuilder;
  orderBy: (...args: any[]) => KnexLikeQueryBuilder;
  limit: (...args: any[]) => KnexLikeQueryBuilder;
  offset: (...args: any[]) => KnexLikeQueryBuilder;
  count: (...args: any[]) => KnexLikeQueryBuilder;
  countDistinct: (...args: any[]) => KnexLikeQueryBuilder;
  sum: (...args: any[]) => KnexLikeQueryBuilder;
  avg: (...args: any[]) => KnexLikeQueryBuilder;
  min: (...args: any[]) => KnexLikeQueryBuilder;
  max: (...args: any[]) => KnexLikeQueryBuilder;
  rank?: (...args: any[]) => KnexLikeQueryBuilder;
  denseRank?: (...args: any[]) => KnexLikeQueryBuilder;
  rowNumber?: (...args: any[]) => KnexLikeQueryBuilder;
  execute?: (...args: any[]) => Promise<Record<string, unknown>[]>;
};

export type KnexLike = {
  table: (...args: any[]) => KnexLikeQueryBuilder;
  queryBuilder: (...args: any[]) => KnexLikeQueryBuilder;
};

export interface ObjectionProviderEntityConfig<
  TContext,
  TRow extends Record<string, unknown> = Record<string, unknown>,
  TColumns extends string = Extract<keyof TRow, string>,
> {
  table?: string;
  shape?: DataEntityShape<TColumns>;
  /**
   * Builds the mandatory scoped root query for this entity.
   */
  base?: (context: TContext) => KnexLikeQueryBuilder;
}

export type ObjectionProviderShape<
  TRowsByEntity extends Record<string, Record<string, unknown>>,
  TContext = any,
> = {
  [K in keyof TRowsByEntity]: ObjectionProviderEntityConfig<
    TContext,
    TRowsByEntity[K],
    Extract<keyof TRowsByEntity[K], string>
  >;
};

export interface CreateObjectionProviderOptions<
  TContext,
  TEntities extends Record<string, ObjectionProviderEntityConfig<TContext, any, string>> = Record<
    string,
    ObjectionProviderEntityConfig<TContext, any, string>
  >,
> {
  name?: string;
  knex: ProviderRuntimeBinding<TContext, KnexLike>;
  entities?: TEntities;
}

export interface ResolvedEntityConfig<TContext> {
  entity: string;
  table: string;
  config: ObjectionProviderEntityConfig<TContext>;
}

export type ScanBinding<TContext> = SqlRelationalScanBinding<ResolvedEntityConfig<TContext>>;

export type InferObjectionEntityColumns<TConfig> = TConfig extends { shape: infer TShape }
  ? Extract<keyof Extract<TShape, DataEntityShape<string>>, string>
  : TConfig extends ObjectionProviderEntityConfig<any, infer TRow, any>
    ? Extract<keyof TRow, string>
    : string;

export type InferObjectionEntityRow<TConfig> =
  TConfig extends ObjectionProviderEntityConfig<any, infer TRow, any>
    ? TRow
    : Record<string, unknown>;

export type NormalizeObjectionEntityRow<TConfig> = InferObjectionEntityRow<TConfig> &
  Partial<Record<InferObjectionEntityColumns<TConfig>, unknown>>;

export type InferObjectionEntityColumnMetadata<TConfig> = TConfig extends { shape: infer TShape }
  ? InferDataEntityShapeMetadata<
      InferObjectionEntityColumns<TConfig>,
      Extract<TShape, DataEntityShape<InferObjectionEntityColumns<TConfig>>>
    >
  : DataEntityReadMetadataMap<
      InferObjectionEntityColumns<TConfig>,
      NormalizeObjectionEntityRow<TConfig>
    >;

export type ObjectionProviderEntities<
  TEntities extends Record<string, ObjectionProviderEntityConfig<any, any, string>>,
> = {
  [K in keyof TEntities]: DataEntityHandle<
    InferObjectionEntityColumns<TEntities[K]>,
    NormalizeObjectionEntityRow<TEntities[K]>,
    InferObjectionEntityColumnMetadata<TEntities[K]>
  >;
};
