import type { Selectable } from "kysely";
import type {
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
  MaybePromise,
  ProviderRuntimeBinding,
  QueryRow,
} from "@tupl/provider-kit";

export type KyselyQueryBuilderLike = {
  select: (...args: any[]) => KyselyQueryBuilderLike;
  innerJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  leftJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  rightJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  fullJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  union?: (...args: any[]) => KyselyQueryBuilderLike;
  unionAll?: (...args: any[]) => KyselyQueryBuilderLike;
  intersect?: (...args: any[]) => KyselyQueryBuilderLike;
  except?: (...args: any[]) => KyselyQueryBuilderLike;
  where: (...args: any[]) => KyselyQueryBuilderLike;
  groupBy?: (...args: any[]) => KyselyQueryBuilderLike;
  orderBy: (...args: any[]) => KyselyQueryBuilderLike;
  limit: (...args: any[]) => KyselyQueryBuilderLike;
  offset: (...args: any[]) => KyselyQueryBuilderLike;
  execute: (...args: any[]) => Promise<QueryRow[]>;
};

export type KyselyDatabaseLike = {
  selectFrom: (...args: any[]) => KyselyQueryBuilderLike;
  with?: (name: string, expression: (db: KyselyDatabaseLike) => unknown) => KyselyDatabaseLike;
};

export interface KyselyProviderEntityConfig<
  TContext,
  TRow extends Record<string, unknown> = Record<string, unknown>,
  TColumns extends string = Extract<keyof TRow, string>,
> {
  table?: string;
  shape?: DataEntityShape<TColumns>;
  /**
   * Applies mandatory scoped constraints to a query rooted at this entity alias.
   */
  base?: (args: {
    db: KyselyDatabaseLike;
    query: KyselyQueryBuilderLike;
    context: TContext;
    entity: string;
    alias: string;
  }) => MaybePromise<KyselyQueryBuilderLike>;
}

export interface CreateKyselyProviderOptions<
  TContext,
  TEntities extends Record<string, KyselyProviderEntityConfig<TContext, any, string>> = Record<
    string,
    KyselyProviderEntityConfig<TContext, any, string>
  >,
> {
  name?: string;
  db: ProviderRuntimeBinding<TContext, unknown>;
  entities?: TEntities;
}

export interface ResolvedEntityConfig<TContext> {
  entity: string;
  table: string;
  config: KyselyProviderEntityConfig<TContext>;
}

export type InferKyselyEntityRow<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = TEntityName extends keyof TDatabase
  ? Selectable<Extract<TDatabase[TEntityName], Record<string, unknown>>>
  : TConfig extends KyselyProviderEntityConfig<any, infer TRow, any>
    ? TRow
    : Record<string, unknown>;

export type InferKyselyEntityColumns<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = TConfig extends {
  shape: infer TShape;
}
  ? Extract<keyof Extract<TShape, DataEntityShape<string>>, string>
  : Extract<keyof InferKyselyEntityRow<TDatabase, TEntityName, TConfig>, string>;

export type NormalizeKyselyEntityRow<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = InferKyselyEntityRow<TDatabase, TEntityName, TConfig> &
  Partial<Record<InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>, unknown>>;

export type InferKyselyEntityColumnMetadata<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = TConfig extends { shape: infer TShape }
  ? InferDataEntityShapeMetadata<
      InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>,
      Extract<TShape, DataEntityShape<InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>>>
    >
  : DataEntityReadMetadataMap<
      InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>,
      NormalizeKyselyEntityRow<TDatabase, TEntityName, TConfig>
    >;

export type KyselyProviderEntities<
  TContext,
  TDatabase,
  TEntities extends Record<string, KyselyProviderEntityConfig<TContext, any, string>>,
> = {
  [K in keyof TEntities]: DataEntityHandle<
    InferKyselyEntityColumns<TDatabase, Extract<K, string>, TEntities[K]>,
    NormalizeKyselyEntityRow<TDatabase, Extract<K, string>, TEntities[K]>,
    InferKyselyEntityColumnMetadata<TDatabase, Extract<K, string>, TEntities[K]>
  >;
};
