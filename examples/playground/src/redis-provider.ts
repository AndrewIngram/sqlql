import type { TableDefinition } from "@tupl/schema";

export const REDIS_PROVIDER_NAME = "redisProvider";
export const REDIS_INPUT_TABLE_NAME = "redis_product_view_counts";

export interface RedisInputRow {
  user_id: string;
  product_id: string;
  view_count: number;
}

export const REDIS_INPUT_TABLE_DEFINITION: TableDefinition = {
  provider: REDIS_PROVIDER_NAME,
  columns: {
    user_id: { type: "text", nullable: false },
    product_id: { type: "text", nullable: false },
    view_count: { type: "integer", nullable: false },
  },
};
