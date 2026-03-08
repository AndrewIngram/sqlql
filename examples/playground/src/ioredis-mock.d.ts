declare module "ioredis-mock" {
  import type { RedisLike, RedisPipelineLike } from "@sqlql/ioredis";

  export default class RedisMock implements RedisLike {
    pipeline(): RedisPipelineLike;
    flushall(): Promise<unknown>;
    hset(key: string, ...args: string[]): Promise<unknown>;
  }
}
