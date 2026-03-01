interface AssetFetcher {
  fetch(input: Request | string | URL, init?: RequestInit): Promise<Response>;
}

export interface PlaygroundEnv {
  ASSETS: AssetFetcher;
}

export default {
  async fetch(request: Request, env: PlaygroundEnv): Promise<Response> {
    return env.ASSETS.fetch(request);
  },
};
