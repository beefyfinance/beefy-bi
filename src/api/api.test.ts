import { diContainer } from "@fastify/awilix";
import { asValue } from "awilix";
import { FastifyInstance } from "fastify";
import { DbClient } from "../utils/db";
import { ProgrammerError } from "../utils/programmer-error";
import { buildApi } from "./api";
import { InvestorService } from "./service/investor";
import { BeefyPortfolioService } from "./service/protocol/beefy";

const AbstractCache: any = require("abstract-cache"); // todo: add or install types

describe("Api tests", () => {
  function buildMocks() {
    let app: FastifyInstance;
    let getInvestorId: jest.Mock<ReturnType<InvestorService["getInvestorId"]>> = jest.fn();
    let getInvestorTimeline: jest.Mock<ReturnType<BeefyPortfolioService["getInvestorTimeline"]>> = jest.fn();

    const abCache = AbstractCache({ useAwait: false });
    app = buildApi({
      rateLimit: {
        timeWindow: "1 minute",
        max: 1, // should be 1 per test
      },
      registerDI: async () => {
        const ni = () => {
          throw new ProgrammerError("Not implemented");
        };
        await diContainer.dispose();

        getInvestorId.mockClear();
        getInvestorTimeline.mockClear();

        diContainer.register({
          db: asValue({ connect: ni, on: ni, end: ni, query: ni } as DbClient),
          investor: asValue({ getInvestorId } as any),
          beefy: asValue({ getInvestorTimeline } as any),
          product: asValue(null as any),
          price: asValue(null as any),
          importState: asValue(null as any),
          abCache: asValue(abCache),
          cache: asValue(null as any),
          redis: asValue(null as any),
        });
      },
    });

    return { app, getInvestorId, getInvestorTimeline };
  }

  /**
   * We have to use it.concurrent to make sure we only run 1 test at a time
   * This is because awilix is a global instance and
   * there is no way to inject the container on a specific fastify instance
   */

  it.concurrent("should expose an openapi json endpoint", async () => {
    const { app } = buildMocks();
    const response = await app.inject({
      method: "GET",
      url: "/api/v1/openapi.json",
    });
    expect(response.statusCode).toBe(200);
  });

  it.concurrent("should expose a timeline endpoint", async () => {
    const { app, getInvestorId, getInvestorTimeline } = buildMocks();
    getInvestorId.mockReturnValueOnce(Promise.resolve(1));
    getInvestorTimeline.mockReturnValueOnce(Promise.resolve([]));

    const response = await app.inject({
      method: "GET",
      url: "/api/v1/beefy/timeline?address=0x0000000000000000000000000000000000000000",
    });

    expect(response.statusCode).toBe(200);
    // has security headers defined
    expect(response.headers["content-security-policy"]).toBeDefined();
    expect(response.headers["cross-origin-embedder-policy"]).toBe("require-corp");
    expect(response.headers["cross-origin-opener-policy"]).toBe("same-origin");
    expect(response.headers["cross-origin-resource-policy"]).toBe("same-origin");
    // has cache headers defined
    expect(response.headers["cache-control"]).toBe("public, max-age=60, s-maxage=60");
    // has rate-limit headers defined
    expect(response.headers["x-ratelimit-limit"]).toBeDefined();
    expect(response.headers["x-ratelimit-remaining"]).toBeDefined();
    expect(response.headers["x-ratelimit-reset"]).toBeDefined();
  });

  it.concurrent("should not cache when endpoint returns a 404", async () => {
    const { app, getInvestorId, getInvestorTimeline } = buildMocks();
    getInvestorId.mockReturnValueOnce(Promise.resolve(null)); // this means not found
    getInvestorTimeline.mockReturnValueOnce(Promise.resolve([]));

    const response = await app.inject({
      method: "GET",
      url: "/api/v1/beefy/timeline?address=0x0000000000000000000000000000000000000000",
    });

    expect(response.statusCode).toBe(404);
    // has cache headers defined
    expect(response.headers["cache-control"]).toBe("no-cache");
  });

  it.concurrent("should not cache when endpoint returns a 400", async () => {
    const { app, getInvestorId, getInvestorTimeline } = buildMocks();
    getInvestorId.mockReturnValueOnce(Promise.resolve(1)); // this means not found
    getInvestorTimeline.mockReturnValueOnce(Promise.resolve([]));

    const response = await app.inject({
      method: "GET",
      url: "/api/v1/beefy/timeline?address=abc",
    });

    expect(response.statusCode).toBe(400);
    // has cache headers defined
    expect(response.headers["cache-control"]).toBe("no-cache");
  });

  it.concurrent("should rate limit", async () => {
    const { app, getInvestorId, getInvestorTimeline } = buildMocks();
    getInvestorId.mockReturnValueOnce(Promise.resolve(1)); // this means not found
    getInvestorTimeline.mockReturnValueOnce(Promise.resolve([{ a: 1 } as any]));

    let response: any;
    for (let i = 0; i < 10; i++) {
      response = await app.inject({
        method: "GET",
        url: "/api/v1/beefy/timeline?address=0x0000000000000000000000000000000000000000",
      });
    }
    response = await response;

    expect(response.statusCode).toBe(429);
    // has cache headers defined
    expect(response.headers["cache-control"]).toBe("no-cache");
  });
});
