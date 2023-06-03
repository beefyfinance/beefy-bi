import { diContainer } from "@fastify/awilix";
import { asValue } from "awilix";
import { FastifyInstance } from "fastify";
import { sample } from "lodash";
import { Chain } from "../types/chain";
import { API_PRIVATE_TOKENS } from "../utils/config";
import { DbClient } from "../utils/db";
import { ProgrammerError } from "../utils/programmer-error";
import { buildPrivateApi } from "./api";
import { BlockService } from "./service/block";

const AbstractCache: any = require("abstract-cache"); // todo: add or install types

describe("Private api tests", () => {
  async function buildMocks() {
    let app: FastifyInstance;
    let getBlockAroundADate: jest.Mock<ReturnType<BlockService["getBlockAroundADate"]>> = jest.fn();

    const abCache = AbstractCache({ useAwait: false });
    app = await buildPrivateApi({
      rateLimit: {
        timeWindow: "1 minute",
        max: 1, // should be 1 per test
      },
      registerDI: async () => {
        const ni = () => {
          throw new ProgrammerError("Not implemented");
        };
        await diContainer.dispose();

        getBlockAroundADate.mockClear();

        diContainer.register({
          db: asValue({ connect: ni, on: ni, end: ni, query: ni } as DbClient),
          investor: asValue(null as any),
          beefy: asValue(null as any),
          product: asValue(null as any),
          price: asValue(null as any),
          block: asValue({ getBlockAroundADate } as any),
          importState: asValue(null as any),
          abCache: asValue(abCache),
          cache: asValue(null as any),
          redis: asValue(null as any),
        });
      },
    });

    return { app, getBlockAroundADate };
  }

  /**
   * We have to use it to make sure we only run 1 test at a time
   * This is because awilix is a global instance and
   * there is no way to inject the container on a specific fastify instance
   */

  it("should expose a public openapi json endpoint", async () => {
    const { app } = await buildMocks();
    const response = await app.inject({
      method: "GET",
      url: "/api/v1/openapi.json",
    });
    expect(response.statusCode).toBe(200);
  });

  it("should expose a private blocks endpoint, and reject unauthenticated calls", async () => {
    const { app } = await buildMocks();

    const response = await app.inject({
      method: "GET",
      url: "/api/v1/block/around-a-date?chain=avax&utc_datetime=2022-10-22T12:12:12",
    });

    expect(response.statusCode).toBe(401);
  });

  it("should expose a private blocks endpoint, and reject calls with bad auth", async () => {
    const { app } = await buildMocks();

    const response = await app.inject({
      method: "GET",
      url: "/api/v1/block/around-a-date?chain=avax&utc_datetime=2022-10-22T12:12:12",
      headers: {
        authorization: "Bearer badtoken",
      },
    });

    expect(response.statusCode).toBe(401);
  });

  it("should expose a private blocks endpoint, and accept calls with the right auth", async () => {
    const { app, getBlockAroundADate } = await buildMocks();

    const data = [
      {
        datetime: new Date("2022-10-22T12:12:12"),
        diff_sec: 123,
        chain: "avax" as Chain,
        block_number: 1,
      },
      {
        datetime: new Date("2022-10-23T12:12:12"),
        diff_sec: -124,
        chain: "avax" as Chain,
        block_number: 2,
      },
    ];
    getBlockAroundADate.mockReturnValueOnce(Promise.resolve(data));

    const response = await app.inject({
      method: "GET",
      url: "/api/v1/block/around-a-date?chain=avax&utc_datetime=2022-10-22T12:12:12",
      headers: {
        authorization: "Bearer " + (sample(API_PRIVATE_TOKENS) as string),
      },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual(JSON.parse(JSON.stringify(data)));
  });
});
