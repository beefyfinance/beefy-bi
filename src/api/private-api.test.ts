import { diContainer } from "@fastify/awilix";
import { asValue } from "awilix";
import { FastifyInstance } from "fastify";
import { sample } from "lodash";
import { API_PRIVATE_TOKENS } from "../utils/config";
import { DbClient } from "../utils/db";
import { ProgrammerError } from "../utils/programmer-error";
import { buildPrivateApi } from "./api";
import { BlockService } from "./service/block";
import { ExportService, InvalidExportCursorError } from "./service/export";

const AbstractCache: any = require("abstract-cache"); // todo: add or install types

describe("Private api tests", () => {
  async function buildMocks() {
    let app: FastifyInstance;
    let getBlockAroundADate: jest.Mock<ReturnType<BlockService["getBlockAroundADate"]>> = jest.fn();
    let getPage: jest.Mock<ReturnType<ExportService["getPage"]>, Parameters<ExportService["getPage"]>> = jest.fn();

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
          dataExport: asValue({ getPage } as any),
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

    return { app, getBlockAroundADate, getPage };
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
        block_number: 1,
      },
      {
        datetime: new Date("2022-10-23T12:12:12"),
        diff_sec: -124,
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

  describe("exports", () => {
    const headers = { authorization: "Bearer " + (sample(API_PRIVATE_TOKENS) as string) };
    const url = "/api/v1/export/vault-stats?from=2023-01-01T00:00:00Z&to=2023-01-02T00:00:00Z";

    it("should reject unauthenticated calls", async () => {
      const { app, getPage } = await buildMocks();
      const response = await app.inject({ method: "GET", url });
      expect(response.statusCode).toBe(401);
      expect(getPage).not.toHaveBeenCalled();
    });

    it("should serialize a page, keeping null, exact decimals and microseconds", async () => {
      const { app, getPage } = await buildMocks();
      const row = {
        datetime: "2023-01-01T00:00:00.123456Z",
        product_id: 1,
        block_number: 2,
        vault_total_supply: "123456789012345678901234567890.123456789012345678",
        share_to_underlying_price: null,
        staked_underlying: null,
        underlying_total_supply: null,
        underlying_capture_percentage: 0.5,
      };
      getPage.mockReturnValueOnce(Promise.resolve({ data: [{ ...row, not_documented: "secret" }], next_cursor: "abc" }));

      const response = await app.inject({ method: "GET", url, headers });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ data: [row], next_cursor: "abc" });
      expect(getPage.mock.calls[0][0]).toBe("vaultStats");
      expect(getPage.mock.calls[0][1].limit).toBe(1000);
    });

    it("should serialize a null capture percentage, the export of a stored NaN", async () => {
      const { app, getPage } = await buildMocks();
      const row = {
        datetime: "2024-07-31T18:42:06.000000Z",
        product_id: 1,
        block_number: 2,
        vault_total_supply: "0",
        share_to_underlying_price: "1",
        staked_underlying: "0",
        underlying_total_supply: "0",
        underlying_capture_percentage: null,
      };
      getPage.mockReturnValueOnce(Promise.resolve({ data: [row], next_cursor: null }));

      const response = await app.inject({ method: "GET", url, headers });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ data: [row], next_cursor: null });
    });

    it.each([
      ["inverted", "from=2023-01-02T00:00:00Z&to=2023-01-01T00:00:00Z"],
      ["empty", "from=2023-01-01T00:00:00Z&to=2023-01-01T00:00:00Z"],
      ["missing", "from=2023-01-01T00:00:00Z"],
      ["sql in a date", "from=2023-01-01T00:00:00Z'--&to=2023-01-02T00:00:00Z"],
    ])("should reject a time window that is %s", async (_, query) => {
      const { app, getPage } = await buildMocks();
      const response = await app.inject({ method: "GET", url: "/api/v1/export/vault-stats?" + query, headers });
      expect(response.statusCode).toBe(400);
      expect(getPage).not.toHaveBeenCalled();
    });

    it("should reject a page bigger than the maximum", async () => {
      const { app, getPage } = await buildMocks();
      const response = await app.inject({ method: "GET", url: url + "&limit=10001", headers });
      expect(response.statusCode).toBe(400);
      expect(getPage).not.toHaveBeenCalled();
    });

    it("should answer 400 on an invalid cursor", async () => {
      const { app, getPage } = await buildMocks();
      getPage.mockRejectedValueOnce(new InvalidExportCursorError("Invalid cursor"));
      const response = await app.inject({ method: "GET", url: url + "&cursor=bad", headers });
      expect(response.statusCode).toBe(400);
    });

    it("should document every export route", async () => {
      const { app } = await buildMocks();
      const spec = (await app.inject({ method: "GET", url: "/api/v1/openapi.json" })).json();
      const paths = Object.keys(spec.paths).filter((path) => path.startsWith("/api/v1/export/"));
      expect(paths).toHaveLength(9);

      const page = spec.paths[url.split("?")[0]].get.responses["200"].content["application/json"].schema;
      expect(page.properties.next_cursor.nullable).toBe(true);
      expect(page.properties.data.items.properties.vault_total_supply).toMatchObject({ type: "string", nullable: true });
      expect(page.properties.data.items.properties.underlying_capture_percentage).toMatchObject({ type: "number", nullable: true });
    });
  });
});
