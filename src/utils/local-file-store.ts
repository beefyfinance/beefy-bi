import { backOff } from "exponential-backoff";
import * as fs from "fs";
import { cloneDeep, isArray } from "lodash";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { getRedlock } from "../lib/shared-resources/shared-lock";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";
import { LOG_LEVEL } from "./config";
import { getFirstLineOfFile } from "./stream";
import { logger } from "./logger";

export class LocalFileStore<
  TRes,
  TArgs extends any[],
  TFormat extends "json" | "jsonl",
  TEmpty extends TFormat extends "json" ? null : TRes
> {
  protected empty: TEmpty;

  constructor(
    protected readonly options: {
      loggerScope: string;
      doFetch: (...parameters: TArgs) => Promise<TRes>;
      getLocalPath: (...parameters: TArgs) => string;
      format: "json" | "jsonl";
      getResourceId: (...parameters: TArgs) => string;
      ttl_ms: number | null;
    }
  ) {
    this.empty = (this.options.format === "jsonl" ? [] : null) as any as TEmpty;
  }

  protected async doWrite(data: TRes, filePath: string): Promise<TRes> {
    if (this.options.format === "json") {
      await fs.promises.writeFile(filePath, JSON.stringify(data));
      return data;
    } else {
      if (!isArray(data)) {
        throw new Error("Local file store expecting an array to write");
      }
      const jsonl = data.map((obj) => JSON.stringify(obj)).join("\n");
      await fs.promises.writeFile(filePath, jsonl);
      return data;
    }
  }

  protected async doRead(filePath: string): Promise<TRes> {
    if (this.options.format === "json") {
      const content = await fs.promises.readFile(filePath, "utf8");
      const data: TRes = JSON.parse(content);
      return data;
    } else {
      let content = await fs.promises.readFile(filePath, "utf8");
      const data: TRes = content
        .trim()
        .split("\n")
        .map((obj) => JSON.parse(obj)) as any as TRes;
      return data;
    }
  }

  protected async localDataExists(...args: TArgs) {
    const filePath = this.options.getLocalPath(...args);

    if (!fs.existsSync(filePath)) {
      return false;
    }

    if (this.options.format === "json") {
      let content = await fs.promises.readFile(filePath, "utf8");
      content = content.trim();
      return content !== "";
    } else {
      let content = await getFirstLineOfFile(filePath);
      content = content.trim();
      return content !== "";
    }
  }

  /**
   * Serves the local version if ttl is not expired, otherwise fetches the remote version and stores it locally
   * While fetching the remote version, get a shared lock
   */
  async fetchData(...args: TArgs): Promise<TRes> {
    const localPath = this.options.getLocalPath(...args);
    const hasLocalData = await this.localDataExists(...args);
    if (hasLocalData) {
      // resolve ttl
      let shouldServeCache = true;
      if (this.options.ttl_ms !== null) {
        const now = new Date();
        const localStat = await fs.promises.stat(localPath);
        const lastModifiedDate = localStat.mtime;
        if (now.getTime() - lastModifiedDate.getTime() > this.options.ttl_ms) {
          shouldServeCache = false;
        }
      }

      if (shouldServeCache) {
        logger.debug(
          `[${
            this.options.loggerScope
          }] Local cache file exists and is not expired, returning it's content for ${JSON.stringify(args)}`
        );
        return this.doRead(localPath);
      } else {
        logger.debug(
          `[${this.options.loggerScope}] Local cache file exists but is expired for ${JSON.stringify(
            args
          )}, fetching remote version`
        );
      }
    }
    const resourceId = "fetch:" + this.options.getResourceId(...args);
    try {
      return this.forceFetchData(...args);
    } catch (error) {
      if (hasLocalData) {
        logger.warn(
          `[${
            this.options.loggerScope
          }] Could not reload local data after ttl expired: ${resourceId}. Serving local data anyway. ${JSON.stringify(
            error
          )}`
        );
        return this.doRead(localPath);
      } else {
        throw error;
      }
    }
  }

  /**
   * Fetches the remote data, do not store it locally
   */
  public async forceFetchData(...args: TArgs) {
    const localPath = this.options.getLocalPath(...args);
    const redlock = await getRedlock();
    const resourceId = "fetch:" + this.options.getResourceId(...args);
    const data = await backOff(
      () => redlock.using([resourceId], 2 * 60 * 1000, async () => this.options.doFetch(...args)),
      {
        delayFirstAttempt: false,
        jitter: "full",
        maxDelay: 5 * 60 * 1000,
        numOfAttempts: 10,
        retry: (error, attemptNumber) => {
          const message = `[${this.options.loggerScope}] Error on attempt ${attemptNumber} fetching ${resourceId}: ${error.message}`;
          if (attemptNumber < 3) logger.verbose(message);
          else if (attemptNumber < 5) logger.info(message);
          else if (attemptNumber < 8) logger.warn(message);
          else logger.error(message);

          if (LOG_LEVEL === "trace") {
            console.error(error);
          }
          // some errors are not recoverable
          if (error instanceof ArchiveNodeNeededError) {
            return false;
          }
          return true;
        },
        startingDelay: 200,
        timeMultiple: 2,
      }
    );
    logger.debug(`[${this.options.loggerScope}] Got new data for ${resourceId}, writing it and returning it`);
    await makeDataDirRecursive(localPath);

    return this.doWrite(data, localPath);
  }

  public async getLocalData(...args: TArgs) {
    if (await this.localDataExists(...args)) {
      const filePath = this.options.getLocalPath(...args);
      return this.doRead(filePath);
    } else {
      return cloneDeep(this.empty);
    }
  }
}
