import * as fs from "fs";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { logger } from "./logger";
import { onExit } from "./process";
import { CastingContext, parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { parse as asyncParser } from "csv-parse";
import * as readLastLines from "read-last-lines";
import { normalizeAddress } from "./ethers";

const CSV_SEPARATOR = ",";

export class CsvStore<RowType extends { datetime: Date }, TArgs extends any[]> {
  constructor(
    private readonly options: {
      loggerScope: string;
      getFilePath: (...args: TArgs) => string;
      csvColumns: {
        name: keyof RowType & string;
        type: "integer" | "address" | "date" | "float" | "string";
      }[];
    }
  ) {}

  /**
   * Append new rows to the end of the file.
   */
  public async getWriter(...args: TArgs): Promise<{
    writeBatch: (events: RowType[]) => Promise<void>;
    close: () => Promise<void>;
  }> {
    const filePath = this.options.getFilePath(...args);
    await makeDataDirRecursive(filePath);
    const writeStream = fs.createWriteStream(filePath, { flags: "a" });

    let closed = false;
    onExit(async () => {
      if (closed) return;
      logger.info(`[${this.options.loggerScope}] SIGINT, closing write object for ${JSON.stringify(args)}`);
      closed = true;
      writeStream.close();
    });

    return {
      writeBatch: async (events) => {
        if (closed) {
          logger.debug(`[${this.options.loggerScope}] write object closed for ${JSON.stringify(args)}, ignoring batch`);
          return;
        }
        const csvData = stringifySync(events, {
          delimiter: CSV_SEPARATOR,
          cast: {
            date: (date) => date.toISOString(),
          },
        });
        writeStream.write(csvData);
      },
      close: async () => {
        if (closed) {
          logger.warn(`[${this.options.loggerScope}] write object already closed for ${JSON.stringify(args)}`);
          return;
        }
        closed = true;
        logger.debug(`[${this.options.loggerScope}] closing write object for ${JSON.stringify(args)}`);
        writeStream.close();
      },
    };
  }

  /**
   * Efficiently find the last written row of the file.
   */
  async getLastRow(...args: TArgs): Promise<RowType | null> {
    const filePath = this.options.getFilePath(...args);
    if (!fs.existsSync(filePath)) {
      return null;
    }
    const lastImportedCSVRows = await readLastLines.read(filePath, 5);
    const data = syncParser(lastImportedCSVRows, {
      delimiter: CSV_SEPARATOR,
      columns: this.options.csvColumns.map((c) => c.name),
      cast: (value, context) => {
        const coltype = this.options.csvColumns[context.index].type;
        if (coltype === "integer") {
          return parseInt(value);
        } else if (coltype === "float") {
          return parseFloat(value);
        } else if (coltype === "address") {
          return normalizeAddress(value);
        } else if (coltype === "date") {
          return new Date(value);
        } else {
          return value;
        }
      },
      cast_date: true,
    });
    if (data.length === 0) {
      return null;
    }
    data.reverse();

    return data[0];
  }

  public getReadStream(...args: TArgs) {
    const filePath = this.options.getFilePath(...args);
    if (!fs.existsSync(filePath)) {
      return null;
    }
    const readStream = fs.createReadStream(filePath).pipe(
      asyncParser({
        delimiter: CSV_SEPARATOR,
        columns: this.options.csvColumns.map((c) => c.name),
        cast: (value: string, context: CastingContext) => {
          const coltype = this.options.csvColumns[context.index].type;
          if (coltype === "integer") {
            return parseInt(value);
          } else if (coltype === "float") {
            return parseFloat(value);
          } else if (coltype === "address") {
            return normalizeAddress(value);
          } else if (coltype === "date") {
            return new Date(value);
          } else {
            return value;
          }
        },
        cast_date: true,
      })
    );
    return readStream;
  }

  public async *getReadIterator(...args: TArgs): AsyncIterable<RowType> {
    const readStream = this.getReadStream(...args);
    if (!readStream) {
      return;
    }
    yield* readStream;

    readStream.destroy();
  }

  public async *getReadIteratorFrom(condition: (row: RowType) => boolean, ...args: TArgs) {
    const rows = this.getReadIterator(...args);
    if (!rows) {
      return;
    }
    for await (const row of rows) {
      if (condition(row)) {
        yield row;
      }
    }
  }

  public async getFirstRow(...args: TArgs): Promise<RowType | null> {
    const rows = this.getReadIterator(...args);
    if (!rows) {
      return null;
    }
    for await (const event of rows) {
      return event;
    }
    return null;
  }
}
