import * as fs from "fs";
import { makeDataDirRecursive } from "./fs";
import { logger } from "./logger";
import { onExit } from "./process";
import { CastingContext, parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { parse as asyncParser, Options as CsvParserOptions } from "csv-parse";
import * as readLastLines from "read-last-lines";
import { normalizeAddress } from "./ethers";
import { fileOrDirExists } from "./fs";
import {
  StreamBufferToLinesTransform,
  StreamConsoleLogDebug,
  StreamFilterTransform,
  StreamLinesToBufferTransform,
} from "./stream";

const CSV_FIELD_DELIMITER = ",";
const CSV_RECORD_DELIMITER = "\n";

export class CsvStore<RowType extends { datetime: Date }, TArgs extends any[]> {
  protected csvParserOptions: CsvParserOptions;

  constructor(
    private readonly options: {
      dateFieldPosition: number | null;
      loggerScope: string;
      getFilePath: (...args: TArgs) => string;
      csvColumns: {
        name: keyof RowType & string;
        type: "integer" | "address" | "date" | "float" | "string";
      }[];
    }
  ) {
    this.csvParserOptions = {
      delimiter: CSV_FIELD_DELIMITER,
      recordDelimiter: CSV_RECORD_DELIMITER,
      skipEmptyLines: true,
      columns: this.options.csvColumns.map((c) => c.name),
      cast: (value, context) => {
        if (this.options.csvColumns[context.index] === undefined) {
          console.log(value, context, this.options.csvColumns);
        }
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
    };
  }

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
          delimiter: CSV_FIELD_DELIMITER,
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
    if (!(await fileOrDirExists(filePath))) {
      return null;
    }
    const lastImportedCSVRows = await readLastLines.read(filePath, 5);
    const data = syncParser(lastImportedCSVRows, this.csvParserOptions);
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
    const readStream = fs.createReadStream(filePath).pipe(asyncParser(this.csvParserOptions));
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

  /**
   * Filter rows before going into the csv parser to make things faster.
   * For very large files we could search by dichotomy the start point, but on HDD that may be slower due to random access seeks
   */
  public getReadStreamAfterDate(after: Date, ...args: TArgs) {
    if (this.options.dateFieldPosition === null) {
      throw new Error(
        `Cannot filter by date if date field is not defined. Please define dateFieldPosition in the options`
      );
    }
    const filePath = this.options.getFilePath(...args);
    if (!fs.existsSync(filePath)) {
      return null;
    }
    const dateFieldPosition = this.options.dateFieldPosition;
    const dateStr = after.toISOString();
    let foundStartPoint = false;
    const readStream = fs
      .createReadStream(filePath)
      .pipe(new StreamBufferToLinesTransform())
      .pipe(
        new StreamFilterTransform<string>((row) => {
          // quick exit if we already found the start point
          if (foundStartPoint) return true;

          const parts = row.split(CSV_FIELD_DELIMITER);
          const date = parts[dateFieldPosition];
          foundStartPoint = date > dateStr;
          return foundStartPoint;
        })
      )
      .pipe(new StreamLinesToBufferTransform(CSV_RECORD_DELIMITER, 1000))
      /*.pipe(
        new StreamConsoleLogDebug<Buffer>("after-batch", (buffer) =>
          buffer.toString("utf8").split("").reverse().slice(0, 200).reverse().join("")
        )
      )*/
      .pipe(asyncParser(this.csvParserOptions));
    return readStream;
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
