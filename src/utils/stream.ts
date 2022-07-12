import { isArray } from "lodash";
import { Transform, TransformCallback } from "stream";
import * as fs from "fs";
import * as readline from "readline";

// adapted from https://futurestud.io/tutorials/node-js-filter-data-in-streams
export class StreamObjectFilterTransform<RowType> extends Transform {
  constructor(protected includeRow: (row: RowType) => boolean) {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
  }

  _transform(row: RowType, encoding: string, next: TransformCallback): void {
    if (this.includeRow(row)) {
      return next(null, row);
    }
    next();
  }
}

// adapted from https://futurestud.io/tutorials/node-js-filter-data-in-streams
export class FlattenStream<RowType> extends Transform {
  constructor() {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
  }

  _transform(rows: RowType[], encoding: string, next: TransformCallback): void {
    if (!isArray(rows)) {
      throw new Error(`Supplied input rows is not an array`);
    }
    for (const row of rows) {
      this.push(row);
    }
    next();
  }
}

export async function getFirstLineOfFile(pathToFile: string): Promise<string> {
  const readable = fs.createReadStream(pathToFile);
  const reader = readline.createInterface({ input: readable });
  const line = await new Promise<string>((resolve) => {
    reader.on("line", (line) => {
      reader.close();
      resolve(line);
    });
  });
  readable.close();
  return line;
}
