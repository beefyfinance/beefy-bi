import { isArray } from "lodash";
import { Transform, TransformCallback } from "stream";
import { StringDecoder } from "string_decoder";

// adapted from https://futurestud.io/tutorials/node-js-filter-data-in-streams
export class StreamFilterTransform<RowType> extends Transform {
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

export class StreamBatchBy<RowType> extends Transform {
  protected currentBatchKey: string | number | null = null;
  protected currentBatch: RowType[] = [];

  constructor(protected getBatchKey: (row: RowType) => string | number) {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
  }

  _transform(row: RowType, encoding: string, next: TransformCallback): void {
    const rowKey = this.getBatchKey(row);
    if (this.currentBatchKey === null) {
      this.currentBatchKey = rowKey;
    }
    if (rowKey === this.currentBatchKey) {
      this.currentBatch.push(row);
      return next();
    }
    // flush current batch
    this.push(this.currentBatch);
    this.currentBatch = [row];
    this.currentBatchKey = rowKey;
    next();
  }
}

export class StreamAggBy<RowType> extends Transform {
  protected currentBatchKey: string | number | null = null;
  protected currentBatch: RowType[] = [];

  constructor(
    protected getBatchKey: (row: RowType) => string | number,
    protected aggregate: (rows: RowType[]) => RowType
  ) {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
  }

  _transform(row: RowType, encoding: string, next: TransformCallback): void {
    const rowKey = this.getBatchKey(row);
    if (this.currentBatchKey === null) {
      this.currentBatchKey = rowKey;
    }
    if (rowKey === this.currentBatchKey) {
      this.currentBatch.push(row);
      return next();
    }
    // flush current batch
    const aggregatedRow = this.aggregate(this.currentBatch);
    this.push(aggregatedRow);
    this.currentBatch = [row];
    this.currentBatchKey = rowKey;
    next();
  }
}

export class StreamBufferToLinesTransform extends Transform {
  protected rest = "";
  protected decoder: StringDecoder;

  constructor(protected lineSeparator = "\n", protected encoding = "utf8") {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
    this.decoder = new StringDecoder(encoding);
  }

  _transform(buffer: Buffer, encoding: string, next: TransformCallback): void {
    const lines = (this.rest + this.decoder.write(buffer)).split(this.lineSeparator);
    this.rest = lines.pop() || "";
    for (const line of lines) {
      this.push(line);
    }
    next();
  }
  _flush(next: TransformCallback) {
    const lines = (this.rest + this.decoder.end()).split(this.lineSeparator);
    for (const line of lines) {
      this.push(line);
    }
    this.rest = "";
    next();
  }
}

export class StreamLinesToBufferTransform extends Transform {
  protected lines: string[] = [];

  constructor(protected lineDelimiter: string = "\n", protected lineCount: number = 100) {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
  }

  _transform(line: string, encoding: string, next: TransformCallback): void {
    this.lines.push(line);
    if (this.lines.length >= this.lineCount) {
      this.push(Buffer.from(this.lines.join(this.lineDelimiter) + this.lineDelimiter));
      this.lines = [];
    }
    next();
  }
  _flush(next: TransformCallback) {
    this.push(Buffer.from(this.lines.join(this.lineDelimiter) + this.lineDelimiter));
    this.lines = [];
    next();
  }
}

export class StreamConsoleLogDebug<Input> extends Transform {
  constructor(protected consoleName: string = "line", protected transform: (item: Input) => any = (item) => item) {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });
  }

  _transform(item: Input, encoding: string, next: TransformCallback): void {
    console.log({ [this.consoleName]: this.transform(item) });
    next();
  }
}
