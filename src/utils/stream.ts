import { isArray } from "lodash";
import { Transform, TransformCallback } from "stream";

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
