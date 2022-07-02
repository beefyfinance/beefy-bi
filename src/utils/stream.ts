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
