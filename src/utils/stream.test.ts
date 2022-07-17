import { PassThrough, Readable } from "stream";
import {
  FlattenStream,
  StreamAggBy,
  StreamBatchBy,
  StreamBufferToLinesTransform,
  StreamLinesToBufferTransform,
} from "./stream";

describe("test stream helpers", () => {
  it("FlattenStream: should batch rows by key", () => {
    interface RowType {
      id: string;
      num: number;
    }
    const input = new PassThrough({ objectMode: true });
    const batchBy = new FlattenStream<RowType>();

    const res: RowType[] = [];
    input.pipe(batchBy).on("data", (row) => res.push(row));

    const rows: RowType[][] = Array.from({ length: 3 }).map((_, i) =>
      Array.from({ length: 4 }).map((_, j) => ({
        id: `${i}-${j}`,
        num: i * j,
      }))
    );

    for (const row of rows) {
      input.push(row);
    }
    expect(res.length).toEqual(12);
    expect(res).toMatchSnapshot();
  });

  it("StreamBatchBy: should batch rows by key", () => {
    interface RowType {
      id: string;
      num: number;
    }
    const input = new PassThrough({ objectMode: true });
    const batchBy = new StreamBatchBy<RowType>((row) => row.id);

    const res: RowType[][] = [];
    input.pipe(batchBy).on("data", (rows) => res.push(rows));

    const rows = Array.from({ length: 10 }).map((_, i) => ({
      id: `id${Math.floor(i / 3)}`,
      num: i,
    }));

    for (const row of rows) {
      input.push(row);
    }
    expect(res.length).toEqual(3); // we don't yield the last batch as we don't know if it's finished
    expect(res).toMatchSnapshot();
  });

  it("StreamAggBy: should aggregate rows by key", () => {
    interface RowType {
      id: string;
      num: number;
    }
    const input = new PassThrough({ objectMode: true });
    const batchBy = new StreamAggBy<RowType>(
      (row) => row.id,
      (rows) => ({
        id: rows[0].id,
        num: rows.reduce((acc, row) => acc + row.num, 0),
      })
    );

    const res: RowType[][] = [];
    input.pipe(batchBy).on("data", (rows) => res.push(rows));

    const rows = Array.from({ length: 10 }).map((_, i) => ({
      id: `id${Math.floor(i / 3)}`,
      num: i,
    }));

    for (const row of rows) {
      input.push(row);
    }
    expect(res.length).toEqual(3); // we don't yield the last batch as we don't know if it's finished
    expect(res).toMatchSnapshot();
  });

  it("StreamBufferToLinesTransform: parses buffer to lines properly", () => {
    const buffer = Buffer.from("hello\nworld\nlorem ipsum\nbut not this one");
    const toLines = new StreamBufferToLinesTransform();

    const res: string[] = [];
    toLines.on("data", (line) => res.push(line));

    toLines.write(buffer);

    // we don't want to yield last one yet as it may be truncated
    expect(res.length).toBe(3);
    expect(res).toMatchSnapshot();

    toLines.end();

    // now we expect the last one
    expect(res.length).toBe(4);
    expect(res).toMatchSnapshot();
  });

  it("StreamLinesToBufferTransform: should transform lines to buffer", () => {
    const lines = "hello\nworld\nlorem ipsum\nlorem ipsum\none more line";
    const toBuffer = new StreamLinesToBufferTransform("\n", 2 /* line count */);

    const res: Buffer[] = [];
    toBuffer.on("data", (buffer) => res.push(buffer));

    for (const line of lines.split("\n")) {
      toBuffer.write(line);
    }

    expect(res.length).toBe(2);
    expect(Buffer.concat(res).toString("utf8")).toMatchSnapshot();

    toBuffer.end();
    // now we expect the last one
    expect(res.length).toBe(3);
    expect(Buffer.concat(res).toString("utf8")).toMatchSnapshot();
  });
});
