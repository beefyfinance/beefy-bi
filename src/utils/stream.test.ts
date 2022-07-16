import { PassThrough } from "stream";
import { FlattenStream, StreamAggBy, StreamBatchBy } from "./stream";

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
});
