import { ExportService, InvalidExportCursorError, exportDatasets } from "./export";

function buildService(rows: any[] = []) {
  const query = jest.fn(async (_: string) => ({ rows, rowCount: rows.length }));
  return { query, service: new ExportService({ db: { query } as any }) };
}

const window = { from: new Date("2023-01-01T00:00:00Z"), to: new Date("2023-01-02T00:00:00Z") };

describe("Export service", () => {
  it("should select every documented column and paginate on the dataset key", async () => {
    const { query, service } = buildService();
    await service.getPage("vaultStats", { limit: 10, ...window });
    const sql = query.mock.calls[0][0];

    for (const col of exportDatasets.vaultStats.columns) {
      expect(sql).toContain(col.name);
    }
    expect(sql).toContain("t.datetime >= '2023-01-01T00:00:00.000Z'");
    expect(sql).toContain("t.datetime < '2023-01-02T00:00:00.000Z'");
    expect(sql).toContain("ORDER BY t.datetime, t.product_id, t.block_number");
    expect(sql).toContain("LIMIT '10'");
  });

  it("should output iso 8601 datetimes and prefixed hex without shadowing the order column", async () => {
    const { query, service } = buildService();
    await service.getPage("investorTimeline", { limit: 10, ...window });
    const sql = query.mock.calls[0][0];
    expect(sql).toContain(`to_char(t.datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as datetime`);
    expect(sql).toContain(`'0x' || encode(t.transaction_hash::bytea, 'hex') as transaction_hash`);
  });

  it("should select non finite doubles as null, on every dataset", async () => {
    for (const [name, dataset] of Object.entries(exportDatasets)) {
      const { query, service } = buildService();
      await service.getPage(name as keyof typeof exportDatasets, { limit: 10, ...window });
      const sql = query.mock.calls[0][0];

      for (const col of (dataset.columns as { name: string; type: string; nullable?: boolean }[]).filter((col) => col.type === "double")) {
        expect(col.nullable).toBe(true);
        expect(sql).toContain(
          `NULLIF(NULLIF(NULLIF(t.${col.name}, 'NaN'::double precision), 'Infinity'::double precision), '-Infinity'::double precision) as ${col.name}`,
        );
      }
    }
  });

  it("should give a cursor on full pages only, and resume after it", async () => {
    const row = { datetime: "2023-01-01T00:00:00.123456Z", product_id: 1, block_number: 2 };

    const lastPage = await buildService([row]).service.getPage("vaultStats", { limit: 2, ...window });
    expect(lastPage.next_cursor).toBeNull();

    const fullPage = await buildService([row]).service.getPage("vaultStats", { limit: 1, ...window });
    expect(fullPage.next_cursor).not.toBeNull();

    const { query, service } = buildService();
    await service.getPage("vaultStats", { limit: 1, cursor: fullPage.next_cursor as string, ...window });
    expect(query.mock.calls[0][0]).toContain(
      "(t.datetime, t.product_id, t.block_number) > ('2023-01-01T00:00:00.123456Z'::timestamptz, '1'::integer, '2'::integer)",
    );
  });

  it.each([
    ["not base64 json", "nope"],
    ["the wrong size", Buffer.from(JSON.stringify([1, 2])).toString("base64url")],
    ["sql in an id", Buffer.from(JSON.stringify(["2023-01-01T00:00:00.000000Z", "1'; drop table investor; --", 2])).toString("base64url")],
    ["sql in a date", Buffer.from(JSON.stringify(["2023'; --", 1, 2])).toString("base64url")],
  ])("should reject a cursor with %s", async (_, cursor) => {
    const { query, service } = buildService();
    await expect(service.getPage("vaultStats", { limit: 1, cursor, ...window })).rejects.toThrow(InvalidExportCursorError);
    expect(query).not.toHaveBeenCalled();
  });

  it("should never export investor_data", async () => {
    const { query, service } = buildService();
    await service.getPage("investors", { limit: 1 });
    expect(query.mock.calls[0][0]).not.toContain("investor_data");
    expect(query.mock.calls[0][0]).not.toContain("*");
  });
});
