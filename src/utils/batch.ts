export async function* batchAsyncStream<TItem>(
  stream: AsyncIterable<TItem>,
  batchSize: number = 100
): AsyncIterable<TItem[]> {
  let batch: TItem[] = [];
  for await (const elem of stream) {
    batch.push(elem);
    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }
  if (batch.length > 0) {
    yield batch;
  }
}
