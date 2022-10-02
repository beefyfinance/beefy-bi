import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { loaderByChain$ } from "./loader-by-chain";

describe("loaderByChain$", () => {
  it("should process items by chain once", async () => {
    const processChain = (chain: string) => (input$: Rx.Observable<{ chain: string }>) =>
      input$.pipe(
        Rx.toArray(),
        Rx.map((items) => ({ chain, items })),
      );
    const pipeline$ = Rx.from([
      { chain: "arbitrum", productId: 10 },
      { chain: "arbitrum", productId: 11 },
      { chain: "arbitrum", productId: 12 },
      { chain: "bsc", productId: 20 },
      { chain: "bsc", productId: 21 },
      { chain: "bsc", productId: 22 },
      { chain: "celo", productId: 31 },
      { chain: "celo", productId: 32 },
    ] as { chain: Chain; productId: number }[]).pipe(loaderByChain$(processChain), Rx.toArray());
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      {
        chain: "arbitrum",
        items: [
          { chain: "arbitrum", productId: 10 },
          { chain: "arbitrum", productId: 11 },
          { chain: "arbitrum", productId: 12 },
        ],
      },
      {
        chain: "bsc",
        items: [
          { chain: "bsc", productId: 20 },
          { chain: "bsc", productId: 21 },
          { chain: "bsc", productId: 22 },
        ],
      },
      {
        chain: "celo",
        items: [
          { chain: "celo", productId: 31 },
          { chain: "celo", productId: 32 },
        ],
      },
    ]);
  });

  it("should process items by chain once from any order", async () => {
    const processChain = (chain: string) => (input$: Rx.Observable<{ chain: string }>) =>
      input$.pipe(
        Rx.toArray(),
        Rx.map((items) => ({ chain, items })),
      );
    const pipeline$ = Rx.from([
      { chain: "celo", productId: 31 },
      { chain: "arbitrum", productId: 10 },
      { chain: "bsc", productId: 21 },
      { chain: "arbitrum", productId: 11 },
      { chain: "bsc", productId: 22 },
      { chain: "arbitrum", productId: 12 },
      { chain: "bsc", productId: 20 },
      { chain: "celo", productId: 32 },
    ] as { chain: Chain; productId: number }[]).pipe(loaderByChain$(processChain), Rx.toArray());
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      {
        chain: "celo",
        items: [
          { chain: "celo", productId: 31 },
          { chain: "celo", productId: 32 },
        ],
      },
      {
        chain: "arbitrum",
        items: [
          { chain: "arbitrum", productId: 10 },
          { chain: "arbitrum", productId: 11 },
          { chain: "arbitrum", productId: 12 },
        ],
      },
      {
        chain: "bsc",
        items: [
          { chain: "bsc", productId: 21 },
          { chain: "bsc", productId: 22 },
          { chain: "bsc", productId: 20 },
        ],
      },
    ]);
  });
});
