import { ethers } from "ethers";
import { chunk } from "lodash";
import * as Rx from "rxjs";
import { isBeefyStandardVault } from "../protocol/beefy/utils/type-guard";
import { productList$ } from "../protocol/common/loader/product";
import { Chain } from "../types/chain";
import { ERC20AbiInterface } from "../utils/abi";
import { sleep } from "../utils/async";
import { RPC_API_KEY_ANKR } from "../utils/config";
import { DbClient, withDbClient } from "../utils/db";
import { addDebugLogsToProvider } from "../utils/ethers";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main(client: DbClient) {
  /**
   * Main goal it to find if we can use events from websocket apis to
   * - replace "recent" events polling
   * - use as source for EOL vaults
   */

  const chain: Chain = "polygon";
  const allProducts = await consumeObservable(
    productList$(client, "beefy", chain).pipe(
      Rx.filter((product) => product.productData.dashboardEol === false),
      Rx.filter(isBeefyStandardVault),
      Rx.toArray(),
      Rx.map((products) => products.slice(0, 100)),
    ),
  );
  if (allProducts === null) {
    throw new Error("no products");
  }

  // ankr's limitation is : 25 subscriptions per connection
  const maxSubscriptionsPerConnection = 25;

  const productGroups = chunk(allProducts, maxSubscriptionsPerConnection);
  for (const productGroup of productGroups) {
    const url = `wss://rpc.ankr.com/${chain}/ws/${RPC_API_KEY_ANKR}`;
    const provider = new ethers.providers.WebSocketProvider(url);
    addDebugLogsToProvider(provider, chain);

    for (const product of productGroup) {
      const contract = new ethers.Contract(product.productData.vault.contract_address, ERC20AbiInterface, provider);
      const eventFilter = contract.filters.Transfer();
      contract.on(eventFilter, (from, to, amount, event) => {
        console.log("event", event);
        console.log("from", from);
        console.log("to", to);
        console.log("amount", amount);
      });
      await sleep(200);
    }
  }

  await sleep(1000000000);
  return true;
}

runMain(withDbClient(main, { appName: "test-ws", logInfos: { msg: "test-ws" } }));
