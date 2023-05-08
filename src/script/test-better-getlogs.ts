import { ethers } from "ethers";
import { EventFragment } from "ethers/lib/utils";
import { chunk, cloneDeep, keyBy } from "lodash";
import * as Rx from "rxjs";
import { Hex, decodeEventLog, encodeEventTopics, getEventSelector, parseAbi } from "viem";
import { EventDefinition } from "viem/dist/types/types";
import { fetchBeefyPPFS$ } from "../protocol/beefy/connector/ppfs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { getProductContractAddress } from "../protocol/beefy/utils/contract-accessors";
import { productList$ } from "../protocol/common/loader/product";
import { ImportBehaviour, ImportCtx, createBatchStreamConfig, defaultImportBehaviour } from "../protocol/common/types/import-context";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { BeefyBoostAbiInterface, BeefyVaultV6AbiInterface } from "../utils/abi";
import { DbClient, withDbClient } from "../utils/db";
import { ContractWithMultiAddressGetLogs, MultiAddressEventFilter } from "../utils/ethers";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main(client: DbClient) {
  const options = {
    mode: "historical",
    chain: "zksync" as Chain,
    disableWorkConcurrency: true,
    //forceRpcUrl: "https://rpc.ankr.com/eth",
    rpcCount: "all",
    ignoreImportState: true,
    skipRecentWindowWhenHistorical: "all", // by default, live products recent data is done by the recent import
  };
  const behaviour: ImportBehaviour = { ...cloneDeep(defaultImportBehaviour), mode: "historical" };
  const rpcConfig = getMultipleRpcConfigsForChain({ chain: options.chain, behaviour })[0];

  const provider = rpcConfig.linearProvider;

  console.log(provider.connection.url);

  const allProducts = await consumeObservable(
    productList$(client, "beefy", options.chain).pipe(
      Rx.filter((product) => product.productData.dashboardEol === false),
      Rx.toArray(),
      Rx.map((products) => products.slice(0, 100)),
    ),
  );
  if (allProducts === null) {
    throw new Error("no products");
  }

  const productBatches = chunk(allProducts, rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize || 1);

  const emptyInterface = new ethers.utils.Interface([]);
  encodeEventTopics;

  const eventDefinitions = [
    "event Transfer(address indexed from, address indexed to, uint256 amount)",
    "event Staked(address indexed user, uint256 amount)",
    "event Withdrawn(address indexed user, uint256 amount)",
  ] as const;
  const eventConfigs = eventDefinitions.map((eventDefinition) => ({
    selector: getEventSelector(eventDefinition),
    abi: parseAbi([eventDefinition]),
  }));
  const eventConfigsByTopic = keyBy(eventConfigs, (c) => c.selector);

  // instanciate any ERC20 contract to get the event filter topics
  for (const productBatch of productBatches) {
    const productAddresses = productBatch.map(getProductContractAddress);
    const addresses = productAddresses.length <= 1 ? productAddresses[0] : productAddresses;
    const filter: MultiAddressEventFilter = {
      address: addresses,
      topics: [eventConfigs.map((e) => e.selector)],
      fromBlock: 2789542 - 50000,
      toBlock: 2789542,
    };
    console.log(filter);

    const logs = await provider.getLogsMultiAddress(filter);

    console.log(logs);
    const events = logs.map((log) => {
      const eventConfig = eventConfigsByTopic[log.topics[0]];
      console.log(eventConfig);
      return {
        address: log.address,
        blockNumber: log.blockNumber,
        removed: log.removed,
        transactionHash: log.transactionHash,
        ...decodeEventLog({
          abi: eventConfig.abi,
          data: log.data as Hex,
          topics: log.topics as any,
        }),
      };
    });
    console.log(events);
  }
}
runMain(withDbClient(main, { appName: "test-ws", logInfos: { msg: "test-ws" } }));
