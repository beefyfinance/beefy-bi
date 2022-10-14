import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { DbBeefyProduct, DbProduct } from "../../common/loader/product";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { addMissingImportState$ } from "../../common/loader/import-state";
import { rootLogger } from "../../../utils/logger";
import { fetchContractCreationInfos$ } from "../../common/connector/contract-creation";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { createRpcConfig } from "../../common/utils/rpc-config";


async function main() {
    const logger = rootLogger.child({ module: "beefy", component: "import-historical-data" });
    const rpcConfig = createRpcConfig(chain);
  
    const streamConfig: BatchStreamConfig = {
      // since we are doing many historical queries at once, we cannot afford to do many at once
      workConcurrency: 1,
      // But we can afford to wait a bit longer before processing the next batch to be more efficient
      maxInputWaitMs: 30 * 1000,
      maxInputTake: 500,
      // and we can affort longer retries
      maxTotalRetryMs: 30_000,
    };
    const { observable: productErrors$, next: emitErrors, complete: completeProductErrors$ } = createObservableWithNext<ImportQuery<DbProduct>>();
  
    const getImportStateKey = (productId: number) => `product:investment:${productId}`;
  productList().pipe(
    Rx.pipe(
        // add typings to the input item
        Rx.filter((_: DbBeefyProduct) => true),
    
        addMissingImportState$({
          client,
          getImportStateKey: (item) => getImportStateKey(item.productId),
          addDefaultImportData$: (formatOutput) =>
            Rx.pipe(
              // initialize the import state
              // find the contract creation block
              fetchContractCreationInfos$({
                rpcConfig: rpcConfig,
                getCallParams: (item) => ({
                  chain: chain,
                  contractAddress:
                    item.productData.type === "beefy:boost" ? item.productData.boost.contract_address : item.productData.vault.contract_address,
                }),
                formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
              }),
    
              // drop those without a creation info
              excludeNullFields$("contractCreationInfo"),
    
              Rx.map((item) =>
                formatOutput(item, {
                  type: "product:investment",
                  productId: item.productId,
                  chain: item.chain,
                  chainLatestBlockNumber: item.contractCreationInfo.blockNumber,
                  contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
                  contractCreationDate: item.contractCreationInfo.datetime,
                  ranges: {
                    lastImportDate: new Date(),
                    coveredRanges: [],
                    toRetry: [],
                  },
                }),
              ),
            ),
          formatOutput: (product, importState) => ({ target: product, importState }),
        }),
  )
}

runMain(main);
