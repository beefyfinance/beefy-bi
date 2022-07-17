import * as path from "path";
import { Chain } from "../../types/chain";
import { DATA_DIRECTORY } from "../../utils/config";
import { normalizeAddress } from "../../utils/ethers";
import { CsvStore } from "../../utils/csv-store";

export interface ERC20EventData {
  blockNumber: number;
  datetime: Date;
  from: string;
  to: string;
  value: string;
}
export const erc20TransferStore = new CsvStore<ERC20EventData, [Chain, string]>({
  dateFieldPosition: 1,
  loggerScope: "ERC20.T.STORE",
  getFilePath: (chain: Chain, contractAddress: string) =>
    path.join(DATA_DIRECTORY, "chain", chain, "contracts", normalizeAddress(contractAddress), "ERC20", "Transfer.csv"),
  csvColumns: [
    { name: "blockNumber", type: "integer" },
    { name: "datetime", type: "date" },
    { name: "from", type: "string" },
    { name: "to", type: "string" },
    { name: "value", type: "string" },
  ],
});
