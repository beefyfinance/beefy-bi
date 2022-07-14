import * as path from "path";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { CsvStore } from "../utils/csv-store";

export interface ERC20TransferFromEventData {
  blockNumber: number;
  datetime: Date;
  to: string;
  value: string;
}

export const erc20TransferFromStore = new CsvStore<ERC20TransferFromEventData, [Chain, string, string]>({
  loggerScope: "ERC20.TF.STORE",
  getFilePath: (chain: Chain, fromContractAddress: string, tokenAddress: string) =>
    path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(fromContractAddress),
      "ERC20_from_self",
      normalizeAddress(tokenAddress),
      "Transfer.csv"
    ),
  csvColumns: [
    { name: "blockNumber", type: "integer" },
    { name: "datetime", type: "date" },
    { name: "to", type: "string" },
    { name: "value", type: "string" },
  ],
});
