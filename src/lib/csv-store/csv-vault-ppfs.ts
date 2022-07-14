import * as path from "path";
import { Chain } from "../../types/chain";
import { DATA_DIRECTORY } from "../../utils/config";
import { normalizeAddress } from "../../utils/ethers";
import { SamplingPeriod } from "../../types/sampling";
import { CsvStore } from "../../utils/csv-store";

export interface BeefyVaultV6PPFSData {
  blockNumber: number;
  datetime: Date;
  pricePerFullShare: string;
}

export const ppfsStore = new CsvStore<BeefyVaultV6PPFSData, [Chain, string, SamplingPeriod]>({
  loggerScope: "PPFS.STORE",
  getFilePath: (chain: Chain, contractAddress: string, samplingPeriod: SamplingPeriod) =>
    path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(contractAddress),
      "BeefyVaultV6",
      `ppfs_${samplingPeriod}.csv`
    ),
  csvColumns: [
    { name: "blockNumber", type: "integer" },
    { name: "datetime", type: "date" },
    { name: "pricePerFullShare", type: "string" },
  ],
});
