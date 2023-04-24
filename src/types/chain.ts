export type Chain =
  | "arbitrum"
  | "aurora"
  | "avax"
  | "bsc"
  | "canto"
  | "celo"
  | "cronos"
  | "emerald"
  | "ethereum"
  | "fantom"
  | "fuse"
  | "harmony"
  | "heco"
  | "kava"
  | "metis"
  | "moonbeam"
  | "moonriver"
  | "optimism"
  | "polygon"
  | "syscoin"
  | "zksync";

export const allChainIds: Chain[] = [
  "arbitrum",
  "aurora",
  "avax",
  "bsc",
  "canto",
  "celo",
  "cronos",
  "emerald",
  "ethereum",
  "fantom",
  "fuse",
  "harmony",
  "heco",
  "kava",
  "metis",
  "moonbeam",
  "moonriver",
  "optimism",
  "polygon",
  "syscoin",
  "zksync",
];

export interface ContractBlockInfos {
  chain: Chain;
  contractAddress: string;
  transactionHash: string;
  blockNumber: number;
  datetime: Date;
}
