export type Chain =
  | "arbitrum"
  | "aurora"
  | "avax"
  | "bsc"
  | "celo"
  | "cronos"
  | "emerald"
  | "fantom"
  | "fuse"
  | "harmony"
  | "heco"
  | "metis"
  | "moonbeam"
  | "moonriver"
  | "optimism"
  | "polygon"
  | "syscoin";

export const allChainIds: Chain[] = [
  "arbitrum",
  "aurora",
  "avax",
  "bsc",
  "celo",
  "cronos",
  "emerald",
  "fantom",
  "fuse",
  "harmony",
  "heco",
  "metis",
  "moonbeam",
  "moonriver",
  "optimism",
  "polygon",
  "syscoin",
];

export interface ContractBlockInfos {
  chain: Chain;
  contractAddress: string;
  transactionHash: string;
  blockNumber: number;
  datetime: Date;
}
