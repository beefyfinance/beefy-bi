// idk why but I couldn't find a json abi type in ethers
export type JsonAbi = {
  inputs?: {
    internalType: string;
    name: string;
    type: string;
    indexed?: boolean;
  }[];
  stateMutability?: string;
  type: string;
  anonymous?: boolean;
  name?: string;
  outputs?: {
    internalType: string;
    name: string;
    type: string;
  }[];
}[];
