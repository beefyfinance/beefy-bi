import { Chain } from "../../types/chain";

export interface BeefyVault {
  id: string;
  chain: Chain;
  token_name: string;
  token_decimals: number;
  contract_address: string;
  want_address: string;
  want_decimals: number;
  eol: boolean;
  is_gov_vault: boolean;
  assets: string[];
}

export interface BeefyFeeRecipientInfoAtBlock {
  chain: Chain;
  contractAddress: string;
  blockTag: number | "latest";
  beefyFeeRecipient: string | null;
  strategist: string;
}

export interface BeefyFeeRecipientInfo {
  chain: Chain;
  contractAddress: string;
  recipientsAtBlock: BeefyFeeRecipientInfoAtBlock[];
}
