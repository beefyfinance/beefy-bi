import { Chain } from "./chain";

export interface BeefyVault {
  id: string;
  token_name: string;
  token_decimals: number;
  token_address: string;
  want_address: string;
  want_decimals: number;
  eol: boolean;
  is_gov_vault: boolean;
  price_oracle: {
    want_oracleId: string;
    assets: string[];
  };
}

export interface BeefyFeeRecipientInfo {
  chain: Chain;
  contractAddress: string;
  strategist: string;
  beefyFeeRecipient: string;
}
