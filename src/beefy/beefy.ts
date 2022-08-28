import { TokenizedVaultConnector } from "../lib/connector";

export interface BeefyVault {
  vaultKey: string;
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

export const beefyConnector: TokenizedVaultConnector<BeefyVault> = {
  fetchVaultConfigs: () => {},
};
