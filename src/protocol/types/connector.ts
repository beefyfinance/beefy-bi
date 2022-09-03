import ethers from "ethers";
import { Observable } from "rxjs";
import { Decimal } from "decimal.js";
import { Chain } from "../../types/chain";

export type TimeFilterBlockNumbers = number[];
export type TimeFilterDateRange = { from: Date; to: Date };
export type TimeFilterBlockNumberRange = { from: number; to: number };
export type TimeFilterLatest = "latest";

export type TimeFilter = TimeFilterBlockNumberRange | TimeFilterBlockNumbers | TimeFilterDateRange | TimeFilterLatest;

export interface TokenizedVaultUserTransfer {
  chain: Chain;

  vaultAddress: string;
  sharesDecimals: number;

  // owner infos
  ownerAddress: string;

  // transaction infos
  blockNumber: number;
  transactionHash: string;

  sharesBalanceDiff: Decimal;
}

export interface TokenizedVaultConnector<VaultConfig extends { vaultKey: string }> {
  fetchVaultConfigs(): Observable<VaultConfig>;
  streamVaultUserActions(vaultConfig: VaultConfig, timeFilter: TimeFilter): Observable<TokenizedVaultUserTransfer>;
  streamVaultSharesToUnderlyingRate(
    vaultConfig: VaultConfig,
    timeFilter: TimeFilter,
  ): Observable<TokenizedVaultUserTransfer>;
}
