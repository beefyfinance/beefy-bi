import ethers from "ethers";
import { Observable } from "rxjs";
import { Chain } from "../../types/chain";

export type TimeFilterBlockNumbers = number[];
export type TimeFilterDateRange = { from: Date; to: Date };
export type TimeFilterBlockNumberRange = { from: number; to: number };
export type TimeFilterLatest = "latest";

export type TimeFilter = TimeFilterBlockNumberRange | TimeFilterBlockNumbers | TimeFilterDateRange | TimeFilterLatest;

export interface TokenizedVaultUserAction {
  chain: Chain;

  vaultAddress: string;

  // owner infos
  ownerAddress: string;

  // transaction infos
  blockNumber: number;
  transactionHash: string;

  sharesBalanceDiff: ethers.BigNumber;
}

export interface TokenizedVaultConnector<VaultConfig extends { vaultKey: string }> {
  fetchVaultConfigs(): Observable<VaultConfig>;
  streamVaultUserActions(vaultConfig: VaultConfig, timeFilter: TimeFilter): Observable<TokenizedVaultUserAction>;
  streamVaultSharesToUnderlyingRate(
    vaultConfig: VaultConfig,
    timeFilter: TimeFilter,
  ): Observable<TokenizedVaultUserAction>;
}
