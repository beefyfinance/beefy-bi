import ethers from "ethers";
import { Observable } from "rxjs";
import { Chain } from "../types/chain";

export type TimeFilterBlockNumbers = number[];
export type TimeFilterDateRange = { from: Date; to: Date };
export type TimeFilterBlockNumberRange = { from: number; to: number };
export type TimeFilterLatest = "latest";

export type TimeFilter = TimeFilterBlockNumberRange | TimeFilterBlockNumbers | TimeFilterDateRange | TimeFilterLatest;

export interface TokenizedVaultUserAction {
  chain: Chain;
  vaultKey: string;
  investorAddress: string;
  blockNumber: number;
  blockUtcDate: Date;
  transactionHash: string;

  actionType: "deposit" | "withdraw" | "boost";

  sharesDiffAmount: ethers.BigNumber;
  sharesBalanceAfter: ethers.BigNumber;

  sharesToUnderlyingRate: ethers.BigNumber;
  underlyingBalanceDiff: ethers.BigNumber;
  underlyingBalanceAfter: ethers.BigNumber;
}

export interface TokenizedVaultConnector<VaultConfig extends { vaultKey: string }> {
  fetchVaultConfigs(): Observable<VaultConfig>;
  streamVaultUserActions(vaultConfig: VaultConfig, timeFilter: TimeFilter): Observable<TokenizedVaultUserAction>;
  streamVaultSharesToUnderlyingRate(
    vaultConfig: VaultConfig,
    timeFilter: TimeFilter
  ): Observable<TokenizedVaultUserAction>;
}
