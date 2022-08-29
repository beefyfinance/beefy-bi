import { TokenizedVaultConnector } from "../../../lib/connector";

export const beefyConnector: TokenizedVaultConnector<BeefyVault> = {
  fetchVaultConfigs: () => {},
};

/**
 * Hardcoded chain list: Subject<Chain>
 *   Fetch vaults from git: Observable<Vault>
 *     batch(1000)
 *     insert to db (beefy_vault) + insert to db (evm_address) + map vault ids
 *
 *
 *
 */

// for each chain, have a different pipeline
// for eac
//    - then, for each vault config, get the list of all the user actions
