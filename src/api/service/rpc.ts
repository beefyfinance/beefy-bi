import { PublicClient, createPublicClient, http } from "viem";
import {
  arbitrum,
  aurora,
  avalanche,
  base,
  bsc,
  canto,
  celo,
  cronos,
  fantom,
  fuse,
  gnosis,
  harmonyOne,
  kava,
  linea,
  mainnet,
  mantle,
  metis,
  moonbeam,
  moonriver,
  optimism,
  polygon,
  polygonZkEvm,
  rollux,
  scroll,
  zkSync,
  type Chain as ViemChain,
} from "viem/chains";
import { type Chain as BeefyChain } from "../../types/chain";
import { AsyncCache } from "./cache";

const VIEM_CHAINS: Record<BeefyChain, ViemChain | null> = {
  arbitrum: arbitrum,
  aurora: aurora,
  avax: avalanche,
  base: base,
  bsc: bsc,
  canto: canto,
  celo: celo,
  cronos: cronos,
  fantom: fantom,
  ethereum: mainnet,
  emerald: null,
  harmony: harmonyOne,
  heco: null,
  fuse: fuse,
  gnosis: gnosis,
  kava: kava,
  linea: linea,
  polygon: polygon,
  scroll: scroll,
  mantle: mantle,
  moonbeam: moonbeam,
  moonriver: moonriver,
  metis: metis,
  optimism: optimism,
  rollux: rollux,
  zkevm: polygonZkEvm,
  zksync: zkSync,
};

export class RpcService {
  protected clients: { [key in BeefyChain]?: PublicClient } = {};

  constructor(private services: { cache: AsyncCache }) {}

  public createPublicClientFromBeefyChain(chain: BeefyChain) {
    if (this.clients[chain]) {
      return this.clients[chain] as PublicClient;
    }

    const viemChain = VIEM_CHAINS[chain];
    if (!viemChain) {
      throw new Error(`Chain ${chain} not supported`);
    }

    return createPublicClient({
      chain: viemChain,
      transport: http(),
      batch: {
        multicall: true,
      },
    });
  }

  /**
   * Fetch the block datetime on chain
   */
  public async getBlockDatetime(chain: BeefyChain, blockNumber: number) {
    const ttlMs = 60 * 60 * 1000; // 1 hour
    const timestamp = await this.services.cache.wrap(`block-datetime-${chain}-${blockNumber}`, ttlMs, async () => {
      const client = this.createPublicClientFromBeefyChain(chain);
      const block = await client.getBlock({ blockNumber: BigInt(blockNumber) });
      return parseInt(block.timestamp.toString()) * 1000;
    });

    return new Date(timestamp);
  }
}