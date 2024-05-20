import { removeSecretsFromRpcUrl } from "./remove-secrets-from-rpc-url";

describe("test we remove secrets from rpc urls", () => {
  it("should format an url without secret properly", () => {
    expect(removeSecretsFromRpcUrl("bsc", "https://user:password@localhost:8545")).toBe("https://localhost:8545");

    expect(removeSecretsFromRpcUrl("aurora", "https://mainnet.aurora.dev/xxxxXXXXXXxxxxXXXXXXxxxXXXXXXXXX")).toBe(
      "https://mainnet.aurora.dev/<RPC_API_KEY_AURORA>",
    );
    expect(removeSecretsFromRpcUrl("avax", "https://api.avax.network/ext/bc/C/rpc")).toBe("https://api.avax.network/ext/bc/C/rpc");
    expect(removeSecretsFromRpcUrl("avax", "https://rpc.ankr.com/avalanche/")).toBe("https://rpc.ankr.com/avalanche");
    expect(removeSecretsFromRpcUrl("avax", "https://rpc.ankr.com/avalanche")).toBe("https://rpc.ankr.com/avalanche");
    expect(removeSecretsFromRpcUrl("bsc", "https://rpc.ankr.com/bsc/xxxXXXxXXXXXXxXXXXXXxXXX")).toBe("https://rpc.ankr.com/bsc/<RPC_API_KEY_ANKR>");
    expect(removeSecretsFromRpcUrl("emerald", "https://emerald.oasis.dev")).toBe("https://emerald.oasis.dev");
    expect(removeSecretsFromRpcUrl("fantom", "https://rpc.ftm.tools")).toBe("https://rpc.ftm.tools");
    expect(removeSecretsFromRpcUrl("fantom", "https://rpcapi.fantom.network")).toBe("https://rpcapi.fantom.network");
    expect(removeSecretsFromRpcUrl("fuse", "https://explorer-node.fuse.io/")).toBe("https://explorer-node.fuse.io");
    expect(removeSecretsFromRpcUrl("harmony", "https://rpc.ankr.com/harmony/")).toBe("https://rpc.ankr.com/harmony");
    expect(removeSecretsFromRpcUrl("heco", "https://http-mainnet.hecochain.com")).toBe("https://http-mainnet.hecochain.com");
    expect(removeSecretsFromRpcUrl("metis", "https://andromeda.metis.io/?owner=1234")).toBe(
      "https://andromeda.metis.io/?owner=<RPC_API_KEY_METIS_OWNER>",
    );
    expect(removeSecretsFromRpcUrl("moonbeam", "https://rpc.api.moonbeam.network")).toBe("https://rpc.api.moonbeam.network");
    expect(removeSecretsFromRpcUrl("moonriver", "https://rpc.api.moonriver.moonbeam.network/")).toBe("https://rpc.api.moonriver.moonbeam.network");
    expect(removeSecretsFromRpcUrl("optimism", "https://opt-mainnet.g.alchemy.com/v2/XXXxxXXXXXxxxXXXXXXxxxxxx")).toBe(
      "https://opt-mainnet.g.alchemy.com/v2/<RPC_API_KEY_ALCHEMY_OPTIMISM>",
    );
    expect(removeSecretsFromRpcUrl("arbitrum", "https://arb-mainnet.g.alchemy.com/v2/XXXxxXXXXXxxxXXXXXXxxxxxx")).toBe(
      "https://arb-mainnet.g.alchemy.com/v2/<RPC_API_KEY_ALCHEMY_ARBITRUM>",
    );
    expect(removeSecretsFromRpcUrl("moonriver", "https://moonriver.api.onfinality.io/public")).toBe("https://moonriver.api.onfinality.io/public");
    expect(removeSecretsFromRpcUrl("polygon", "https://polygon-rpc.com/")).toBe("https://polygon-rpc.com");
    expect(removeSecretsFromRpcUrl("bsc", "https://bsc-mainnet.nodereal.io/v1/XXXxxXXXXXxxxXXXXXXxxxxxx")).toBe(
      "https://bsc-mainnet.nodereal.io/v1/<RPC_API_KEY_NODEREAL>",
    );
    expect(removeSecretsFromRpcUrl("celo", "https://celo-mainnet--rpc.datahub.figment.io/apikey/XXXxxXXXXXxxxXXXXXXxxxxxx/")).toBe(
      "https://celo-mainnet--rpc.datahub.figment.io/apikey/<RPC_API_KEY_FIGMENT>",
    );
    expect(removeSecretsFromRpcUrl("bsc", "https://bsc.getblock.io/XXXxxXXXXXxxxXXXXXXxxxxxx/mainnet/")).toBe(
      "https://bsc.getblock.io/<RPC_API_KEY_GETBLOCK>/mainnet",
    );
    expect(removeSecretsFromRpcUrl("optimism", "https://optimism-mainnet.infura.io/v3/XXXxxXXXXXxxxXXXXXXxxxxxx")).toBe(
      "https://optimism-mainnet.infura.io/v3/<RPC_API_KEY_INFURA>",
    );
    expect(removeSecretsFromRpcUrl("arbitrum", "https://arbitrum-mainnet.infura.io/v3/XXXxxXXXXXxxxXXXXXXxxxxxx")).toBe(
      "https://arbitrum-mainnet.infura.io/v3/<RPC_API_KEY_INFURA>",
    );
    expect(removeSecretsFromRpcUrl("cronos", "https://ab-000-000-000.p2pify.com/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_CHAINSTACK_CRONOS>",
    );
    expect(
      removeSecretsFromRpcUrl("arbitrum", "https://something-smth-s0mething.arbitrum-mainnet.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX"),
    ).toBe("<RPC_API_URL_QUIKNODE_ARBITRUM>");
    expect(removeSecretsFromRpcUrl("avax", "https://something-smth-s0mething.avalanche-mainnet.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_AVAX>",
    );
    expect(removeSecretsFromRpcUrl("base", "https://something-smth-s0mething.base-mainnet.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_BASE>",
    );
    expect(removeSecretsFromRpcUrl("fantom", "https:///something-smth-s0mething.fantom.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_FANTOM>",
    );
    expect(removeSecretsFromRpcUrl("gnosis", "https://something-smth-s0mething.xdai.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_GNOSIS>",
    );
    expect(removeSecretsFromRpcUrl("mantle", "https:///something-smth-s0mething.mantle-mainnet.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_MANTLE>",
    );
    expect(removeSecretsFromRpcUrl("polygon", "https:///something-smth-s0mething.matic.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_POLYGON>",
    );
    expect(removeSecretsFromRpcUrl("zkevm", "https:///something-smth-s0mething.zkevm-mainnet.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_ZKEVM>",
    );
    expect(removeSecretsFromRpcUrl("zksync", "https:///something-smth-s0mething.zksync-mainnet.quiknode.pro/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "<RPC_API_URL_QUIKNODE_ZKSYNC>",
    );

    expect(removeSecretsFromRpcUrl("ethereum", "https://eth.llamarpc.com/rpc/xxxxxxxxxxxxxXXxXXXXXxXXXxxxXXXX")).toBe(
      "https://eth.llamarpc.com/rpc/<RPC_API_KEY_LLAMARPC>",
    );
  });
});
