import { removeSecretsFromRpcUrl } from "./remove-secrets-from-rpc-url";

describe("test we remove secrets from rpc urls", () => {
  it("should format an url without secret properly", () => {
    expect(removeSecretsFromRpcUrl("https://user:password@localhost:8545")).toBe("https://localhost:8545");

    expect(removeSecretsFromRpcUrl("https://mainnet.aurora.dev/xxxxXXXXXXxxxxXXXXXXxxxXXXXXXXXX")).toBe("https://mainnet.aurora.dev");
    expect(removeSecretsFromRpcUrl("https://api.avax.network/ext/bc/C/rpc")).toBe("https://api.avax.network");
    expect(removeSecretsFromRpcUrl("https://rpc.ankr.com/avalanche")).toBe("https://rpc.ankr.com/avalanche");
    expect(removeSecretsFromRpcUrl("https://rpc.ankr.com/bsc/xxxXXXxXXXXXXxXXXXXXxXXX")).toBe("https://rpc.ankr.com/bsc");
    expect(removeSecretsFromRpcUrl("https://emerald.oasis.dev")).toBe("https://emerald.oasis.dev");
    expect(removeSecretsFromRpcUrl("https://rpc.ftm.tools")).toBe("https://rpc.ftm.tools");
    expect(removeSecretsFromRpcUrl("https://rpcapi.fantom.network")).toBe("https://rpcapi.fantom.network");
    expect(removeSecretsFromRpcUrl("https://explorer-node.fuse.io/")).toBe("https://explorer-node.fuse.io");
    expect(removeSecretsFromRpcUrl("https://rpc.ankr.com/harmony/")).toBe("https://rpc.ankr.com/harmony");
    expect(removeSecretsFromRpcUrl("https://http-mainnet.hecochain.com")).toBe("https://http-mainnet.hecochain.com");
    expect(removeSecretsFromRpcUrl("https://andromeda.metis.io/?owner=1234")).toBe("https://andromeda.metis.io");
    expect(removeSecretsFromRpcUrl("https://rpc.api.moonbeam.network")).toBe("https://rpc.api.moonbeam.network");
    expect(removeSecretsFromRpcUrl("https://rpc.api.moonriver.moonbeam.network/")).toBe("https://rpc.api.moonriver.moonbeam.network");
    expect(removeSecretsFromRpcUrl("https://opt-mainnet.g.alchemy.com/v2/XXXxxXXXXXxxxXXXXXXxxxxxx")).toBe("https://opt-mainnet.g.alchemy.com");
    expect(removeSecretsFromRpcUrl("https://polygon-rpc.com/")).toBe("https://polygon-rpc.com");
    expect(removeSecretsFromRpcUrl("https://rpc.syscoin.org/")).toBe("https://rpc.syscoin.org");
  });
});