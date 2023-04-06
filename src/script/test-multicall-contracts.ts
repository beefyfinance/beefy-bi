import { ContractCallContext, Multicall } from "ethereum-multicall";
import { ethers } from "ethers";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { BeefyVaultV6AbiInterface } from "../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../utils/config";
import { runMain } from "../utils/process";

async function main() {
  const chain = "bsc" as Chain;
  const cmdParams = {
    client: {} as any,
    rpcCount: "all" as const,
    task: "historical" as const,
    includeEol: true,
    filterChains: [chain],
    filterContractAddress: null,
    forceCurrentBlockNumber: null,
    forceRpcUrl: null,
    forceGetLogsBlockSpan: null,
    productRefreshInterval: null,
    loopEvery: null,
    loopEveryRandomizeRatio: 0,
    ignoreImportState: true,
    disableWorkConcurrency: true,
    generateQueryCount: null,
    skipRecentWindowWhenHistorical: "none" as const,
    waitForBlockPropagation: null,
  };

  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);
  const rpcConfigs = getMultipleRpcConfigsForChain({ chain: chain, behaviour });
  const rpcConfig = rpcConfigs[0];
  const provider = rpcConfig.linearProvider;

  const mcMap = MULTICALL3_ADDRESS_MAP[chain];
  if (!mcMap) {
    throw new Error("Unsupported mc call");
  }
  const multicall = new Multicall({ ethersProvider: provider, tryAggregate: true, multicallCustomContractAddress: mcMap.multicallAddress });

  const ppfsAbi = {
    inputs: [],
    name: "getPricePerFullShare",
    outputs: [{ internalType: "uint256", name: "", type: "uint256" }],
    stateMutability: "view",
    type: "function",
  };

  const blockNumber = 16896415;
  const vaults = [
    // eth
    //"0x83BE6565c0758f746c09f95559B45Cfb9a0FFFc4",
    //"0xCc19786F91BB1F3F3Fd9A2eA9fD9a54F7743039E",
    //"0x5Bcd31a28D77a1A5Ef5e0146Ab91e6f43D7100b7",
    // bsc
    "0xF80d1196119B65aFc91357Daebb02F368F3Bca02",
    "0x6A3B5A4952791F15Da5763745AfEA8366Ad06C7d",
    "0x3C3bFaFe7c4570eF6061AaaB6dac576aB1896B7d",
    "0x92E586d7dB14483C103c2e0FE6A596F8b55DA752", // this one has zero TVL so ppfs will throw
  ];
  /*
  // check that we can indeed fetch ppfs normally
  for (const vaultAddress of vaults) {
    const contract = new ethers.Contract(vaultAddress, BeefyVaultV6AbiInterface, provider);
    const ppfss = await contract.functions.getPricePerFullShare({ blockTag: blockNumber });
    const ppfs = ppfss[0].toString();
    console.dir({ vaultAddress, ppfs });
  }
*/

  // now use a multicall
  const calls: ContractCallContext[] = [];
  for (const vaultAddress of vaults) {
    const reference = vaultAddress.toLocaleLowerCase();
    calls.push({
      reference: reference,
      contractAddress: vaultAddress,
      abi: [ppfsAbi] as any[],
      calls: [{ reference: reference, methodName: "getPricePerFullShare", methodParameters: [] }],
    });
  }

  const res = await multicall.call(calls, { blockNumber: ethers.utils.hexValue(blockNumber) });
  console.dir(res, { depth: null });

  for (const vaultAddress of vaults) {
    const reference = vaultAddress.toLocaleLowerCase();
    const value = res.results[reference].callsReturnContext.find((c) => c.reference === reference);

    console.log({ value });
    if (!value) {
      throw new Error("Call return not found");
    }
    // when vault is empty, we get an empty result array
    // but this happens when the RPC returns an error too so we can't process this
    // the sad story is that we don't get any details about the error
    if (value.returnValues.length === 0 || value.decoded === false || value.success === false) {
      throw new Error("PPFS result coming from multicall could not be parsed");
    }
    const rawPpfs: { type: "BigNumber"; hex: string } | ethers.BigNumber = value.returnValues[0];
    const ppfs = "hex" in rawPpfs ? ethers.BigNumber.from(rawPpfs.hex) : rawPpfs instanceof ethers.BigNumber ? rawPpfs : null;
    if (!ppfs) {
      throw new Error("Could not parse ppfs: " + rawPpfs);
    }
    console.log({ ppfs: ppfs.toString() });
  }

  /*
      {"level":10,"time":1679651295285,"module":"utils","component":"ethers","msg":"RPC request","data":{"request":{"method":"eth_call","params":[{"to":"0x9da9f3c6c45f1160b53d395b0a982aeee1d212fe","data":"0x399542e9000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000e0000000000000000000000000000000000000000000000000000000000000016000000000000000000000000083be6565c0758f746c09f95559b45cfb9a0fffc40000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc00000000000000000000000000000000000000000000000000000000000000000000000000000000cc19786f91bb1f3f3fd9a2ea9fd9a54f7743039e0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc000000000000000000000000000000000000000000000000000000000000000000000000000000005bcd31a28d77a1a5ef5e0146ab91e6f43d7100b70000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc00000000000000000000000000000000000000000000000000000000"},"0x101d19f"],"id":42,"jsonrpc":"2.0"},"rpcUrl":"https://rpc.ankr.com/eth/<RPC_API_KEY_ANKR>"}}
      {"level":50,"time":1679651295826,"module":"process","component":"exit-handler","err":{"type":"Error","message":"missing revert data in call exception; Transaction reverted without a reason string [ See: https://links.ethers.org/v5-errors-CALL_EXCEPTION ] (data=\"0x\", transaction={\"to\":\"0x9dA9f3C6c45F1160b53D395b0A982aEEE1D212fE\",\"data\":\"0x399542e9000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000e0000000000000000000000000000000000000000000000000000000000000016000000000000000000000000083be6565c0758f746c09f95559b45cfb9a0fffc40000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc00000000000000000000000000000000000000000000000000000000000000000000000000000000cc19786f91bb1f3f3fd9a2ea9fd9a54f7743039e0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc000000000000000000000000000000000000000000000000000000000000000000000000000000005bcd31a28d77a1a5ef5e0146ab91e6f43d7100b70000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc00000000000000000000000000000000000000000000000000000000\",\"accessList\":null}, error={\"reason\":\"processing response error\",\"code\":\"SERVER_ERROR\",\"body\":\"{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":42,\\\"error\\\":{\\\"code\\\":-32000,\\\"message\\\":\\\"execution reverted\\\"}}\",\"error\":{\"code\":-32000},\"requestBody\":\"{\\\"method\\\":\\\"eth_call\\\",\\\"params\\\":[{\\\"to\\\":\\\"0x9da9f3c6c45f1160b53d395b0a982aeee1d212fe\\\",\\\"data\\\":\\\"0x399542e9000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000e0000000000000000000000000000000000000000000000000000000000000016000000000000000000000000083be6565c0758f746c09f95559b45cfb9a0fffc40000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc00000000000000000000000000000000000000000000000000000000000000000000000000000000cc19786f91bb1f3f3fd9a2ea9fd9a54f7743039e0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc000000000000000000000000000000000000000000000000000000000000000000000000000000005bcd31a28d77a1a5ef5e0146ab91e6f43d7100b70000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000477c7b8fc00000000000000000000000000000000000000000000000000000000\\\"},\\\"0x101d19f\\\"],\\\"id\\\":42,\\\"jsonrpc\\\":\\\"2.0\\\"}\",\"requestMethod\":\"POST\",\"url\":}
  */
}

runMain(main);
