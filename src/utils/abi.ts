import { ethers } from "ethers";
import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import Multicall3AbiRaw from "../../data/interfaces/standard/Multicall3";

// parse ABI only once by using a global interface variable
export const BeefyVaultV6AbiInterface = new ethers.utils.Interface(BeefyVaultV6Abi);
export const ERC20AbiInterface = new ethers.utils.Interface(ERC20Abi);
export const Multicall3AbiInterface = new ethers.utils.Interface(Multicall3AbiRaw);
export const Multicall3Abi = Multicall3AbiRaw;
