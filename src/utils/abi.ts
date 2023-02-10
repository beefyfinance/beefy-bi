import { ethers } from "ethers";
import BeefyBoostAbi from "../../data/interfaces/beefy/BeefyBoost/BeefyBoost.json";
import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";

// parse ABI only once by using a global interface variable
export const BeefyVaultV6AbiInterface = new ethers.utils.Interface(BeefyVaultV6Abi);
export const BeefyBoostAbiInterface = new ethers.utils.Interface(BeefyBoostAbi);
export const ERC20AbiInterface = new ethers.utils.Interface(ERC20Abi);
