import { BlockTag } from "@ethersproject/abstract-provider";
import { ethers } from "ethers";
import { Hex } from "../../types/address";

export type FunctionCall<TParams, TResult, TMap = any> = {
  interface: ethers.utils.Interface;
  function: ethers.utils.FunctionFragment | string;
  contractAddress: Hex;
  blockTag: BlockTag;
  params: TParams;
  originalRequest: TMap;
};

type FunctionResult<TParams, TResult> = {};

export type MulticallCallOneCallParamStructure = {
  allowFailure: boolean;
  callData: string;
  target: Hex;
};
