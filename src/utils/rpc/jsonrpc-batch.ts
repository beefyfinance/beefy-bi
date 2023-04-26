import { BlockTag } from "@ethersproject/abstract-provider";
import axios from "axios";
import { ethers } from "ethers";
import { keyBy } from "lodash";

interface RpcRequest {
  interface: ethers.utils.Interface;
  function: ethers.utils.FunctionFragment | string;
  contractAddress: string;
  blockTag: BlockTag;
  callData: undefined | any[];
}
export type RpcRequestBatch = RpcRequest[];

interface SuccessResponse {
  jsonrpc: `${number}`;
  id: number;
  error?: never;
  result: any;
}
interface ErrorResponse {
  jsonrpc: `${number}`;
  id: number;
  error: any;
  result?: never;
}
type Response = SuccessResponse | ErrorResponse;

let globalId = 1;
export async function jsonRpcBatchEthCall({ url, requests, timeout = 10_000 }: { url: string; requests: RpcRequestBatch; timeout?: number }) {
  const requestsAndId = requests.map((request) => {
    const id = globalId++;
    return {
      id,
      originalRequest: request,
      jsonRpcRequest: {
        jsonrpc: "2.0",
        id,
        method: "eth_call",
        params: [
          {
            from: null,
            to: request.contractAddress,
            data: request.interface.encodeFunctionData(request.function, request.callData),
          },
          ethers.utils.hexValue(request.blockTag),
        ],
      },
    };
  });

  const res = await axios.post<ErrorResponse | Response[]>(
    url,
    requestsAndId.map((r) => r.jsonRpcRequest),
    {
      timeout,
    },
  );

  if (res.status !== 200) {
    throw new Error("Batch request failed");
  }
  const result = res.data;

  if (!Array.isArray(result)) {
    if (result.error) {
      const error = new Error(result?.error?.message);
      (error as any).code = result?.error?.code;
      (error as any).data = result?.error?.data;
      throw error;
    } else {
      throw new Error("Batch result is not an array");
    }
  }

  const resultById = keyBy(result, (r) => r.id);
  return new Map(
    requestsAndId.map(({ originalRequest, id }) => {
      const result = resultById[id];
      if (!result) {
        return [originalRequest, Promise.reject(`Request id ${id} not in response ids`)];
      } else if (result.error) {
        return [originalRequest, Promise.reject(result.error)];
      }
      return [originalRequest, Promise.resolve(originalRequest.interface.decodeFunctionResult(originalRequest.function, result.result)[0])];
    }),
  );
}
