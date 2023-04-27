import { BlockTag } from "@ethersproject/abstract-provider";
import axios from "axios";
import { ethers } from "ethers";
import { keyBy } from "lodash";
import type { FunctionCall } from "./call";

export type RpcRequestBatch<TParams, TResult> = FunctionCall<TParams, TResult>[];

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
export async function jsonRpcBatchEthCall<TParams extends undefined | any[], TResult>({
  url,
  requests,
  timeout = 10_000,
}: {
  url: string;
  requests: RpcRequestBatch<TParams, TResult>;
  timeout?: number;
}) {
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
            data: request.interface.encodeFunctionData(request.function, request.params),
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
      return [
        originalRequest,
        Promise.resolve(originalRequest.interface.decodeFunctionResult(originalRequest.function, result.result)[0] as TResult),
      ];
    }),
  );
}
