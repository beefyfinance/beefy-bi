import {} from "viem";
/*
 ==== Caller interface ====
 goal: make it easy for users to use the batching logic without having to know about it
 constraints:
    1: don't use time-based or event-loop based batching, because it's hidden control flow which makes it difficult to reason about and easy to create bad request patterns
    2: users should not expect the requests to have side effects so we can offload the batching to a different process or even a different machine later down the line
    3: have typescript support for the response at the time of creating the requests
 to be decided:
    1: should this expose low level batching primitives or should it be a higher level abstraction?
       for example, should a "request" mean: "call contract X with parameters Y" or should it mean "get me the current user balance of token X"
       the former is more flexible, but the later would allow the caller to fetch from endpoints that are not RPCs (ex: a database, a webservice, etc)
 */

type Request = {}; // TBD

class UnitOfWork<TKey> {
  private requests: Map<TKey, Request>;

  constructor() {
    this.requests = new Map();
  }

  public addRequest(key: any, request: Request) {
    this.requests.set(key, request);
  }
}

/**
 ==== Batcher system ====
 goal: responsible for batching requests to the proper RPC client (based on the rpc limitations)
 constraint 1: should be able to handle multiple RPCs
 constraint 2: should resolve rpc constraints (ex: archive / non-archive nodes, rate limiting, etc)

*/

// anything in here helps route and batch requests to the right RPC
type RpcConfig = {
  url: string;
  limitations: {
    blocksAvailable: number; // <- Infinity for archive nodes, probably something around 250 for non-archive nodes
    stateChangeOnSameBlock: boolean; // <- if true, you can read the transaction result on the same block, ex: getBalance on the same block as the trx happened
    // ... other limitations that helps make the batching logic
    maxRequestMb: number | null; // <- max request size in MB if relevant
    optimalResponseTimeMs: number; // <- optimal response time in ms, if RPC suddenly becomes less efficient we should reduce the batch size or log span at runtime
    callBatchSize: number; // <- initial batch size to use for this RPC, should be adapted on the fly based on the response time
    logMaxRange: number; // <- max range to use for the log queries
  };
  multicall3: {
    address: string;
    canGetBlockTimestampWithBlockTag: boolean; // <-- cosmos-based chains are broken here
  } | null;
  rateLimitProfile:
    | {
        type: "no-limit";
      }
    | { type: "rate-limit-seconds"; maxCallsPerSecond: number };
};

class BeefyClient {
  private rpcs: RpcConfig[];

  public async execute<TKey>(work: UnitOfWork<TKey>): Promise<Map<TKey, any>> {
    // 1: enqueue the unit of work
    // 2: every now and then, after some time or when sufficient work is available, merge all the pending work
    // 3: decide which RPC to use for which work (based on the limitations)
    // 4: dispatch requests to the RPCs
    // 5: yield results as they come in
    return Promise.resolve() as any;
  }
}
