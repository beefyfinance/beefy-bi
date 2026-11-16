import { ethers } from "ethers";
import { BeefyVaultV6AbiInterface } from "../../../../utils/abi";
import { decodePpfsMulticallResult, isEmptyReturnDataCallError, isNoCodeReturnData } from "./share-rate-utils";

describe("decodePpfsMulticallResult", () => {
  it("decodes the ppfs of a successful call", () => {
    const returnData = BeefyVaultV6AbiInterface.encodeFunctionResult("getPricePerFullShare", [ethers.BigNumber.from(1234)]);
    expect(decodePpfsMulticallResult({ success: true, returnData })?.toString()).toBe("1234");
  });

  it("returns 0 when there is no code: success with empty return data", () => {
    expect(decodePpfsMulticallResult({ success: true, returnData: "0x" })?.toString()).toBe("0");
  });

  it("returns null when the call failed, so the caller keeps its default", () => {
    expect(decodePpfsMulticallResult({ success: false, returnData: "0x" })).toBeNull();
  });
});

describe("isEmptyReturnDataCallError", () => {
  it("matches a call exception with empty data", () => {
    expect(isEmptyReturnDataCallError({ code: "CALL_EXCEPTION", data: "0x" })).toBe(true);
    expect(isEmptyReturnDataCallError({ code: "CALL_EXCEPTION", error: { data: "0x" } })).toBe(true);
  });

  it("does not match other errors", () => {
    expect(isEmptyReturnDataCallError({ code: "CALL_EXCEPTION", data: "0x08c379a0" })).toBe(false);
    expect(isEmptyReturnDataCallError({ code: "NETWORK_ERROR", data: "0x" })).toBe(false);
    expect(isEmptyReturnDataCallError(new Error("SafeMath: division by zero"))).toBe(false);
  });
});

describe("isNoCodeReturnData", () => {
  it("is true only for empty return data", () => {
    expect(isNoCodeReturnData("0x")).toBe(true);
    expect(isNoCodeReturnData("0x0000000000000000000000000000000000000000000000000000000000000001")).toBe(false);
  });
});
