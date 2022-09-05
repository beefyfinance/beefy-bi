import * as util from "util";

// this type of error is thrown when there is a misconfiguration in the code
// it shouldn't be retried
export class ProgrammerError extends Error {
  constructor(logInfos: any) {
    super(logInfos?.msg || "Programmer Error");
    // use console log as the result is easier to read
    console.log(util.inspect(logInfos, { showHidden: false, depth: 6, colors: true }));
  }
}

export function shouldRetryProgrammerError(err: any): boolean {
  return !(err instanceof ProgrammerError);
}
