// this type of error is thrown when there is a misconfiguration in the code

import { DISABLE_PROGRAMMER_ERROR_DUMP } from "./config";

// it shouldn't be retried
export class ProgrammerError extends Error {
  constructor(logInfos: any) {
    super(logInfos?.msg || "Programmer Error");
    // use console log as the result is easier to read
    if (!DISABLE_PROGRAMMER_ERROR_DUMP) {
      console.dir(logInfos, { showHidden: false, depth: 50, colors: true });
    }
  }
}

export function isProgrammerError(err: any): boolean {
  if (err instanceof ProgrammerError) {
    return true;
  }
  return false;
}
