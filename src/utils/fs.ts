import * as fs from "fs";
import * as path from "path";
import { DATA_DIRECTORY } from "./config";
import nReadline from "n-readlines";

export async function makeDataDirRecursive(filePath: string) {
  // extract dir path from file path
  const dirPath = path.dirname(filePath);
  // check that dir is inside data dir
  if (!dirPath.startsWith(DATA_DIRECTORY)) {
    throw new Error(`${filePath} is not inside ${DATA_DIRECTORY}`);
  }

  if (!fs.existsSync(dirPath)) {
    await fs.promises.mkdir(dirPath, { recursive: true });
  }
}

export async function fileOrDirExists(filePath: string) {
  try {
    const stat = await fs.promises.stat(filePath);
    return true;
  } catch (e) {
    return false;
  }
}

export async function getFirstLineOfFile(pathToFile: string, onlyNotEmpty: boolean = true): Promise<string> {
  const liner = new nReadline(pathToFile);
  let line;
  while ((line = liner.next())) {
    let lineTxt = line.toString("utf-8");
    if (onlyNotEmpty) {
      lineTxt = lineTxt.trim();
      if (lineTxt.length > 0) {
        return lineTxt;
      }
    } else {
      return lineTxt;
    }
  }
  return "";
}
