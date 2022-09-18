import * as fs from "fs";

export async function fileOrDirExists(filePath: string) {
  try {
    const stat = await fs.promises.stat(filePath);
    return true;
  } catch (e) {
    return false;
  }
}
