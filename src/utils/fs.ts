import * as fs from "fs";
import * as path from "path";
import { DATA_DIRECTORY } from "./config";
import * as readline from "readline";

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

export async function getFirstLineOfFile(pathToFile: string): Promise<string> {
  const readable = fs.createReadStream(pathToFile);
  const reader = readline.createInterface({ input: readable });
  const line = await new Promise<string>((resolve, reject) => {
    let hasLine = false;
    reader.on("line", (line) => {
      reader.close();
      hasLine = true;
      resolve(line);
    });
    reader.on("error", (err) => reject(err));
    reader.on("close", () => {
      if (hasLine) return;
      else resolve(""); // if file is empty, return empty string
    });
  });
  readable.close();
  return line;
}
