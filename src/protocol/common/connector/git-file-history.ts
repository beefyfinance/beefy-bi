import * as path from "path";
import { simpleGit, SimpleGit, SimpleGitOptions } from "simple-git";
import { GIT_WORK_DIRECTORY } from "../../../utils/config";
import { sortBy } from "lodash";
import { callLockProtectedGitRepo } from "../../../lib/shared-resources/shared-gitrepo";
import { fileOrDirExists } from "../../../utils/fs";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "beefy", component: "vault-list" });

export async function* gitStreamFileVersions(options: {
  remote: string;
  workdir: string;
  branch: string;
  filePath: string;
  order: "recent-to-old" | "old-to-recent";
  throwOnError: boolean;
}): AsyncGenerator<{ latestVersion: boolean; commitHash: string; date: Date; fileContent: string }> {
  const baseOptions: Partial<SimpleGitOptions> = {
    binary: "git",
    maxConcurrentProcesses: 1,
  };

  // we can't make concurrent pulls
  await callLockProtectedGitRepo(options.workdir, async () => {
    // pull latest changes from remote or just clone remote
    if (!(await fileOrDirExists(options.workdir))) {
      logger.debug({ msg: "cloning remote locally", data: { remote: options.remote, workdir: options.workdir } });
      const git: SimpleGit = simpleGit({
        ...baseOptions,
        baseDir: GIT_WORK_DIRECTORY,
      });
      await git.clone(options.remote, options.workdir);
    } else {
      logger.debug({ msg: "Local repo found", data: { remote: options.remote, workdir: options.workdir } });
    }
  });

  // get a new git instance for the current git repo
  const git = simpleGit({
    ...baseOptions,
    baseDir: options.workdir,
  });
  // switch to the target branch
  await callLockProtectedGitRepo(options.workdir, async () => {
    logger.debug({ msg: "Changing branch to", data: { branch: options.branch } });
    await git.checkout(options.branch);
    logger.debug({ msg: "Pulling changes", data: { branch: options.branch } });
    await git.pull("origin", options.branch);
  });

  if (!(await fileOrDirExists(path.join(options.workdir, options.filePath)))) {
    logger.debug({ msg: "No file found", data: { filePath: options.filePath, branch: options.branch } });
    return;
  }

  // get all commit hashes for the target file
  logger.debug({
    msg: "Pulling all commit hashes for file",
    data: { filePath: options.filePath, branch: options.branch },
  });
  const log = await git.log({
    file: options.filePath,
    format: "%H",
  });
  let logs = log.all as any as { hash: string; date: string }[];

  let latestVersionIndex = 0;
  if (options.order === "old-to-recent") {
    logs = sortBy(logs, (log) => log.date);
    latestVersionIndex = logs.length - 1;
  } else if (options.order === "recent-to-old") {
    logs = sortBy(logs, (log) => log.date).reverse();
    latestVersionIndex = 0;
  }

  // for each hash, get the file content
  for (const [logIndex, log] of logs.entries()) {
    logger.debug({ msg: "Pulling file content", data: { hash: log.hash, date: log.date, filePath: options.filePath } });
    try {
      const fileContent = await git.show([`${log.hash}:${options.filePath}`]);
      yield {
        latestVersion: logIndex === latestVersionIndex,
        commitHash: log.hash,
        date: new Date(log.date),
        fileContent,
      };
    } catch (error) {
      if (error instanceof Error && error.message.includes("exists on disk, but not in")) {
        logger.debug({
          msg: "File not found in commit, most likely the file was renamed",
          data: { hash: log.hash, filePath: options.filePath },
        });
      } else {
        logger.error({
          msg: "Could not get file content",
          data: { hash: log.hash, filePath: options.filePath },
          error,
        });
        logger.trace(error);
      }
      if (options.throwOnError) {
        throw error;
      }
    }
  }
}
