import pino from "pino";
import { LOG_LEVEL } from "./config";

export const rootLogger = pino(
  {
    level: LOG_LEVEL,
    formatters: {
      bindings() {
        //return { pid: bindings.pid, hostname: bindings.hostname };
        return {};
      },
    },
  },
  pino.destination({
    dest: 1, // stdout
    // disable buffering to avoid doing OOMs on development when too many logs are emitted
    sync: process.env.NODE_ENV !== "production",
  }),
);

export type LogInfos = {
  msg: string;
  data?: object;
};
export function mergeLogsInfos(a: LogInfos, b: LogInfos): LogInfos {
  return {
    msg: a.msg + ". " + b.msg,
    data: { ...a.data, ...b.data },
  };
}
