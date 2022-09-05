import pino from "pino";
import { LOG_LEVEL } from "./config";

export const rootLogger = pino({
  level: LOG_LEVEL,
  formatters: {
    bindings(bindings) {
      //return { pid: bindings.pid, hostname: bindings.hostname };
      return {};
    },
  },
});
