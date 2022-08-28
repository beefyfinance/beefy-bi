import pino from "pino";
import { LOG_LEVEL } from "./config";

export const rootLogger = pino({
  level: LOG_LEVEL,
});
