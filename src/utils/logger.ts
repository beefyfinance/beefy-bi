import { createLogger, transports, format } from "winston";
import { LOG_LEVEL } from "./config";
export const logger = createLogger({
  levels: {
    error: 0,
    warn: 1,
    info: 2,
    verbose: 4,
    debug: 5,
    trace: 6,
  },
  level: LOG_LEVEL,
  transports: [
    new transports.Console({
      format: format.combine(
        format.timestamp(),
        format.printf(({ level, message, timestamp }) => {
          return `${timestamp} [${level}] ${message}`;
        })
      ),
    }),
  ],
});
