import { createLogger, transports, format } from "winston";
import { LOG_LEVEL } from "./config";
export const logger = createLogger({
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
