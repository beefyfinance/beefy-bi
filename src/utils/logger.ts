import winston from "winston";
export const logger = winston.createLogger({
  //level: "verbose",
  level: "debug",
  transports: [
    new winston.transports.Console({
      format: winston.format.simple(),
    }),
  ],
});
