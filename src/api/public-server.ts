import { API_LISTEN, API_PORT } from "../utils/config";
import { buildPublicApi } from "./api";

buildPublicApi().then((server) =>
  server.listen({ port: API_PORT, host: API_LISTEN }, (err, address) => {
    if (err) {
      console.error(err);
      process.exit(1);
    }
    console.log(`Server listening at ${address}`);
  }),
);
