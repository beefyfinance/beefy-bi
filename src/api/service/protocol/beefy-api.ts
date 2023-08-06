import axios from "axios";
import { BEEFY_DATA_KEY, BEEFY_DATA_URL } from "../../../utils/config";

export class BeefyApiService {
  constructor() {}

  public async getVaultTvl(beefyDataVaultId: string, fromDatetime: Date, toDatetime: Date) {
    const params = {
      vault: beefyDataVaultId,
      from: Math.floor(fromDatetime.getTime() / 1000),
      to: Math.ceil(toDatetime.getTime() / 1000),
      key: BEEFY_DATA_KEY,
    };

    const res = await axios.get<{ t: number; v: number }[]>(BEEFY_DATA_URL + "/api/v2/tvls/range", { params });

    return res.data.map((r) => ({ datetime: new Date(r.t * 1000), tvl: r.v }));
  }
}
