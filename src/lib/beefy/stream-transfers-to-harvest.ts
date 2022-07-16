import { isArray } from "lodash";
import { Transform, TransformCallback } from "stream";
import { BeefyVault } from "../../types/beefy";
import { Chain } from "../../types/chain";
import { ERC20TransferFromEventData } from "../csv-store/csv-transfer-from-events";

// adapted from https://futurestud.io/tutorials/node-js-filter-data-in-streams
export class StreamTransfersToHarvestRow extends Transform {
  protected state: {};
  constructor(protected chain: Chain, protected vault: BeefyVault, protected strategyImplementation: string) {
    super({
      readableObjectMode: true,
      writableObjectMode: true,
    });

    this.state = {};
  }

  _transform(row: ERC20TransferFromEventData, encoding: string, next: TransformCallback): void {
    if (this.includeRow(row)) {
      return next(null, row);
    }
    next();
  }
}
