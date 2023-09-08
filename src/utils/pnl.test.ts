import Decimal from "decimal.js";
import { sortBy } from "lodash";
import { BIG_ZERO } from "./decimal";
import { PnL } from "./pnl";

describe("pnl", () => {
  it("should compute PnL properly", () => {
    let pnl = new PnL();
    let currentPrice = new Decimal(25);
    expect(pnl.getRealizedPnl()).toEqual(BIG_ZERO);
    expect(pnl.getUnrealizedPnl(currentPrice)).toEqual(BIG_ZERO);

    let trx = { shares: new Decimal(2), price: new Decimal(10) };
    pnl.addTransaction(trx);
    expect(pnl.getRealizedPnl()).toEqual(BIG_ZERO);
    expect(pnl.getUnrealizedPnl(trx.price)).toEqual(BIG_ZERO);
    expect(pnl.getUnrealizedPnl(new Decimal(12))).toEqual(new Decimal(4));

    trx = { shares: new Decimal(2), price: new Decimal(15) };
    pnl.addTransaction(trx);
    expect(pnl.getRealizedPnl()).toEqual(BIG_ZERO);
    expect(pnl.getUnrealizedPnl(trx.price)).toEqual(new Decimal(10));
    expect(pnl.getUnrealizedPnl(new Decimal(17))).toEqual(new Decimal(18));

    trx = { shares: new Decimal(-3), price: new Decimal(20) };
    pnl.addTransaction(trx);
    expect(pnl.getRealizedPnl()).toEqual(new Decimal(25));
    expect(pnl.getUnrealizedPnl(trx.price)).toEqual(new Decimal(5));
  });

  it("should compute PnL properly from a real world use case", () => {
    const timeline = [
      {
        datetime: "2022-11-17T14:48:03.000Z",
        share_to_underlying_price: 1.0072088101378458,
        underlying_to_usd_price: 314.89874339460806,
        share_balance: 20.27876253618866,
        underlying_balance: 20.4249482851425,
        usd_balance: 6431.790548891228,
        share_diff: 20.27876253618866,
        underlying_diff: 20.4249482851425,
        usd_diff: 6431.790548891228,
      },
      {
        datetime: "2022-11-20T19:31:20.000Z",
        share_to_underlying_price: 1.0075970905895832,
        underlying_to_usd_price: 347.50914177774484,
        share_balance: 20.891856301143548,
        underlying_balance: 21.05057362604789,
        usd_balance: 7315.266774717132,
        share_diff: 0.6130937649548895,
        underlying_diff: 0.6177514938271605,
        usd_diff: 214.6742914517964,
      },
      {
        datetime: "2022-11-23T21:36:19.000Z",
        share_to_underlying_price: 1.0079955733953208,
        underlying_to_usd_price: 338.9687613836563,
        share_balance: 21.873125794444825,
        underlying_balance: 22.04801397711939,
        usd_balance: 7473.587988793701,
        share_diff: 0.9812694933012761,
        underlying_diff: 0.9891153055555556,
        usd_diff: 335.2791899897834,
      },
      {
        datetime: "2022-12-02T13:42:28.000Z",
        share_to_underlying_price: 1.0091310138565124,
        underlying_to_usd_price: 357.83013067482005,
        share_balance: 23.214705725876705,
        underlying_balance: 23.426679525534542,
        usd_balance: 8382.771795899156,
        share_diff: 1.3415799314318817,
        underlying_diff: 1.3538299163754048,
        usd_diff: 484.4411358880918,
      },
      {
        datetime: "2022-12-15T21:56:20.000Z",
        share_to_underlying_price: 1.0109650927669185,
        underlying_to_usd_price: 363.0589258226089,
        share_balance: 0,
        underlying_balance: 0,
        usd_balance: 0,
        share_diff: -23.214705725876705,
        underlying_diff: -23.469257127717658,
        usd_diff: -8520.72328264378,
      },
      {
        datetime: "2023-02-02T19:03:44.000Z",
        share_to_underlying_price: 1.0181657075053454,
        underlying_to_usd_price: 489.8806056476361,
        share_balance: 29.516921045823203,
        underlying_balance: 30.0531168,
        usd_balance: 14722.439059583148,
        share_diff: 29.516921045823203,
        underlying_diff: 30.0531168,
        usd_diff: 14722.439059583148,
      },
    ].map((row) => ({
      share_diff: new Decimal(row.share_diff),
      share_to_underlying_price: new Decimal(row.share_to_underlying_price),
      underlying_to_usd_price: new Decimal(row.underlying_to_usd_price),
    }));

    const sortedTimeline = sortBy(timeline, "datatime");

    const rawCurrentValue = {
      share_to_underlying_price: "1.021708071749255723",
      underlying_to_usd_price: "477.57009640452765",
    };
    const currentValue = {
      share_to_underlying_price: new Decimal(
        rawCurrentValue.share_to_underlying_price
      ),
      underlying_to_usd_price: new Decimal(
        rawCurrentValue.underlying_to_usd_price
      ),
    };

    const yieldPnL = new PnL();
    const usdPnL = new PnL();

    for (const row of sortedTimeline) {
      if (
        row.share_diff &&
        row.share_to_underlying_price &&
        row.underlying_to_usd_price
      ) {
        usdPnL.addTransaction({
          shares: row.share_diff,
          price: row.share_to_underlying_price.times(
            row.underlying_to_usd_price
          ),
        });
      }
    }

    for (const row of sortedTimeline) {
      if (row.share_diff && row.share_to_underlying_price) {
        yieldPnL.addTransaction({
          shares: row.share_diff,
          price: row.share_to_underlying_price,
        });
      }
    }

    const currentUsdPrice = (
      currentValue.share_to_underlying_price || BIG_ZERO
    ).times(currentValue.underlying_to_usd_price || BIG_ZERO);

    const currentYieldPrice =
      currentValue.share_to_underlying_price || BIG_ZERO;

    // ensures yield pnl
    expect(yieldPnL.getRemainingShares().toNumber()).toBeCloseTo(
      29.516921045823203,
      10
    );
    expect(yieldPnL.getRemainingSharesAvgEntryPrice().toNumber()).toBeCloseTo(
      1.0181657075053454,
      10
    );
    expect(yieldPnL.getRealizedPnl().toNumber()).toBeCloseTo(
      0.08361212681703376,
      10
    );
    expect(yieldPnL.getUnrealizedPnl(currentYieldPrice).toNumber()).toBeCloseTo(
      0.10455968570304802,
      10
    );

    // ensures usd pnl
    expect(usdPnL.getRemainingShares().toNumber()).toBeCloseTo(
      29.516921045823203,
      10
    );
    expect(usdPnL.getRemainingSharesAvgEntryPrice().toNumber()).toBeCloseTo(
      498.7796334423725,
      10
    );
    expect(usdPnL.getRealizedPnl().toNumber()).toBeCloseTo(
      1054.5381164228797,
      10
    );
    expect(usdPnL.getUnrealizedPnl(currentUsdPrice).toNumber()).toBeCloseTo(
      -320.0345929693857,
      10
    );
  });
});