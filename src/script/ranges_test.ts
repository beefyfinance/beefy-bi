import * as fs from "fs";
import { isArray, range } from "lodash";
import { rangeMerge, rangeOverlap } from "../utils/range";
/*
const output = {
  successRanges: [
    {
      from: 14316268,
      to: 14336267,
    },
    {
      from: 14341268,
      to: 14456267,
    },
    {
      from: 14461268,
      to: 14506267,
    },
    {
      to: 14636267,
      from: 14631268,
    },
    {
      from: 14641268,
      to: 14681267,
    },
    {
      from: 14686268,
      to: 14786267,
    },
    {
      from: 14791268,
      to: 14816267,
    },
    {
      from: 14821268,
      to: 14836267,
    },
    {
      from: 14841268,
      to: 14851267,
    },
    {
      from: 14856268,
      to: 14891267,
    },
    {
      from: 14896268,
      to: 14931267,
    },
    {
      from: 14936268,
      to: 15141267,
    },
    {
      from: 15146268,
      to: 15171267,
    },
    {
      from: 15176268,
      to: 15231267,
    },
    {
      from: 15236268,
      to: 15331267,
    },
    {
      from: 15336268,
      to: 15346267,
    },
    {
      from: 15351268,
      to: 15401267,
    },
    {
      from: 15516268,
      to: 15571267,
    },
    {
      from: 15576268,
      to: 15611267,
    },
    {
      from: 15616268,
      to: 15626267,
    },
    {
      from: 15631268,
      to: 15686267,
    },
    {
      from: 15691268,
      to: 15701267,
    },
    {
      from: 15706268,
      to: 15716267,
    },
    {
      from: 15721268,
      to: 15731267,
    },
    {
      from: 15736268,
      to: 15746267,
    },
    {
      from: 15751268,
      to: 15826267,
    },
    {
      from: 15831268,
      to: 15861267,
    },
    {
      from: 15866268,
      to: 15976267,
    },
    {
      from: 15981268,
      to: 16101267,
    },
    {
      from: 16106268,
      to: 16121267,
    },
    {
      from: 16126268,
      to: 16376267,
    },
    {
      from: 16381268,
      to: 16711267,
    },
    {
      from: 16721268,
      to: 16861267,
    },
    {
      from: 16866268,
      to: 16956267,
    },
    {
      from: 16961268,
      to: 16994267,
    },
    {
      from: 23330332,
      to: 23331910,
    },
    {
      from: 23333559,
      to: 23337401,
    },
  ],
  errorRanges: [
    {
      from: 14511268,
      to: 14626267,
    },
    {
      from: 14686268,
      to: 14786267,
    },
  ],
};
function convert(integer: number) {
  var str = Number(integer).toString(16);
  return "0x" + (str.length == 1 ? "0" + str : str);
}

const { successRanges, errorRanges } = output;
console.log(successRanges.length);
console.log(errorRanges.length);
console.log(rangeMerge(successRanges).length);
console.log(rangeMerge(errorRanges).length);
console.log(rangeMerge([...successRanges, ...errorRanges]).length);
//console.log(rangeMerge([...successRanges, ...errorRanges]));

const hasOverlap = successRanges.some((successRange) => errorRanges.some((errorRange) => rangeOverlap(successRange, errorRange)));
console.log(hasOverlap);
for (const successRange of successRanges) {
  for (const errorRange of errorRanges) {
    if (rangeOverlap(successRange, errorRange)) {
      console.log("overlap", { successRange, errorRange, hex: { from: convert(successRange.from), to: convert(successRange.to) } });
    }
  }
}*/

const data = fs.readFileSync(__dirname + "/logs_14686268.log", "utf-8").split("\n");

const filtered = data
  .map((row) => {
    if (row.includes('"component":"execute-sub-pipeline"')) {
      try {
        const content = JSON.parse(row);
        if (content?.data?.parent?.importState) {
          content.data.parent.importState = "<redacted>";
          row = JSON.stringify(content);
        }
      } catch (e) {}
    }
    if (row.includes('"component":"import-product-block-range","msg":"loading transfer"')) {
      try {
        const content = JSON.parse(row);
        if (content?.data?.transferData?.parent?.importState) {
          content.data.transferData.parent.importState = "<redacted>";
          row = JSON.stringify(content);
        }
      } catch (e) {}
    }
    return row;
  })
  .filter((row) => row.includes("14686268"))
  .join("\n");
console.log(filtered);
/*
data.items = data.items.filter(
  (item: any) => (isArray(item.transfers) && item.transfers.length > 0) || item.success === false || item.range.from === 23330050,
);
console.log(data.items.length);
console.log(JSON.stringify(data.items, null, 2));

// { "from": 23330050, "to": 23334799 }
*/
