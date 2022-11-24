const Decimal = require("decimal.js");
const readline = require("readline");

Decimal.set({
  // make sure we have enough precision
  precision: 50,
  // configure the Decimals lib to format without exponents
  toExpNeg: -250,
  toExpPos: 250,
});

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false,
});

rl.on("line", (line) => {
  const [address, num] = line.split(",");
  const d = new Decimal(num).toDecimalPlaces(18).toFixed(18);
  if (d !== "0.000000000000000000") {
    const res = `${address.toLocaleLowerCase()},${d}`;
    process.stdout.write(res + "\n");
  }
});

rl.once("close", () => {
  // end of input
});
