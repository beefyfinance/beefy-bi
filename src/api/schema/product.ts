import S from "fluent-json-schema";

export const productKeyExamples = [
  "beefy:vault:ethereum:0xa7739fd3d12ac7f16d8329af3ee407e19de10d8d",
  "beefy:vault:zksync:0xd3da44e34e5c57397ea56f368b9e609433ef1d03",
  "beefy:boost:zksync:0x829e669bfb45ab51e853572920d9258a4907f89d",
];

export const productKeySchema = S.string().minLength(10).maxLength(100).examples(productKeyExamples);
