import { DbProduct } from "../../common/loader/product";

export const getProductContractAddress = (product: DbProduct) =>
  product.productData.type === "beefy:boost" ? product.productData.boost.contract_address : product.productData.vault.contract_address;
