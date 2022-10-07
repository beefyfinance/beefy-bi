import { Range } from "../../../utils/range";
import { DbProduct } from "../loader/product";

export interface ProductImportQuery<TProduct extends DbProduct = DbProduct> {
  product: TProduct;
  blockRange: Range;
  latestBlockNumber: number;
}

export type ErrorEmitter<TProduct extends DbProduct = DbProduct> = (importQuery: ProductImportQuery<TProduct>) => void;
