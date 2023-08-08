export const getInvestmentsImportStateKey = (product: { productId: number }) => `product:investment:${product.productId}`;

export const getPriceFeedImportStateKey = (priceFeed: { priceFeedId: number }) => `price:feed:${priceFeed.priceFeedId}`;

export const getProductStatisticsImportStateKey = (product: { productId: number }) => `product:statistics:${product.productId}`;
