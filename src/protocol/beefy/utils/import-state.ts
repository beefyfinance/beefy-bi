export const getInvestmentsImportStateKey = (product: { productId: number }) => `product:investment:${product.productId}`;

export const getPriceFeedImportStateKey = (priceFeed: { priceFeedId: number }) => `price:feed:${priceFeed.priceFeedId}`;
