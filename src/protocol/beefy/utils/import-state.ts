export const getInvestmentsImportStateKey = (product: { productId: number }) => `product:investment:${product.productId}`;

export const getPendingRewardImportStateKey = (item: { product: { productId: number }; investor: { investorId: number } }) =>
  `product:investment:pending-reward:${item.product.productId}:${item.investor.investorId}`;

export const getPriceFeedImportStateKey = (priceFeed: { priceFeedId: number }) => `price:feed:${priceFeed.priceFeedId}`;
