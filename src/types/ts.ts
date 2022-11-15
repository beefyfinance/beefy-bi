// https://stackoverflow.com/a/71996712/2523414
export type OverwriteKeyType<Base, Overrides> = Omit<Base, keyof Overrides> & Overrides;
