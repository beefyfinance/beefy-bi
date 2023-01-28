// https://stackoverflow.com/a/71996712/2523414
export type OverwriteKeyType<Base, Overrides> = Omit<Base, keyof Overrides> & Overrides;

// https://typeofnan.dev/making-every-object-property-nullable-in-typescript/
export type Nullable<T> = { [K in keyof T]: T[K] | null };
