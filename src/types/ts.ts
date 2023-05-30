// https://typeofnan.dev/making-every-object-property-nullable-in-typescript/
export type Nullable<T> = { [K in keyof T]: T[K] | null };

// https://stackoverflow.com/a/73083447/2523414
type NonNullFields<T> = {
  [P in keyof T]: NonNullable<T[P]>;
};
export type NonNullField<T, K extends keyof T> = Omit<Omit<T, K> & NonNullFields<Pick<T, K>>, "">;

type NullFields<T> = {
  [P in keyof T]: null;
};
export type NullField<T, K extends keyof T> = Omit<Omit<T, K> & NullFields<Pick<T, K>>, "">;
