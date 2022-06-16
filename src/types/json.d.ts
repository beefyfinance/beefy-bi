// error TS2307: Cannot find module '../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json' or its corresponding type declarations.
// https://github.com/aurelia/cli/issues/493#issuecomment-281916899
declare module "*.json" {
  const value: any;
  export default value;
}
