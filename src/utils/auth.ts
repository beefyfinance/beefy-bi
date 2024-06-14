const bearerPreamble = 'bearer ';
const bearerPreambleLength = bearerPreamble.length;

export function isValidBearerAuth(authHeader: string | undefined, validTokens: string[]) {
  if (validTokens.length === 0) {
    return false;
  }
  if (!authHeader || authHeader.length < bearerPreambleLength) {
    return false;
  }
  if (authHeader.toLowerCase().substring(0, bearerPreambleLength) !== bearerPreamble) {
    return false;
  }
  const token = authHeader.substring(bearerPreambleLength).trim();
  if (!token.length) {
    return false;
  }
  return validTokens.includes(token);
}
