

-- merge ppfs and oracle price of want to get moo token price
with vault_scope
select token_address, vault_id, want_price_oracle_id, want_decimals
from beefy_raw.vault
