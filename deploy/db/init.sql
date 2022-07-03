-- this will revoke default database privileges (CREATE, CONNECT ...) from roles in 'PUBLIC' (all roles). 
REVOKE ALL ON DATABASE beefy FROM public; 
REVOKE ALL ON SCHEMA public FROM public;
REVOKE ALL ON DATABASE postgres FROM public;

CREATE SCHEMA IF NOT EXISTS beefy_raw;
ALTER SCHEMA beefy_raw OWNER TO beefy;

-- drop owned by grafana_ro cascade;
-- drop user grafana_ro;
CREATE USER grafana_ro WITH PASSWORD 'grafana_ro'
NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
ALTER USER grafana_ro SET search_path = public,beefy_raw,beefy_derived;

GRANT CONNECT ON DATABASE beefy TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;

GRANT USAGE ON SCHEMA beefy_raw TO grafana_ro;
GRANT USAGE ON SCHEMA beefy_derived TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA beefy_raw TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA beefy_derived TO grafana_ro;