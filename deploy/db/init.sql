-- this will revoke default database privileges (CREATE, CONNECT ...) from roles in 'PUBLIC' (all roles). 
REVOKE ALL ON DATABASE beefy FROM public; 
REVOKE ALL ON SCHEMA public FROM public;
REVOKE ALL ON DATABASE postgres FROM public;

CREATE SCHEMA IF NOT EXISTS data_raw;
ALTER SCHEMA data_raw OWNER TO beefy;

-- drop owned by grafana_ro cascade;
-- drop user grafana_ro;
CREATE USER grafana_ro WITH PASSWORD 'grafana_ro'
NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
ALTER USER grafana_ro SET search_path = public,data_raw,data_derived,data_report;
ALTER USER beefy SET search_path = public,data_raw,data_derived,data_report;

GRANT CONNECT ON DATABASE beefy TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;

GRANT USAGE ON SCHEMA data_raw TO grafana_ro;
GRANT USAGE ON SCHEMA data_derived TO grafana_ro;
GRANT USAGE ON SCHEMA data_report TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA data_raw TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA data_derived TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA data_report TO grafana_ro;
