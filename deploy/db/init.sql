-- this will revoke default database privileges (CREATE, CONNECT ...) from roles in 'PUBLIC' (all roles). 
REVOKE ALL ON DATABASE beefy FROM public; 
REVOKE ALL ON SCHEMA public FROM public;
REVOKE ALL ON DATABASE postgres FROM public;

-- drop owned by grafana_ro cascade;
-- drop user grafana_ro;
CREATE USER grafana_ro WITH PASSWORD 'grafana_ro'
NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';

GRANT CONNECT ON DATABASE beefy TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
