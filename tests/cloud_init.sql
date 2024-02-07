CREATE ROLE tsdbadmin WITH LOGIN;
CREATE DATABASE tsdb WITH OWNER tsdbadmin;

\c tsdb;

ALTER FUNCTION public.timescaledb_pre_restore() SET search_path = pg_catalog,pg_temp SECURITY DEFINER;
ALTER FUNCTION public.timescaledb_post_restore() SET search_path = pg_catalog,pg_temp SECURITY DEFINER;
REVOKE EXECUTE ON FUNCTION public.timescaledb_pre_restore() FROM public;
REVOKE EXECUTE ON FUNCTION public.timescaledb_post_restore() FROM public;
GRANT EXECUTE ON FUNCTION public.timescaledb_pre_restore() TO tsdbadmin;
GRANT EXECUTE ON FUNCTION public.timescaledb_post_restore() TO tsdbadmin;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA _timescaledb_cache FROM tsdbadmin;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA _timescaledb_catalog FROM tsdbadmin;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA _timescaledb_config FROM tsdbadmin;
-- only revoke permissions from non-chunks tables in _timescaledb_internal
DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class
            WHERE relname = 'bgw_job_stat' AND
                    relnamespace = '_timescaledb_internal'::regnamespace::oid
        )
        THEN
            REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE _timescaledb_internal.bgw_job_stat FROM tsdbadmin;
        END IF;
        IF EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class
            WHERE relname = 'bgw_policy_chunk_stats' AND
                    relnamespace = '_timescaledb_internal'::regnamespace::oid
        )
        THEN
            REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE _timescaledb_internal.bgw_policy_chunk_stats FROM tsdbadmin;
        END IF;
        IF EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class
            WHERE relname = 'hypertable_chunk_local_size' AND
                    relnamespace = '_timescaledb_internal'::regnamespace::oid
        )
        THEN
            REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE _timescaledb_internal.hypertable_chunk_local_size FROM tsdbadmin;
        END IF;
        IF EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class
            WHERE relname = 'compressed_chunk_stats' AND
                    relnamespace = '_timescaledb_internal'::regnamespace::oid
        )
        THEN
            REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE _timescaledb_internal.compressed_chunk_stats FROM tsdbadmin;
        END IF;
    END
$$;

REVOKE UPDATE ON ALL SEQUENCES IN SCHEMA _timescaledb_cache FROM tsdbadmin;
REVOKE UPDATE ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog FROM tsdbadmin;
REVOKE UPDATE ON ALL SEQUENCES IN SCHEMA _timescaledb_config FROM tsdbadmin;

REVOKE CREATE ON SCHEMA _timescaledb_cache FROM tsdbadmin;
REVOKE CREATE ON SCHEMA _timescaledb_catalog FROM tsdbadmin;
REVOKE CREATE ON SCHEMA _timescaledb_config FROM tsdbadmin;
REVOKE CREATE ON SCHEMA _timescaledb_internal FROM tsdbadmin;


-- we don't use CREATE OR REPLACE FUNCTION here, as REPLACE will keep the permissions of the original user,
-- opening us to an attack where the less-privileged user creates a "bait" function, waiting for the extension
-- to be created or updated and replacing the trigger function subsequently to its own version. For that reason,
-- we also check precisely the properties of the function (owner, arguments, language) when skipping its creation.
DO $do$
    BEGIN
        IF NOT EXISTS(
            SELECT 1
            FROM pg_catalog.pg_proc
            WHERE pronamespace = 'public'::regnamespace::oid AND
                    proname = 'validate_job_role' AND
                    pg_catalog.pg_get_userbyid(proowner) = 'postgres' AND
                    pronargs = 0 AND
                    prorettype = 'trigger'::regtype::oid AND
                    prolang = (SELECT oid FROM pg_catalog.pg_language WHERE lanname = 'plpgsql')
        )
        THEN
            -- if the function exists, but not with the ownership or other properties of the desired one, complain here
            -- We avoid CREATE FUNCTION to error out because of a duplicate, as its error message would be confusing to the user.
            IF EXISTS(
                SELECT 1
                FROM pg_catalog.pg_proc
                WHERE pronamespace = 'public'::regnamespace::oid AND
                        proname = 'validate_job_role'
            )
            THEN
                RAISE EXCEPTION 'Function public.validate_job_role is already defined by the user'
                    USING HINT='Drop the function and retry';
            END IF;
            CREATE FUNCTION public.validate_job_role() RETURNS TRIGGER LANGUAGE plpgsql AS $tg$
      DECLARE
        roleowner    name;
      BEGIN
        IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
          roleowner = NEW.owner;
        ELSE
          RAISE EXCEPTION '% is installed for %', TG_NAME, TG_OP
            USING HINT = 'trigger should only be installed for INSERT or UPDATE';
        END IF;
        if roleowner IS NULL THEN
          RAISE EXCEPTION 'owner field of %.% cannot be NULL', TG_TABLE_SCHEMA, TG_TABLE_NAME;
        END IF;
        IF (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_role)
        THEN
          -- superusers do what they want
          RETURN NEW;
        END IF;
        IF (SELECT current_setting('timescaledb.restoring')::bool = true AND roleowner = 'postgres') THEN
          -- if the owner was postgres and we are restoring (i.e. this is a
          -- dump from self-hosted timescale), rewrite owner to current_user.
          NEW.owner := current_user;
          return NEW;
        END IF;
        IF NOT pg_catalog.pg_has_role(current_role, roleowner, 'MEMBER') THEN
          RAISE EXCEPTION 'Cannot % table %.% row ID %', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.id USING HINT = 'owner is not a member of the current role',
          ERRCODE = '42501';
        END IF;
        RETURN NEW;
      END
      $tg$ SET search_path = pg_catalog;
            -- make sure the function is dropped together with the extension
            ALTER EXTENSION timescaledb ADD FUNCTION public.validate_job_role();
        END IF;
    END
$do$;


DO $$
    DECLARE
        typname text;
    BEGIN
        IF pg_catalog.to_regclass('_timescaledb_internal.job_errors') IS NOT NULL THEN
            -- We need to ensure the job_id 2, which is the policy job for error retention is owned by the database owner
            -- The job having id == 2 is hardcoded in the TimescaleDB code base:
            -- https://github.com/timescale/timescaledb/blob/7a6101a441ca4ad02018ffddd225e7abdea4385f/sql/job_error_log_retention.sql#L60

            SELECT
                pg_catalog.format_type(atttypid, atttypmod)
            INTO STRICT
                typname
            FROM
                pg_catalog.pg_attribute
            WHERE
                    attrelid = '_timescaledb_config.bgw_job'::regclass
              AND attname = 'owner';

            -- In TimescaleDB 2.10.2 the data type of the column changed. This in turn requires a different sql
            -- statement to run depending on what datatype the column is
            IF typname = 'regrole' THEN
                UPDATE
                    _timescaledb_config.bgw_job
                SET
                    owner = 'tsdbadmin'::regrole
                WHERE
                        id = 2
                  AND owner != 'tsdbadmin'::regrole
                  AND proc_name = 'policy_job_error_retention'
                  -- The procedure schema changes from ts2.12.0 onwards, so we check for both
                  -- old and new names
                  AND proc_schema IN ('_timescaledb_internal', '_timescaledb_functions');
            ELSE
                UPDATE
                    _timescaledb_config.bgw_job
                SET
                    owner = 'tsdbadmin'
                WHERE
                        id = 2
                  AND owner != 'tsdbadmin'
                  AND proc_name = 'policy_job_error_retention'
                  AND proc_schema IN ('_timescaledb_internal', '_timescaledb_functions');
            END IF;

            -- tsdbadmin should also be allowed to purge/clean the job errors
            GRANT DELETE ON _timescaledb_internal.job_errors TO tsdbadmin;
        END IF;
    END;
$$;

-- we need to block manually inserting or changing the owner field of the jobs timescale catalog, to avoid
-- a job running as a superuser or a user the current_user is not a member of. Otherwise, we may get privileges
-- escalation. Check the trigger is not disabled and is BEFORE ROW INSERT or UPDATE (tgtype controls that) one.
DO $$
    BEGIN
        IF NOT EXISTS(
            SELECT 1
            FROM pg_catalog.pg_trigger
            WHERE tgname = 'validate_job_role_trigger'
              AND tgrelid = '_timescaledb_config.bgw_job'::regclass::oid
              AND tgfoid = 'public.validate_job_role()'::regprocedure::oid
              AND tgenabled IN ('O', 'A') AND tgtype = 23
        )
        THEN
            -- see the comment at the function creation about avoiding a confusing error message
            IF EXISTS(
                SELECT 1 FROM pg_catalog.pg_trigger
                WHERE tgname = 'validate_job_role_trigger'
            )
            THEN
                RAISE EXCEPTION 'Trigger validate_job_role_trigger is already defined by the user'
                    USING HINT='Drop the trigger and retry';
            END IF;
            CREATE TRIGGER validate_job_role_trigger
                BEFORE INSERT OR UPDATE ON _timescaledb_config.bgw_job
                FOR EACH ROW EXECUTE FUNCTION public.validate_job_role();
        END IF;
    END
$$;


-- add mostly insert permissions to be able to restore a dump. We only add those for the tables that have their
-- data dumped separately from the `CREATE EXTENSION` (the so-called extension configuration tables, see
-- https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES

DO $$
    DECLARE
        relid name;
    BEGIN
        -- we need to grant insert on the extension tables (pg_restore must be able to add the data dumped previously, and
        -- update on sequences, so that pg_restore or the user would able to use setval for sequences to fix them).
        FOR relid in (
            SELECT cfg.relid
            FROM pg_catalog.pg_extension
                     JOIN LATERAL unnest(extconfig) as cfg(relid) on true
            WHERE extname = 'timescaledb'
              AND (pg_catalog.pg_identify_object('pg_catalog.pg_class'::regclass::oid, cfg.relid, 0)).type = 'sequence'
        )
            LOOP
                EXECUTE pg_catalog.format('GRANT UPDATE ON SEQUENCE %s TO tsdbadmin', relid::regclass::text);
            END LOOP;

        -- we only need extension configuration tables (their data is dumped separately from CREATE EXTENSION)
        FOR relid in (
            SELECT cfg.relid
            FROM pg_catalog.pg_extension
                     JOIN LATERAL pg_catalog.unnest(extconfig) as cfg(relid) on true
            WHERE extname = 'timescaledb'
              AND (pg_catalog.pg_identify_object('pg_catalog.pg_class'::regclass::oid, cfg.relid, 0)).type = 'table'
        )
            LOOP
                EXECUTE pg_catalog.format('GRANT INSERT ON TABLE %s TO tsdbadmin', relid::regclass::text);
                -- tsdbadmin requires update permissions on cagg migration tables (related PR timescale/timestaledb#4552)
                IF relid::regclass::text ~ 'continuous_agg_migrate_plan(|_step)$' THEN
                    EXECUTE pg_catalog.format('GRANT UPDATE ON TABLE %s TO tsdbadmin', relid::regclass::text);
                END IF;
            END LOOP;

        -- The telemetry events table is intended for application data (like cli tools)
        -- that should be included in telemetry.
        IF EXISTS (
            SELECT FROM pg_tables
            WHERE schemaname = '_timescaledb_catalog'
              AND tablename = 'telemetry_event'
        ) THEN
            GRANT INSERT ON TABLE _timescaledb_catalog.telemetry_event TO tsdbadmin;
        END IF;

        -- We should allow the creation of new chunk tables within the _timescaledb_internal schema using pg_restore.
        -- Although we could implement an event trigger to specifically filter chunk tables, this may be unnecessary since
        -- the schema primarily contains _stat tables and chunks, and it's improbable that we would introduce any new stat
        -- tables in the future. We add GRANT OPTION to allow tsdbadmin to grant create privileges TO other roles; this is
        -- necessary, i.e. to change the continuous aggregate owner, matching owner of the job that does the referesh policy.
        -- See https://github.com/timescale/timescaledb/issues/5745 for the bugreport that would be impossible to fix without
        -- the GRANT OPTION.
        GRANT CREATE ON SCHEMA _timescaledb_internal TO tsdbadmin WITH GRANT OPTION;

        -- Workaround for https://github.com/timescale/timescaledb/issues/5449
        -- Long story short:
        -- - pg_dump takes out locks on *all* extension tables
        -- - excluding them from pg_dump does not work
        -- - without this fix, a user gets: pg_dump: error: query failed: ERROR:  permission denied for table job_errors
        DECLARE
            job_table text := '_timescaledb_internal.job_errors';
        BEGIN
            -- This only returns TRUE if:
            -- - grantee exists
            -- - job_table exists
            -- - SELECT privs are missing
            IF NOT pg_catalog.has_table_privilege('tsdbadmin', pg_catalog.to_regclass(job_table), 'SELECT WITH GRANT OPTION')
            THEN
                EXECUTE format('GRANT SELECT ON %s TO tsdbadmin WITH GRANT OPTION', job_table);
            END IF;
        END;
    END
$$;
