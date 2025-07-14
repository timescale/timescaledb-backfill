CREATE ROLE tsdbadmin WITH LOGIN SUPERUSER;
CREATE DATABASE tsdb WITH OWNER tsdbadmin;

--
-- For schema: _timescaledb_internal
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal GRANT ALL PRIVILEGES ON TABLES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal GRANT ALL PRIVILEGES ON SEQUENCES TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA _timescaledb_internal TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA _timescaledb_internal TO tsdbadmin;
GRANT USAGE, CREATE ON SCHEMA _timescaledb_internal TO tsdbadmin;

-- For schema: _timescaledb_config
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_config GRANT ALL PRIVILEGES ON TABLES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_config GRANT ALL PRIVILEGES ON SEQUENCES TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA _timescaledb_config TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA _timescaledb_config TO tsdbadmin;
GRANT USAGE, CREATE ON SCHEMA _timescaledb_config TO tsdbadmin;

-- For schema: _timescaledb_catalog
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_catalog GRANT ALL PRIVILEGES ON TABLES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_catalog GRANT ALL PRIVILEGES ON SEQUENCES TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA _timescaledb_catalog TO tsdbadmin;
GRANT USAGE, CREATE ON SCHEMA _timescaledb_catalog TO tsdbadmin;

-- For schema: _timescaledb_cache
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_cache GRANT ALL PRIVILEGES ON TABLES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_cache GRANT ALL PRIVILEGES ON SEQUENCES TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA _timescaledb_cache TO tsdbadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA _timescaledb_cache TO tsdbadmin;
GRANT USAGE, CREATE ON SCHEMA _timescaledb_cache TO tsdbadmin;

\c tsdb;

-- The pre_restore and post_restore function can only be successfully executed by a very highly privileged
-- user. To ensure the database owner can also execute these functions, we have to alter them
-- from SECURITY INVOKER to SECURITY DEFINER functions. Setting the search_path explicitly is good practice
-- for SECURITY DEFINER functions.
-- As this function does have high impact, we do not want anyone to be able to execute the function,
-- but only the database owner.
ALTER FUNCTION public.timescaledb_pre_restore() SET search_path = pg_catalog,pg_temp SECURITY DEFINER;
ALTER FUNCTION public.timescaledb_post_restore() SET search_path = pg_catalog,pg_temp SECURITY DEFINER;
REVOKE EXECUTE ON FUNCTION public.timescaledb_pre_restore() FROM public;
REVOKE EXECUTE ON FUNCTION public.timescaledb_post_restore() FROM public;
GRANT EXECUTE ON FUNCTION public.timescaledb_pre_restore() TO tsdbadmin;
GRANT EXECUTE ON FUNCTION public.timescaledb_post_restore() TO tsdbadmin;

-- This fixes an issue where tsdbexplorer calls this function for all hypertables, but in TimescaleDB
-- 2.12.0 this function was altered in a way that requires more privileges due to the LOCK acquired
-- when the query is invoked.
DO $$
BEGIN
  IF EXISTS (
    SELECT
    FROM
      pg_extension
    WHERE
      extname = 'timescaledb'
      AND extversion > '2.12.'
  ) THEN
    ALTER FUNCTION public.hypertable_detailed_size(regclass) SET search_path = pg_catalog,pg_temp SECURITY DEFINER;
  END IF;
END
$$;

DO $$
  DECLARE
    rolname name;
  BEGIN
    FOREACH rolname IN ARRAY '{tsdbadmin, tsdbadmin}'::text[]
      LOOP
        IF pg_catalog.to_regrole(rolname) IS NULL
        THEN
          RAISE NOTICE 'Creating user: %', rolname;
          EXECUTE pg_catalog.format('CREATE USER %I', rolname);
        END IF;
      END LOOP;
  END$$;

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

        -- When roleowner has capital letters, it is wrapped in quotes, eg, "testRole". This fails pg_has_role() check.
        -- Using `roleowner::regrole` fixes the issue.
        IF NOT pg_catalog.pg_has_role(current_role, roleowner::regrole, 'MEMBER') THEN
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
  -- TimescaleDB 2.15 renamed _timescaledb_internal.job_errors => _timescaledb_internal.bgw_job_stat_history
  IF COALESCE(pg_catalog.to_regclass('_timescaledb_internal.job_errors'),
              pg_catalog.to_regclass('_timescaledb_internal.bgw_job_stat_history')) IS NOT NULL THEN
    -- We need to ensure the policy job for error retention is owned by the database owner
    -- The job having id 2 initially is hardcoded in the TimescaleDB code base:
    -- https://github.com/timescale/timescaledb/blob/7a6101a441ca4ad02018ffddd225e7abdea4385f/sql/job_error_log_retention.sql#L60
    -- in TimescaleDB 2.15 the job for the policy_job_error_retention has been left on id 2,
    -- policy_job_stat_history_retention got the id 3:
    -- https://github.com/timescale/timescaledb/commit/e298ecd532f40214a0d20a90a37b51b53aa23e91

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
        -- TimescaleDB 2.15 introduced job id 3 for the policy_job_stat_history_retention.
        id in (2, 3)
        AND owner != 'tsdbadmin'::regrole
        AND proc_name IN ('policy_job_error_retention', 'policy_job_stat_history_retention')
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
        AND proc_name IN ('policy_job_error_retention', 'policy_job_stat_history_retention')
        AND proc_schema IN ('_timescaledb_internal', '_timescaledb_functions');
    END IF;

      -- tsdbadmin should also be allowed to purge/clean the job errors
    IF pg_catalog.to_regclass('_timescaledb_internal.job_errors') IS NOT NULL THEN
      GRANT DELETE ON _timescaledb_internal.job_errors TO tsdbadmin;
    ELSE
      GRANT DELETE ON _timescaledb_internal.bgw_job_stat_history TO tsdbadmin;
    END IF;
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

--

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
        -- tsdbadmin requires update permissions on cagg migration tables (related PR https://github.com/timescale/timescaledb/pull/4552)
        IF relid::regclass::text ~ 'continuous_agg_migrate_plan(|_step)$' THEN
            EXECUTE pg_catalog.format('GRANT UPDATE ON TABLE %s TO tsdbadmin', relid::regclass::text);
        END IF;
        -- tsdbadmin requires update permissions on the metadata table from 2.14 onwards (related PR https://github.com/timescale/timescaledb/pull/6554)
        IF relid::regclass::text = '_timescaledb_catalog.metadata' THEN
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
    BEGIN
      -- The NOT has_table_privilege only returns TRUE if:
      -- - grantee exists (if not, it will raise an exception)
      -- - job_table exists (otherwise, it will return NULL)
      -- - SELECT privs are missing
      -- 2.15 renamed job_errors => bgw_job_stat_history, we try both, since IF will not proceed when the table is not there.
      IF NOT pg_catalog.has_table_privilege('tsdbadmin', pg_catalog.to_regclass('_timescaledb_internal.job_errors'), 'SELECT WITH GRANT OPTION')
      THEN
        GRANT SELECT ON _timescaledb_internal.job_errors TO tsdbadmin WITH GRANT OPTION;
      END IF;
      IF NOT pg_catalog.has_table_privilege('tsdbadmin', pg_catalog.to_regclass('_timescaledb_internal.bgw_job_stat_history'), 'SELECT WITH GRANT OPTION')
      THEN
        GRANT SELECT ON _timescaledb_internal.bgw_job_stat_history TO tsdbadmin WITH GRANT OPTION;
      END IF;
    END;
  END
$$;

-- In the below SQL we explicitly set the search_path for all functions belonging
-- to the TimescaleDB extension that are not c or internal functions.
--
-- public.time_bucket functions get special treatment to encourage inlining of the sql
--
-- The below sql should be able to be run multiple times, but it would only affect
-- anything on its first run. This script is therefore safe to run multiple times
-- against the same database.
--
-- Docker images created after 2022-02-16 will *also* contain the fix, however,
-- we'd rather be safe and double fix this for some instances. As the script
-- is a no-op if run multiple times, this should be fine.
DO $dynsql$
DECLARE
    alter_sql text;
BEGIN

    SET local search_path to pg_catalog, pg_temp;

    FOR alter_sql IN
        SELECT
            format(
                $$ALTER FUNCTION %I.%I(%s) SET search_path = pg_catalog, pg_temp$$,
                nspname,
                proname,
                pg_catalog.pg_get_function_identity_arguments(pp.oid)
            )
        FROM
            pg_depend
        JOIN
            pg_extension ON (oid=refobjid)
        JOIN
            pg_proc pp ON (objid=pp.oid)
        JOIN
            pg_namespace pn ON (pronamespace=pn.oid)
        JOIN
            pg_language pl ON (prolang=pl.oid)
        LEFT JOIN LATERAL (
                SELECT * FROM unnest(proconfig) WHERE unnest LIKE 'search_path=%'
            ) sp(search_path) ON (true)
        WHERE
            deptype='e'
            AND extname='timescaledb'
            AND extversion < '2.5.2'
            AND lanname NOT IN ('c', 'internal')
            AND prokind = 'f'
            -- Only those functions/procedures that do not yet have their search_path fixed
            AND search_path IS NULL
            AND proname != 'time_bucket'
        ORDER BY
            search_path
    LOOP
        EXECUTE alter_sql;
    END LOOP;

    -- And for the sql time_bucket functions we prefer to *not* set the search_path to
    -- allow inlining of these functions
    WITH sql_time_bucket_fn AS (
        SELECT
            pp.oid
        FROM
            pg_depend
        JOIN
            pg_extension ON (oid=refobjid)
        JOIN
            pg_proc pp ON (objid=pp.oid)
        JOIN
            pg_namespace pn ON (pronamespace=pn.oid)
        JOIN
            pg_language pl ON (prolang=pl.oid)
        WHERE
            deptype = 'e'
            AND extname='timescaledb'
            AND extversion < '2.5.2'
            AND lanname = 'sql'
            AND proname = 'time_bucket'
            AND prokind = 'f'
            AND prosrc NOT LIKE '%OPERATOR(pg_catalog.%'
    )
    UPDATE
        pg_proc
    SET
        prosrc = regexp_replace(prosrc, '([-+]{1})', ' OPERATOR(pg_catalog.\1) ', 'g')
    FROM
        sql_time_bucket_fn AS s
    WHERE
        s.oid = pg_proc.oid;
END;
$dynsql$;

-- set cloud-specific metadata for TimescaleDB telemetry
DO $sql$
DECLARE
    has_new_function_schema bool;
    exported_uuid text;
    uuid text;
BEGIN
    SELECT EXISTS
           (SELECT 1 FROM pg_proc p
              JOIN pg_namespace n ON p.pronamespace = n.oid
             WHERE p.proname = 'generate_uuid'
               AND n.nspname = '_timescaledb_functions')
    INTO STRICT has_new_function_schema;

    IF has_new_function_schema THEN
        SELECT _timescaledb_functions.generate_uuid(), _timescaledb_functions.generate_uuid()
        INTO STRICT exported_uuid, uuid;
    ELSE
        SELECT _timescaledb_internal.generate_uuid(), _timescaledb_internal.generate_uuid()
        INTO STRICT exported_uuid, uuid;
    END IF;

    INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry)
    VALUES
        ('forge_project_id', COALESCE(pg_catalog.current_setting('cloud.project_id', true), 'undefined'), TRUE),
        ('forge_service_id', COALESCE(pg_catalog.current_setting('cloud.service_id', true), 'undefined'), TRUE),
        ('forge_env', COALESCE(pg_catalog.current_setting('cloud.environment', true), 'undefined'), TRUE),
        ('exported_uuid', exported_uuid, TRUE),
        ('uuid', uuid, TRUE)
    ON CONFLICT (key)
        DO UPDATE SET
            value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry;
END;
$sql$;

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
      END LOOP;

    -- we need to allow new chunk tables to be created by pg_restore in the _timescaledb_internal. We could also
    -- create an even trigger to filter only chunk tables, but there are no only stat objects in this schema, and
    -- it is unlikely we would create any new ones.
    GRANT CREATE ON SCHEMA _timescaledb_internal TO tsdbadmin;
  END
$$;
