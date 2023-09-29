-- Copyright 2023 Timescale, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
with
    agg_types as materialized(
        select distinct
            proname, pg_type.oid, case when typcollation != 0 then 'C' end as c_collate
        from pg_type
        join
            pg_proc as pp
            on (pg_get_function_arguments(pp.oid) = format_type(pg_type.oid, null))
        join pg_namespace as pn on (pronamespace = pn.oid)
        where
            nspname = 'pg_catalog'
            and (
                proname in ('max', 'min')
                or (proname = 'sum' and typname not in ('float4', 'float8'))
            )
    ),
    verification_items as (
        select
            -- we create jsonb objects which contain all the columns
            -- that we want to calculate the count,min,max and sum for.
            -- these column lists may differ, one can do max(text),
            -- but not sum(text).
            -- we populate the values with the collation to sort on, if any
            jsonb_object(
                coalesce(all_columns, '{}'::text[]), coalesce(all_empty, '{}'::text[])
            ) as count,
            jsonb_object(
                coalesce(max_columns, '{}'::text[]), coalesce(max_values, '{}'::text[])
            ) as max,
            jsonb_object(
                coalesce(min_columns, '{}'::text[]), coalesce(min_values, '{}'::text[])
            ) as min,
            jsonb_object(
                coalesce(sum_columns, '{}'::text[]), coalesce(sum_values, '{}'::text[])
            ) as sum
        from pg_class as pc
        join pg_namespace as pn on (relnamespace = pn.oid)
        join
            lateral(
                select
                    array_agg(attname order by attnum) filter (
                        where proname is null or proname = 'max'
                    ),
                    array_agg(null::text) filter (
                        where proname is null or proname = 'max'
                    ),
                    array_agg(attname order by attnum) filter (where proname = 'max'),
                    array_agg(c_collate order by attnum) filter (where proname = 'max'),
                    array_agg(attname order by attnum) filter (where proname = 'min'),
                    array_agg(c_collate order by attnum) filter (where proname = 'min'),
                    array_agg(attname order by attnum) filter (where proname = 'sum'),
                    array_agg(c_collate order by attnum) filter (where proname = 'sum')
                from pg_attribute
                left join agg_types on (atttypid = oid)
                where attrelid = pc.oid and attnum > 0 and not attisdropped
            ) as a(
                all_columns,
                all_empty,
                max_columns,
                max_values,
                min_columns,
                min_values,
                sum_columns,
                sum_values
            )
            on (true)
        where format('%I.%I', nspname, relname)::text::regclass = $1::text::regclass
    )
select format('count(*) AS total_count,
    jsonb_build_object(%s)::text AS count,
    jsonb_build_object(%s)::text AS min,
    jsonb_build_object(%s)::text AS max,
    jsonb_build_object(%s)::text AS sum
    ', build_count, build_min, build_max, build_sum) as query
from verification_items
cross join
    lateral(
        select string_agg(format('%L, count(%I)', cname, cname), ', ' order by cname)
        from jsonb_object_keys(count) as _(cname)
    ) as countc(build_count)
cross join
    lateral(
        select
            string_agg(
                case
                    when value is not null
                    then format('%L, min(%I COLLATE %I)::text', key, key, value)
                    else format('%L, min(%I)::text', key, key)
                end,
                ', '
                order by key
            )
        from jsonb_each_text(min)
    ) as bmin(build_min)
cross join
    lateral(
        select
            string_agg(
                case
                    when value is not null
                    then format('%L, max(%I COLLATE %I)::text', key, key, value)
                    else format('%L, max(%I)::text', key, key)
                end,
                ', '
                order by key
            )
        from jsonb_each_text(max)
    ) as bmax(build_max)
cross join
    lateral(
        select string_agg(format('%L, sum(%I)', cname, cname), ', ' order by cname)
        from jsonb_object_keys(sum) _(cname)
    ) as bsum(build_sum)
;
