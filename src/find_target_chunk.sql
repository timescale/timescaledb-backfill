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
select
  x.chunk_schema
, x.chunk_name
, x.hypertable_schema
, x.hypertable_name
from
(
    select
      c.schema_name as chunk_schema
    , c.table_name as chunk_name
    , h.schema_name as hypertable_schema
    , h.table_name as hypertable_name
    , count(distinct ds.id) as nbr_dimension_slices
    from _timescaledb_catalog.hypertable h
    inner join _timescaledb_catalog.dimension d on (h.id = d.hypertable_id)
    inner join jsonb_to_recordset($3::text::jsonb) i(column_name name, column_type regtype, range_start bigint, range_end bigint)
    on (i.column_name = d.column_name and i.column_type = d.column_type)
    inner join _timescaledb_catalog.dimension_slice ds on (d.id = ds.dimension_id and ds.range_start = i.range_start and ds.range_end = i.range_end)
    inner join _timescaledb_catalog.chunk_constraint cc on (ds.id = cc.dimension_slice_id)
    inner join _timescaledb_catalog.chunk c on (cc.chunk_id = c.id)
    where h.schema_name = $1
    and h.table_name = $2
    group by 1, 2, 3, 4
) x
where x.nbr_dimension_slices = jsonb_array_length($3::text::jsonb)
;
