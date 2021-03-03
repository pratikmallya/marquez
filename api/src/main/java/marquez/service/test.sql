select lineage.job_name,
       d_in.agg as inputs,
       d_out.agg as outputs
from (
WITH RECURSIVE search_graph(job_name, namespace_name, depth, path, cycle) AS (
select r.job_name, r.namespace_name, 1, ARRAY[r.job_name], false from (select distinct on (job_name, namespace_name) job_name, namespace_name, transitioned_at, uuid
from runs
where current_run_state = 'COMPLETED' and runs.job_name = 'prepare_apple_filling'
order by job_name, namespace_name, transitioned_at DESC) r
UNION ALL
-- upstream jobs
select j.job_name, j.namespace_name, depth+1, path || j.job_name, j.job_name = ANY(path) from search_graph sg, (
select r2.job_name as job_name, r2.namespace_name, r.job_name as jx
from datasets ds
inner join dataset_versions dv on ds.uuid = dv.dataset_uuid
inner join runs_input_mapping rim on rim.dataset_version_uuid = dv.uuid
inner join (
  select distinct on (job_name, namespace_name) job_name, namespace_name, transitioned_at, uuid
  from runs
  where current_run_state = 'COMPLETED'
  order by job_name, namespace_name, transitioned_at DESC) r on r.uuid = rim.run_uuid
inner join dataset_versions dv2 on ds.uuid = dv2.dataset_uuid
inner join (
  select distinct on (job_name, namespace_name) job_name, namespace_name, transitioned_at, uuid
  from runs
  where current_run_state = 'COMPLETED'
  order by job_name, namespace_name, transitioned_at DESC) r2 on r2.uuid = dv2.run_uuid
UNION
-- Downstream jobs
select r2.job_name, r2.namespace_name, r.job_name as jx
from datasets ds
inner join dataset_versions dv on ds.uuid = dv.dataset_uuid
inner join (
  select distinct on (job_name, namespace_name) job_name, namespace_name, transitioned_at, uuid
  from runs
  where current_run_state = 'COMPLETED'
  order by job_name, namespace_name, transitioned_at DESC) r on r.uuid = dv.run_uuid
inner join dataset_versions dv2 on ds.uuid = dv2.dataset_uuid
inner join runs_input_mapping rim on rim.dataset_version_uuid = dv2.uuid
inner join (
  select distinct on (job_name, namespace_name) job_name, namespace_name, transitioned_at, uuid
  from runs
  where current_run_state = 'COMPLETED'
  order by job_name, namespace_name, transitioned_at DESC) r2 on r2.uuid = rim.run_uuid
) j
where jx = sg.job_name and NOT cycle
)
SELECT job_name, namespace_name FROM search_graph where NOT cycle and depth <= 2) lineage
inner join (select distinct on (job_name, namespace_name) job_name, namespace_name, transitioned_at, uuid
from runs
where current_run_state = 'COMPLETED'
order by job_name, namespace_name, transitioned_at DESC) r on lineage.job_name = r.job_name and lineage.namespace_name = r.namespace_name
-- input datasets
left outer join
     (select ru.uuid, jsonb_agg((SELECT x FROM (SELECT ds_in.name, n_in.name as namespace) AS x)) as agg
      from runs_input_mapping rim
          inner join runs ru on rim.run_uuid = ru.uuid
          inner join dataset_versions dv_in on dv_in.uuid = rim.dataset_version_uuid
          inner join datasets ds_in on ds_in.uuid = dv_in.dataset_uuid
          inner join namespaces n_in on n_in.uuid = ds_in.namespace_uuid
      group by ru.job_name, ru.namespace_name, ru.uuid
     ) d_in on d_in.uuid = r.uuid

-- output datasets
left outer join
     (select ru.uuid, jsonb_agg((SELECT x FROM (SELECT ds_out.name, n_out.name as namespace) AS x)) as agg
      from dataset_versions dv_out
               inner join datasets ds_out on ds_out.uuid = dv_out.dataset_uuid
               inner join namespaces n_out on n_out.uuid = ds_out.namespace_uuid
               inner join runs ru on dv_out.run_uuid = ru.uuid
                group by ru.job_name, ru.namespace_name, ru.uuid
     ) d_out on d_out.uuid = r.uuid;