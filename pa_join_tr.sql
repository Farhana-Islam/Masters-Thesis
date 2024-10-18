
with proxy_string_all as (
	SELECT sap_project_id,
		lower(proxy_project_string) AS proxy_project_string
	FROM rd_hde_crystalball.proxy_project_to_sciforma
	UNION ALL
	(
		SELECT DISTINCT PANIC_STR AS sap_project_id,
			lower(PANIC_SCIFORMA_NAME) AS proxy_project_string
		FROM rd_dl_panic_hudi.panic_reg
		WHERE length(trim(PANIC_SCIFORMA_NAME)) > 0
			AND length(PANIC_STR) = 6
	)
),
proxy_string as (
	select *
	from proxy_string_all
	group by sap_project_id,
		proxy_project_string
),
proxy_project_id as (
	select sap_project_id,
		proxy_project_string
	from proxy_string
	where sap_project_id in (
			select DISTINCT(split(project_name, '.') [ 1 ])
			from "rd_dl_pa_hudi"."rpt_jobmart_raw"
			where job_description LIKE '%~%'
				and year = 2024
				and month in (1)
				and day between 1 and 2
				and cluster_name IN ('nl-cdc01', 'ie-awsc1-03')
				and queue_name NOT IN ('interq', 'spotq')
                and job_name not like 'DCV%'
				and job_exit_status = 'DONE'
				and length(split(project_name, '.') [ 1 ]) = 6
		)
),
pa_data as (
	select split(project_name, '.') [ 1 ] as project_name,
		user_name,
		job_description,
		job_name,
		date(start_time) as start_time,
		job_run_time,
		job_cmd,
		row_sha2
	from rd_dl_pa_hudi.rpt_jobmart_raw
	where project_name in (
			select DISTINCT(proxy_project_string)
			from proxy_project_id
		)
		and year = 2024
		and month in (1)
		and day between 1 and 4
		and job_description LIKE '%~%'
		and cluster_name IN ('nl-cdc01', 'ie-awsc1-03')
		and queue_name NOT IN ('interq', 'spotq')
        and job_name not like 'DCV%'
		and job_exit_status = 'DONE'
),
pa_data_all as (
	(
		select proxy_string.sap_project_id as project_name,
			user_name,
			job_description,
			job_name,
			start_time,
			job_run_time,
			job_cmd,
			row_sha2
		from pa_data
			left join proxy_string on pa_data.project_name = proxy_string.proxy_project_string
	)
	UNION
	(
		select split(project_name, '.') [ 1 ] as project_name,
			user_name,
			job_description,
			job_name,
			date(start_time) as start_time,
			job_run_time,
			job_cmd,
			row_sha2
		from rd_dl_pa_hudi.rpt_jobmart_raw
		where year = 2024
			and month in (1)
			and day between 1 and 4
			and job_description LIKE '%~%'
			and cluster_name IN ('nl-cdc01', 'ie-awsc1-03')
			and queue_name NOT IN ('interq', 'spotq')
            and job_name not like 'DCV%'
			and job_exit_status = 'DONE'
			and length(split(project_name, '.') [ 1 ]) = 6
	)
),
pa_data_aggregate as (
	SELECT *,
		row_number() over (
			PARTITION by job_description,
			user_name
			order by job_run_time desc
		) as row_rank
	FROM pa_data_all
),
pa as (
	select *
	from pa_data_aggregate
	where row_rank = 1
),
-- ________________________________________________________________________________________________________________
tr_all as (
	SELECT user,
		lm_project,
		split(package, '-') [ 2 ] as tool,
		split(package, '-') [ 1 ] as vendor
	FROM "rd_dl_tool_repository"."tr_tool_log_v2"
	WHERE year = 2024
		AND lm_project IS NOT NULL
		AND (Cardinality("split"(package, '-')) > 1)
		AND month in (1)
		AND lm_project LIKE '%~%'
),
tr as (
	Select array_agg(distinct(tool)) as tool,
		array_agg(distinct(vendor)) as vendor,
		user,
		lm_project
	from tr_all
	GROUP BY user,
		lm_project
)
select pa.project_name,
	pa.user_name,
	pa.job_description,
	pa.job_cmd,
	pa.row_sha2,
	tr.tool,
	tr.vendor
FROM pa
	LEFT join tr on (
		pa.job_description = tr.lm_project
		and pa.user_name = tr.user
	)
