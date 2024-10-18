
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
		(
			CASE
				WHEN (cardinality(split(project_name, '.')) > 1) THEN split(project_name, '.') [ 2 ] ELSE ''
			END
		) sub_str,
		user_name,
		user_group,
		cluster_name,
		job_description,
		start_time_gmt as start_time_gmt,
		finish_time_gmt,
		date(start_time) as start_time,
		queue_name,
		mem_req,
		num_slots,
		job_cmd,
		job_run_time,
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
			sub_str,
			user_name,
			user_group,
			cluster_name,
			job_description,
			start_time_gmt,
			finish_time_gmt,
			date(start_time) as start_time,
			queue_name,
			mem_req,
			num_slots,
			job_cmd,
			job_run_time,
			row_sha2
		from pa_data
			left join proxy_string on pa_data.project_name = proxy_string.proxy_project_string
	)
	UNION
	(
		select split(project_name, '.') [ 1 ] as project_name,
			(
				CASE
					WHEN (cardinality(split(project_name, '.')) > 1) THEN split(project_name, '.') [ 2 ] ELSE ''
				END
			) sub_str,
			user_name,
			user_group,
			cluster_name,
			job_description,
			start_time_gmt as start_time_gmt,
			finish_time_gmt,
			date(start_time) as start_time,
			queue_name,
			mem_req,
			num_slots,
			job_cmd,
			job_run_time,
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
		AVG(job_run_time) OVER(PARTITION BY job_description, user_name) AS target_runtime,
		STDDEV(job_run_time) OVER(PARTITION BY job_description, user_name) AS STDEV_runtime,
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
-- __________________________________________________________________________________________________________________________________________________
mile_code as(
	SELECT plan.sap_project_id,
		plan.design_revision_code,
		plan.milestone_code,
		plan.phase_gate_milestone_code,
		actual_date,
		forecast_date,
		milestone_code_mapping,
		row_number() over (
			partition by actual_date,
			sap_project_id,
			milestone_code_mapping
			order by actual_date
		) as row_rank
	FROM "rd_dl_sciforma_hudi"."sf_project_plan" plan
		LEFT join "rd_hde_crystalball"."sf_milestone_code_mapping" mapping ON (
			plan.milestone_code = mapping.milestone_code
			OR plan.phase_gate_milestone_code = mapping.milestone_code
		)
	where version_code = 'Published'
-- 	and plan.sap_project_id in (select project_name from pa)
		and(
			plan.milestone_code is not null
			OR plan.phase_gate_milestone_code IS not NUll
		)
),
milestone as (
	select mile_code.sap_project_id,
		array_except(
			array_agg(mile_code.design_revision_code),
			array_agg(
				case
					when mile_code.design_revision_code is null then mile_code.design_revision_code
				end
			)
		) as design_revision_code,
		mile_code.forecast_date,
		array_agg(mile_code.milestone_code_mapping) as milestone_code_mapping
	from mile_code
	where milestone_code_mapping is not NUll
		and row_rank = 1
	group by sap_project_id,
		forecast_date
	order by sap_project_id,
		forecast_date
),
pa_add_mile as (
	select 
		pa.*,
		ml.forecast_date as milestone_forecast_date,
		ml.milestone_code_mapping,
		ml.design_revision_code,
		row_number() OVER(
			partition by pa.row_sha2
			order by ml.forecast_date desc
		) as milestone_rank
	from pa
		left join milestone as ml on pa.project_name = ml.sap_project_id
		and pa.start_time >= ml.forecast_date
),
sciforma_v2 as (
	SELECT v2.sap_project_id,
		v2.project_mag_code,
		v2.project_life_cycle,
		v2.project_type,
		v2.pf_product_type,
		v2.pf_product_group_name,
		v2.complexity_code,
		v2.ifrs_type,
		v2.ifrs_code,
		v2.technology,
		v2.pf_segment_name,
		v2.pf_level_6,
		date_diff('week', v2.start_date, v2.finish_date) as project_length
	FROM "rd_dl_sciforma_hudi"."sf_project_v2" as v2
	WHERE version_code = 'Published'
		AND sap_project_id != '000000'
-- 		and sap_project_id in (select project_name from pa)
		AND pf_product_group_name not in (
			'Support IT',
			'The Support IT',
			'The Demo Portfolio',
			'The Archive'
		)
)
select pa_add_mile.*,
	v2.project_mag_code,
	v2.project_life_cycle,
	v2.project_type,
	v2.pf_product_type,
	v2.pf_product_group_name,
	v2.complexity_code,
	v2.ifrs_type,
	v2.ifrs_code,
	v2.technology,
	v2.pf_segment_name,
	v2.pf_level_6,
	v2.project_length
from pa_add_mile
	left join sciforma_v2 as v2 on pa_add_mile.project_name = v2.sap_project_id
where pa_add_mile.milestone_rank = 1
-- order by project_name

