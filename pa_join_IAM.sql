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
        submit_time_gmt,
		date(start_time) as start_time,
		job_run_time,
		job_cmd,
        job_mem_usage,
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
            submit_time_gmt,
			start_time,
			job_run_time,
			job_cmd,
            job_mem_usage,
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
            submit_time_gmt,
			date(start_time) as start_time,
			job_run_time,
			job_cmd,
            job_mem_usage,
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
user_skill as (
	select wbi_id,
    array_agg(distinct(job_classification)) as job_class,
	array_agg(distinct(skills)) as user_skills
	from "rd_dl_sciforma_hudi"."sf_project_allocations"
	where wbi_id in (
			select distinct(user_name)
			from pa
		)
	group by wbi_id
	order by wbi_id
),
iam_skill as (
	SELECT iam.wbiacc as wbiacc,
		bl,
		pl,
		mag,
        city,
        country,
		iam.directmanagerid,
		sitecode,
		identitystatus,
		identitytype,
		user_skill.user_skills as user_skills,
		user_skill.job_class as job_class
	FROM "rd_dl_org_hudi"."iam" as iam
		left join user_skill on iam.wbiacc = user_skill.wbi_id
	where datalake_ingestion_time = (
			SELECT max(datalake_ingestion_time)
			from "rd_dl_org_hudi"."iam"
		)
		and iam.wbiacc in (
			select distinct(user_name)
			from pa
		)
),
pa_iam_skill as (
	select pa.project_name,
		pa.user_name,
		pa.start_time,
		pa.job_description,
        pa.submit_time_gmt,
		pa.job_cmd,
		pa.job_name,
        pa.job_mem_usage,
		pa.row_sha2,
		iam_skill.bl,
		iam_skill.pl,
		iam_skill.mag,
        iam_skill.city,
        iam_skill.country,
		iam_skill.directmanagerid,
		iam_skill.sitecode,
		iam_skill.identitystatus,
		iam_skill.identitytype,
		iam_skill.user_skills,
		iam_skill.job_class
	from pa
		left join iam_skill on pa.user_name = iam_skill.wbiacc
),
pa_avg as (
	select project_name,
		user_name,
		start_time,
		AVG(job_run_time) as avg_runtime_per_day_for_user
	from pa
	group by project_name,
		user_name,
		start_time
	order by start_time
),
pa_rolling_avg as (
	select *,
		avg(avg_runtime_per_day_for_user) OVER(
			partition by project_name,
			user_name
			ORDER BY start_time ROWS BETWEEN 6 PRECEDING
				AND CURRENT ROW
		) as moving_average
	from pa_avg
),
pa_all_feature as (
	select pa_iam_skill.*,
		pa_rolling_avg.moving_average
	from pa_iam_skill
		left join pa_rolling_avg on pa_iam_skill.project_name = pa_rolling_avg.project_name
		and pa_iam_skill.user_name = pa_rolling_avg.user_name
		and pa_iam_skill.start_time = pa_rolling_avg.start_time
)
SELECT
a.project_name,
		a.user_name,
		a.job_description,
        a.submit_time_gmt,
        a.job_mem_usage,
		a.row_sha2,
		a.bl,
		a.pl,
		a.mag,
        a.city,
        a.country,
		a.directmanagerid,
		a.sitecode,
		a.identitystatus,
		a.identitytype,
		a.user_skills,
		a.job_class,
		a.moving_average
	
FROM pa_all_feature as a


