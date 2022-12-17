-- Create dim state (id, country_id, state_code)

create table if not exists dim_state (
	id uuid unique,
	country_id uuid,
	state_code varchar,
	primary key(id),
	foreign key(country_id) references dim_country(id),
	constraint country_state unique (country_id, state_code)
);

insert into dim_state (
  id, 
  country_id,
  state_code
)
(
	with combined_data as (
		-- get data from companies
		select distinct
			case when country_code is null or country_code = '' then 'others' else country_code end as country_code,
			case when state_code is null or state_code = '' then 'others' else state_code end as state_code
		from sample_training_companies
		
		union
		
		-- get data from zips
		select distinct 
			'others' as country_code,
			state as state_code
		from sample_training_zips
	)
	
	-- impute null values and add uuid generator
	, imputed as (
		select 
			gen_random_uuid() as id, 
			country_code,
			state_code
		from combined_data
	)
	
	-- combine with dim_country to get country uuid
	select
		imputed.id,
		dc.id as country_id,
		imputed.state_code
	from imputed
	left join dim_country dc 
		on dc.country_code = imputed.country_code
)
on conflict (country_id, state_code) do nothing 
;