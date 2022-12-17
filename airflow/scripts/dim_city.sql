-- Create dim city (id, country_id, state_id, city_name, zip_code)

create table if not exists dim_city (
	id uuid unique,
	country_id uuid,
	state_id uuid,
	city_name varchar,
	zip_code varchar,
	primary key(id),
	constraint city_zip unique (city_name, zip_code)
);

/* NOTE: 
cannot create foreign key on state_id since the state_code in dim_state is not unique (same state_code can have multiple country_code)
*/ 

insert into dim_city (
  id, 
  country_id,
  state_id,
  city_name,
  zip_code
)
(
	with combined_data as (
		-- get data from companies
		select distinct 
			case when country_code is null or country_code = '' then 'others' else country_code end as country_code,
			case when state_code is null or state_code = '' then 'others' else state_code end as state_code,
			city as city_name,
			zip_code as zip_code
		from sample_training_companies
		
		union
		
		-- get data from zips
		select distinct 
			'others' as country_code,
			state as state_code,
			city as city_name,
			zip as zip_code
		from sample_training_zips
	)
	
	-- impute null values and add uuid generator
	, imputed as (
		select 
			gen_random_uuid() as id, 
			country_code,
			state_code,
			city_name,
			zip_code
		from combined_data
		where (city_name is not null and city_name != '')
			and (zip_code is not null and zip_code != '')	
	)
	
	-- combine with dim_state to get country uuid and state_uuid
	select
		imputed.id,
		dc.id as country_id,
		ds.id as state_id,
		imputed.city_name,
		imputed.zip_code
	from imputed
	left join dim_country dc 
		on dc.country_code = imputed.country_code	
	left join dim_state ds 
		on ds.state_code  = imputed.state_code
		and ds.country_id = dc.id
)
on conflict (city_name, zip_code) do nothing 
;