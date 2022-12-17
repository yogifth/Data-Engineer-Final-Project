--Create dim currency (id, currency_name, currency_code)

create table if not exists dim_currency (
	id uuid unique,
	currency_name varchar,
	currency_code varchar unique,
	primary key(id)
);

insert into dim_currency (
  id, 
  currency_name,
  currency_code
)
(
select 
	gen_random_uuid() as id, 
    currency_name, 
    currency_code 
from(
	-- get data from topic_currency
	select distinct 
        currency_name, 
        currency_id as currency_code
	from currency_topic  
	) ct
)
on conflict (currency_code) do nothing
;