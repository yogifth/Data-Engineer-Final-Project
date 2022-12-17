drop table if exists fact_monthly_avg_currency;
create table if not exists fact_monthly_avg_currency(
	id uuid unique primary key,
	monthly_avg float8 not null,
	timestamp timestamp not null
);

insert into fact_monthly_avg_currency (id, monthly_avg, timestamp)
select gen_random_uuid() as id, avg(rate), current_timestamp from topic_currency tc 
	where tc."timestamp" > now() - interval '1 month' 