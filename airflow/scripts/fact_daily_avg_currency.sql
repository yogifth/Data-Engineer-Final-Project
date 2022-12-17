drop table if exists fact_daily_avg_currency;
-- create table fact_daily_avg_currency

create table if not exists fact_daily_avg_currency (
	id uuid unique primary key,
	daily_avg float8 not null,
	timestamp timestamp not null
);

insert into fact_daily_avg_currency (id, daily_avg, timestamp)
select gen_random_uuid() as id, avg(rate), current_timestamp from topic_currency tc 
	where tc."timestamp" > now() - interval '1 day'