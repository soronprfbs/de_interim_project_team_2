insert into cdm.sales_by_dt (dt, sales_count)
select
	date_trunc('hour', event_timestamp) as dt,
	count(distinct event_id) as sales_count
from dds.fct_events
where
	page_1 = '/confirmation'
group by 1
on conflict (dt) do update
set
	sales_count = excluded.sales_count