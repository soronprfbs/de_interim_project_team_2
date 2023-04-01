insert into cdm.events_by_dt (dt, events_count)
select
	date_trunc('hour', event_timestamp) as dt,
	count(distinct event_id) as events_count
from dds.fct_events
group by 1
on conflict (dt) do update
set
	events_count = excluded.events_count