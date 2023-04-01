insert into cdm.top_urls (dt, page_path, sales)
select
	date_trunc('hour', event_timestamp) as dt,
	page_1 as page_path,
	count(distinct event_id) as sales
from dds.fct_events
where
	page_1 not in ('/home', '/cart', '/payment', '/confirmation')
	and page_2 = '/cart'
	and page_3 = '/payment'
	and page_4 = '/confirmation'
group by 1, 2
order by 3 desc
on conflict (dt, page_path) do update
set
	sales = excluded.sales;