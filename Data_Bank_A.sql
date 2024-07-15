--A. Customer Nodes Exploration
use til_playground.cs4_data_bank;

select * from customer_nodes;

--How many unique nodes are there on the Data Bank system?
select node_id
from customer_nodes
group by 1;

select count(distinct node_id)
from customer_nodes;
--What is the number of nodes per region?
select * 
from regions;  --view whats in region table

select n.region_id
      ,r.region_name
      ,count(distinct n.node_id) as total_regional_nodes
from customer_nodes n
join regions r on n.region_id=r.region_id --use a join to display region names too.
group by 1,2
order by 1;
--How many customers are allocated to each region?
select region_id
      ,customer_id
from customer_nodes order by 2
;  --this will help to decide between count & countd
select n.region_id
      ,r.region_name
      ,count(distinct n.customer_id) as total_regional_customers
from customer_nodes n
join regions r on n.region_id=r.region_id --use a join to display region names too.
group by 1,2
order by 1;
--How many days on average are customers reallocated to a different node?
with cte as
(select customer_id
       ,node_id
      ,sum(datediff('day',start_date, end_date)) as days
from customer_nodes
where end_date != '9999-12-31' --exclude this to avoid distortions
group by 1,2
order by customer_id -- order to have an idea of whats happening with 1 customer
) 
select round(avg(days),0) average_days
from cte
; 

--What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
with cte as
(select n.customer_id
       ,n.node_id
       ,r.region_name
      ,sum(datediff('day',start_date, end_date)) as days
from customer_nodes n
join regions r on n.region_id=r.region_id
where end_date != '9999-12-31' --exclude this to avoid distortions
group by 1,2,3
order by customer_id -- order to have an idea of whats happening with 1 customer
) 
select region_name
      ,round(avg(days),0) average_days
      ,median(days)
      ,percentile_cont(0.8) within group (order by days) as pc_80
      ,percentile_cont(0.95) within group (order by days) as pc_95
from cte
group by 1; --snowflake

--without snowflake
with cte as
(select n.customer_id
       ,n.node_id
       ,r.region_name
      ,sum(datediff('day',start_date, end_date)) as days
from customer_nodes n
join regions r on n.region_id=r.region_id
where end_date != '9999-12-31' --exclude this to avoid distortions
group by 1,2,3
order by customer_id -- order to have an idea of whats happening with 1 customer
)
, Rownumber as
(
select region_name
      ,days
      ,row_number() over (partition by region_name order by days) as rn  --get row number by each region to allow picking the max
from cte)
,Maxi as
(select region_name
      ,max(rn) as maxi_rows
      from Rownumber
      group by 1  --select the maximun row numbers for each region to be used later to calculate median, 80p and 95p
),
Main as
(
select m.region_name,days, rn, m.maxi_rows 
from Rownumber w
join Maxi m on m.region_name=w.region_name
)
select Region_Name
--    ,round(maxi_rows*0.5,0) as median_row clean up this row at the end
     ,case 
      when rn=round(maxi_rows*0.5,0) then 'Median'
      when rn=round(maxi_rows*0.8,0) then '80th Percentile'
      when rn=round(maxi_rows*0.95,0) then '95th Percentile'
      end as Metric,
      days
from Main
where rn in (round(maxi_rows*0.5,0), round(maxi_rows*0.8,0), round(maxi_rows*0.95,0));--use in instead of equals to allow multiple inputs 
