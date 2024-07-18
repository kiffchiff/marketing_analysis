-- campaign별 월간 가입 유저수
   	
with non_organic_userinfo as (
    select b.attribute_type, b.campaign
        , b.install_time, a.create_date, a.create_year_month
        , a.attribute_id, a.user_id, a.nickname, a.account_type, a.platform
        , a.region_1depth_name, a.region_2depth_name, a.region_3depth_name
        , a.h_code, a.x, a.y
    from marketing_myinfos as a
    left join (
    	select 'organic' as attribute_type, install_time, attribute_id, campaign
    	from organic_install
    
    	union all
    
    	select 'non_organic' as attribute_type, install_time, attribute_id, campaign
    	from non_organic_install
    	) as b
    on a.attribute_id = b.attribute_id and a.create_date = b.install_time
 ),
  
  daily_orders as (
  	select order_date, user_id, sum(payment_amount) as payment_amount
  	from orders
  	where order_date >= '2022-05-01'
  	group by order_date, user_id
  ),
  
  ltv_set as (
  	select 'D15' as ltv_period, a.user_id, a.campaign
  		 , a.create_year_month
         , a.attribute_type 
  		 , case when datediff('2022-11-30', a.create_date) < 14 then 0 else d15.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d15
  	on a.user_id = d15.user_id and a.create_date <= d15.order_date and date_add(a.create_date, interval 14 day) >= d15.order_date
 
  	union all
  	
  	select 'D30' as ltv_period, a.user_id, a.campaign
  		 , a.create_year_month
         , a.attribute_type 
  		 , case when datediff(DATE '2022-11-30', a.create_date) < 29 then 0 else d30.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d30
  	on a.user_id = d30.user_id and a.create_date <= d30.order_date and date_add(a.create_date, interval 29 day) >= d30.order_date
  	
  	union all

    select 'D45' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 44 then 0 else d45.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d45
    on a.user_id = d45.user_id and a.create_date <= d45.order_date and date_add(a.create_date, interval 44 day) >= d45.order_date 

    union all

    select 'D60' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 59 then 0 else d60.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d60
    on a.user_id = d60.user_id and a.create_date <= d60.order_date and date_add(a.create_date, interval 59 day) >= d60.order_date 

    union all

    select 'D75' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 74 then 0 else d75.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d75
    on a.user_id = d75.user_id and a.create_date <= d75.order_date and date_add(a.create_date, interval 74 day) >= d75.order_date 

    union all

    select 'D90' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 89 then 0 else d90.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d90
    on a.user_id = d90.user_id and a.create_date <= d90.order_date and date_add(a.create_date, interval 89 day) >= d90.order_date
    ),
    
   agg_ltv_group as (
   	select ltv_period, create_year_month
         , campaign
         , attribute_type 
   		 , count(distinct user_id) as user_cnt
   		 , sum(payment_amount) as sales
   		 , round(sum(payment_amount)/count(distinct user_id)) as ltv
   		 , count(distinct case when payment_amount = 0 then user_id end) as check_cnt
   	from ltv_set
   	group by ltv_period, create_year_month, campaign, attribute_type 
   ),
   
   agg_ltv_group_final as (
   		select ltv_period, create_year_month
   		 , case when attribute_type is null then 'all' else attribute_type end as attribute_type
   		 , campaign
   		 , user_cnt
   		 , case when check_cnt = 0 then sales else null end as sales
   		 , case when check_cnt = 0 then ltv else null end as ltv
   	from agg_ltv_group
   	where create_year_month is not null
   	order by ltv_period, create_year_month, attribute_type
   	),
   	
   	monthly_cost as (
   		select date_format(aggregate_date, '%Y-%m') as aggregate_year_month, campaign, sum(cost) as cost
   		from daily_cost
   		group by aggregate_year_month, campaign
   		order by aggregate_year_month, campaign
   	)
   	
   	select a.ltv_period, a.create_year_month, a.attribute_type, a.campaign
   		 , a.user_cnt, a.sales, a.ltv, b.cost
   		 , round(a.sales/b.cost, 3)as roas
   	from agg_ltv_group_final as a
   	left join monthly_cost as b
   	on a.create_year_month = b.aggregate_year_month and a.campaign = b.campaign
   	order by create_year_month, attribute_type, ltv_period