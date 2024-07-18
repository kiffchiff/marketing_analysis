use local_parm;
-- orders 테이블에서 1월 주문건에 대한 일간 결제액 조회
SELECT order_date, sum(payment_amount) as payment_amount
from orders
where order_date between '2022-11-01' and '2022-11-30'
GROUP BY order_date
ORDER BY order_date;

-- orders 테이블에서 11월 주문건에 대한 결제상태별 일간 결제액 조회
SELECT order_date, payment_status, sum(payment_amount) as payment_amount
FROM orders
where order_date BETWEEN '2022-11-01' and '2022-11-30'
GROUP BY order_date, payment_status
ORDER BY order_date, payment_amount;

-- orders 테이블에서 결제 상태별 11월 결제액의 합 / 평균 / 최대 / 최소값 조회
SELECT payment_status, DATE_FORMAT(order_date, '%Y-%m') AS order_year_month
	 , sum(payment_amount) AS payment_sum
	 , avg(payment_amount) AS payment_avg
	 , max(payment_amount) AS payment_max
	 , min(payment_amount) AS payment_min
FROM orders
WHERE order_date BETWEEN '2022-11-01' and '2022-11-30'
GROUP BY payment_status, order_year_month;

-- orders 테이블에서 결제 상태별 11월 주문건수, 주문 유저수 조회
SELECT DATE_FORMAT(order_date, '%Y-%m') as order_year_month
	 , count(order_id) as purchase_cnt
	 , count(distinct user_id) as pu
FROM orders
WHERE order_date = '2022-11-01'

-- orders 테이블에서 월간결제액 조회
SELECT DATE_FORMAT(order_date, '%Y-%m') as order_year_month
	 , SUM(payment_amount) as payment_amount
FROM orders
WHERE payment_status = 'completed'
GROUP BY order_year_month
ORDER BY order_year_month;

-- orders 테이블에서 월간결제액 3000만원 이상 5500만원 이하인 월의 결제 유저수 조회
select DATE_FORMAT(order_date, '%Y-%m') as order_year_month
 	 , count(distinct user_id) as pu
from orders
group by order_year_month
having sum(payment_amount) >= 30000000 and sum(payment_amount) <= 55000000
order by order_year_month;

-- order 테이블에서 15000원이상 주문건에 대한 11월 누적 결제액
select date_format(order_date, '%Y-%m') as order_year_month
	 , sum(payment_amount) as payment_amount
from orders
where payment_amount >= 15000 and order_date between '2022-11-01' and '2022-11-30'
group by order_year_month;

-- 7월 이전 가입자 중 당산동에 거주하는 유저수를 플랫폼 별로 나누어 조회
select platform, count(distinct user_id) as user_cnt
from myinfos
where region_3depth_name like '당산동%'
and create_date < '2022-07-01'
group by platform 
order by platform;

-- 위의것 조회 중 로그인 타입으로 나누어 조회
select account_type, count(distinct user_id) as user_cnt
from myinfos
where region_3depth_name like '당산동%'
and create_date < '2022-07-01'
group by account_type
order by account_type;

-- 전체 기간 중 DAU가 1500명 이상 2000명 이하인 날 조회
select login_date, count(distinct user_id) as DAU
from login_users
group by login_date
having DAU between 1500 and 2000
order by login_date desc;

-- MAU check
select date_format(login_date, '%Y-%m') as login_year_month
	 , count(distinct user_id) as MAU
from login_users
group by login_year_month
order by login_year_month;

-- 가입연월 컬럼이 추가된 all_myinfos 테이블 생성
create table all_myinfos as (
select create_date, date_format(create_date, '%Y-%m') as create_year_month
	 , user_id, nickname, account_type, platform
	 , region_1depth_name, region_2depth_name, region_3depth_name
	 , h_code, x, y
from myinfos
);

select * from all_myinfos;

-- 접속연월 컬럼이 추가된 daily_login_user테이블 생성
create table daily_login_users as (
select login_date, date_format(login_date, '%Y-%m') as login_year_month
	 , user_id, nickname, platform
from login_users
);

select * from daily_login_users;

-- with절을 사용하여 작성된 쿼리 재사용
with daily_login_users as (
select login_date, date_format(login_date, '%Y-%m') as login_year_month
	 , user_id, nickname, platform
from login_users
,

all_myinfos as (
select create_date, date_format(create_date, '%Y-%m') as create_year_month
	 , user_id, nickname, account_type, platform
	 , region_1depth_name, region_2depth_name, region_3depth_name
	 , h_code, x, y
from myinfos
);

select a.*, b.h_code, b.region_2depth_name
from daily_login_users as a
left join all_myinfos as b
on a.user_id = b.user_id

-- marketing_myinfos table 생성
create table marketing_myinfos as (
	select *, concat('attribute-', user_id) as attribute_id
	from all_myinfos
	where create_date >= '2022-05-01'
	order by create_date
);
select * from marketing_myinfos; 

-- 월간 모객유저 LTV >> 추적기간이 차지 않은 그룹은 sales, ltv에 null 할당
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
  	from daily_orders
  	where order_date >= '2022-05-01'
  	group by order_date, user_id
  ),
  
  ltv_set as (
  	select 'D15' as ltv_period, a.user_id
  		 , a.create_year_month
         , a.attribute_type 
  		 , case when datediff('2022-11-30', a.create_date) < 14 then 0 else d15.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d15
  	on a.user_id = d15.user_id and a.create_date <= d15.order_date and date_add(a.create_date, interval 14 day) >= d15.order_date
 
  	union all
  	
  	select 'D30' as ltv_period, a.user_id
  		 , a.create_year_month
         , a.attribute_type 
  		 , case when datediff(DATE '2022-11-30', a.create_date) < 29 then 0 else d30.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d30
  	on a.user_id = d30.user_id and a.create_date <= d30.order_date and date_add(a.create_date, interval 29 day) >= d30.order_date
  	
  	union all

    select 'D45' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 44 then 0 else d45.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d45
    on a.user_id = d45.user_id and a.create_date <= d45.order_date and date_add(a.create_date, interval 44 day) >= d45.order_date 

    union all

    select 'D60' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 59 then 0 else d60.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d60
    on a.user_id = d60.user_id and a.create_date <= d60.order_date and date_add(a.create_date, interval 59 day) >= d60.order_date 

    union all

    select 'D75' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 74 then 0 else d75.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d75
    on a.user_id = d75.user_id and a.create_date <= d75.order_date and date_add(a.create_date, interval 74 day) >= d75.order_date 

    union all

    select 'D90' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 89 then 0 else d90.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d90
    on a.user_id = d90.user_id and a.create_date <= d90.order_date and date_add(a.create_date, interval 89 day) >= d90.order_date
    ),
    
   agg_ltv_group as (
   	select ltv_period, create_year_month, attribute_type
   		 , count(distinct user_id) as user_cnt
   		 , sum(payment_amount) as sales
   		 , round(sum(payment_amount)/count(distinct user_id)) as ltv
   		 , count(distinct case when payment_amount = 0 then user_id end) as check_cnt
   	from ltv_set
   	group by ltv_period, create_year_month, attribute_type with rollup
   )
   
   select ltv_period, create_year_month
   		 , case when attribute_type is null then 'all' else attribute_type end as attribute_type
   		 , user_cnt
   		 , case when check_cnt = 0 then sales else null end as sales
   		 , case when check_cnt = 0 then ltv else null end as ltv
   	from agg_ltv_group
   	where create_year_month is not null
   	order by ltv_period, create_year_month, attribute_type

-- Attribute_type별 월간 가입 유저 ROAS
   	
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
  	from daily_orders
  	where order_date >= '2022-05-01'
  	group by order_date, user_id
  ),
  
  ltv_set as (
  	select 'D15' as ltv_period, a.user_id
  		 , a.create_year_month
         , a.attribute_type 
  		 , case when datediff('2022-11-30', a.create_date) < 14 then 0 else d15.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d15
  	on a.user_id = d15.user_id and a.create_date <= d15.order_date and date_add(a.create_date, interval 14 day) >= d15.order_date
 
  	union all
  	
  	select 'D30' as ltv_period, a.user_id
  		 , a.create_year_month
         , a.attribute_type 
  		 , case when datediff(DATE '2022-11-30', a.create_date) < 29 then 0 else d30.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d30
  	on a.user_id = d30.user_id and a.create_date <= d30.order_date and date_add(a.create_date, interval 29 day) >= d30.order_date
  	
  	union all

    select 'D45' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 44 then 0 else d45.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d45
    on a.user_id = d45.user_id and a.create_date <= d45.order_date and date_add(a.create_date, interval 44 day) >= d45.order_date 

    union all

    select 'D60' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 59 then 0 else d60.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d60
    on a.user_id = d60.user_id and a.create_date <= d60.order_date and date_add(a.create_date, interval 59 day) >= d60.order_date 

    union all

    select 'D75' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 74 then 0 else d75.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d75
    on a.user_id = d75.user_id and a.create_date <= d75.order_date and date_add(a.create_date, interval 74 day) >= d75.order_date 

    union all

    select 'D90' as ltv_period, a.user_id
        , a.create_year_month
        , a.attribute_type 
        , case when datediff(DATE '2022-11-30', a.create_date) < 89 then 0 else d90.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d90
    on a.user_id = d90.user_id and a.create_date <= d90.order_date and date_add(a.create_date, interval 89 day) >= d90.order_date
    ),
    
   agg_ltv_group as (
   	select ltv_period, create_year_month
         , attribute_type 
   		 , count(distinct user_id) as user_cnt
   		 , sum(payment_amount) as sales
   		 , round(sum(payment_amount)/count(distinct user_id)) as ltv
   		 , count(distinct case when payment_amount = 0 then user_id end) as check_cnt
   	from ltv_set
   	group by ltv_period, create_year_month, attribute_type with rollup
   ),
   
   agg_ltv_group_final as (select ltv_period, create_year_month
   		 , case when attribute_type is null then 'all' else attribute_type end as attribute_type
   		 , user_cnt
   		 , case when check_cnt = 0 then sales else null end as sales
   		 , case when check_cnt = 0 then ltv else null end as ltv
   	from agg_ltv_group
   	where create_year_month is not null
   	order by ltv_period, create_year_month, attribute_type
   	),
   	
   	monthly_cost as (
   		select date_format(aggregate_date, '%Y-%m') as aggregate_year_month, sum(cost) as cost
   		from daily_cost
   		group by aggregate_year_month
   		order by aggregate_year_month
   	)
   	
   	select a.ltv_period, a.create_year_month, a.attribute_type
   		 , a.user_cnt, a.sales, a.ltv, b.cost
   		 , round(a.sales/b.cost, 3)as roas
   	from agg_ltv_group_final as a
   	left join monthly_cost as b
   	on a.create_year_month = b.aggregate_year_month
   	order by create_year_month, attribute_type, ltv_period

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
   	
-- ROAS 지표집계

with non_organic_userinfo as (
    select b.campaign
        , b.install_time, a.create_date, a.create_year_month
        , a.attribute_id, a.user_id, a.nickname, a.account_type, a.platform
        , a.region_1depth_name, a.region_2depth_name, a.region_3depth_name
        , a.h_code, a.x, a.y
    from marketing_myinfos as a
    inner join (
      select install_time, attribute_id, campaign
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
  		 , case when datediff('2022-11-30', a.create_date) < 14 then 0 else d15.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d15
  	on a.user_id = d15.user_id and a.create_date <= d15.order_date and date_add(a.create_date, interval 14 day) >= d15.order_date
 
  	union all
  	
  	select 'D30' as ltv_period, a.user_id, a.campaign
  		 , a.create_year_month
  		 , case when datediff(DATE '2022-11-30', a.create_date) < 29 then 0 else d30.payment_amount end as payment_amount
  	from non_organic_userinfo as a
  	left join daily_orders as d30
  	on a.user_id = d30.user_id and a.create_date <= d30.order_date and date_add(a.create_date, interval 29 day) >= d30.order_date
  	
  	union all

    select 'D45' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , case when datediff(DATE '2022-11-30', a.create_date) < 44 then 0 else d45.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d45
    on a.user_id = d45.user_id and a.create_date <= d45.order_date and date_add(a.create_date, interval 44 day) >= d45.order_date 

    union all

    select 'D60' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , case when datediff(DATE '2022-11-30', a.create_date) < 59 then 0 else d60.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d60
    on a.user_id = d60.user_id and a.create_date <= d60.order_date and date_add(a.create_date, interval 59 day) >= d60.order_date 

    union all

    select 'D75' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , case when datediff(DATE '2022-11-30', a.create_date) < 74 then 0 else d75.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d75
    on a.user_id = d75.user_id and a.create_date <= d75.order_date and date_add(a.create_date, interval 74 day) >= d75.order_date 

    union all

    select 'D90' as ltv_period, a.user_id, a.campaign
        , a.create_year_month
        , case when datediff(DATE '2022-11-30', a.create_date) < 89 then 0 else d90.payment_amount end as payment_amount
    from non_organic_userinfo as a
    left join daily_orders as d90
    on a.user_id = d90.user_id and a.create_date <= d90.order_date and date_add(a.create_date, interval 89 day) >= d90.order_date
    ),
    
   agg_ltv_group as (
   	select ltv_period, create_year_month
   		 , campaign
   		 , count(distinct user_id) as user_cnt
   		 , sum(payment_amount) as sales
   		 , round(sum(payment_amount)/count(distinct user_id)) as ltv
   		 , count(distinct case when payment_amount = 0 then user_id end) as check_cnt
   	from ltv_set
   	group by ltv_period, create_year_month, campaign
   ),
   
  agg_ltv_group_final as (
    select ltv_period, create_year_month
        , campaign
        , user_cnt
        , case when check_cnt = 0 then sales else null end as sales
        , case when check_cnt = 0 then ltv else null end as ltv
    from agg_ltv_group
    order by ltv_period, create_year_month
  ), 

  monthly_cost as (
    select date_format(aggregate_date, '%Y-%m') as aggregate_year_month, campaign, sum(cost) as cost
    from daily_cost
    group by aggregate_year_month, campaign
    order by aggregate_year_month, campaign
  ),

  roas_set as (
    select a.ltv_period, a.create_year_month, b.aggregate_year_month, a.campaign
        , a.user_cnt, a.sales, a.ltv, b.cost
        , round(a.sales/b.cost, 3) as roas 
    from agg_ltv_group_final as a
    left join monthly_cost as b
    on a.create_year_month = b.aggregate_year_month and a.campaign = b.campaign
    order by campaign, create_year_month, ltv_period
  )

  select campaign
       , aggregate_year_month as ua_year_month
       , min(user_cnt) as user_cnt
       , min(round(cost, 1)) as cost
       , min(case when ltv_period = 'D15' then roas end) as roas_D15
       , min(case when ltv_period = 'D30' then roas end) as roas_D30
       , min(case when ltv_period = 'D45' then roas end) as roas_D45
       , min(case when ltv_period = 'D60' then roas end) as roas_D60
       , min(case when ltv_period = 'D75' then roas end) as roas_D75
       , min(case when ltv_period = 'D90' then roas end) as roas_D90
  from roas_set
  group by campaign, ua_year_month
  having aggregate_year_month not in ('2022-11')
  order by case when campaign = 'SEOUL' then 1
                when campaign = 'LC_Mok-dong' then 2
                when campaign = 'LC_Guro-dong' then 3
                when campaign = 'LC_Mullae-dong' then 4
                when campaign = 'LC_Sindorim-dong' then 5
                when campaign = 'LC_Sillim-dong' then 6
                when campaign = 'LC_Dangsan-dong' then 7
                when campaign = 'LC_Yeouido-dong' then 8
                else 999
           end, aggregate_year_month
   
   
   	
-- 신규/재방문/복귀 유저 분류
with return_group as (
	select a.login_date, a.user_id,a.nickname, count(b.user_id) as bef_login_cnt #이전 로그인 유저
	from (
		select *
		from daily_login_users
		where login_year_month = '2022-11'
		) as a left join (
		select login_date, user_id
		from daily_login_users
		where login_year_month in ('2022-10', '2022-11')
		) as b
		on a.user_id = b.user_id and b.login_date between date_add(a.login_date, interval -7 day) and date_add(a.login_date, interval -1 day)
		group by a.login_date, a.user_id, a.nickname
	)
	
	select a.login_date, a.user_id, a.nickname, a.bef_login_cnt, b.user_id as new_user_id
		, case when b.user_id is not null then '신규유저'
			   when bef_login_cnt = 0 then '복귀유저' #신규유저도 ber_login_cnt = 0 일 수 있어서 신규유저 조건 먼저 걸어놓
			   else '재방문유저'
		  end as user_type
	from return_group as a
	left join all_myinfos as b
	on a.user_id = b.user_id and a.login_date = b.create_date
	order by user_id, login_date;
	
-- DAU
select login_date, count(distinct user_id) as dau
from daily_login_users
group by login_date
order by login_date;

-- MAU
with login_info as (
	select user_id, login_date
		 , extract(DAYOFWEEK from date_add(login_date, interval -1 day)) as dayofweek_num
		 , extract(week from date_add(login_date, interval -1 day)) as week_num #week울 사용하여 날짜의 주 번호(0~53) 반환 주 기준은 일요
	from daily_login_users
	)
	
select min(login_date) as first_date, week_num, count(distinct user_id) as wau
from login_info
group by week_num
order by week_num

-- MAU
select login_year_month, count(distinct user_id) as mau, min(login_date) as first_date
from daily_login_users
group by login_year_month 
order by login_year_month;

-- N-day Retention가입 후 n일동안 얼마나 많은 사용자가 서비스를 이용하는지 확인하는 지표
with login_check as (
	select a.login_date, a.user_id
		 , case when d1.user_id is not null then 1 end as login_d1
		 , case when d7.user_id is not null then 1 end as login_d7
		 , case when d14.user_id is not null then 1 end as login_d14
		 , case when d30.user_id is not null then 1 end as login_d30
	from daily_login_users as a
	left join daily_login_users as d1
	on a.user_id = d1.user_id and date_add(a.login_date, interval 1 day) = d1.login_date
	left join daily_login_users as d7
	on a.user_id = d7.user_id and date_add(a.login_date, interval 7 day) = d7.login_date
	left join daily_login_users as d14
	on a.user_id = d14.user_id and date_add(a.login_date, interval 14 day) = d14.login_date
	left join daily_login_users as d30
	on a.user_id = d30.user_id and date_add(a.login_date, interval 30 day) = d30.login_date
	)
	
	select login_date, count(distinct user_id) as user_cnt
		 , round(sum(login_d1)/count(distinct user_id), 3) as d1_ret
		 , round(sum(login_d7)/count(distinct user_id), 3) as d7_ret
		 , round(sum(login_d14)/count(distinct user_id), 3) as d14_ret
		 , round(sum(login_d30)/count(distinct user_id), 3) as d30_ret
	from login_check
	group by login_date
	order by login_date

	-- power_user_curve
	select login_year_month, login_cnt, count(distinct user_id) as user_cnt
	from (
		-- 월별 유저별 로그인 횟수(loign_cnt) : 한달에 몆번 로그인 했는가?
		select login_year_month as login_year_month
			 , user_id
			 , count(*) as login_cnt
		from daily_login_users
		group by login_year_month, user_id
		order by login_year_month, user_id
		) as a
	group by login_year_month, login_cnt
	order by login_year_month, login_cnt
	
	-- 일간기준 매출 지표
	with dau_table as (
		select login_date, count(distinct user_id) as dau
		from daily_login_users
		group by login_date
	),
	
	sales_index_table as (
		select order_date
			 , count(distinct user_id) as pu
			 , count(distinct order_id) as purchase_cnt
			 , sum(payment_amount) as sales
		from orders
		where payment_status = 'completed' -- 정상적으로 완료된 건에 대해서만 집계
		group by order_date
	)
	
	select a.login_date, a.dau, b.pu, b.purchase_cnt
		 , round(b.pu/a.dau, 3) as pur
		 , b.sales
		 , round(b.sales/b.pu, 3) as arppu
		 , round(b.sales/a.dau, 3) as arpdau
	from dau_table as a
	left join sales_index_table as b
	on a.login_date = b.order_date
	order by login_date

	
-- 월간기준 매출지표
create table daily_orders as (
	select *, DATE_FORMAT(order_date, '%Y-%m') as order_year_month
	from orders
)
select * from daily_orders;
	
with mau_table as (
	select login_year_month, count(distinct user_id) as mau
	from daily_login_users
	group by login_year_month
),

sales_index_table as (
	select order_year_month
		 , count(distinct user_id) as pu
		 , count(distinct order_id) as purchase_cnt
		 , sum(payment_amount) as sales
	from daily_orders
	where payment_status = 'completed'
	group by order_year_month
)

	select a.login_year_month, a.mau, b.pu, b.purchase_cnt
		 , round(b.pu/a.mau, 3) as pur
		 , b.sales
		 , round(b.sales/b.pu, 3) as arppu
		 , round(b.sales/a.mau, 3) as arpdau
	from mau_table as a
	left join sales_index_table as b
	on a.login_year_month = b.order_year_month
	order by login_year_month
	
-- 일별 매출 누계
with daily_sales as (
	select order_date, sum(payment_amount) as sales
	from daily_orders
	where payment_status = 'completed'
	group by order_date
)

select order_date, sales, sum(sales) over(order by order_date) as cum_sales
from daily_sales
order by order_date

-- 각 월별로 일별 누계
with daily_sales as (
	select order_year_month, order_date, sum(payment_amount) as sales
	from daily_orders
	where payment_status = 'completed'
	group by order_year_month, order_date
)

select order_year_month, order_date, sales, sum(sales) over(partition by order_year_month order by order_date) as cum_saels
from daily_sales
order by order_date

-- 최근 7일 기준 이동평균
with daily_sales as (
	select order_date, sum(payment_amount) as sales
	from daily_orders
	where payment_status = 'completed'
	group by order_date
)

select order_date, sales
	 , round(avg(sales) over(order by order_date rows between 6 preceding and current row), 2) as avg_7days_sales
	 , case when 7 = count(order_date) over(order by order_date rows between 6 preceding and current row)
	 		then round(avg(sales) over(order by order_date rows between 6 preceding and current row), 2)
	 		end as avg_7days_sales_strict
from daily_sales
order by order_date

-- 판매상품 분석 1)ABC분석
with nov_sales_by_product as (
	select b.product_title, sum(payment_amount) as sales
	from (
		select product_id, payment_amount
		from daily_orders
		where payment_status = 'completed' and order_year_month = '2022-11'
		) as a
	left join products as b
	on a.product_id = b.product_id
	group by b.product_title
),

nov_sales_and_cum_by_product as (
	select product_title, sales, sum(sales) over(order by sales desc rows unbounded preceding) as cum_sales
		 , sum(sales) over() as total_cum_sales
	from nov_sales_by_product
	order by sales desc
)

select product_title, sales, cum_sales, total_cum_sales
	 , round(cum_sales/total_cum_sales, 5) as ratio_of_cum_sales
	 , case when round(cum_sales/total_cum_sales, 5) < 0.3 then 'A'
	 	 	when round(cum_sales/total_cum_sales, 5) < 0.6 then 'B'
	 	 	else 'C'
	 	 	end as product_grade
from nov_sales_and_cum_by_product
order by sales desc 

-- 매장 정보 테이블 (shopsInfo) 생성
with shopinfos as (
	select case when a.pickup_shop_name is null then '픽업존(통합)' else a.pickup_shop_name end as pickup_shop_name
		 , case when a.pickup_shop_name is not null then a.region_1depth_name else '서울' end as region_1depth_name
		 , case when a.pickup_shop_name is not null then a.region_2depth_name else '-' end as region_2depth_name
		 , case when a.pickup_shop_name is not null then a.region_3depth_name else '-' end as region_3depth_name
		 		else min(order_date)
		   end as shop_open_date -- 최초 오픈일 수정(사업 담당자에게 확인한 정보 기준)
		   
		 -- 매장별 월별 매




























		
  


