--  MAU(월간 활성화 유저수 : 로그인한 유저를 기준으로 count)
select login_year_month, count(distinct user_id) as login_user_cnt
from daily_login_users
group by login_year_month 
order by login_year_month 

-- NRU(월간 가입 유저수)  
select create_year_month, count(distinct user_id) as user_cnt
from all_myinfos
group by create_year_month
order by create_year_month 

-- 11월 매출 기준 매장별 구매자 거주지역별 매출
with pu_region as (
	select a.user_id, a.order_year_month, a.order_date, a.payment_amount, a.pickup_shop_name, b.region_3depth_name as region
	from daily_orders as a
	left join all_myinfos as b
	on a.user_id = b.user_id
),

nov_shop_and_region as (
	select order_year_month, pickup_shop_name
		 , case when region = '신림동' then 'Sillim_dong'
		 		when region = '조원동' then 'Jowan_dong'
               when region = '구로2동' then 'Guro2_dong'
               when region = '신도림동' then 'Sindorim_dong'
               when region = '독산3동' then 'Doksan3_dong'
               when region = '목동' then 'Mok_dong'
               when region = '당산동4가' then 'Dangsandong4_ga'
               when region = '당산동5가' then 'Dangsandong5_ga'
               when region = '대림2동' then 'Daerim2_dong'
               when region = '문래동3가' then 'Mullaedong3_ga'
               when region = '양평동3가' then 'Yangpyeongdong3_ga'
               when region = '양평동4가' then 'Yangpyeongdong4_ga'
               when region = '여의도동' then 'Yeouido_dong'
           else 'none'
           end as region
           ,sum(payment_amount) as sales
    from pu_region
    group by order_year_month, pickup_shop_name, region
    having order_year_month = '2022-11'
    order by pickup_shop_name, region
)

select order_year_month, pickup_shop_name,
    MIN(CASE WHEN region = 'Sillim_dong' THEN sales END) AS Sillim_dong,
    MIN(CASE WHEN region = 'Jowon_dong' THEN sales END) AS Jowon_dong,
    MIN(CASE WHEN region = 'Guro2_dong' THEN sales END) AS Guro2_dong,
    MIN(CASE WHEN region = 'Sindorim_dong' THEN sales END) AS Sindorim_dong,
    MIN(CASE WHEN region = 'Doksan3_dong' THEN sales END) AS Doksan3_dong,
    MIN(CASE WHEN region = 'Mok_dong' THEN sales END) AS Mok_dong,
    MIN(CASE WHEN region = 'Dangsandong4_ga' THEN sales END) AS Dangsandong4_ga,
    MIN(CASE WHEN region = 'Dangsandong5_ga' THEN sales END) AS Dangsandong5_ga,
    MIN(CASE WHEN region = 'Daerim2_dong' THEN sales END) AS Daerim2_dong,
    MIN(CASE WHEN region = 'Mullaedong3_ga' THEN sales END) AS Mullaedong3_ga,
    MIN(CASE WHEN region = 'Yangpyeongdong3_ga' THEN sales END) AS Yangpyeongdong3_ga,
    MIN(CASE WHEN region = 'Yangpyeongdong4_ga' THEN sales END) AS Yangpyeongdong4_ga,
    MIN(CASE WHEN region = 'Yeouido_dong' THEN sales END) AS Yeouido_dong
from nov_shop_and_region
group by order_year_month, pickup_shop_name
order by case when pickup_shop_name = '픽업존(유승상가)' then 1
              when pickup_shop_name = '픽업존(메이비카페)' then 2 
              when pickup_shop_name = '픽업존(서울드림신용협동조합)' then 3 
              when pickup_shop_name = '픽업존(GS25 목동3동점)' then 4 
              when pickup_shop_name = '픽업존(참맛부대찌개아구찜)' then 5 
              when pickup_shop_name = '픽업존(GS영등포당산점)' then 6 
              when pickup_shop_name = '픽업존(다이소 난곡사거리점)' then 7 
              when pickup_shop_name = '픽업존(더현대서울)' then 8 
         end
         
-- 월간 지역별 활성 유저수
         
 with active_user as (
 	select a.login_date, a.login_year_month, a.user_id, b.region_1depth_name, b.region_2depth_name, b.region_3depth_name
 	from daily_login_users as a
 	inner join all_myinfos as b
 	on a.user_id = b.user_id
),

monthly_active_user as (
	select region, login_year_month, user_cnt
		 , lag(user_cnt, 1) over(partition by region order by login_year_month) as bef_1m_user_cnt
		 , rank() over(partition by region order by login_year_month) as rank_asc -- 지역별 최초 접속시점 가려내기 위해서
	from (
		select concat(region_2depth_name, '-', region_3depth_name) as region
			 , login_year_month
			 , count(distinct user_id) as user_cnt
		from active_user
		group by region, login_year_month
	) as a
	order by region, login_year_month
)

select region, login_year_month
	 , rank_asc
	 , user_cnt
	 , bef_1m_user_cnt
	 , (user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt as rate_of_change
	 -- 각 지역별 11월 AU의 전월대비 증감률 계산, 유저그룹 나누기
     , last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by login_year_month rows between unbounded preceding and unbounded following) as nov_rate_of_change
     , case when rank_asc = 1 and login_year_month = '2022-11' then 'new'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by login_year_month rows between unbounded preceding and unbounded following) < 0 then 'decrease'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by login_year_month rows between unbounded preceding and unbounded following) between 0 and 0.1 then 'increase_0_10'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by login_year_month rows between unbounded preceding and unbounded following) <= 0.2 then 'increase_10_20'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by login_year_month rows between unbounded preceding and unbounded following) <= 0.3 then 'increase_20_30'
              else 'increase_30_more'
         end as user_group
 from monthly_active_user
 order by region, login_year_month

-- 월간 지역별 가입 유저수
with register_user as (
	select user_id, create_date, create_year_month
		 , region_1depth_name, region_2depth_name, region_3depth_name
		 , concat(region_2depth_name, '-', region_3depth_name) as region
	from all_myinfos
),

monthly_register_user as (
	select region, create_year_month, user_cnt
		, lag(user_cnt, 1) over(partition by region order by create_year_month) as bef_1m_user_cnt
		, rank() over(partition by region order by create_year_month) as rank_asc
	from (
		select region, create_year_month, count(distinct user_id) as user_cnt
		from register_user
		group by region, create_year_month
		having create_year_month >= '2022-05'
	) as a
	order by region, create_year_month
)

 select region, create_year_month
       , rank_asc
       , user_cnt
       , bef_1m_user_cnt
       -- 각 지역별 11월 NRU의 전월대비 증감률을 계산하고, 이를 통해 유저 그룹을 나눈다.
       , (user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt as rate_of_change
       , last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by create_year_month rows between unbounded preceding and unbounded following) as nov_rate_of_change
       , case when rank_asc = 1 and create_year_month = '2022-11' then 'new'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by create_year_month rows between unbounded preceding and unbounded following) < 0 then 'decrease'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by create_year_month rows between unbounded preceding and unbounded following) between 0 and 0.1 then 'increase_0_10'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by create_year_month rows between unbounded preceding and unbounded following) <= 0.2 then 'increase_10_20'
              when last_value((user_cnt - bef_1m_user_cnt)/bef_1m_user_cnt) over(partition by region order by create_year_month rows between unbounded preceding and unbounded following) <= 0.3 then 'increase_20_30'
              else 'increase_30_more'
         end as user_group
  from monthly_register_user
  order by region, create_year_month

-- 가입연도별/월별 MAU
with active_user as(
	select a.login_date, a.login_year_month, b.create_year_month, date_format(b.create_date, '%Y') as create_year
		 , a.user_id, b.region_1depth_name, b.region_2depth_name, b.region_3depth_name
	from daily_login_users as a
	inner join all_myinfos as b
	on a.user_id = b.user_id
)

select login_year_month, create_year
	 , count(distinct user_id) as user_cnt
from active_user
group by login_year_month, create_year
order by login_year_month, create_year

-- 가입 연월별 활성 유저수
with active_user as (
	select a.login_date, a.login_year_month, b.create_year_month, a.user_id, b.region_1depth_name, b.region_2depth_name, b.region_3depth_name
	from daily_login_users as a
	inner join all_myinfos as b
	on a.user_id = b.user_id
)

select login_year_month, create_year_month
	 , count(distinct user_id) as user_cnt
from active_user
group by login_year_month, create_year_month
order by login_year_month, create_year_month

-- 가입연월별 최초 유입시점 볼륨 대비 미 접속률
with active_user as (
	select a.login_date, a.login_year_month,b.create_date,  b.create_year_month, a.user_id, b.region_1depth_name, b.region_2depth_name, b.region_3depth_name
	from daily_login_users as a
	inner join all_myinfos as b
	on a.user_id = b.user_id
),
-- 가입월별, 접속 월별 접속 유저수 & 가입시점 기준 최초 접속 유저수 테이블 
active_user_by_create_month as (
	select login_year_month, create_year_month, user_cnt
		 , first_value(user_cnt) over(partition by create_year_month order by login_year_month) as first_user_cnt -- 가입시점 기준 최초 접속유저수
	from (
		select login_year_month, create_year_month
			 , count(distinct user_id) as user_cnt
		from active_user
		group by login_year_month, create_year_month
	) as a
	order by create_year_month, login_year_month
)

select login_year_month, create_year_month
	 , substr(create_year_month, 1, 4) as create_year
	 , concat('M', rank() over (partition by create_year_month order by login_year_month) - 1) as time_order
	 , user_cnt, first_user_cnt, round(user_cnt/first_user_cnt, 4) as rate_of_change
from active_user_by_create_month

-- 가입연월별 지역별 최초 유입시점 볼륨 대비 미접속률 
with active_user as (
	select a.login_date, a.login_year_month,b.create_date,  b.create_year_month, a.user_id, b.region_1depth_name, b.region_2depth_name, b.region_3depth_name
	from daily_login_users as a
	inner join all_myinfos as b
	on a.user_id = b.user_id
),
active_user_by_create_month as (
	select login_year_month, create_year_month, region, user_cnt
		 , first_value(user_cnt) over(partition by create_year_month, region order by login_year_month) as first_user_cnt
		 from (
		 	select login_year_month, create_year_month, region_3depth_name as region, count(distinct user_id) as user_cnt
		 	from active_user
		 	group by login_year_month, create_year_month, region
		 ) as a
		 order by create_year_month, login_year_month, region
)
select login_year_month, create_year_month
	 , substr(create_year_month, 1, 4) as create_year
	 , concat('M', rank() over (partition by create_year_month order by login_year_month) - 1) as time_order
	 , user_cnt, first_user_cnt, round(user_cnt/first_user_cnt, 4) as rate_of_change
from active_user_by_create_month

-- retention 분석

-- 유저 타입별 월간 일평균 D+day 잔존율 

WITH monthly_new_user_list AS (
    SELECT create_year_month, create_date, user_id
    FROM all_myinfos
),

daily_login AS (
    SELECT DISTINCT login_date, login_year_month, user_id
    FROM daily_login_users
),

d_plus_day_check_login AS (
    SELECT DISTINCT a.login_date, a.login_year_month,
           DATEDIFF(CAST('2022-11-30' AS DATE), a.login_date) AS date_diff_count,
           a.user_id,
           CASE WHEN d1.user_id IS NOT NULL THEN 1 ELSE 0 END AS d1_login,
           CASE WHEN d7.user_id IS NOT NULL THEN 1 ELSE 0 END AS d7_login,
           CASE WHEN d14.user_id IS NOT NULL THEN 1 ELSE 0 END AS d14_login,
           CASE WHEN d30.user_id IS NOT NULL THEN 1 ELSE 0 END AS d30_login
    FROM daily_login a
    LEFT JOIN daily_login d1 ON a.user_id = d1.user_id AND DATE_ADD(a.login_date, INTERVAL 1 DAY) = d1.login_date
    LEFT JOIN daily_login d7 ON a.user_id = d7.user_id AND DATE_ADD(a.login_date, INTERVAL 7 DAY) = d7.login_date
    LEFT JOIN daily_login d14 ON a.user_id = d14.user_id AND DATE_ADD(a.login_date, INTERVAL 14 DAY) = d14.login_date
    LEFT JOIN daily_login d30 ON a.user_id = d30.user_id AND DATE_ADD(a.login_date, INTERVAL 30 DAY) = d30.login_date
    ORDER BY a.login_date, a.user_id
)

SELECT login_year_month,
       CASE WHEN user_type IS NULL THEN '전체' ELSE user_type END AS user_type,
       COUNT(DISTINCT user_id) AS mau,
       COUNT(user_id) AS mau_not_unique,
       ROUND(IFNULL(SUM(CASE WHEN date_diff_count >= 1 THEN d1_login END) / NULLIF(COUNT(CASE WHEN date_diff_count >= 1 THEN user_id END), 0), 0), 3) AS d1_ret,
       ROUND(IFNULL(SUM(CASE WHEN date_diff_count >= 7 THEN d1_login END) / NULLIF(COUNT(CASE WHEN date_diff_count >= 7 THEN user_id END), 0), 0), 3) AS d7_ret,
       ROUND(IFNULL(SUM(CASE WHEN date_diff_count >= 14 THEN d1_login END) / NULLIF(COUNT(CASE WHEN date_diff_count >= 14 THEN user_id END), 0), 0), 3) AS d14_ret,
       ROUND(IFNULL(SUM(CASE WHEN date_diff_count >= 30 THEN d1_login END) / NULLIF(COUNT(CASE WHEN date_diff_count >= 30 THEN user_id END), 0), 0), 3) AS d30_ret
FROM (
    SELECT a.login_date, a.login_year_month, date_diff_count,
           a.user_id, d1_login, d7_login, d14_login, d30_login,
           CASE WHEN b.user_id IS NOT NULL THEN '신규유저' ELSE '재방문유저' END AS user_type
    FROM d_plus_day_check_login a
    LEFT JOIN monthly_new_user_list b ON a.user_id = b.user_id AND a.login_date = b.create_date
) a
GROUP BY login_year_month, user_type WITH ROLLUP
HAVING login_year_month IS NOT NULL
ORDER BY login_year_month, user_type;


-- 유저 타입별 D+day 잔존율
with monthly_new_user_list as (
	select create_year_month
		 , create_date
		 , user_id
	from all_myinfos
),
daily_login as (
	select distinct login_date
		 , login_year_month
		 , user_id
	from daily_login_users
),

d_plus_day_check_login AS (
    SELECT DISTINCT a.login_date, a.login_year_month,
           DATEDIFF(CAST('2022-11-30' AS DATE), a.login_date) AS date_diff_count,
           a.user_id,
           CASE WHEN d1.user_id IS NOT NULL THEN 1 ELSE 0 END AS d1_login,
           CASE WHEN d7.user_id IS NOT NULL THEN 1 ELSE 0 END AS d7_login,
           CASE WHEN d14.user_id IS NOT NULL THEN 1 ELSE 0 END AS d14_login,
           CASE WHEN d30.user_id IS NOT NULL THEN 1 ELSE 0 END AS d30_login
    FROM daily_login a
    LEFT JOIN daily_login d1 ON a.user_id = d1.user_id AND DATE_ADD(a.login_date, INTERVAL 1 DAY) = d1.login_date
    LEFT JOIN daily_login d7 ON a.user_id = d7.user_id AND DATE_ADD(a.login_date, INTERVAL 7 DAY) = d7.login_date
    LEFT JOIN daily_login d14 ON a.user_id = d14.user_id AND DATE_ADD(a.login_date, INTERVAL 14 DAY) = d14.login_date
    LEFT JOIN daily_login d30 ON a.user_id = d30.user_id AND DATE_ADD(a.login_date, INTERVAL 30 DAY) = d30.login_date
    ORDER BY a.login_date, a.user_id
)

select login_date
	 , case when user_type is null then '전체' else user_type end as user_type
	 , count(distinct user_id) as mau
	 , count(user_id) as mau_not_unique
	 , count(distinct login_date) as day_cnt
	 , count(distinct user_id) as dau
	 , count(user_id) as dau_not_unique
	 , round(sum(d1_login)/count(user_id), 3) as d1_ret
	 , round(sum(d7_login)/count(user_id), 3) as d7_ret
	 , round(sum(d14_login)/count(user_id), 3) as d14_ret
	 , round(sum(d30_login)/count(user_id), 3) as d30_ret
from (
	select a.login_date, a.login_year_month, date_diff_count
		 , a.user_id
		 , d1_login
		 , d7_login
		 , d14_login
		 , d30_login
		 , case when b.user_id is not null then '신규유저' else '재방문유저' end as user_type
	from d_plus_day_check_login as a
	left join monthly_new_user_list as b
	on a.user_id = b.user_id and a.login_date = b.create_date
	) as a 
	group by a.login_date, a.user_type with rollup
	having a.login_date is not null
	order by login_date, user_type
