-- marketing_myinfos table 생성
create table marketing_myinfos as (
	select *, concat('attribute-', user_id) as attribute_id
	from all_myinfos
	where create_date >= '2022-05-01'
	order by create_date
);
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