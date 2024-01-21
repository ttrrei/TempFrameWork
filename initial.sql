drop schema if exists public cascade ;
drop schema if exists tier1 cascade ;
drop schema if exists tier2 cascade ;
drop schema if exists tier3 cascade ;
drop schema if exists tier4 cascade ;

--create schema public;
create schema tier1;
create schema tier2;
create schema tier3;
create schema tier4;

drop table if exists tier1.code_list;
create table tier1.code_list (
id varchar,
code varchar(100),
market_cap varchar(100),
sys_load_time timestamp default current_timestamp);

drop table if exists tier1.index_list;
create table tier1.index_list (
idx varchar(100),
comment varchar(500),
sys_load_time timestamp default current_timestamp);


drop table if exists tier1.short;
CREATE TABLE IF NOT EXISTS tier1.short
(
    company_full_name varchar(100),
    product_code varchar(30),
    short_position varchar(100),
    total_in_issue varchar(100),
    reported_position varchar(100),
    sys_load_time timestamp without time zone DEFAULT current_timestamp
);

drop table if exists tier1.full_code_history;
CREATE TABLE IF NOT EXISTS tier1.full_code_history
(
    code character varying COLLATE pg_catalog."default",
    date character varying COLLATE pg_catalog."default",
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric
);

drop view if exists tier1.vw_full_code_history;
create view tier1.vw_full_code_history as
select code
	, to_date(date, 'MM/DD/YY') as date
	, open::numeric(10,3) as open
	, high::numeric(10,3) as high
	, low::numeric(10,3) as low
	, close::numeric(10,3) as close
	, volume::numeric(20,0) as volume
from tier1.full_code_history;

CREATE OR REPLACE VIEW tier1.vw_full_code_history  AS
SELECT full_code_history.code,
    to_date(full_code_history.date::text, 'MM/DD/YY'::text) AS date,
    full_code_history.open::numeric(10,3) AS open,
    full_code_history.high::numeric(10,3) AS high,
    full_code_history.low::numeric(10,3) AS low,
    full_code_history.close::numeric(10,3) AS close,
    full_code_history.volume::numeric(20,0) AS volume,
    row_number() OVER (PARTITION BY full_code_history.code ORDER BY (to_date(full_code_history.date::text, 'MM/DD/YY'::text))) AS idx
   FROM tier1.full_code_history;

drop view if exists tier1.vw_full_code_history_mv;
CREATE OR REPLACE VIEW tier1.vw_full_code_history_mv
 AS
WITH temp AS (
	SELECT *
		, lag(close,1, close) over (partition by code order by idx) as last_close
		, cast (abs (close - lag(close,1, close) over (partition by code order by idx))
			/lag(close,1, close) over (partition by code order by idx) as numeric(15,5))
	as mv_diff
	FROM tier1.vw_full_code_history
), output as (
	select code, date, idx, open, high, low, close, volume, mv_diff
		, avg(mv_diff) OVER (PARTITION BY code ORDER BY idx RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS avg_10
		, max(mv_diff) OVER (PARTITION BY code ORDER BY idx RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS max_10
		, min(mv_diff) OVER (PARTITION BY code ORDER BY idx RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS min_10
	from temp	
)
SELECT code, date, idx, open, high, low, close, volume
	, ((avg_10 * 10::numeric - min_10 - max_10) / 8.0)::numeric(10,5) AS avg_movement
from output ;

drop view if exists tier1.vw_top_trading;
create view tier1.vw_top_trading as
with temp as (
	select extract( month from date) as month
		, extract( year from date) as year
		, code
		, sum((high + low)/2 * volume) as flow
	from tier1.vw_full_code_history
	group by extract( month from date)
		, extract( year from date)
		, code
), list as (
	select month, year, code
		, row_number() over (partition by month, year order by flow desc) as rnum
	from temp
)  SELECT list.month, list.year, list.code,
  	case when rnum <= 20 then 1 else 0 end as is_top20,
  	case when rnum <= 50 then 1 else 0 end as is_top50,
  	case when rnum <= 100 then 1 else 0 end as is_top100,
  	case when rnum <= 200 then 1 else 0 end as is_top200
     FROM list
    WHERE list.rnum <= 300;


drop view if exists tier1.vw_sub_code_history;
create view tier1.vw_sub_code_history as
select * from tier1.vw_full_code_history
where code in (
select code from tier1.vw_top_trading group by code
);

CREATE OR REPLACE FUNCTION tier1.calculate_heikin_ashi(
	input_code character varying)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

DROP TABLE IF EXISTS temp_org_price;
CREATE temp TABLE temp_org_price AS
	SELECT * FROM tier1.vw_full_code_history
	WHERE code = input_code;
DROP TABLE IF EXISTS temp_heikin_ashi ;
CREATE TEMP TABLE temp_heikin_ashi AS
WITH recursive heikin_ashi_calculate AS (
	SELECT code, date, OPEN, high, low, CLOSE, volume,
	round((OPEN +CLOSE) / 2, 3) AS ha_open,
	round((OPEN + high + low +CLOSE) / 4, 3) AS ha_high,
	round((OPEN + high + low +CLOSE) / 4, 3) AS ha_low,
	round((OPEN + high + low +CLOSE) / 4, 3) AS ha_close,
	volume AS ha_volume, idx
	FROM temp_org_price WHERE idx = 1
	UNION
	SELECT fh.code, fh.date, fh.OPEN, fh.high, fh.low, fh.CLOSE, fh.volume,
		round((ec.ha_open + ec.ha_close) / 2, 3) AS ha_open,
		greatest(fh.high, round((ec.ha_open + ec.ha_close) / 2, 3), round((fh.OPEN + fh.high + fh.low + fh.CLOSE) / 4, 3)) AS ha_high,
		least(fh.low, round((ec.ha_open     + ec.ha_close) / 2, 3), round((fh.OPEN + fh.high + fh.low + fh.CLOSE) / 4, 3)) AS ha_low,
		round((fh.OPEN + fh.high + fh.low + fh.CLOSE) / 4, 3) AS ha_close,
		fh.volume AS ha_volume, fh.idx
	FROM heikin_ashi_calculate AS ec
	INNER JOIN temp_org_price AS fh
	ON ec.code = fh.code AND ec.idx = fh.idx-1 )
SELECT code, date, OPEN, high, low, CLOSE, volume, ha_open, ha_high, ha_low, ha_close, ha_volume, idx
FROM heikin_ashi_calculate;

insert into tier2.heikin_ashi
select code, date, idx, ha_open, ha_high, ha_low, ha_close
from temp_heikin_ashi;

END;
$BODY$;

DROP TABLE IF EXISTS tier2.heikin_ashi;
CREATE TABLE tier2.heikin_ashi
(
    code character varying COLLATE pg_catalog."default",
    date date,
    idx bigint,
    ha_open numeric(15,3),
    ha_high numeric(15,3),
	ha_low numeric(15,3),
    ha_close numeric(15,3)
);

DROP TABLE IF EXISTS tier2.technical_indicator;

CREATE TABLE IF NOT EXISTS tier2.technical_indicator
(
    code character varying(100) COLLATE pg_catalog."default",
    date_idx character varying(100) COLLATE pg_catalog."default",
    xema character varying(100) COLLATE pg_catalog."default",
    sema character varying(100) COLLATE pg_catalog."default",
    lema character varying(100) COLLATE pg_catalog."default",
    tema character varying(100) COLLATE pg_catalog."default",
    macd character varying(100) COLLATE pg_catalog."default",
    psar character varying(100) COLLATE pg_catalog."default",
    scmo character varying(100) COLLATE pg_catalog."default",
    lcmo character varying(100) COLLATE pg_catalog."default",
    srsi character varying(100) COLLATE pg_catalog."default",
    lrsi character varying(100) COLLATE pg_catalog."default",
    srsisema character varying(100) COLLATE pg_catalog."default",
    srsilema character varying(100) COLLATE pg_catalog."default",
    lrsisema character varying(100) COLLATE pg_catalog."default",
    lrsilema character varying(100) COLLATE pg_catalog."default",
    scci character varying(100) COLLATE pg_catalog."default",
    lcci character varying(100) COLLATE pg_catalog."default",
    stck character varying(100) COLLATE pg_catalog."default",
    stcd character varying(100) COLLATE pg_catalog."default",
    cpcv character varying(100) COLLATE pg_catalog."default",
    sdpo character varying(100) COLLATE pg_catalog."default",
    ldpo character varying(100) COLLATE pg_catalog."default",
	gppo character varying(100) COLLATE pg_catalog."default",
	sroc character varying(100) COLLATE pg_catalog."default",
	lroc character varying(100) COLLATE pg_catalog."default",
	sadx character varying(100) COLLATE pg_catalog."default",
	ladx character varying(100) COLLATE pg_catalog."default",
    sstrsi character varying(100) COLLATE pg_catalog."default",
   	lstrsi character varying(100) COLLATE pg_catalog."default"
);

DROP TABLE IF EXISTS tier2.eavt_source;

CREATE TABLE tier2.eavt_source
(
    entity character varying(100) COLLATE pg_catalog."default",
    idx integer,
    evaluation character varying[] COLLATE pg_catalog."default",
    attribute text COLLATE pg_catalog."default",
    len integer
);

-- FUNCTION: tier2.reformat_eavt(character varying, character varying, integer)

-- DROP FUNCTION IF EXISTS tier2.reformat_eavt(character varying, character varying, integer);

CREATE OR REPLACE FUNCTION tier2.reformat_eavt(
	symbol character varying,
	attribute character varying,
	len integer)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    query_text text;
BEGIN

 query_text := FORMAT('
insert into tier2.eavt_source
WITH list AS (
	SELECT i.code,
		i.date_idx AS rec_dt,
		t.date_idx AS t_dt,
		t.%s AS prc
	FROM tier2.vw_technical_indicator i
    JOIN tier2.vw_technical_indicator t ON i.code::text = t.code::text
	WHERE i.date_idx::integer >= t.date_idx::integer AND i.date_idx::integer <= (t.date_idx::integer + %s -1) AND i.code::text = ''%s''::text
), output AS (
	SELECT list.code,
		list.rec_dt,
		array_agg(list.prc ORDER BY (list.t_dt::integer)) AS val
	FROM list
	GROUP BY list.code, list.rec_dt
	ORDER BY list.code, (list.rec_dt::integer)
)
 SELECT o.code as entity,
 	cast (o.rec_dt as integer) as record_date,
    o.val as evaluation,
    ''%s'' as attribute,
	%s as len
   FROM output o
  WHERE array_length(o.val, 1) = %s
  ORDER BY (o.rec_dt::integer)
    ', attribute, len, symbol, attribute, len, len);

EXECUTE query_text;
END;
$BODY$;

ALTER FUNCTION tier2.reformat_eavt(character varying, character varying, integer)
    OWNER TO postgres;

DROP TABLE if exists tier2.eavt_target;

CREATE TABLE tier2.eavt_target
(
    symbol character varying COLLATE pg_catalog."default",
    date_index character varying COLLATE pg_catalog."default",
    lens character varying COLLATE pg_catalog."default",
    attr character varying COLLATE pg_catalog."default",
    dgr character varying COLLATE pg_catalog."default",
    vle jsonb
);

drop view if exists tier2.vw_heikin_ashi_type;
create view tier2.vw_heikin_ashi_type as
select code, date, idx
, case when ha_open < ha_close then 1
	   when ha_open > ha_close then -1
	   when ha_open = ha_close then 0
	else null end as ha_movement
, case when ha_high = ha_open then 1
	else 0 end as strong_down
, case when ha_low = ha_open then 1
	else 0 end as strong_up
from tier2.heikin_ashi;


---TODO!!!!
create view tier2.transform_indicator as
select code, date_idx
 , case when sema >= lema then 1 else 0 end as s_l_ema

from tier2.technical_indicator;

-- View: tier2.vw_technical_indicator

-- DROP VIEW tier2.vw_technical_indicator;

CREATE OR REPLACE VIEW tier2.vw_technical_indicator
 AS
 SELECT code,
    date_idx::integer AS date_idx,
        CASE
            WHEN xema::text = 'NaN'::text THEN NULL::numeric
            ELSE xema::numeric(20,4)
        END AS xema,
        CASE
            WHEN sema::text = 'NaN'::text THEN NULL::numeric
            ELSE sema::numeric(20,4)
        END AS sema,
        CASE
            WHEN lema::text = 'NaN'::text THEN NULL::numeric
            ELSE lema::numeric(20,4)
        END AS lema,
        CASE
            WHEN tema::text = 'NaN'::text THEN NULL::numeric
            ELSE tema::numeric(20,4)
        END AS tema,
        CASE
            WHEN macd::text = 'NaN'::text THEN NULL::numeric
            ELSE macd::numeric(20,4)
        END AS macd,
        CASE
            WHEN psar::text = 'NaN'::text THEN NULL::numeric
            ELSE psar::numeric(20,4)
        END AS psar,
        CASE
            WHEN scmo::text = 'NaN'::text THEN NULL::numeric
            ELSE scmo::numeric(20,4)
        END AS scmo,
        CASE
            WHEN lcmo::text = 'NaN'::text THEN NULL::numeric
            ELSE lcmo::numeric(20,4)
        END AS lcmo,
        CASE
            WHEN srsi::text = 'NaN'::text THEN NULL::numeric
            ELSE srsi::numeric(20,4)
        END AS srsi,
        CASE
            WHEN lrsi::text = 'NaN'::text THEN NULL::numeric
            ELSE lrsi::numeric(20,4)
        END AS lrsi,
        CASE
            WHEN srsisema::text = 'NaN'::text THEN NULL::numeric
            ELSE srsisema::numeric(20,4)
        END AS srsisema,
        CASE
            WHEN srsilema::text = 'NaN'::text THEN NULL::numeric
            ELSE srsilema::numeric(20,4)
        END AS srsilema,
        CASE
            WHEN lrsisema::text = 'NaN'::text THEN NULL::numeric
            ELSE lrsisema::numeric(20,4)
        END AS lrsisema,
        CASE
            WHEN lrsilema::text = 'NaN'::text THEN NULL::numeric
            ELSE lrsilema::numeric(20,4)
        END AS lrsilema,
        CASE
            WHEN scci::text = 'NaN'::text THEN NULL::numeric
            ELSE scci::numeric(20,4)
        END AS scci,
        CASE
            WHEN lcci::text = 'NaN'::text THEN NULL::numeric
            ELSE lcci::numeric(20,4)
        END AS lcci,
        CASE
            WHEN stck::text = 'NaN'::text THEN NULL::numeric
            ELSE stck::numeric(20,4)
        END AS stck,
        CASE
            WHEN stcd::text = 'NaN'::text THEN NULL::numeric
            ELSE stcd::numeric(20,4)
        END AS stcd,
        CASE
            WHEN cpcv::text = 'NaN'::text THEN NULL::numeric
            ELSE cpcv::numeric(20,4)
        END AS cpcv,
        CASE
            WHEN sdpo::text = 'NaN'::text THEN NULL::numeric
            ELSE sdpo::numeric(20,4)
        END AS sdpo,
        CASE
            WHEN ldpo::text = 'NaN'::text THEN NULL::numeric
            ELSE ldpo::numeric(20,4)
        END AS ldpo,
        CASE
            WHEN gppo::text = 'NaN'::text THEN NULL::numeric
            ELSE gppo::numeric(20,4)
        END AS gppo,
        CASE
            WHEN sroc::text = 'NaN'::text THEN NULL::numeric
            ELSE sroc::numeric(20,4)
        END AS sroc,
        CASE
            WHEN lorc::text = 'NaN'::text THEN NULL::numeric
            ELSE lorc::numeric(20,4)
        END AS lorc,
        CASE
            WHEN sadx::text = 'NaN'::text THEN NULL::numeric
            ELSE sadx::numeric(20,4)
        END AS sadx,
        CASE
            WHEN ladx::text = 'NaN'::text THEN NULL::numeric
            ELSE ladx::numeric(20,4)
        END AS ladx,
        CASE
            WHEN sstrsi::text = 'NaN'::text THEN NULL::numeric
            ELSE sstrsi::numeric(20,4)
        END AS sstrsi,
        CASE
            WHEN lstrsi::text = 'NaN'::text THEN NULL::numeric
            ELSE lstrsi::numeric(20,4)
        END AS lstrsi
   FROM tier2.technical_indicator;

ALTER TABLE tier2.vw_technical_indicator
    OWNER TO postgres;



create view tier2.vw_outcome as 
with temp as (
select a.*
	, a.avg_movement*4 as target4
	, a.avg_movement*5 as target5
	, b.idx as tidx, b.high as thigh, b.low as tlow
	, case when b.high >= a.close * (1+ a.avg_movement*4) then b.idx else 99999 end as reach_h_t4
	, case when b.low <= a.close * (1- a.avg_movement*4) then b.idx else 99999 end as reach_l_t4
	, case when b.high >= a.close * (1+ a.avg_movement*5) then b.idx else 99999 end as reach_h_t5
	, case when b.low <= a.close * (1- a.avg_movement*5) then b.idx else 99999 end as reach_l_t5
	, case when b.idx -14 = a.idx then cast(abs(b.close / a.close - 1) as numeric(10,5)) else 0 end as final_outcome
	, case when b.idx -14 = a.idx and b.close > a.close then 1 else 0 end as final_l_flag
	, case when b.idx -14 = a.idx and b.close < a.close then 1 else 0 end as final_s_flag
from tier1.vw_full_code_history_mv  a
inner join tier1.vw_full_code_history_mv  b
on a.code = b.code 
where a.idx > b.idx - 15
and a.idx < b.idx
), agge as (
	select code, date, idx, open, high, low, close, volume
	, avg_movement, target4, target5
	, min (reach_h_t4) as reach_h_t4
	, min (reach_l_t4) as reach_l_t4
	, min (reach_h_t5) as reach_h_t5
	, min (reach_l_t5) as reach_l_t5
	, max(final_outcome) as final_outcome
	, max(final_s_flag) as final_s_flag
	, max(final_l_flag) as final_l_flag
from temp
group by code, date, idx, open, high, low, close, volume, avg_movement, target4, target5
), output as ( 
	select code, date, idx, open, high, low, close, volume, avg_movement, target4, target5
		, max(idx) over (partition by code) as max_idx
		, reach_h_t4, reach_l_t4, reach_h_t5, reach_l_t5, final_outcome, final_s_flag, final_l_flag
		, case	when reach_h_t4 < reach_l_t4 then cast(target4 as numeric(10,5))
				when reach_h_t4 > reach_l_t4 then cast(0-target4 as numeric(10,5))
				when reach_h_t4 = reach_l_t4 and reach_h_t4 <> 99999 and reach_l_t4 <> 99999 then null
				when reach_h_t4 = reach_l_t4 and reach_h_t4 = 99999 and final_l_flag = 1 then cast(final_outcome as numeric(10,5))
				when reach_h_t4 = reach_l_t4 and reach_l_t4 = 99999 and final_s_flag = 1 then cast(0-final_outcome as numeric(10,5))
		 else cast(0 as numeric(10,5)) end as outcome4
		 , case	when reach_h_t5 < reach_l_t5 then cast(target5 as numeric(10,5))
				when reach_h_t5 > reach_l_t5 then cast(0-target5 as numeric(10,5))
				when reach_h_t5 = reach_l_t5 and reach_h_t5 <> 99999 and reach_l_t5 <> 99999 then null
				when reach_h_t5 = reach_l_t5 and reach_h_t5 = 99999 and final_l_flag = 1 then cast(final_outcome as numeric(10,5))
				when reach_h_t5 = reach_l_t5 and reach_l_t5 = 99999 and final_s_flag = 1 then cast(0-final_outcome as numeric(10,5))
		 else cast(0 as numeric(10,5)) end as outcome5
	from agge
)	select code, date, idx, open, high, low, close, volume, outcome4, outcome5 
from output where idx >5 and max_idx -idx > 15
			  order by idx;
