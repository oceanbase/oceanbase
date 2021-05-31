-- input string.
drop table dt;
create table dt(id int primary key auto_increment, pre varchar(100), str varchar(100), suf varchar(100),
                dt_val datetime(6), d_val date, t_val time(6),
                dt_useconds bigint, d_days int, t_useconds bigint);
set sql_mode = 'no_zero_date,no_zero_in_date';
-- good format.
insert into dt(str) values ('2015-05-30 11:12:13.1415');
insert into dt(str) values ('2015.05.30 11.12.13.1415');
insert into dt(str) values ('2015/05/30 11/12/13.1415');
insert into dt(str) values ('2015+05=30 11*12&13.1415');
insert into dt(str) values ('2015^05%30 11$12#13.1415');
insert into dt(str) values ('2015@05!30 11~12`13.1415');
-- good format but bad values.
insert into dt(str) values ('0000-05-30 11:12:13.1415');
insert into dt(str) values ('00002015-00000005-00000030 11:12:13.1415');
insert into dt(str) values ('00000015-00000005-00000030 11:12:13.1415');
insert into dt(str) values ('00000015.00000005.00000030 11:12:13.1415');
insert into dt(str) values ('10000-05-30 11:12:13.1415');
insert into dt(str) values ('10000.05.30 11:12:13.1415');
insert into dt(str) values ('2015-13-30 11:12:13.1415');
insert into dt(str) values ('2015-05-32 11:12:13.1415');
insert into dt(str) values ('2015-05-30 24:12:13.1415');
insert into dt(str) values ('2015-05-30 11:60:13.1415');
insert into dt(str) values ('2015-05-30 11:12:60.1415');
insert into dt(str) values ('2015-12-31 23:59:59.9999999');
-- bad format with no delimiter.
insert into dt(str) values ('10');
insert into dt(str) values ('101');
insert into dt(str) values ('1011');
insert into dt(str) values ('10111');
insert into dt(str) values ('101112');
insert into dt(str) values ('1011121');
insert into dt(str) values ('10111213');
insert into dt(str) values ('101112131');
insert into dt(str) values ('1011121314');
insert into dt(str) values ('10111213141');
insert into dt(str) values ('101112131415');
insert into dt(str) values ('1011121314151');
insert into dt(str) values ('10111213141516');
insert into dt(str) values ('101112131415161');
insert into dt(str) values ('1011121314151617');
insert into dt(str) values ('10111213141516171');
insert into dt(str) values ('101112131415161718');
-- bad format with only one delimiter '.'.
insert into dt(str) values ('10');
insert into dt(str) values ('10.1');
insert into dt(str) values ('10.11');
insert into dt(str) values ('1011.1');
insert into dt(str) values ('1011.12');
insert into dt(str) values ('101112.1');
insert into dt(str) values ('101112.13');
insert into dt(str) values ('10111213.1');
insert into dt(str) values ('10111213.14');
insert into dt(str) values ('1011121314.1');
insert into dt(str) values ('1011121314.15');
insert into dt(str) values ('101112131415.1');
insert into dt(str) values ('101112131415.16');
insert into dt(str) values ('101112131415.16.17');
insert into dt(str) values ('101112131415.16-17');
insert into dt(str) values ('101112131415..16');
insert into dt(str) values ('101112131415..16.17');
insert into dt(str) values ('101112131415..16-17');
insert into dt(str) values ('101112131415.-.16');
insert into dt(str) values ('101112131415.-.16.17');
insert into dt(str) values ('101112131415.-.16-17');
insert into dt(str) values ('101112131415-16');
insert into dt(str) values ('101112131415-16.17');
insert into dt(str) values ('101112131415-16-17');
insert into dt(str) values ('101112131415.161111.17');
insert into dt(str) values ('101112131415.1611112.17');
insert into dt(str) values ('10111213141516.1');
insert into dt(str) values ('10111213141516.17');
insert into dt(str) values ('1011121314151617.1');
insert into dt(str) values ('1011121314151617.18');
insert into dt(str) values ('101112131415161.718');
insert into dt(str) values ('10111213141516.1718');
insert into dt(str) values ('1011121314151.61718');
insert into dt(str) values ('101112131415.161718');
insert into dt(str) values ('10111213141.5161718');
insert into dt(str) values ('1011121314.15161718');
insert into dt(str) values ('101112131.415161718');
insert into dt(str) values ('10111213.1415161718');
insert into dt(str) values ('1011121.31415161718');
insert into dt(str) values ('101112.131415161718');   -- mysql is wrong when update to time: 10:11:12.131416, usecond part round to 6 digits should be 131415.
                                                      -- the reason is mysql continue reading digits after has gotten 6 digits for usecond until reach the first
                                                      -- non-digit, and uses the last digit to determine whether should plus 1 to usecond when it convert a string
                                                      -- to time, but no bug when convert to datetime because they use different code to read digits after usecond.
insert into dt(str) values ('10111.2131415161718');
insert into dt(str) values ('1011.12131415161718');
insert into dt(str) values ('101.112131415161718');
insert into dt(str) values ('10.1112131415161718');
-- bad format with two delimiters '.'.
insert into dt(str) values ('10.11121314151617.18');
insert into dt(str) values ('101.112131415161.718');
insert into dt(str) values ('1011.1213141516.1718');
insert into dt(str) values ('10111.21314151.61718');
insert into dt(str) values ('101112.131415.161718');
insert into dt(str) values ('1011121.3141.5161718');
insert into dt(str) values ('10111213.14.15161718');
insert into dt(str) values ('101112131415.1.61718');
insert into dt(str) values ('101112131415.16.1718');
insert into dt(str) values ('101112131415.161.718');
insert into dt(str) values ('10111213141516.1.718');
insert into dt(str) values ('10111213141516.17.18');
insert into dt(str) values ('10111213141516.171.8');
insert into dt(str) values ('10..1112131415161718');
insert into dt(str) values ('101..112131415161718');
insert into dt(str) values ('1011..12131415161718');
insert into dt(str) values ('10111213141516..1718');
insert into dt(str) values ('101112131415161..718');
insert into dt(str) values ('1011121314151617..18');
-- bad format with more delimiters except space.
insert into dt(str) values ('2012.010112345');
insert into dt(str) values ('2012+010112345');
insert into dt(str) values ('2012.01=0112345');
insert into dt(str) values ('2012*01=0112345');
insert into dt(str) values ('2012.01=011*2345');
insert into dt(str) values ('2012!01=011*2345');
insert into dt(str) values ('2012.*01^011&2345');
insert into dt(str) values ('2012*.01^011&2345');
insert into dt(str) values ('2012**01^011&2345');
insert into dt(str) values ('201201.0112345');
insert into dt(str) values ('201201%0112345');
insert into dt(str) values ('201201.01#12345');
insert into dt(str) values ('201201%01#12345');
insert into dt(str) values ('201201.01@12!345');
insert into dt(str) values ('201201%01@12!345');
insert into dt(str) values ('2012.01%01@12!34:5');
insert into dt(str) values ('2012;01%01@12!34:5');
-- bad format with more delimiters include space.
insert into dt(str) values ('201-2-1 1-12-34');
insert into dt(str) values ('201-0201 1-12-34');
insert into dt(str) values ('201. 0201 1-12-34');
insert into dt(str) values ('201.2 1 1-12 34');
-- time string.
insert into dt(str) values ('12:59:59');
insert into dt(str) values ('123:59:59');
insert into dt(str) values ('1234:59:59');
insert into dt(str) values ('12345:59:59');
insert into dt(str) values ('123456:59:59');
insert into dt(str) values ('1234567:59:59');
insert into dt(str) values ('12345678:59:59');
insert into dt(str) values ('123456789:59:59');
insert into dt(str) values ('1234567890:59:59');
insert into dt(str) values ('1011131314151617');
insert into dt(str) values ('1011123214151617');
insert into dt(str) values ('10.55.55');
insert into dt(str) values ('101.55.55');
insert into dt(str) values ('1011.55.55');
insert into dt(str) values ('10111.55.55');
insert into dt(str) values ('101112.55.55');
insert into dt(str) values ('1011121.55.55');
insert into dt(str) values ('10111213.55.55');
insert into dt(str) values ('101112131.55.55');
insert into dt(str) values ('1011121314.55.55');
insert into dt(str) values ('10111213141.55.55');
insert into dt(str) values ('101112131415.22.22');
insert into dt(str) values ('101112131415.55.55');
insert into dt(str) values ('1011121314151.55.55');
insert into dt(str) values ('10111213141516.55.55');
insert into dt(str) values ('101112131415161.55.55');
insert into dt(str) values ('1011121314151617.55.55');
insert into dt(str) values ('10111213141516171.55.55');
insert into dt(str) values ('101112131415161718.55.55');
insert into dt(str) values ('1');
insert into dt(str) values ('12');
insert into dt(str) values ('123');
insert into dt(str) values ('1234');
insert into dt(str) values ('12345');
insert into dt(str) values ('123456');
insert into dt(str) values ('1123456');
insert into dt(str) values ('12+3456');
insert into dt(str) values ('12:3456');
insert into dt(str) values ('1234=56');
insert into dt(str) values ('1234:56');
insert into dt(str) values ('12!34@56');
insert into dt(str) values ('12:34@56');
insert into dt(str) values ('12!34:56');
insert into dt(str) values ('12:34:56');
insert into dt(str) values ('12:34::56');
insert into dt(str) values ('12:34:=56');
insert into dt(str) values ('12:34+:56');
insert into dt(str) values ('12:34: 56');
insert into dt(str) values ('12:34 :56');
insert into dt(str) values ('12:34:56..789');
insert into dt(str) values ('12:34:56:.789');
insert into dt(str) values ('12:34:56.:789');
insert into dt(str) values ('12:34:56 .789');
insert into dt(str) values ('12:34:56. 789');
insert into dt(str) values ('12:34.789');
insert into dt(str) values ('12:34:61.789');
insert into dt(str) values ('11 12:34:56.789');

insert into dt(str) values ('200 : 59 : 59');
insert into dt(str) values ('200 :59 :59');
insert into dt(str) values ('200: 59: 59');
-- these case is too strange to compatible, give up temporarily.
insert into dt(str) values ('11: 12:34:56.789');
insert into dt(str) values ('11. 12:34:56.789');
insert into dt(str) values ('11 :12:34:56.789');
insert into dt(str) values ('11 .12:34:56.789');

insert into dt(str) values ('11 12:3456.789');
insert into dt(str) values ('11 12+3456.789');
insert into dt(str) values ('11 1234:56.789');
insert into dt(str) values ('11 1234=56.789');
insert into dt(str) values ('11 1234.789');
insert into dt(str) values ('11 12:34.789');
insert into dt(str) values ('11 12.789');
insert into dt(str) values ('11 12 .789');
insert into dt(str) values ('11 12. 789');
-- some critical case.
insert into dt(str) values ('1000-01-01 00:00:00');
insert into dt(str) values ('1969-12-31 23:59:59');
insert into dt(str) values ('1970-01-01 00:00:00');
insert into dt(str) values ('9999-12-31 23:59:59');
insert into dt(str) values ('838:59:59');
insert into dt(str) values ('838:59:59.000001');
insert into dt(str) values ('840:00:00');
--
-- generate datetime, date, time.
update dt set dt_val = str;
update dt set d_val = str;
update dt set t_val = str;
select str, dt_val, d_val, t_val from dt order by id;
update dt set dt_val = null where dt_val = '0000-00-00 00:00:00.000000';
update dt set d_val = null where d_val = '0000-00-00';
update dt set t_val = null where t_val = '00:00:00.000000';
select str, dt_val, d_val, t_val from dt order by id;
--
-- generate datetime useconds, date days, time useconds.
update dt set dt_useconds = unix_timestamp(dt_val);
update dt set d_days = datediff(d_val, '1970-1-1');
update dt set t_useconds = time_to_sec(t_val);
--
-- generate str_to_ob_time (datetime) unittest.
update dt set pre = "STR_TO_OB_TIME_FAIL(\"",
              suf = "\", DT_TYPE_DATETIME);"
          where dt_val is null;
update dt set pre = "STR_TO_OB_TIME_SUCC(\"",
              suf = concat("\", DT_TYPE_DATETIME, ",
                           year(dt_val), ", ",
                           month(dt_val), ", ",
                           day(dt_val), ", ",
                           hour(dt_val), ", ",
                           minute(dt_val), ", ",
                           second(dt_val), ", ",
                           microsecond(dt_val), ");")
          where dt_val is not null;
select str, dt_val from dt order by id;
select concat(pre, str, suf) from dt order by id;
--
-- generate str_to_ob_time (date) unittest.
update dt set pre = "STR_TO_OB_TIME_FAIL(\"",
              suf = "\", DT_TYPE_DATE);"
          where d_val is null;
update dt set pre = "STR_TO_OB_TIME_SUCC(\"",
              suf = concat("\", DT_TYPE_DATE, ",
                           year(d_val), ", ",
                           month(d_val), ", ",
                           day(d_val), ", -1, -1, -1, -1);")
          where d_val is not null;
select str, d_val from dt order by id;
select concat(pre, str, suf) from dt order by id;
--
-- generate str_to_ob_time (time) unittest.
update dt set pre = "STR_TO_OB_TIME_FAIL(\"",
              suf = "\", DT_TYPE_TIME);"
          where t_val is null;
update dt set pre = "STR_TO_OB_TIME_SUCC(\"",
              suf = concat("\", DT_TYPE_TIME, -1, -1, -1, ",
                           hour(t_val), ", ",
                           minute(t_val), ", ",
                           second(t_val), ", ",
                           microsecond(t_val), ");")
          where t_val is not null and t_val != '838:59:59.000000';
update dt set pre = "STR_TO_OB_TIME_SUCC(\"",
              suf = "\", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);"
          where t_val is not null and t_val = '838:59:59.000000';
select str, t_val from dt order by id;
select concat(pre, str, suf) from dt order by id;


drop table dt;
create table dt(id int primary key auto_increment, pre varchar(100), dec_val decimal(30, 8), int_val bigint unsigned, dbl_val double, suf varchar(100),
                dt_val datetime(6), d_val date, t_val time(6),
                dt_useconds bigint, d_days int, t_useconds bigint);
set sql_mode = '';
-- decimal value.
insert into dt(dec_val) values (22.5555555);
insert into dt(dec_val) values (212.5555555);
insert into dt(dec_val) values (2112.5555555);
insert into dt(dec_val) values (21112.5555555);
insert into dt(dec_val) values (211112.5555555);
insert into dt(dec_val) values (2111112.5555555);
insert into dt(dec_val) values (21111112.5555555);
insert into dt(dec_val) values (211111112.5555555);
insert into dt(dec_val) values (2111111112.5555555);
insert into dt(dec_val) values (21111111112.5555555);
insert into dt(dec_val) values (211111111112.5555555);
insert into dt(dec_val) values (2111111111112.5555555);
insert into dt(dec_val) values (21111111111112.5555555);
insert into dt(dec_val) values (211111111111112.5555555);
insert into dt(dec_val) values (2111111111111112.5555555);
insert into dt(dec_val) values (21111111111111112.5555555);
insert into dt(dec_val) values (211111111111111112.5555555);
insert into dt(dec_val) values (9223372036854775807.5555555);
insert into dt(dec_val) values (18446744073709551615.5555555);
insert into dt(dec_val) values (10.2222222);
insert into dt(dec_val) values (101.2222222);
insert into dt(dec_val) values (1011.2222222);
insert into dt(dec_val) values (10111.2222222);
insert into dt(dec_val) values (101112.2222222);
insert into dt(dec_val) values (1011121.2222222);
insert into dt(dec_val) values (10111213.2222222);
insert into dt(dec_val) values (101112131.2222222);
insert into dt(dec_val) values (1011121314.2222222);
insert into dt(dec_val) values (10111213141.2222222);
insert into dt(dec_val) values (101112131415.2222222);
insert into dt(dec_val) values (1011121314151.2222222);
insert into dt(dec_val) values (10111213141516.2222222);
insert into dt(dec_val) values (101112131415161.2222222);
insert into dt(dec_val) values (1011121314151617.2222222);
insert into dt(dec_val) values (10111213141516171.2222222);
insert into dt(dec_val) values (101112131415161718.2222222);
insert into dt(dec_val) values (9223372036854775807.2222222);
insert into dt(dec_val) values (18446744073709551615.2222222);
--
-- generate int value and double value.
update dt set int_val = dec_val, dbl_val = dec_val;
--
-- generate datetime, date, time from dec_val.
update dt set dt_val = dec_val;
update dt set d_val = dec_val;
update dt set t_val = dec_val;
select dec_val, dt_val, d_val, t_val from dt order by id;
update dt set dt_val = null where dt_val = '0000-00-00 00:00:00.000000';
update dt set d_val = null where d_val = '0000-00-00';
--update dt set t_val = null where t_val = '838:59:59.000000';
select dec_val, dt_val, d_val, t_val from dt order by id;
--
-- generate datetime useconds, date days, time useconds from dec_val.
update dt set dt_useconds = unix_timestamp(dt_val);
update dt set d_days = datediff(d_val, '1970-1-1');
update dt set t_useconds = time_to_sec(t_val);
--
-- generate datetime, date, time from int_val.
update dt set dt_val = int_val;
update dt set d_val = int_val;
update dt set t_val = int_val;
select int_val, dt_val, d_val, t_val from dt order by id;
update dt set dt_val = null where dt_val = '0000-00-00 00:00:00.000000';
update dt set d_val = null where d_val = '0000-00-00';
select int_val, dt_val, d_val, t_val from dt order by id;
--
-- generate datetime useconds, date days, time useconds from int_val.
update dt set dt_useconds = unix_timestamp(dt_val);
update dt set d_days = datediff(d_val, '1970-1-1');
update dt set t_useconds = time_to_sec(t_val);
--
-- generate int_to_ob_time (datetime) unittest.
update dt set pre = "INT_TO_OB_TIME_FAIL(",
              suf = ", DT_TYPE_DATETIME);"
          where dt_val is null;
update dt set pre = "INT_TO_OB_TIME_SUCC(",
              suf = concat(", DT_TYPE_DATETIME, ",
                           year(dt_val), ", ",
                           month(dt_val), ", ",
                           day(dt_val), ", ",
                           hour(dt_val), ", ",
                           minute(dt_val), ", ",
                           second(dt_val), ", ",
                           microsecond(dt_val), ");")
          where dt_val is not null;
select int_val, dt_val from dt order by id;
select concat(pre, int_val, suf) from dt order by id;
--
-- generate int_to_ob_time (date) unittest.
update dt set pre = "INT_TO_OB_TIME_FAIL(",
              suf = ", DT_TYPE_DATE);"
          where d_val is null;
update dt set pre = "INT_TO_OB_TIME_SUCC(",
              suf = concat(", DT_TYPE_DATE, ",
                           year(d_val), ", ",
                           month(d_val), ", ",
                           day(d_val), ", -1, -1, -1, -1);")
          where d_val is not null;
select int_val, d_val from dt order by id;
select concat(pre, int_val, suf) from dt order by id;
--
-- generate int_to_ob_time (time) unittest.
update dt set pre = "INT_TO_OB_TIME_FAIL(",
              suf = ", DT_TYPE_TIME);"
          where t_val is null;
update dt set pre = "INT_TO_OB_TIME_SUCC(",
              suf = concat(", DT_TYPE_TIME, -1, -1, -1, ",
                           hour(t_val), ", ",
                           minute(t_val), ", ",
                           second(t_val), ", ",
                           microsecond(t_val), ");")
          where t_val is not null and t_val != '838:59:59.000000';
update dt set pre = "INT_TO_OB_TIME_SUCC(",
              suf = ", DT_TYPE_TIME, -1, -1, -1, 839, -1, -1, -1);"
          where t_val is not null and t_val = '838:59:59.000000';
select int_val, t_val from dt order by id;
select concat(pre, int_val, suf) from dt order by id;
--
-- generate datetime, date, time from dbl_val.
update dt set dt_val = dbl_val;
update dt set d_val = dbl_val;
update dt set t_val = dbl_val;
select dbl_val, dt_val, d_val, t_val from dt order by id;
update dt set dt_val = null where dt_val = '0000-00-00 00:00:00.000000';
update dt set d_val = null where d_val = '0000-00-00';
--update dt set t_val = null where t_val = '838:59:59.000000';
--
-- generate datetime useconds, date days, time useconds from dbl_val.
update dt set dt_useconds = unix_timestamp(dt_val);
update dt set d_days = datediff(d_val, '1970-1-1');
update dt set t_useconds = time_to_sec(t_val);


drop table dt; create table dt(d_val date);
drop table md; create table md (m_val int);
insert into dt values
  (10000101), (10000103), (10000105), (10000107), (10001231),
  (15000101), (15000103), (15000105), (15000107), (15001231),
  (20000101), (20000103), (20000105), (20000107), (20001231);
insert into md values (0), (1), (2), (3), (4), (5), (6), (7);
select d_val, m_val, week(d_val, m_val) from dt, md order by d_val, m_val;
select concat('int_to_week_succ(', cast(d_val as unsigned), ', ', m_val, ', ', week(d_val, m_val), ', __LINE__);') from dt, md order by d_val, m_val;
