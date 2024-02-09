use opt;
# case 1
insert into t1 values(0, 2);
insert into t1 values(1, 2);
insert into t1 values(2, 2);
insert into t1 partition (p3, p0) values(3, 2);
insert into opt.t1 values(4, 2);
insert into t1 values (1,2) on duplicate key update c2 = c2+1;
insert into t2 values (3,4,'test') on duplicate key update t2.c2 = t2.c2 + 1;

# case 2
insert into t1(c1, c2) values(1, 2);

# case 3
insert into t1(c1) values(1);

# case 4 (expect to fail)
#insert into t1(c2) values(2);

# case 5
update t1 set c2 = 3 where c1 = 0;
update t1 set c2 = 3 where c1 = 1;
update t1 set c2 = 3 where c1 = 2;
update t1 set c2 = 3 where c1 = 3;
update t1 as t set c2 = 3 where c1 = 4;
update opt.t1 t set c2 = 3 where c1 = 5;
update t10 set c3 = 5 where c2 = 3;

# case 7
#update t1 set c2 = c1 + 3;

# case 8
#update t1 set c2 = c1 + 3 where c2 > 5;

# case 9
update t1 set c2 = 3 where c1 = 1 order by c1 limit 100;

# case 10
#update t1 set c2 = c2 + 3 where c2 > 5 order by c2 limit 100;
#update t1 set c2 = c2 + 3, c2 = c1 where c2 > 5 order by c2 limit 100;

# case 11
#delete from t1;
delete from t2 where c1 = 5 order by c3 limit 1;

# case 12
delete from t1 where c1 = 5;

# case 13 distributed insert sql not supported
#insert into t1 values (1, 2), (4, 5);

# case 14
insert into t1 values(1,null);

# case 15
insert into t3(c1) values(1);

# case 16
#update t1 set c1 = 1 where c1 = 2;
update t1 set c1 = 1 where c1 = 1;
update t1 set c1 = 1 + 1 where c1 = 2;
#part key expr now depend obj_type
#update t5 set c2 = 2, c3 = 1 where c2 = 2 and c3 =1;

#case 17 partition by key
#update t5 set c1 = 1 where c2 = 3 and c3 = 4;
#UPDATE __all_root_table SET sql_port = 51997, zone = 'test', partition_cnt = 1, role = 0, row_count = 0, data_size = 0, data_version = 1, data_checksum = 0, row_checksum = 0, column_checksum = '' WHERE tenant_id = 1 AND table_id = 2 AND partition_id = 0 AND ip = '127.0.0.1' AND port = 51951;

#test for partition selection in dml_stmt
insert into t1 partition (p3, p0) values(3, 2);
replace into t1 partition (p3, p0) values(0, 3);
delete from t1 partition (p0) where c1 = 5;
update t1 partition(p3) set c2 = 2 where c1 = 3;

insert into t9(c1, c2) values(0, 2);
update t2_no_part set c2 = (c3 = 1 and 1 = 1) where c1 = 1;

# dml using subquery
delete from t7 where c1 in (select c1 from t8 where t7.c2=c2);
update t7 set c1=1 where c1 in (select c1 from t8 where t7.c2=c2);
update t7 set c1=(select c1 from t8);
update t7 set c1=(select c1 from t8 where t7.c2=c2);

##bug:
delete from t7 where abs(c1) > 0 order by c1 limit 1;
update t7 set c1 = 1 where abs(c2) > 0 order by c1 limit 1;

use insert_db;
#test insert into...select...
insert into t1 select * from t2;
insert into t2 select * from t1;
insert into t1 select * from t1;
insert into t2 select * from t2;
insert into t2 select c1, c2 from t3;
insert into t3 select c1, c2, c1 from t2;
#insert into t1 select * from t4;
#insert into t1 select t3.c1, t3.c2 from t3 join t4 using(c1);
#insert into t1 (select c1, c2 from t3 union select c1, c2 from t4);
use opt;

## select list elimination ( -> 1)
## group by, order by elimination
#update
update t7 set c1 = 1 where exists(select c2+2 from t8);
update t7 set c1 = 1 where not exists(select c2+2 from t8);
update t7 set c1 = 1 where exists(select c2+2 from t8 group by c2 order by c2);
update t7 set c1 = 1 where exists(select c2+2 from t8 group by c2 order by c2 limit 2);

#delete
delete from t7 where exists(select c2+2 from t8);
delete from t7 where not exists(select c2+2 from t8);
delete from t7 where exists(select c2+2 from t8 group by c2 order by c2);
delete from t7 where exists(select c2+2 from t8 group by c2 order by c2 limit 2);

# UPDATE + sub-queries
UPDATE t1 SET c2 = CONCAT(c2, (SELECT c2 FROM t2 where t2.c1=1)) WHERE c1 = 1;
UPDATE t7 SET c2 = CONCAT(c2, (SELECT c2 FROM t2 where t2.c1=1));
UPDATE t1 SET c2 = CONCAT(c2, (SELECT c2 FROM t7 limit 1)) WHERE c1 = 0;
UPDATE t7 SET c2 = CONCAT(c2, (SELECT c2 FROM t8 limit 1));
UPDATE t1 SET c2 = CONCAT(c2, (SELECT c2 FROM t2 where t2.c1=1)) WHERE c1 = 0;

# DELETE + sub-queries
delete from t7 where c1 in (select c1 from t1);

#part p1
insert into t_u values(18446744073709551612);
#part p0
insert into t_u values(18446744073709551613);
#part p0
insert into t_u values(0);
#part p1
insert into t_u values(1);
#part p2
insert into t_u values(2);
#part p1
insert into t_u values(9223372036854775807);
#part p1 这里和MySQL不同,因为该值对应的int64_t为-9223372036854775808(INT64_MIN),在OB内部会先转成INT64_MAX;
#MySQL是先做%part取余，然后负值取反.OB先负值取反,再取余,INT64_MIN就需要先转成INT64_MAX
insert into t_u values(9223372036854775808);
#part p1
insert into t_u values(9223372036854775809);
#part p0
insert into t_u values(9223372036854775810);

# LIMIT on TABLE SCAN push down
select c2 from t7 where c2 >=1 and c2 <= 10 limit 3,4;
select c2 from t7 where c2 = 2 limit 2,4;
select * from t8 where c2 in (select c2 from t7 where c2 >=1 and c2 <= 10 limit 3,4);
select * from t7 as t limit 789,456;
select * from t7 as t limit 789,345;
update t7 set c1 = 2 limit 3,4;
delete from t7 limit 5,6;
insert into t7 select (select c1 from t7)+1, c2 from t7;
#range partition not considered range is local case now.
insert into t_s values(1, '2013-10-10');
insert into t_s values(1, '2013-10-9'), (3, '2013-2-1');
insert into t_s1 values(1, '2016-1-2');
insert into tp values(2, 1);
insert into ts2 values(1, '2013-10-13', c1+1, c2);
insert into ts2 values(1, '2013-10-13', 1, c2);
insert into ts2 values(1, '2013-10-9', 2, c2);
insert into ts2 values(1, '2013-10-10', 2, c2);
insert into ts2 values(1, '2013-10-9', 2, c2), (3, '2013-2-1', 2, c2);
update ts set c1 = 1 where c1 = 3 and c2 = '2013-10-10';
update ts set c2 = '2016-3-29' where c1 = 3 and c2 = '2013-10-10';
insert into t15 (c2, c3) values(2,'a');
insert into test_insert set c1 = 4, c4 = c1 + 3;
insert into tg(c1, c3) values ('bcde', 10);
insert into tg(c1, c3) values ('caaaa', 10);
