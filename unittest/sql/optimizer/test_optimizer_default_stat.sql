# case 1
select * from t1 where c2 > 0 and c2 < 10;
select/*+index(t1 idx_t1_c2)*/ * from t1 where c2 > 0 and c2 < 10;

# case 2
select c1 from t1;

# case 3
select c2 from t1 order by c2;

# case 4
select c1, c2 from t1;

# case 5
select c1 + c2 from t1;

# case 6
select c1 from t1 limit 100;

# case 7
select * from t1 limit 100;

# case 8
select * from t1 order by c1,c2 limit 100;

# case 9
select c1 from t1 order by c1 limit 100;

# case 10
select c1 from t1 order by c2 limit 100;

# case 11
select c1 from t1 where c1 > 0 order by c2 limit 100;

# case 12
select c1 from t1 where c1 > 0 and c2 < 0 order by c2 limit 100;

# case 13
select c1 from t1 group by c1;

# case 14
select c2, sum(c1) from t1 group by c2;

# case 15
select c2, avg(c1) from t1 group by c2;

# case 16
select c1, c1 + c2 from t1 where c1 > 0 limit 100;

# case 17
select c1 from t1 where c1 > 0;

# case 18 For partition table, stream exchange should complete
select c2 from t1 group by c2 order by c2 limit 100;

# case 19 For partition table, stream exchange should complete
select sum(c1) from t1 group by c2 order by c2 limit 100;

# case 20
select sum(c1) from t1 where c1 > 0 group by c2 order by c2 limit 100;

# case 21
select count(c1) from t1 group by c2 order by c2 limit 100;

# case 22
select count(*) from t1 group by c2 order by c2 limit 100;

# case 23
select sum(c1), count(*) from t1 group by c2 order by c2 limit 100;


# case 24
select c2, count(c1) from t1 group by c2;

# case 25 join
select t1.c1 from t1, t2 limit 100;

# case 26 join with predicate
select t1.c1 from t1, t2 where t1.c1=t2.c1 limit 100;

# case 27 3-table join
select t1.c1
from t1, t2, t3
where t1.c1=t2.c1
      and t1.c2>t2.c2
      and t2.c3=t3.c3
      and t3.c1>10;

#select * from t1, t2;
select opt.t3.c2 from opt.t1,t2,t3 where t1.c1+t2.c1=t3.c1;
select t1.c1 from t1,t2,t3 where t1.c1=t3.c1 and t1.c2=t2.c2 and t2.c3=t3.c3;
select t1.c1 from t1,t2,t3 where t1.c1=t2.c1 and t1.c1+t2.c1=t3.c1;
select t1.c1 from t1,t2,t3, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;

# case 28 //join with subquery
select t1.c1 from t1, (select * from t2) as t where t1.c1=t.c1;
select t1.c1 from t1, (select * from t2 where c2>1 order by c1 limit 10) as t where t1.c1=t.c1;

# case 29
select c1, c1+c2 from t1 where c1 > 100 order by c2 + c1;

# case 30
select * from t1,t2 where t1.c1 = t2.c1;

# case 31
select t1.c1, t2.c2, t2.c3 from t1,t2 where t1.c1 = t2.c1;

# case 32 orderby using index
select c2 from t1 order by c1;

# case 33 join using index
select t1.c1, t2.c2, t2.c3
from t1,t2
where t1.c1 = t2.c1
order by t1.c2;

# case 34
select /*+ INDEX(t1 INVALID_INDEX) */ c1 from t1;

# case 35 access a alias table
select c1 from t1 as t order by c1;

# case 36
select c1, c1+c2 from t1 where c1 > 100 limit 1, 10;

# case 37
select * from t1 where c1 in (3, 4);
select * from t1 where c1 in (3);

# case 38 outer join
select * from t2 left join t3 on t2.c1=t3.c1 and t2.c2<t3.c2 where t2.c3>t3.c3;
select t1.c1 from t1 left join t2 on t1.c1=t2.c1 where t2.c2>1;
select * from t1 left join t2 on t1.c1=t2.c1 and t1.c1>1;
select * from t1 left join t2 on t1.c1>1 where t2.c1 is null;
select * from t1 left join t2 on t1.c1>1 where t2.c1 is null and t2.c2>1;

# case 38 outer join inside inner join
select /*+no_use_px*/ t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2 left join t3 on t2.c1=t3.c1 where t1.c1=t3.c1;
select t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2,t3, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;
select /*+no_use_px*/ t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2 left join t3 on t2.c1=t3.c1, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;

# case 39 const predicates
select * from t1 where true;
# TODO shengle, core in pre calc expr, mysqltest not core, may unitest problem
#
# select * from t1 where 1=2;
# select * from t1, t2 where 1+1=2 and t1.c1=t2.c1+1;
select * from t1 left join t2 t on t1.c1=t.c1 where false;
# select * from t1 left join t2 t on 1=1 where false;

# case sys functions
select usec_to_time(c1) as modify_time_us from t1;
select c1, repeat('ob', 2) as db_name from t1 order by c2 limit 100;

#case const expr
select 'ob' from t1;
select c1, 'ob' from t1 where c1 > 100 order by c2;
select c1, 1 + 1 from t1;
select c1, 1 + 1 from t1 order by c1 limit 100;

#case set operator
select c1 from t1 union select c2 from t2;
select c1 from t1 union all select c2 from t2;
(select c1 from t1) except (select c2 from t2) order by c1;
(select c1 from t1) intersect (select c2 from t2) order by c1 limit 100;

#case multi-set
(select c1 from t1) union (select c2 from t1) union (select c2 from t2);
(select /*+no_use_px*/ c1 from t1) union (select c2 from t1) union (select c2 from t2) order by c1 limit 100;
(select c1 from t1) union (select c2 from t1) intersect (select c2 from t2) order by c1 limit 100;

#case distinct
#case distinct without rowkey
select distinct(c2) from t1;
select distinct(c1) from t4;
#case distinct with rowkey
select distinct c1, c2 from t1;
select distinct c1, c2 from t4;
#case distinct multi-tables without rowkey
select distinct(t1.c2), t2.c2 from t1, t2 where t1.c1 = t2.c1;
#case distinct multi-tables with rowkey
select distinct(t1.c1), t2.c2 from t1, t2 where t1.c1 = t2.c1;
select distinct t4.c1, t4.c2, t2.c2 from t4, t2 where t4.c1 = t2.c1;

#case distinct alias_name, expr and const
select distinct c1 as ali_name from t1;
select distinct c2 as ali_name from t1;
select distinct c1 * c2 from t1;
select distinct 1, 2 from t1;

#distinct
select distinct c2 from t4 order by c2 limit 3;
select distinct c2 from t4 order by c2;
select distinct c2 from t4 order by c3;
#As in test no considered index_back cost, primary not chosen.
select distinct c1, c2 from t4 order by c3;
select distinct c2 from t4 order by c3 limit 3;
select distinct c2 from t4 order by c3 limit 3;
select distinct c2 from t5 order by c3 limit 3;
select distinct c2 from t5 order by c3;

#case table location by hash
select * from t1 where c1 = 7;
select * from t1 where c1 = 5 or c1 = 7;
select * from t1 where c1 = 5 or c1 = 7 or c1 = 8;
select c1 as alias_name from t1 where c1 = 7;

#case table location by partition by
select * from t5;
select * from t5 where c2 =3;
select * from t5 where c2 = 5 and c3 = 7;
select * from t5 where c2 = 6 and c3 = 7;
select * from t5 where c2 = 11 and c3 = 8;
select c1 from t6;
select * from t6 where c1 = 3;
select * from t6 where c1 = 10;

# case expresions from both branches
select t1.c2 + t2.c1 from t1, t2 where t1.c1 = t2.c2 and t1.c1 and t1.c1 = 1 and t2.c1 = 2;

#case always false and c2 is null and = null
#select * from t5 where c2 = 11 and c2 =5;
#select * from t5 where c2 is null and c3 = 5;
#select * from t5 where c2 = null and c3 =5;
#select * from t6 where c1 = 5 and c1 = 6;
select t1.c1 from t1,t2 where t1.c1+1=t2.c1;


# redundant expression generated by deduce_equal_condition
select t1.c1, t2.c1 from t1, t2 where t1.c1 = 1 and t2.c1 = 1 and t1.c1 = t2.c1;

#case leading hint for from t1, t2, t3...
select /*+ leading(t2 t1 t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ leading(t2)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;

#case use_merge use_nl ordered hint
select /*+ use_merge(t2)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ use_merge(t1)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ use_merge(t2 t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ use_nl(t2 t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ use_merge(t2), use_nl(t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ ordered, use_merge(t2), use_nl(t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ use_nl(t2), use_merge(t3)*/ * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1;
select /*+ use_nl(t2), use_merge(t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
select /*+ ordered, use_nl(t2), use_merge(t3)*/ * from t1, t2, t3 where t1.c1=t2.c1 and t3.c1 = t2.c1;
##case index, full  hint
select /*+ index(t5 idx_t5_c2) */ c1, c2 from t5 where c1 = 2 or c2 =5;
select /*+ index(t5 idx_t5_c3) */ c1, c2 from t5 where c1 = 2 or c2 =5;
select /*+ index(t5 primary) */ c1, c2 from t5 where c1 = 2 or c2 =5;
select /*+ index(t5 idx_t5_c2) */ * from t5;
select /*+ full(t5) */ c1, c2 from t5 where c1 = 2 or c2 =5;
#mysql hint
#for join, for order by non-sense
select c1, c2 from t5 use index for join (idx_t5_c2) where c1 =2 or c2 = 5;
#only first index usefull.
select c1, c2 from t5 use index for join (idx_t5_c2, primary) where c1 =2 or c2 = 5;
select c1, c2 from t5 use index for order by (idx_t5_c2) where c1 =2 or c2 = 5;
select c1, c2 from t5 force index for group by (idx_t5_c2) where c1 =2 or c2 = 5;
select c1, c2 from t5 use index (primary) where c1 =2 or c2 = 5;
select c1, c2 from t1 ignore index (idx_t1_c2) where c1 =2 or c2 = 5;
select c1, c2 from t5 ignore index (idx_t5_c2) where c1 =2 or c2 = 5;
select c1, c2 from t5 ignore index (idx_t5_c2, idx_t5_c3) where c1 =2 or c2 = 5;
select c1, c2 from t5 ignore index (idx_t5_c3, primary) where c1 =2 or c2 = 5;


select max(c2) from t1 where c1 = 2;

select @@sql_mode, c1 from t1 limit 1;

# support from dual
select 1+2 from dual;
# TODO shengle, core in pre calc expr, mysqltest not core, may unitest problem
# select 1 + 2 from dual where 1 > 2;

select c1 from t1 where c1 = 0 and c2 = 2;


# test for merge join. Make sure that the sort operator use the correct sort keys
select /*+ ordered, use_merge(t3) */ * from t2, t3
where t2.c2 = t3.c1
and t2.c1 = 0
and t3.c1 = 0;
select /*+ use_merge(t3) */ * from t2, t3
where t2.c1 = t3.c2
and t2.c1 = 0
and t3.c1 = 0;

#partition selection for select_stmt
select c1 as c from t1 partition (p4, p3) as t where c1 = 3;
select c1 from t1 partition (p1) where c1 = 3;
select count(*) from t1 partition(p1);
select * from t1 partition(p1) join t2 partition(p2) on t1.c1 = t2.c1;
#explain stmt
explain select * from t1;
explain format = json select * from t1;
#
select sum(c1) from t1 where c1 = 3 group by c2;
select sum(1) as c from t1 where c1 = 3 group by c2 having c2 = 5 order by c1 limit 1;
select sum(t7.c2) from t7, t8  where t7.c1 = t8.c1 and t7.c2 = 4 group by t7.c1 order by t7.c1 limit 1;

#
select c1, sum(c1+c2) from t2_no_part where c3 > 100 group by c2, c1;
select * from t2_no_part X, t2_no_part Y where X.c1 = Y.c1;
select count(*) from t1 group by c1 having c1 = 2;

select sum(c2) from t1 group by c1 having c1 = 2;
select sum(c1) + count(c2) from t2_no_part;
select sum(c1) from t2_no_part group by c2 order by sum(c1);

#
select * from t4 where c2 = 3 and c3 = 4;
select * from t4 where c1 = 2 and c2 = 3;
select * from t4, t7 where t4.c1 + 2 + 2 = t7.c1;
# cover pull up agg function
select sum(c1 + 1) from t4 group by c1, c2;

select /*+ frozen_version(10) */ * from t1;
select /*+index(t4 idx_t4_c2_c3)*/ c2, c3 from t4 where c2 = 3 and c3 = 4 and c1 = 5;


# test filters with different selectivity

#######################################################
#  sel(c1 = 1) = 1/10 = 0.1
#  sel(c2 > 5) = (20 - 5) / 20 = 0.75
#  sel(c3 > 8) = (20 - 8) / 20 = 0.6

#  expect: c1 = 1, c3 > 8, c2 > 5
#######################################################
select * from t4 where c1 = 1 and c2 > 5 and c3 > 8;

#######################################################
#  sel(c1 = in (1)) = 1/10 = 0.1
#  sel(c2 in (1, 2, 3, 4, 5)) = 5 / 20 = 0.25

#  expect: c1 = in (1), c2 in (1, 2, 3, 4, 5)

#  Note: to be supported.
#######################################################
select * from t4 where c1 in (1) and c2 in (1, 2, 3, 4, 5);
select * from t1 where c1 in (c1, 2, 3, 4);
# TODO shengle, core in pre calc expr, mysqltest not core, may unitest problem
# select * from t1 where (1+ 1) in (2);
select * from t1 where 1 in (c1);
select * from t1 where 1 in (c1, c2);
select * from t1 where c1 = 1 or c2 = 1;
# select * from t1 where (1+ 1) in (1);
select * from t1 where c1 in (1, 2, 3, 4);

select * from t1 where exists (select * from t2 limit 0);

# eliminate sort using unique flag
select * from t7,t8 where t7.c1=t8.c1 order by t7.c1,t7.c2;
select * from t7,t8 where t7.c1=t8.c2 order by t7.c1,t7.c2;

# group by expr
select c1/c2 as v,sum(c1) from t1 group by v order by v;

# select for update
select c2 from t1 for update;
select * from t2 where exists (select * from t6 where t2.c1=t6.c1 limit 1) for update;

# 'case when' expr generation
select case when t4.c1=0 then 'a' else 'b' end  from t4 order by c3, c2;
select case when t4.c1=0 then 'a' else 'b' end  from t4, t9 where t4.c1 = t9.c1;

# make sure irrelevant index would not be added as a path
# t4 has a composite primary key of (c1, c2) and none of the secondary indexes includes c1
# EXPECT: primary index should be chosen
select * from t4 where t4.c1 = 1;

# group and order by same expr, and desc
select * from t7 group by c1 order by c1 desc;

# order by different direction
select * from t4 order by c1,c2 desc;
select * from t4 order by c1 desc,c2 desc;

# subquery is a set op
select * from (select * from t4 union select * from t4) as a;

# right join and full join
select * from t1 left join t2 on t1.c1=t2.c1 and t2.c1=2;
select * from t1 left join t2 on t1.c1=t2.c1 where t2.c1=2;
select * from t1 right join t2 on t1.c1=t2.c1 and t2.c1=2;
select * from t1 right join t2 on t1.c1=t2.c1 where t2.c1=2;
select * from t1 full join t2 on t1.c1=t2.c1 and t2.c1=2;
select * from t1 full join t2 on t1.c1=t2.c1 where t2.c1=2;

# correlated subplan
select c2 from t1 where exists (select * from t2 where t1.c1=t2.c1 limit 1);
# from subquery
select c1 from (select c1, c2 from t1 limit 1) t;

# group upon subquery
select sum(c) from (select c1 as c from t1 union select c1 as c from t1) as a;

# calculate selectivity of a range predicate of varchar
select * from t2 where 'cb' <= c3;

select t7.c1 = t8.c2 from t7, t8 where t7.c1 = t8.c1;

# join with subquery and use columns of subquery
select t11.c2 from (select c1,c2 from t4 limit 1) as t11 , t6 where t11.c1>t6.c1;
select t11.c2 from (select c1,c2 from t4) t11 left join t6 on t11.c1=1;

# distribute join
select * from t2,t4 where t2.c1=t4.c1;
select * from t2,t6 where t2.c1=t6.c1;
select * from t2,t6,t7 where t2.c1=t6.c1 and t6.c1=t7.c1;

# distribute set op
select c1 from t2 union select c1 from t6;
select c1 from t2 union all select c1 from t6;

# distribute subplan
select * from t2 where exists (select * from t6 where t2.c1=t6.c1 limit 1);
select * from t2 where exists (select * from t4 where t2.c1=t4.c1 limit 1);
select * from t2 where exists (select * from t1 where t2.c1=t1.c1 limit 1);
select sum(c1) as c from t1 union select sum(c1) as c from t1;
select sum(c) from (select sum(c1) as c from t1 union select sum(c1) as c from t1) as a;

# join predicate is a column
select * from t4 where c1;
select * from t4, t4 t where t4.c1;
select * from t4 left join t4 a on t4.c1;

# in comulns from a same table
select t1.c2  from t1,t2 where t2.c1 in(t1.c1);
select  * from t3 join t4 on t3.c1>t4.c1 where t4.c1 in (t3.c1, t3.c1);

# pull up outer join from where expr
select * from t1 where c1 in (select t2.c1 from t2 left join t3 on t2.c1=t3.c1);

# part key is not primary key, and merge join locally
select /*+use_merge(t10,t11)*/* from t10, t11 where t10.c2=t11.c2;

# 3-level subquery
select c1 from t1 where c1 not in (select c1 from t2 where c2 not in (select c2 from t2));

# order expr is not contained in distinct
select distinct c1 from t1 order by c2;
SELECT c1, c2  FROM t1 WHERE c2 IN (ROUND(-1), 0, '5', '1');

select count(*) from (select count(*) as a  from t4) t ;
(select * from t1 where c1 = 2) union all (select * from t1 where c1 = 2);
select * from t1 X, t1 Y where X.c1 = Y.c1 and X.c1 = 1 and Y.c1 = 1;
select distinct c2, c1 from t1;

# subqueryin select/group/orderby clause
select (select c1 from t7),c2 from t8;
select (select c1 from t7 where c2=t8.c2), c2 from t8;
select c1 from t7 group by (select c1 from t8), c2;
select c1 from t7 group by (select c1 from t8 where c2=t7.c2),c2;
select c1, sum(c2) from t4 group by c1 having sum(c2) < (select 1);
select c1 from t7 order by (select c1 from t8), c2;
select c1 from t7 order by (select c1 from t8 where c2=t7.c2),c2;
select (select 1, 2, 3)=row(1, 2, 3);

# count(1), not count(*)
select count(1) from t1;
select (select 1);
(select (select 1)) union (select (select 1));

# distinct on top of dual
SELECT distinct '' , 1 FROM DUAL limit 2 offset 1;

# no equal condition and partitioin compatible
select * from t1,t1 t;
select * from t1,t1 t where t1.c1<t.c1;

# subquery in order by clause of set op
(select * from t4) union (select * from t4) order by (select c1 from t4 limit 1);

# subquery is filter of expr values operator
select 1 from dual where 1 in (select 1 from dual);

# test agg push down
select count(c1) + 1 from t1;
select count(c1) + 1 + 1 from t1;

# group by alias select sub query
SELECT (select max(t1.c1) from t1) as field from t1 group by field;

#BUG 6389736
( SELECT * FROM t12 WHERE 69 > ROUND ( 3075 ) ) UNION ALL ( SELECT * FROM t12  ) UNION ( SELECT * FROM t13) ;

#BUG 6312965 6358209 complex expr should not pass OJ
select t1.c1, nvl(t2.c2,0) from (select c1,c2 from t1 where c1=0 or c1=1) as t1 left join (select c1,c2 from t1 where c1=0) as t2 on t1.c1=t2.c1;

#BUG 6386936
(select * from t12 where a != 1 limit 7 ) union (select * from t13) union ( select * from t13);

#BUG 6403026
select /*+ leading(t3, t2, t1) use_merge(t3,t2) */* from t1,t2,t3;

#BUG 6417487
select /*+leading(t10, t11) use_nl(t11) */ * from t10 join t11 on t10.c2 = t11.c2;

select * from t4 where c1 not in (select c1 from t7 where t4.c1+t7.c1 >(select c1 from t8));
select c1 from t4 where c1 not in (select c1 from t7 where t7.c1>(select c1 from t8));
select c1 from t4 where c1 not in (select c1 from t7 where t4.c1>(select c1 from t8));
select c1 from t4 where c1 not in (select c1 from t7 where 1>(select c1 from t8));
select c1 from t7 where c1 <>ALL (select c1 from t8 where 1>(select 2));
select c1 from t4 where c1 in (select c1 from t7 where t4.c1>(select c1 from t8));
select * from t7 where c1 not in (select 1 from t8 where t8.c1 >1);
select * from t7 where c1 not in (select t7.c1 from t8 where t8.c1 >1);
select * from t7 where c1 not in (select (select c1 from t1) from t8 where t8.c1 >1);
select * from t7 where c1 in (select t8.c1 from t8 where t8.c1 >1);
select * from t7 where c1 in (select 1 from t8 where t8.c1 >t7.c1);
select * from t7 where c1 in (select 1 from t8 where t8.c1 >1);

#Bug 6460707 in expr selectivity
select * from t1 where c1 = 1 and c2 in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33);

#TODO: T_OP_ROW has selectivity bug
#Bug 6468955
select * from t1 partition(p1)  where c2 =1 or c2 = 2;
select * from t14 partition(p1) where (c1, c2) =(1, 2) or (c1, c2) = (2, 3);
select/*+index(t14 primary)*/* from t14  partition(p1) where (c1, c2) =(1, 2) or (c1, c2) = (2, 3);

#Bug 6447504
select c1 from t1 where (1, 2) in (select t1.c1, t2.c1 from t2);
select c1 from t1 where (1, 2) in (select t2.c1, t2.c2 from t2 where t1.c1);
select c1 from t1 where (1, 2) in (select distinct t1.c1, t1.c2 from t2 where t1.c1);
select c2 from t1 where exists (select * from t2 where t1.c1 and t1.c2 limit 1);

#Bug 6502916/6439385
select * from t4 where c1 in (select t7.c1 from t7 left join t8 on t7.c1<=t8.c1 where (t7.c1,t4.c1) in (select c1,c2 from t9));
select * from t7 where 1 in (select c1 from t8 where t7.c1);
select * from t7 where 1 not in (select 2 from t8 where t7.c1=t8.c1);
select * from t4 where (select c1 from t7)+1 in (select 2 from t8 where t4.c1=t8.c1);

#Bug 6510754 group by下压
select sum(c1)+sum(c2), sum(c1) from t1 group by c2 having sum(c1) > 5;
select sum(c1), c2, count(c1) from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1;

#Bug 6512309 group by下压引起的表达式调整
select distinct sum(c1) from t1 group by c2 having sum(c1) > 5;
select distinct sum(c1), c2, count(c1) from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1;
select distinct sum(c1), c2, count(c1), avg(c1) from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1;

# TODO shengle, core in pre calc expr, mysqltest not core, may unitest problem, todo later
# select distinct sum(c1)+1, sum(c1) + 2 from t1 group by c2 having sum(c1) > 5 and count(c1) + 2 > 2;
#Bug 6410490
select * from t1, t2 where t1.c1 > any(select t3.c2 from t2,t3 where t2.c1 > any(select t4.c1 from t4,t5 where exists (select c1 from t2 )));

#Bug 6514027
select * from t4,t4 t5 where t4.c1+t5.c1 in (select t7.c1 from t7,t8);
select * from t4,t4 t5 where t4.c1 in (select t7.c1 from t7,t8);
select * from t4,t4 t5 where t4.c1+t5.c1 in (select t7.c1 from t7,t8 where t7.c1=t8.c1);

##bug 6567691 带别名的远程计划group by下压
select count(*) v from t1 where c2 = 1;
select count(*) v, sum(c1) as v2 from t1 where c1 > 0;
select distinct sum(c1), c2, count(c1), avg(c1) as alias from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1 and alias > 100;

## Test table with no primary key
select 1 from t15;
select c1 from t15 where false;
select count(1) from t15 where false;

## Order by with keys.count() < column count in order by list
select * from t1 order by c2,c1,c2;

## construct different ording directions on two sides of join
select * from t1 left outer join t2 using(c1) order by t1.c1 desc, t2.c1;

## aggregate expr with join
select count(*) from t1 left outer join t2 using(c1) group by (t1.c1) having count(*) > 1 order by t1.c1 desc, t2.c1;

## EXPR_SYS coverage
select now() from t1 left outer join t2 using(c1);

##bug:
select a1.c2 from t1 left join t2 a1 on (a1.c1= t1.c1) where least(t1.c2, a1.c2) > 1;
select a1.c2 from t1 left join t2 a1 on (a1.c1= t1.c1) where length(t1.c2) > 1;
select a2.c2, t1.c2, a1.c2 from t1  left join t2 a1 on (a1.c1 = t1.c1), t2 a2 where least(t1.c2, a1.c2) =a2.c2;
select f_acc.c2, a1.c2, a2.c2 from t2 left join t2 f1 on (f1.c1 = 1 and t2.c3 = f1.c3) left join t3 a1 on (a1.c1 = f1.c1) left join t2 f2 on (f2.c1 = 3 and f2.c3=t2.c3) left join t3 a2 on (a2.c1 = f2.c1), t3 f_acc where least(a1.c2, a2.c2) = f_acc.c2;
select f_acc.c2, a1.c2, a2.c2 from t2 left join t2 f1 on (f1.c1 =1 and f1.c3 = t2.c3) left join t3 a1 on (a1.c1 = f1.c1) left join t3 a2 on (a2.c1 = f1.c1) , t3 f_acc where least(a1.c2, a2.c2) = f_acc.c2;

# merge join with composite directions
select * from t9,t10,t11 where t9.c2=t10.c2 and t9.c3=t10.c3 and t9.c2=t11.c2 and t9.c3=t11.c3 order by t11.c3,t11.c2;
select t9.c2, t9.c3, t9.c2, t10.c3 from t9, t10 where t9.c2 = t10.c2 and t9.c3 = t10.c3 order by t9.c2, t9.c3 desc;

# bug 6526652
select c1, (select count(c1) from t7 where c1=t4.c1) calc_total, (select count(c1) from t8 where c1=0 and c1=t4.c1) calc_new from t4 where c1 in (select distinct c1 from t1);
select c1, (select count(c1) from t7 ) calc_total, (select count(c1) from t8 ) calc_new from t4  where c1 in (select distinct c1 from t1);
select distinct (select c1) from t4 limit 100;

#bug
select * from t4 where c1 > 0 and c1 < 100 order by c2 limit 1;

#like and between selectivity
select * from tt1 where c2 like "a%";
select * from tt1 where c2 like "ab%";
select * from tt1 where c2 between "aa" and "ab";
select * from tt1 where c2 between "aa" and "ai";
select * from tt1 where c2 not between "aa" and "ab";
select * from tt1 where c2 not between "aa" and "ai";
select * from tt1 where "ag" between c2 and "ai";
select * from tt1 where "ag" between "aa" and "c2";
select * from tt1 where 'ag' not between c1 and 'ai';
SELECT id, k, c, pad FROM sbtest_sysbench WHERE k IN(1,2,3,4,5,6,7,8,9,10);

# test limit push down re-estimate-cost
select a1 from test_limit limit 1000;
select a1,a2,a3 from test_limit limit 1000;
select * from test_limit limit 1000;

select a1 from test_limit limit 500;
select a1,a2,a3 from test_limit limit 500;
select * from test_limit limit 500;
select * from cb_dep_acctbal_54 s,  cb_dep_acct_54 a where s.acctnbr = a.acctnbr and (a.curracctstatcd != 'CLS' or (a.curracctstatcd = 'CLS' and exists(select 1 from cb_dep_rxtnbal_54 r where r.acctnbr = a.acctnbr)));
