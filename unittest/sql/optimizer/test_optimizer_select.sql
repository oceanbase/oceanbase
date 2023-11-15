# case 1
select * from t1;

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

# case 18
select c2 from t1 group by c2 order by c2 limit 100;

# case 19
select sum(c1) from t1 group by c2 order by c2 limit 100;

# case 20
select sum(c1) from t1 where c1 > 0 group by c2 order by c2 limit 100;

# case 21
select count(c1) from t1 group by c2 order by c2 limit 100;

select count(distinct c1) from t1 group by c2 order by c2 limit 100;

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
select c1,c2 from t19 where c1 = 1 order by c1, c2 desc;
select c1,c2 from t19 where c1 = 1 and c2 = 1 order by c2, c3 desc;

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
# TODO shengle bug: issue/37559700
# select * from t1 where 1=2;
# select * from t1, t2 where 1+1=2 and t1.c1=t2.c1+1;
select * from t1 left join t2 t on t1.c1=t.c1 where false;
# select * from t1 left join t2 t on 1=1 where false;

# case time range
## NULL
select * from t_time where c1 is NULL;
select * from t_time where c1 is NULL and c1 < '2017-01-02';
select * from t_time where c1 is NULL or c1 < '2017-01-02';
select * from t_time where c1 is NULL and c1 > '2017-01-02';
select * from t_time where c1 is NULL or c1 > '2017-01-02';
select * from t_time where c1 <=> '2017-01-02';
select * from t_time where not c1 <=> '2017-01-02';

select * from t_time where c1 > '2017-01-02';
select * from t_time where c1 < '2017-01-02';
select * from t_time where c1 = '2017-01-02';

select * from t_time where c1 > '2017-01-02' and c1 < '2017-01-03';
select * from t_time where c1 > '2017-01-02' and c1 < '2017-01-03' and c1 < '2017-01-02';
select * from t_time where c1 > '2017-01-01' and c1 < '2017-01-03' and c1 = '2017-01-02';
select * from t_time where c1 > '2017-01-02' and c1 < '2017-01-03' and c1 = '2017-01-02';

select * from t_time where c1 >= '2017-01-02' and c1 <= '2017-01-03';
select * from t_time where c1 >= '2017-01-02' and c1 <= '2017-01-03' and c1 <= '2017-01-02';
select * from t_time where c1 >= '2017-01-01' and c1 <= '2017-01-03' and c1 = '2017-01-02';
select * from t_time where c1 >= '2017-01-02' and c1 <= '2017-01-03' and c1 = '2017-01-02';

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
(select c1 from t1) union (select c1 from t2) order by c1;
(select c1 from t1) union all (select c1 from t2) order by c1;
(select c2 from t1 order by c1) union (select c1 from t1 order by c2);
(select c2 from t1 order by c1) union all (select c1 from t1 order by c2);
(select c1 from t1 order by c2) union (select c1 from t2 order by c2 limit 2);
(select c1 from t1 order by c2) union all (select c1 from t2 order by c2 limit 2);
(select c1 from t1 order by c2) union (select c1 from t2 order by c2 limit 2) union (select c1 from t1 order by c1);
(select c1 from t1 order by c2) union all (select c1 from t2 order by c2 limit 2) union all (select c1 from t1 order by c1);
(select c1 from t1 order by c2 limit 2) union (select c1 from t2 order by c2 limit 2);
(select c1 from t1 order by c2 limit 2) union all (select c1 from t2 order by c2 limit 2);

#case multi-set
(select c1 from t1) union (select c2 from t1) union (select c2 from t2);
(select /*+no_use_px*/ c1 from t1) union (select c2 from t1) union (select c2 from t2) order by c1 limit 100;
(select c1 from t1) union (select c2 from t1) intersect (select c2 from t2) order by c1 limit 100;

#case distinct
#case distinct without rowkey
select distinct(c2) from t1;
select/*+(use_order_distinct)*/ distinct(c1) from t4;
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
select 1 + 2 from dual where 1 > 2;

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
select * from t1 where (1+ 1) in (2);
select * from t1 where 1 in (c1);
select * from t1 where 1 in (c1, c2);
select * from t1 where c1 = 1 or c2 = 1;
select * from t1 where (1+ 1) in (1);
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

#对于distribute，当分支有序的时候，limit下压
select * from t1 order by c1 limit 2;
select * from t1 order by c2 limit 2;
select * from t2 order by c1 limit 2;
select * from t2 order by c2 limit 2;

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
select/*+index(t4 primary)*/ * from t4 where c1;
select * from t4, t4 t where t4.c1;
select/*+index(t4 primary)*/ * from t4, t4 t where t4.c1;
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
select c1 from t4 where c1 not in (select c1 from t7 where t7.c1>(select c1 from t8 where t4.c1=1));
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
select c1 from t1 where (1, 2) in (select t2.c1, t2.c2 from t2 where (t2.c1 + 1));
select c1 from t1 where (1, 2) in (select t2.c1, t2.c2 from t2 where (t1.c1 + 1));
select c1 from t1 where (1, 2) in (select t2.c1, t2.c2 from t2 where (t1.c1 + 1) = 2);
select c1 from t1 where (1, 2) in (select distinct t1.c1, t1.c2 from t2 where t1.c1);
select c2 from t1 where exists (select * from t2 where t1.c1 and t1.c2 limit 1);

#Bug 6502916/6439385
select * from t4 where c1 in (select t7.c1 from t7 left join t8 on t7.c1<=t8.c1 where (t7.c1,t4.c1) in (select c1,c2 from t9));
select * from t7 where 1 in (select c1 from t8 where t7.c1);
select * from t7 where 1 not in (select 2 from t8 where t7.c1=t8.c1);
select * from t4 where (select c1 from t7)+1 in (select 2 from t8 where t4.c1=t8.c1);
select * from t4 where (select 2)+(select c1 from t7)+1 in (select 2 from t8 where t4.c1=t8.c1);

#Bug 6510754 group by下压
select sum(c1)+sum(c2), sum(c1) from t1 group by c2 having sum(c1) > 5;
select sum(c1), c2, count(c1) from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1;

#Bug 6512309 group by下压引起的表达式调整
select distinct sum(c1) from t1 group by c2 having sum(c1) > 5;
select distinct sum(c1), c2, count(c1) from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1;
select distinct sum(c1), c2, count(c1), avg(c1) from t1 group by c2 having sum(c1) > 5 and count(c1) > 0 and c2 > 1;
select distinct sum(c1)+1, sum(c1) + 2 from t1 group by c2 having sum(c1) > 5 and count(c1) + 2 > 2;
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

# startup filter of having and correlated expr
select * from t7,t8 where t7.c1=t8.c1 having false;
select c1,sum(c2) from t7 where c1>1 group by c1 having c1+1>1 and sum(c2) >1 and 1=2;
select * from t7 where c1 in (select c1 from t8 where t7.c1>1 limit 1);

#test  intelligent connection of inner join
select * from t1,t14 where t1.c1 = t14.c3;
select * from t5,t16 where t5.c2 = t16.c2;
select * from t5,t16 where t5.c2 = t16.c2 and t5.c3 = t16.c3;
select * from t5,t16 where t5.c2 = t16.c2 and t5.c3 = t16.c3 and t5.c1 = t16.c1;
select * from t17,t18 where t17.c2 + t17.c3 = t18.c2 + t18.c3;
select * from z1,z2 where z1.a = z2.a;
select * from t2,t6 where t2.c1 = t6.c1;
select * from t1,t2 where t1.c1 = t2.c1;
select * from t5,t6 where t5.c2 = t6.c1;
select * from t1,t14 where t1.c1 = t14.c3 and t1.c2 = t14.c2;
select * from t1,t14 where t1.c1 + 1 = t14.c3 + 1;

#test intelligent connection of outer join
select * from t1 left join t14 on t1.c1 = t14.c3;
select * from t5 left join t16 on t5.c2 = t16.c2;
select * from t5 left join t16 on t5.c2 = t16.c2 and t5.c3 = t16.c3;
select * from t5 left join t16 on t5.c2 = t16.c2 and t5.c3 = t16.c3 and t5.c1 = t16.c1;
select * from t17 left join t18 on t17.c2 + t17.c3 = t18.c2 + t18.c3;
select * from z1 left join z2 on z1.a = z2.a;
select * from t2 left join t6 on t2.c1 = t6.c1;
select * from t1 left join t2 on t1.c1 = t2.c1;
select * from t5 left join t6 on t5.c2 = t6.c1;
select * from t1 left join t14 on t1.c1 = t14.c3 and t1.c2 = t14.c2;
select * from t1 left join t14 on t1.c1 + 1 = t14.c3 + 1;

#bug 6715786
select SQL_CALC_FOUND_ROWS c2,count(*) as c from t1 group by c2 order by c desc ;
select count(1)+1 from t1 order by count(1)+1;

#bug 6716586
delete from t7 where exists(select 1 from t8);

select t3.c1 from t3, t1 where  t3.c2 = t1.c1 and t3.c1 = 1 and exists(select 'X' from t2 where t2.c2 = t1.c2);

#bug6726125
select c1, c2 from t3 where c1 in (select a.c1 from t1 a, t2 b where b.c1=100 and a.c2=b.c2);

#bug6716037
select t7.c1, t8.c1 from t7 inner join t8 on t7.c1 > t8.c2 where t7.c1 = t8.c2 or exists ( select 1);

#bug6790460
(select substr('a',1,1) ) union (select 1);
## Test predicate pusher
# regular two table join
select * from t4, t7 where t4.c1 = t7.c1 and t4.c1 = 5;
select * from t4, t7 where t4.c2 = t7.c2 and t4.c2 = 5;
# three table join
select * from t4, t7, t8 where t4.c1 = t7.c1 and t7.c1 = t8.c1 and t4.c1 = 5;
select * from t4, t7, t8 where t4.c1 = t7.c1 and t7.c2 = t8.c2 and t4.c1 = 5;
select * from t4, t7, t8 where t4.c1 = t7.c1 and t7.c2 = t8.c2 and t4.c1 = 5 and t7.c2 = 9;
# inner join
select * from t4 join t7 on t4.c1 = t7.c1 where t4.c1 = 1;
select * from t4 join t7 on t4.c1 = t7.c1 where t4.c2 = 1;
select * from t4 join t7 on t4.c1 = t7.c1 where t7.c1 = 1;
select * from t4 join t7 on t4.c1 = t7.c1 where t7.c2 = 1;

# left join
select * from t4 left join t7 on t4.c1 = t7.c1 where t4.c1 = 5;
select * from t4 left join t7 on t4.c1 = t7.c1 where t4.c2 = 5;
select * from t4 left join t7 on t4.c1 = t7.c1 where t7.c1 = 5;

## test aggregation transform
select /*+ FROZEN_VERSION(1) */max(c1) as c from t7;

## subplan ordering
select * from t4 where c1 in (select t7.c1 from t7,t8 where t4.c1=t7.c1+t8.c1 limit 1);
select * from t4 where c1 in (select t7.c1 from t7,t8 where t4.c1+t7.c1=t8.c1 limit 1);
select * from t4 where c1 in (select t7.c1 from t7,t8 where t4.c1+t7.c1=t4.c1+t8.c1 limit 1);


select * from (select * from t1 where c1 > 0 group by c2) as v order by v.c2;
select * from (select * from t1 where c1 > 0 limit 1) as v order by v.c2;
select * from (select t2.c1 from t1,t2 where t1.c1=t2.c1 limit 1) v order by v.c1;
select * from (select t1.c1 from t1,t2 where t1.c1=t2.c1 limit 1) v order by v.c1;

##having filter pushdown
##not contain aggregate function and not contain subquery
#没有分组列，那么having条件下压后上层必须任然保留having条件，不能改变分组信息
select c1 from t1 having false;
select min(c1) from t1 having false;
select count(1) from t1 having false;
#包含分组列，having条件下压上层可以不保留，但是为了安全，任然保留
select c1 from t1 group by c1 having false;

select c1 from t1 group by c1 having c1>0;

##having filter could not pushdown
#contain aggregate function
select c1 from t1 group by c1 having count(c2)>0;
select count(c1) from t1 group by c1 having count(c2)>0;
#contain subquery
select c1 from t1 group by c1 having c1 in (select c1 from t1);
select count(c1) from t1 group by c1 having c1 in (select c1 from t1);
#one-time filter
select c1 from t1 group by c1 having 1 in (select c1 from t1);
select count(c1) from t1 group by c1 having 1 in (select c1 from t1);
#related subquery
select c1 from t1 group by c1 having c1 in (select t2.c1 from t2 where t1.c1=t2.c2);
select count(c1) from t1 group by c1 having c1 in (select t2.c1 from t2 where t1.c1=t2.c2);

#bug6794538
select c1 from t1 where c1 in (select c1 from t2 where c2 >= some(select c1 from t3 where t1.c2=t3.c1));
select * from (select c1 from t4 group by "adg") as a;

#bug7532610
select c1 from t1 having count(c2)>0;
select c1 from t1 having 3<(select 2);
select c1 from t1 having c1>(select 1);
select c1 from t1 group by -1 having 3<(select 2);
##bug 7700751
select * from t4 where c1 =1 order by c2;
select * from t4 where c1 =1 group by c2;
select * from t19 where c1 = 1 order by c2, c3, c4;
select * from t19 where c1 = 1 group by c2, c3, c4;
select * from t19 where c1 = 1 and c3 = 1 order by c2, c4;
select * from t19 where c1 = 1 and c3 = 1 group by c2, c4;
select c1 from t7 where c2 =1 order by c2, c1 desc;
select c1 from t7 where c2 in (1) order by c2, c1 desc;

##group by asc and desc
select c1 from t7 group by (select c1 from t8), c2;
select c1 from t7 group by (select c1 from t8) desc, c2 desc;
select c1 from t7 group by (select c1 from t8) asc, c2 asc;
select c1 from t7 group by (select c1 from t8) asc, c2 asc order by c1 desc;
select c1 from t7 group by c2 desc order by c2 asc;
select c1 from t7 group by (select c1 from t8) desc, c2 desc order by (select c1 from t8) asc, c2 asc;
select c1 from t7 group by (select c1 from t8) asc , c2 asc  order by (select c1 from t8) desc, c2 asc;
select c1 from t7 group by (select c1 from t8)     , c2      order by (select c1 from t8) desc, c2 desc;
select c1 from t7 group by (select c1 from t8) desc, c2 desc;
select c1 from t7 group by c2 desc order by c2 asc, c1 desc;
select c1 from t7 group by c2 order by c2 asc, c1 desc;
select c1 from t7 order by c2 asc, c1 desc;
select c1 from t7 group by (select c1 from t8) order by (select c1 from t8) asc;
select c1 from t7 group by (select c1 from t8) asc;
select c1 from t7 group by c2 order by c2, c1;

###bug 7380150  distribute plan group by push down
select avg(c2), count(c2) from t2;
select count(c2) from t7 group by c2;
select sum(c2) from t7 group by c2;

## partition key is distinct column
select avg(distinct c1), count(c2) from t2;
select avg(distinct c1), count(c2) from t2 group by c1;
select count(distinct c1) from t2 group by c2;
select count(distinct c1, c2) from t2 group by c2;
select sum(distinct c1) from t7 group by c2;

##do push down modification group
select count(distinct c2) from t2;
select sum(distinct c2) from t17;
select avg(distinct c1) from t1 group by c2;
select count(distinct c2) from t2 group by c2;
select count(distinct c1) from t2 group by c2;
select sum(distinct c2) from t15 group by c3;
select sum(distinct c1) from t15 group by c3;
select avg(distinct c2) from t15 group by c3;
select avg(distinct c1) from t15 group by c3;
select count(distinct c1, c2) from t15 group by c3;
select count(distinct c1, c2) from t17 group by c2;
select sum(distinct c1) from t17 group by c2;

#fix index back bug
select/*+index(t5 idx_t5_c3)*/ t3.c2 from t5, t3;

SELECT * FROM (SELECT c1, c2 FROM t1 GROUP BY c1) v1 JOIN (SELECT c1, c2 FROM t2 WHERE c2 IN (2,3)) v2 ON v1.c1=v2.c1;

SELECT  GRANDPARENT1 . col_varchar_10 AS G1 FROM CC AS GRANDPARENT1 LEFT JOIN CC AS GRANDPARENT2 USING ( col_varchar_10 ) WHERE GRANDPARENT1 . `col_varchar_10` IN ( SELECT  PARENT1 . `col_varchar_20` AS P1 FROM CC AS PARENT1 WHERE ( ( PARENT1 . `col_varchar_10` > PARENT1 . `col_varchar_20` ) OR NOT ( GRANDPARENT1 . `col_int_key` = 4 OR NOT GRANDPARENT1 . `col_int_key` > 2 ) ) ORDER BY PARENT1 . col_varchar_10 ) AND ( GRANDPARENT1 . col_varchar_10 IS NULL OR GRANDPARENT1 . `col_int_key` >= 3 );

SELECT * FROM sbtest WHERE c1=1;
##bug:part expr item of alias table is not pulled up
select * from (select t1.c1 from t1 left join t2 as b on b.c1 = effective_tenant_id() where t1.c1 = effective_tenant_id()) as yyy;
select * from (select t1.c1 from t1 left join t2 as b on b.c1 = 5 where t1.c1 = 5) as yyy;
##
select * from t1, t2 order by t1.c1 limit 10;
select * from t1, t7 order by t1.c1 limit 10;
##should wise-join
select * from (select * from t1 group by c1) as a join (select * from t1 group by c1) as b on a.c1 = b.c1;
select * from (select c1 as a1 from t1 group by c1) as a join (select c1 as b1 from t1 group by c1) as b on a.a1 = b.b1;
select a2, b1 from (select c1 as a1, c1 as a2 from t1 group by c1) as a join (select c1 as b1 from t1 group by c1) as b on a.a2 = b.b1;
select a2, b1 from (select c1 as a1, c2 as a2 from t1 group by c1) as a join (select c1 as b1 from t1 group by c1) as b on a.a2 = b.b1;
select * from (select c2 from t1 group by c2) as a join (select c1 as b1 from t1 group by c1) as b on a.c2 = b.b1;

SELECT * FROM sbtest1 WHERE (c1, c2)=(1, NULL);
#fix bug:7894720
select * from t1 where c1 = 65535;

#fix bug : 7897247
select t6.c1, t6.c2, t6_1.c1, t6_1.c2 from t6 left join t6_1 using (c1) order by t6.c2 desc;
select t6.c1, t6.c2, t6_1.c1, t6_1.c2 from t6 left join t6_1 using (c1);

select * from t1 where (0 = 1) or c1 = 1;
select * from t1 where (1 = 1) or c1 = 1;
select * from t1 where 0 or c1 = 1;
select * from t1 where 1 or c1 = 1;
select * from t1 join t2 on (1 =1 or t1.c1 = t2.c1);
select * from t1 left join t2 on (1 = 0 or t1.c1 = t2.c1) left join t3 on (1=2 and t2.c1 = t3.c1);
select * from t1 left join t2 on (1 = 0 or t1.c1 = t2.c1) left join t3 on (1=2 or t2.c1 = t3.c1);
select c1, c2 from t1 group by c1 having (1 = 0 or c1 > 5);
select * from t1 where c1 in (select c1 from t2 where (1 = 0 or c1 > 5));

# test case ObRawExpr::EXPR_SYS in ob_join_order.cpp
select * from t7,t8 where  concat(t7.c2, t8.c2) > 1;

# Test extraction of onetime_exprs in select items
select exists(select 1) + 1;
select exists(select 1) + c1 from t7;
select ( select max(c1) from t7 ) = (select min(c1) from t8) from t9;
select 1 in (select c1 from t7) from t8;
select c1 in (select c1 from t7) from t8;
select abs((select sum(c1) from t7)) > round((select sum(c1) from t7)) from t8;
select abs((select sum(c1) from t7)) > round((select sum(c1) from t7)) from t8 left outer join t9 using(c1);
select ((select 1) < c1) + ((select 5) > c1) from t7;
select * from t7 where ((select 1) < c1) + ((select 5) > c1);
select * from t7 having ((select 1) < c1) + ((select 5) > c1);
select * from t7 having ((select 1) < sum(c1)) + ((select 5) > sum(c1));
select * from t7 where 1 in (select c1 from t9 having (select count(1) from t7) > 0) having (select max(c1) from t8) > 1;
select * from t7 having count(*) > (select 1);
select * from t7 having count(*) > (select c1 from t8 limit 1);
select * from t7 having count(*) > (select c1 from t8 where t7.c1=t8.c1);
select *, (select 9) > count(*) FROM t7;
select *, (select 9) > count(*) FROM t7  HAVING count(*) > (SELECT 5);
select count(*) FROM t7  HAVING count(*) > (SELECT 5) ;
select count(*) FROM t7 GROUP BY c1  HAVING count(*) > (SELECT 5) ;
select * from t7 order by exists(select 1);
select * from t7 order by (select count(*) from t8) > 1;
select * from t7 group by exists(select 1);
select * from t7 group by (select count(*) from t8) > 1;

## select list elimination ( -> 1)
## group by, order by elimination
#select
select * from t7 where exists( select c1, c2 from t8 );
select * from t7 where exists( select sum(c2), c2 from t8 group by c2 order by c2 desc limit 10 );
select 100 + exists( select c1, c2 from t8 ) from t7;
select * from t7 where exists( select c1, count(*) from t8 );
select * from t7 where exists( select distinct c1 from t8 );
select * from t7 where exists( select * from t8 limit 10);
select * from t7 where exists( select distinct c1 from t8 limit 10);
select * from t7 where exists( select c1 from t8 group by c2);
select * from t7 where exists( select c1 from t8 group by c2 limit 10);

select * from t7 where t7.c1 in ( select c1 from t8 group by c2 order by c2);
select * from t7 where t7.c1 >= ( select max(c1) from t8 group by c2 order by c2);
select * from t7 where t7.c1 <=> ( select c1 from t8 group by c2 order by c2 limit 1);
select * from t7 where t7.c1 = ( select sum(c1) from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= any( select c1 from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= all( select c1 from t8 group by c2 order by c2);
select * from t7 where t7.c1 in ( select c2 from t8 group by c2 order by c2);
select * from t7 where t7.c1 >= ( select c2 from t8 group by c2 order by c2);
select * from t7 where t7.c1 <=> ( select c2 from t8 group by c2 order by c2 limit 1);
select * from t7 where t7.c1 = ( select c2 from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= any( select c2 from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= all( select c2 from t8 group by c2 order by c2);
select * from t7 where t7.c1 >= ( select c2 from t8 group by c2, c1 order by c2);
select * from t7 where t7.c1 = ( select c2 + 1 from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= any( select c2 + 1 from t8 group by c2 + 1, c2 order by c2);
select * from t7 where t7.c1 in ( select c1 from t8 group by c2 desc order by c1 asc);
select * from t7 where t7.c1 = ( select 100 from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= any( select 'abc' from t8 group by c2 order by c2);
select * from t7 where t7.c1 <= all( select (select 1+1) from t8 group by c2 order by c2);
select * from t7 where t7.c1 in ( select c2 from t8 group by now());
select * from t7 where t7.c1 in ( select now() from t8 group by c2);
select * from t7 where t7.c1 in ( select t7.c2 from t8 group by c2);

select * from t7 where exists(select c1 + c2 from t8) and exists(select c3 * 3 from t9);
select * from t7 where exists(select c1 + c2 from t8) and exists(select c3 * 3 from t9);
select * from t7 where exists(select c1 + c2 from t8) having exists(select c3 * 3 from t9);
select * from t7 where exists(select c1 + c2 from t8) group by exists(select c3 * 3 from t9);
select * from t7 where exists(select c1 + c2 from t8) order by exists(select c3 * 3 from t9);

select * from t7 where not exists(select c1 + c2 from t8) and not exists(select c3 * 3 from t9);
select * from t7 where not exists(select c1 + c2 from t8) having not exists(select c3 * 3 from t9);
select * from t7 where not exists(select c1 + c2 from t8) group by not exists(select c3 * 3 from t9);
select * from t7 where not exists(select c1 + c2 from t8) order by not exists(select c3 * 3 from t9);

#nested subq
select c3 + 3 from t9 where exists(select exists(select c1+1 from t7) from t8);
select c3 + 3 from t9 where exists(select exists(select c1+1 from t7), c2 from t8);
# group by c2 should not be eliminated
select c3 from t9 where ( select count(1) from (select c2 from t8 group by c2) t999 ) > 10;

#group by, order by elimination
select * from t7 where exists(select c1, round(c2) from t8 group by c2);
select * from t7 where exists(select c1, round(c2) from t8 order by c2);
select * from t7 where exists(select c1, round(c2) from t8 group by c2 order by c2);

select * from t7 where not exists(select c1, round(c2) from t8 group by c2);
select * from t7 where not exists(select c1, round(c2) from t8 order by c2);
select * from t7 where not exists(select c1, round(c2) from t8 group by c2 order by c2);

select * from t7 where exists(select c1, round(c2) from t8 group by c2 having round(c2) > 0);
select * from t7 where exists(select c1, round(c2) from t8 order by c2 limit 0);
select * from t7 where exists(select c1, round(c2) from t8 group by c2 having round(c2) > 0 and c1 != 1 order by c2 limit 10);

#join
select * from t7,t8 where exists( select *, round(c2) from t9 );
select * from t7,t8 where exists( select *, round(c2) from t9 group by c2);
select * from t7,t8 where exists( select *, round(c2) from t9 order by c2);
select * from t7,t8 where exists( select *, round(c2) from t9 group by c2 order by c2);
select * from t7,t8 where exists( select *, round(c2) from t9 order by c2 limit 10);
select * from t7,t8 where exists( select *, round(c2) from t9 group by c2 order by c2 limit 10);
select * from t7,t8 where exists( select *, round(c2) from t9 group by c2 having round(c2) > 0 order by c2);

#union
(select * from t7 where exists(select c1, round(c2) from t8 group by c2 order by c2)) union (select * from t8);

# subquery pull up test
select c1, c2 from t7 where exists (select 1 from t8 where t8.c1 = t7.c1);
select c1, c2 from t7 where not exists (select 1 from t8 where t8.c1 = t7.c1);
SELECT 1 FROM t7 WHERE NOT EXISTS(SELECT 1 FROM t8 WHERE c2 = (SELECT c2 FROM t8 WHERE c1 >= 1) ORDER BY c2);

# eliminate DISTINCT in SELECT DISTINCT const exprs
select distinct 1, 1+2, ABS(-1) from t7;
select distinct 1, c2 from t7;
select count(distinct 1) from t7;
select distinct (select c1 from t7 limit 1) from t8;
select distinct 1, 1 + @var from t7;
Select distinct 1, 1 + (@var:=1) from t7;

# A(remote) + B(remote) on same server, generate remote plan
select t1.c2 + t2.c1 from t1, t2 where t1.c1 = t2.c2 and t1.c1 and t1.c1 = 1 and t2.c1 = 1;

# UNION and onetime expr related bug fix:
(select 'b') union select cast((select 'a') as char(20));
select cast((select 'a' from dual order by 1 limit 100) as char(20)) union (select 'b') union select cast((select 'a' from dual order by 1 limit 100) as char(20));
(select (select b from t12) from t3) union (select c3 from t3);

#bug_id 8088412
select distinct c2 from t1 partition(p1);

# onetime expr extraction for no real table
select cast((select 'a') as char(20)) as c1 union (select 'b') order by c1;
select (select 'a') as c1;

select avg(c1) as a from t1 union select sum(c1) as b from t1;
select * from t1 group by 1>(select count(*) from t2);

##bug:
(select count(*) from t1) UNION (select count(*) from t1);
select min(1) from t1;
select max(1) from t1;
select sum(1) from t1;
select avg(1) from t1;

select t1.c3, t2.c3 from (select c3, c2 from t3 order by c3) as t1, t2 where t1.c2 = t2.c2;

##test set operator
(select c2, c1 from t19 order by c2) union (select c2, c1 from t19 order by c2);
select a1, a2 from set_t1 group by a1 asc, a2 desc union select b1, b2 from set_t2 group by b1 asc, b2 desc;
select a1, a2 from set_t1 group by a1 asc, a2 desc intersect select b1, b2 from set_t2 group by b1 asc, b2 desc;
select a1, a2 from set_t1 group by a1 asc, a2 desc except select b1, b2 from set_t2 group by b1 asc, b2 desc;
(select * from t1 where c1 = 2) union all (select * from t1 where c1 = 2);
(select 1) union (select 1);
(select (select 1)) union (select (select 1));
(select (select c1 from t1) from  t2 order by ((select c1 from t1))) union (select 1);
(select (select c1 from t1) from  t2) union (select 1)  order by (select c1 from t1);
(select c1 from t1) union (select c2 from t1) union (select c2 from t2);
(select c1 from t1) union (select 1);
(select a from t12) union (select 1);

(select pk1 from set_t1) union distinct (select pk2 from set_t2);
(select pk1 from set_t1) union all (select pk2 from set_t2);
(select pk1 from set_t1) except (select pk2 from set_t2);
(select pk1 from set_t1) intersect (select pk2 from set_t2);

(select a2, a1 from set_t1) union distinct (select b2, b1 from set_t2);
(select a2, a1 from set_t1) union all (select b2, b1 from set_t2);
(select a2, a1 from set_t1) except (select b2, b1 from set_t2);
(select a2, a1 from set_t1) intersect (select b2, b1 from set_t2);

(select a2, a1 from set_t1) union distinct (select b2, b1 from set_t2) order by a2 desc;
(select a2, a1 from set_t1) union all (select b2, b1 from set_t2) order by a2 desc;
(select a2, a1 from set_t1) except (select b2, b1 from set_t2) order by a2 desc;
(select a2, a1 from set_t1) intersect (select b2, b1 from set_t2) order by a2 desc;
(select c1 from t15) union all (select c1 from t14) union select 7;


select 1 from t1,t2 order by t1.c2+t2.c2;
select 'a', c1 from t1 union select 'b', c1 from t7;
SELECT b FROM t13 WHERE b=1 UNION SELECT b FROM t13 WHERE b=1;

# row in join conditition, not allowed to generate merge join plan
select * from t7, t8 where (t7.c1, t7.c2) = (t8.c1, t8.c2);
select * from t7 inner join t8 on (t7.c1, t7.c2) = (t8.c1, t8.c2);
select * from t7, t8 where (t7.c1, t7.c2) > (t8.c1, t8.c2);
select * from t7 inner join t8 on (t7.c1, t7.c2) <= (t8.c1, t8.c2);

select distinct c1+@var from t1;
select distinct c1+(@a:=c2) from t1;
select distinct c1+(@a:=c1) from t1;
select distinct c2+(@a:=c2) from t1;

# equal condition with different expr type or collation
select /*+use_merge(tt1 tt2)*/* from tt1,tt2 where tt1.c1=tt2.c1;
select /*+use_merge(tt1 tt2)*/* from tt1,tt2 where tt1.c2=tt2.c2;
select /*+use_merge(t1, t2)*/* from t1,t2 where t1.c1=t2.c3;
select /*+use_merge(t1, t12)*/* from t1,t12 where t1.c1=t12.a;
select /*+use_merge(t1, t13)*/* from t1,t13 where t1.c1=t13.b;
select /*+use_merge(t1, t_u)*/* from t1,t_u where t1.c1=t_u.c1;
select /*+use_merge(t_u, t12)*/* from t_u,t12 where t12.a=t_u.c1;
select /*+use_merge(t2, t3)*/* from t2,t3 where t2.c3=t3.c3;
select * from yuming where c2 in ('-99999.99999','0.0');
select * from query_range where (c1, c2, c3) in ((1, 2, 3), (4, 5, 6));
select * from query_range where (c1, c2, c3) in ((1, 2, 3), (4, (select 5), 6));
select * from query_range where (c1, c2, c3) in (((select 1), 2, 3), ((select 4), 5, 6));
select * from query_range where (c1, c2, c3) in ((1, 2, 3), (4, 5, 6)) and c4>0;
select * from query_range where c1=1 and (c2, c3) in ((1, 2), (3, 4));
select * from query_range where c1>1 and (c2, c3) in ((1, 2), (3, 4));

select c2 from t4 where c2=1 and c3 is null group by c2;
select count(1) as c from t7 having (select c from t7 t)>0;
SELECT t1.c1 FROM t7 t1 GROUP BY t1.c1 HAVING t1.c1 < ALL(SELECT t2.c1 FROM t7 t2 GROUP BY t2.c1 HAVING EXISTS(SELECT t3.c1 FROM t7 t3 GROUP BY t3.c1 HAVING SUM(t1.c1+t2.c1) < t3.c1/4));
select count(1) from t_func group by c1,c2;
select count(1) from t_func group by c1;
select count(1) from t_func group by floor(c2+c1);
select distinct c1 from t_f;
select distinct c1,c2 from t_f;
select distinct c1,c2,c3 from t_f;
select c1 from t7 group by c1 having (select count(t7.c1) from t7 t)>0;
select c1 from t7 having (select t7.c1 from t7 t)>0;
select c1 from t7 group by c2 having (select t7.c1 from t7 t)>0;
select /*+leading(b, a) use_nl(a, b) read_consistency(weak) query_timeout(120000000)*/ count(*) as COUNT, nvl(sum(b.receive_fee),0) as AMOUNT from trade_process_000 b, trade_base_000 a where a.trade_no = b.trade_no and b.gmt_receive_pay >= '2023-09-26 10:21:55' and b.gmt_receive_pay < '2023-09-26 15:55:15' and (a.product IN ('S','M','L') or a.product not IN ('M','L','XL'))and (a.trade_type IN ('S','FP','RM','MT') or a.trade_type not IN ('FP','RM','MT','COD'));
select * from t0, t0 t where t0.c1=t.c2 and t0.c1=t.c1 order by t0.c1, t.c1;
select * from t0, t0 t where t0.c1=t.c1 and t0.c1=t.c2 order by t0.c1, t.c1;
select * from t0, t0 t where t0.c1=t.c1 and t0.c1=t.c1 order by t0.c1, t.c1;
select * from t17 where c1=1 and c2 is null and c3=1;
select c3 from t_normal_idx order by c3;
select c3 from t_normal_idx where c2=1 order by c3;
select c3 from t_normal_idx where c2 is null order by c3;
select c3 from t_normal_idx where c2=1 and c3 is null order by c4,c5;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 order by c5,c6;
select c3 from t_normal_idx where c2 is null and c3 is null and c4 is null and c5 is null order by c6;
select c3 from t_normal_idx where c2 is null and c3=1 order by c4;
select c3 from t_normal_idx where c2 is null and c3=1 and c4 is null order by c5,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 order by c3,c5,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 order by c2,c3,c4,c5,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 and c6 is null order by c5,c6;
select c3 from t_normal_idx where c2=1 and c4 is null and c5=1 order by c3,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c5=1 order by c4,c6;
select c3 from t_normal_idx where c2 is not null order by c3;
select c3 from t_normal_idx group by c3;
select c3 from t_normal_idx where c2=1 group by c3;
select c3 from t_normal_idx where c2 is null group by c3;
select c3 from t_normal_idx where c2=1 and c3 is null group by c4,c5;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 group by c5,c6;
select c3 from t_normal_idx where c2 is null and c3 is null and c4 is null and c5 is null group by c6;
select c3 from t_normal_idx where c2 is null and c3=1 group by c4;
select c3 from t_normal_idx where c2 is null and c3=1 and c4 is null group by c5,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 group by c3,c5,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 group by c2,c3,c4,c5,c6     ;
select c3 from t_normal_idx where c2=1 and c3 is null and c4=1 and c6 is null group by      c5,c6;
select c3 from t_normal_idx where c2=1 and c4 is null and c5=1 group by c3,c6;
select c3 from t_normal_idx where c2=1 and c3 is null and c5=1 group by c4,c6;
select c3 from t_normal_idx where c2 is not null group by c3;

select c1 from t7 where c1=(select t7.c1 as c from t7 t having c1>(select c from t7));
select c1+1 as c from t7 group by (select c from t7 t);
select count(c1) + 1 from t7 having (select count(t7.c1) from t7 t)>0;
select count(c1) + 1 as c from t7 having (select c from t7 t)>0;
select (select c1 from t7 t) as c from t7 group by (select c from t7 tt);
select 1 as c, c1 from t7 having c1>(select c from t7 t);
SELECT c7 FROM query_range1 t8_1 WHERE ( ( t8_1.c7 in ((450) , (2001)) ) ) OR ( ( t8_1.c7 < 220 ) );
select (1>(select count(*) from t2 where c1=1)) from t1 where c1=1;
select c1 from t1 group by c1, 'a' asc;
select c1 from t1 order by c1, 'a' asc;
select c1, exists(select c1 from t7) from t7 group by 2;

select * from t1 where c1 is false;
select * from t1 where (c1 + 1) is false;
#c1 is primary key
select * from t1 where c1 is null;
select * from t1 where c2 is null;
select * from t1 where c2 is not null;
select * from t1 where c1 is not null;
select * from t1 where (c1 + 1) is null;
select /*+ index(y_t1 i1)*/* from y_t1 where c2=10 order by c1;
select /*+ index(y_t1 y_t1_i1) */ * from y_t1 where c2=10 order by c1;
select /*+ index(y_t1 y_t1_i1) */ * from y_t1 where c2='10' order by c1;
select /*+ index(y_t1 y_t1_i1) */ * from y_t1 where c2=c1 order by c1;
select * from y_t1 where c2=c1 order by c1, c2;
select /*+ index(y_t1 y_t1_i1) */ * from y_t1 where c2=c1 order by c1, c2;
select /*+ index(y_t1 y_t1_i2) */ * from y_t1 where c3=c1 order by c1, c3;
select /*+ index(y_t1 y_t1_i2) */ * from y_t1 where c3=10 order by c1;
select /*+ index(y_t1 y_t1_i2) */ * from y_t1 where c3='10' order by c1;
select /*+ index(y_t1 y_t1_i2) */ * from y_t1 where c3=c1 order by c1;
select a from y_t3 where a = 'A' collate utf8mb4_general_ci order by a;
#bug:
select * from hint.t1 group by b having (select b from hint.t2 limit 1);
select * from hint.t1 group by b having (select b from hint.t2 limit 1) = 1;
select * from hint.t1 group by b having (select b from hint.t2 limit 1) = 1 or sum(b) = 1;

select c1 from t0 where pk is null;
select c1 from t0 where pk <=> null;
select c1 from t4 where c1 <=> null and c2 <=> null;
select c1 from t4 where c1 <=> 1 and c2 <=> 1;
select c1 from t4 where c1 is null and c2 <=> null;
#sub_query
select c1 from (select c1, c2 from t1 limit 1) ta group by ta.c1;
#sub_query with aggr-function
select * from (select count(c1) from t1) ta;
select c1 from (select c1, c2 from (select * from t4 limit 10)ta limit 5) ta1 group by ta1.c1;
#project pruning
select tb.a from (select a, b from (select * from t12 limit 10)ta limit 5) ta1 join (select a, b from t13 limit 10) tb on ta1.a = tb.a;
select t13.a from (select * from t12) ta, t13;
select ta.a from (select * from t12) ta where ta.a > (select t13.a from t13 where t13.b < ta.b limit 1) ;

select (select min(c1) from agg_t2) from agg_t1;
select * from (select c1 from t1 order by c1) ta limit 1;
select * from t4,t7 order by t4.c3;
#range的is_get方法测试
select /*+use_nl(tg1 tg2)*/* from tg1, tg1 tg2 where tg1.c1=tg2.c1 and tg1.c2=1 and tg1.c3=1 and tg2.c2=2 and tg2.c3=2;
select /*+use_nl(tg1 tg2)*/* from tg1, tg1 tg2 where tg1.c1=tg2.c1 and tg1.c2=1 and tg1.c3=1 and tg2.c2=2 and tg2.c3=2 and tg1.c2 = tg2.c2;
select /*+use_nl(tg1 tg2)*/* from tg1, tg1 tg2 where tg1.c1=tg2.c1 and tg1.c2=1 and tg1.c3 = 1 and tg2.c2=1 and tg2.c3=1 or tg2.c1=2 and tg2.c2=2 and tg2.c3 = 4 and tg1.c1 = 1 and tg1.c2 = 2 and tg1.c3 = 4;

select /*+use_nl(tg2 tg1)*/* from tg2,tg1 where tg1.c1=tg2.c1 and tg1.c2=1 and tg1.c3=1 or tg1.c2=2 and tg1.c3=2 and tg1.c1 = 2 and tg2.c1 = 3 and tg2.c2 =  3 and tg2.c3 = 3;
select /*+use_nl(tg1 tg2)*/* from tg2,tg1 where tg1.c1=tg2.c1 and tg1.c2=1 and tg1.c3=1 or tg1.c2=2 and tg1.c3=2 and tg1.c1 = 2 and tg2.c1 = 3 and tg2.c2 =  3 and tg2.c3 = 3 or tg1.c2=3 and tg1.c3=3 and tg1.c1 = 3 and tg2.c1 = 4 and tg2.c2 =  4 and tg2.c3 = 4;
select /*+use_nl(tg1 tg2)*/* from tg2,tg1 where tg1.c1=tg2.c1 and tg1.c2=1 and tg1.c3=1 or tg1.c2=2 and tg1.c3=2 and tg1.c1 = 2 and tg2.c1 = 3 and tg2.c2 =  3 and tg2.c3 = 3 or tg1.c2=3 and tg1.c3=3 and tg1.c1 = 3 and tg2.c1 = 4 and tg2.c2 =  4 and tg2.c3 = 4;
select /*+ use_nl(t1, t2) */ t2.c2, t2.c3 from (select * from y_t4 t where t.c1=100 order by t.c2 DESC, t.c3 DESC limit 1) t1 join y_t4 t2 on t1.c2=t2.c2 and t1.c3=t2.c3 order by t2.c2 DESC, t2.c3 DESC;

select count(1) from (select * from t1 order by c2 limit 10) t;
select count(1) from (select c1, c2, (select c3 from t2) as ca from t1 order by ca limit 10) t;
#distinct 覆盖了索引前缀(c2, c3, c1)
select distinct c3, c2 from t4;
#group by 覆盖了索引前缀(c2, c3, c1)
select count(c2) from t4 group by c3, c2;
#group by 覆盖了索引前缀(c2, c3, c1)
select count(c2) from t4 group by c3, c1, c2;
#group by 覆盖了索引前缀(c1, c2, c3)
select count(c2) from y_t4 group by c2, c1;
#group by 包含了唯一索引(c1, c2, c3)
select count(c4) from tg1 group by c4, c3, c5, c2, c1;
select distinct c3, c2, c4, c5, c1 from tg1;
select distinct b from y_t5 where (a2 >= 'b') and (b = 'a');
select distinct c2 from y_t6 where c1=c2;

#like and between selectivity
select * from tt1 where c2 like "a%";
select * from tt1 where c2 like "%a";
select * from tt1 where c2 like "ab%";
select * from tt1 where c2 between "aa" and "ab";
select * from tt1 where c2 between "aa" and "ai";
select * from tt1 where c2 not between "aa" and "ab";
select * from tt1 where c2 not between "aa" and "ai";
select * from tt1 where "ag" between c2 and "ai";
select * from tt1 where "ag" between "aa" and "c2";
select * from tt1 where 'ag' not between c1 and 'ai';

#late materialization
select /*+index(t_normal_idx idx)*/* from t_normal_idx order by c3 limit 1;
select /*+index(t_normal_idx idx)*/* from t_normal_idx where c4>1 order by c3 limit 1;
select /*+index(t_normal_idx idx)*/* from t_normal_idx order by c2 limit 1;
select /*+index(t_normal_idx idx)*/* from t_normal_idx where c10>1 order by c3 limit 1;
select /*+index(t_normal_idx idx)*/c2,c9,c10 from t_normal_idx order by c3 limit 1;
select /*+index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c3 limit 1;
select /*+index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c1 limit 1;
select /*+index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c3 limit 1 offset 10;
select /*+index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c3 desc limit 1 offset 10;
select /*+index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c3 desc ,c4 desc limit 1 offset 10;
select /*+index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c3 desc, c4 limit 1 offset 10;
select /*+no_use_late_materialization index(t_normal_idx idx)*/c1,c9 from t_normal_idx order by c3 desc limit 1 offset 10;
select /*+use_late_materialization index(t_normal_idx idx)*/* from t_normal_idx where c10>1 order by c3 limit 1;
#limit re_cost influence on NESTLOOP JOIN and MERGE JOIN
select * from t7, t8 where t7.c1 = t8.c1 limit 1;
select * from t7, t8 where t7.c1 = t8.c2 limit 1;
select * from t7, t8 where t7.c2 = t8.c2 limit 1;
select * from t7 where c1=c2 and c2='1';
select * from y_t7 where c1=c2 and c1=cast('2010-10-10 00:00:00' as datetime);

# bug: 8589376/8665435
select * from t1 order by c1,(select c1 from t2);
select * from (select * from t1 order by c1)v;
select * from (select * from t1 order by c1)v,t2;
select * from (select * from t1 order by c1)v,(select * from t2 order by c1)vv;
select * from t1 order by c1,(select c1 from t2 order by c1,(select c1 from t3 order by c1));
(select c1 from t1) intersect (select c1 from t2);
(select c1 from t1 order by c1) intersect (select c1 from t2 order by c1);
(select c1 from t1 order by c2,(select c1 from t3 order by c1)) intersect (select c1 from t2 order by c2,(select c1 from t3 order by c1));
select * from t1 order by c1,(select c1 from t2 where t1.c1=t2.c1 order by c1,(select c1 from t3 where t1.c1=t3.c1));

select t12.c1 from t1 t12,t2 t22 where t12.c1 in (select c3 from t1 where c1=5 AND c2>=t22.c1);
select t12.c1 from t1 t12,t2 t22 where t12.c1 not in (select c3 from t1 where c1=5 AND c2>=t22.c1);
select t12.c1 from t7 t12,t8 t22 where t12.c1 in (select c3 from t4 where c1=5 AND c2>=t22.c1);
select t12.c1 from t1 t12,t2 t22 where t12.c1+t22.c1 in (select c3 from t1 where c1=5);
select t12.c1 from t1 t12,t2 t22 where t12.c1+t22.c1 not in (select c3 from t1 where c1=5);
select count(t12.c1) from t1 t12,t2 t22 where t12.c1>SOME(select c3 from t1 where c1=5 AND c1=10 AND c2>=t22.c1) AND t22.c2>=5 AND t12.c2<SOME(select c1 from t1 where c2=3 AND c2<10 AND c3<=t12.c1);
select c1 from t7 group by c1, (select c2 from t7);
select c1 from t7 order by c1, (select c2 from t7);
select * from t7 where c2 in (select c2 from (select c2, sum(c1) from t7 group by c2) t);
select * from ts where c1 =1 and c2 > '2011-01-10' order by c2;
select * from tr where c1 > 1 order by c1;

#storing column index
select/*+index(t_idx_back t_idx_c2)*/ c3 from t_idx_back;
select /*+no_use_hash_distinct*/ distinct c2 from t0;
select * from tidx where c3 = 1;
select * from tidx where c2 > 0 and c2 < 1000 order by c2 limit 100;
select * from tidx where c2 > 0 and c2 < 1000 limit 100;

#window function
select c1, max(c3) over (partition by c2 order by c3) from t2;
select c1, max(c3) over (partition by c2 order by c3), max(c3) over (partition by c2) from t2;
# ordering can be used by order by
select c1, max(c3) over (partition by c2 order by c3 rows between 1 preceding and 2 following) from t2 order by c2,c3;
select c1, max(c3) over (partition by c2 order by c3 rows between 1 preceding and 2 following) from t2 order by c3,c2;
# variable combination bwtween lower and upper
# TODO@njia.nj 不同的upper/lower组合只影响算子参数, 不影响plan的结构, 更适合专门写一个单测
select c1, max(c3) over (partition by c2 order by c3 rows between 1 preceding and unbounded following) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between 1 preceding and current row) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between 1 preceding and 2 following) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between 2 preceding and 1 preceding) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between 1 following and 2 following) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between current row and unbounded following) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between current row and current row) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between current row and 2 following) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between unbounded preceding and 2 following) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between unbounded preceding and current row) from t2;
#select c1, max(c3) over (partition by c2 order by c3 rows between unbounded preceding and unbounded following) from t2;
# float window
select c1, max(c3) over (partition by c2 order by c3 rows between 1 preceding and 2 following) from t2;

#generated column
select * from tg where c1 = 'bcde';
select * from tg where c1 = 'baaaa';
select * from tg where c1 = 'bcde' and (c2 = 'cde' or c2 = 'baaaa');

#bug:
select z1.a, z2.a from z1 full outer join z2 on z1.a = z2.a order by z1.a, z2.a;

### test for distributing column expr to relations

select t7.c1 from t7,t8 where t7.c1 > (select t0.c1 from t0 where t7.c1);
select t7.c1 from t7,t8 where t7.c1 > (select t0.c1 from t0 where t7.c1 + 1);
select t7.c1 from t7,t8 where t7.c1 > (select t0.c1 from t0 where 1);

#select t12.c1 from t1 t12,t2 t22 where t12.c1 in (select /*+index(t1 primary)*/ c3 from t1 where c1=5 AND c2 = t22.c1);
select t12.c1 from t1 t12,t2 t22 where t12.c1 in (select c3 from t1 where c1 = 5 AND c2 = t22.c1);
select * from t1 join t5 on t1.c1 = t5.c3 where t5.c3 > 0 and t5.c3 < 100 order by t5.c3 limit 100;
select * from t1 join t5 on t1.c1 = t5.c3 where t5.c3 > 0 and t5.c3 < 100 order by t5.c3 limit 10000;
select * from tidx where c2 > 0 and c2 < 1000 order by c2 limit 10000;
select * from tidx where c2 > 0 and c2 < 1000 limit 10000;

# -- bug: group by/order by position column been pruned
select c2 from (select c1, c2 from t1 group by 1) a;
select c2 from (select c1, c2 from t1 order by 1) a;
select c2 from (select c1 + c2, c2 from t1 order by 1) a;

#refine query range after extract exec params from subplan
#
select * from t1 where t1.c2 = 5 or exists (select 1 from t2 where t1.c1 = t2.c1);
select * from t1 where t1.c2 = 5 or exists (select 1 from t2 where t1.c1 > t2.c1);
select * from t1 where t1.c2 = 5 or exists (select 1 from t2 where t1.c1 < t2.c1);
select * from t1, t2 where t1.c1 > exists(select c1 from t2 where t2.c1 = t1.c1);
select * from t1 where (select c1 from t2 limit 1)+1 in (select 2 from t3 where t1.c1=t3.c1);
select * from t1 having count(*) > (select c1 from t2 where t1.c1=t2.c1);
select * from t1, t2 where t2.c1 = t1.c1 and t2.c1 = (select c1 from t3 where t3.c1 = t1.c1);
select * from (select c1+1 as a1 from t1 where t1.c2 = 2) a, t2 where a.a1 = t2.c2 or t2.c1 = ANY(select c3 from t3 where t3.c1 > a.a1);
select * from t1 where t1.c1 > (select sum(c1) from t2 where t2.c1 = t1.c1);
select * from t1 where t1.c1 > (select sum(c1) from t2 where t2.c1 = t1.c1 and t2.c2 > (select max(c2) from t3 where t3.c1 = t2.c1));
select * from t1 where t1.c2 in (select avg(c1) from t2 where t2.c1 = t1.c1 union select count(1) from t3 where t3.c1 = t1.c1);
select * from t1 where t1.c1 != (select c2 from t2 where t2.c1 = (select max(c2) from t3 where t3.c1 = t1.c1) order by t2.c2 limit 1);

# right expr is const other than NULL
select /*both need not sort*/ * from t7 left join t8 on t7.c2 = t8.c2 where t8.c2 = 5;
# right expr is NULL 
select /*both need sort*/ * from t7 left join t8 on t7.c2 = t8.c2 where t8.c2 IS NULL;

# right expr is const other than NULL, and right expr has order
select /*both need not sort*/* from t7 left join t8 on t7.c2 = t8.c1 where t8.c1 = 5;
# right expr is NULL, and right expr has order
select /*left need sort*/ * from t7 left join t8 on t7.c2 = t8.c1 where t8.c1 IS NULL;

# right expr is const other than NULL, exchange
select /*both need not sort*/ * from t2 left join t3 on t2.c2 = t3.c2 where t3.c2 = 5;
# right expr is NULL 
select /*both need sort*/ * from t2 left join t3 on t2.c2 = t3.c2 where t3.c2 IS NULL;

# right expr is const other than NULL, and right expr has order, exchange
select /*both need not sort*/* from t2 left join t3 on t2.c2 = t3.c1 where t3.c1 = 5;
# right expr is NULL, and right expr has order
select /*left need sort*/ * from t2 left join t3 on t2.c2 = t3.c1 where t3.c1 IS NULL;

select count(1) from rpt_adgroup_tag_realtime where thedate = '2017-09-25 00:00:00' and custid = '1102225352' and (custid,thedate,productlineid,campaignid,adgroupid,targetingtagid,tagvalue,pid,hour,traffictype,mechanism,productid) > (1102225352,'2017-09-25 00:00:00',-1,16138889,761211164,358940752715,358940752716,'420651_1007',12,1,2,-1);

select * from query_range where c1=3 and (c1, c2, c3)>(1, 2, 3) and (c1, c2, c3)>(2, 1, 2);

select * from cb_loan_acctbal_01 a where (a.balcatcd, a.baltypcd) in (('NOTE', 'BAL'), ('NOTE', 'GINT'), ('ODP', 'GINT'), ('RCVB', 'INT'), ('RCVB', 'ODPI')) and a.preeffdate and '2017-10-16' < a.effdate;
use query_range;
select * from range_t1 where (a, b)>(1, 1) and (a, b)=(1, 2);
select * from range_t1 where (a, b)>(1, 1) and a=1;
select * from range_t1 where (a, b)>(1, 1) and b=2;
select * from test1 where (id, dt) > (0, '2017-01-02') and (id, dt) <= (1, '2017-01-03');
select * from test1 where (id, dt) > (0, '2017-01-02') and (id, dt) <= (1, '2017-01-03') and dt < '2017-01-02';
select * from test1 where (id, dt) > (0, '2017-01-01') and (id, dt) <= (1, '2017-01-03') and dt = '2017-01-02';
select * from test1 where (id, dt) > (0, '2017-01-02') and (id, dt) <= (1, '2017-01-03') and dt = '2017-01-02';
