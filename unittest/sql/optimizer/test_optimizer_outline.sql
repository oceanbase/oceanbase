#################################### test for Used Hint ################################
#测试rewrite时subquery中的qb_name是否正确
select * from t4 where c1 in (select c1 from t1);

### 测试rewrite时指定hint是否生效 ###
## subquery 被提升的情况
select /*+INDEX(@"SEL$2" "opt.t4"@"SEL$2" "idx_t4_c3")*/* from t1 where c1 in (select c1 from t4);
select * from t1 where c1 in (select c1 from t4);

## subquery被下降的情况
select /*+ BEGIN_OUTLINE_DATA INDEX(@"SEL$1" "opt.t4"@"SEL$1" "idx_t4_c2_c3") INDEX(@"SEL$2" "opt.t1"@"SEL$2" "idx_t1_c2") END_OUTLINE_DATA */* from t4 where c1 in (select max(c1) from t1);
select * from t4 where c1 in (select max(c1) from t1);
select   /*+ BEGIN_OUTLINE_DATA USE_NL(@"SEL$3" "opt.t2"@"SEL$3") LEADING(@"SEL$3" "opt.t1"@"SEL$3" "opt.t2"@"SEL$3") END_OUTLINE_DATA */* from t1 where c1 in (select * from (select max(t1.c1) from t1, t2) as tt);
## 由于bug，暂时注释
## select   /*+ BEGIN_OUTLINE_DATA USE_NL(@"SEL$3" "opt.t2"@"SEL$3") LEADING(@"SEL$3" "opt.t1"@"SEL$3" "opt.t2"@"SEL$3") END_OUTLINE_DATA */* from t1 where c1 in (select * from (select max(t1.c1) from t1, t2) as tt);
## select * from t1 where c1 in (select * from (select max(t1.c1) from t1, t2) as tt);


#测试指定hint的结果是否正确
select /*+index(t4 idx_t4_c3)*/ * from t4 where c1 = 1;
select /*+ BEGIN_OUTLINE_DATA INDEX(@"SEL$1" "opt.t4"@"SEL$1" "idx_t4_c3") END_OUTLINE_DATA  */ * from t4 where c1 = 1;
select /*+full(t4)*/ * from t4 where c2 = 1;
select /*+ BEGIN_OUTLINE_DATA FULL(@"SEL$1" "opt.t4"@"SEL$1") END_OUTLINE_DATA  */ * from t4 where c2 = 1;

#test for join_hint
select * from t1,t2;
select * from t1,t2 where t1.c1=t2.c1;
select * from t1,(select count(*) from t2) as tt;
select * from (select count(*) from t2) as tt,t1,t9 where t9.c1=t1.c1;
select * from (select count(*) from t2, t10) as tt,t1,t9 where t9.c1=t1.c1;
select * from t1,t2,t9;
select * from t1,t2 where (t1.c1 + t2.c1) in (select t9.c1 from t9, t10);
select * from t1,t2 where (t1.c1 + t2.c1) in (select max(t9.c1) from t9, t10);
select * from t8, (select count(*) from t1 where t1.c1 > any (select t2.c1 from t2)) as tt;

#测试join的outline是否正确
select  /*+ BEGIN_OUTLINE_DATA  USE_MERGE(@"SEL$1" "opt.t2"@"SEL$1") LEADING(@"SEL$1" "opt.t1"@"SEL$1" "opt.t2"@"SEL$1")  FULL(@"SEL$1" "opt.t1"@"SEL$1")  FULL(@"SEL$1" "opt.t2"@"SEL$1")  END_OUTLINE_DATA  */* from t1,t2 where t1.c1=t2.c1;
select /*+ BEGIN_OUTLINE_DATA USE_MERGE(@"SEL$1" "opt.t1"@"SEL$1") LEADING(@"SEL$1" "opt.t2"@"SEL$1" "opt.t1"@"SEL$1") FULL(@"SEL$1" "opt.t2"@"SEL$1") FULL(@"SEL$1" "opt.t1"@"SEL$1")    END_OUTLINE_DATA */* from t1,t2 where t1.c1=t2.c1;
select /*+ BEGIN_OUTLINE_DATA USE_NL(@"SEL$1" "opt.tt2"@"SEL$1") LEADING(@"SEL$1" "opt.tt1"@"SEL$1" "opt.tt2"@"SEL$1") FULL(@"SEL$1" "opt.tt1"@"SEL$1") FULL(@"SEL$1" "opt.tt2"@"SEL$1") END_OUTLINE_DATA */* from t1 as tt1,t2 as tt2 where tt1.c1=tt2.c1;

#test for global hint
select /*+read_consistency("weak")*/* from t1;
select /*+hotspot*/* from t1;
select /*+topk(1 100)*/ * from t1;
select /*+query_timeout(100)*/ * from t1;
select /*+frozen_version(1)*/ * from t1;
select /*+use_plan_cache(none)*/ * from t1;
select /*+use_plan_cache(default)*/ * from t1;
select /*+use_plan_cache(nothing)*/ * from t1;
select /*+activate_buried_point(1,FIX_MODE,1,1)*/ * from t1;
select /*+no_rewrite*/ * from t1;
select * from t1 where c1 > any (select /*+no_rewrite*/ count(*) from t2);
select /*+trace_log*/ * from t1;
select /*+log_level('INFO')*/ * from t1;
select * from t1,(select /*+log_level('INFO')*/ count(*) from t2) as tt;
select * from t1,(select /*+trace_log*/ count(*) from t2) as tt;
select * from t1,(select /*+use_plan_cache(none)*/ count(*) from t2) as tt;
select * from t1,(select /*+use_plan_cache(default)*/ count(*) from t2) as tt;
select * from t1,(select /*+use_plan_cache(nothing)*/ count(*) from t2) as tt;

#测试指定hint的结果是否正确
select   /*+ BEGIN_OUTLINE_DATA FULL(@"SEL$1" "t1"@"SEL$1") READ_CONSISTENCY("WEAK") HOTSPOT TOPK(1 100) QUERY_TIMEOUT(100) FROZEN_VERSION(1) USE_PLAN_CACHE("EXACT") NO_REWRITE TRACE_LOG LOG_LEVEL('info') END_OUTLINE_DATA */* from t1;
#test for group
select max(c1) from t1 group by c1;
select min(c2) from t1 group by c2;

#test for dml_stmt
insert into t1 (c1) values(1);
update t7 set c1=100 where c1=1;
delete from t1 where c1=1;


#test for late materialization
select /*+index(t_normal_idx idx)*/* from t_normal_idx order by c3 limit 1;
select /*+no_use_late_materialization index(t_normal_idx idx)*/* from t_normal_idx where c2=1 order by c4 limit 1;

##### test for subquery #####

## 根节点是SPF的情况
select * from t1 where t1.c1 > (select c1 from t2);
select * from t1,t2 where (t1.c1 + t2.c1) > (select c1 from t3);
select * from t1,t2 where (t1.c1 + t2.c1) > (select t3.c1 from t3, t4);

## SPF在JOIN节点下面的情况
select * from t1,t2 where t1.c1 > (select c1 from t2);
select /*+use_nl(t2) leading(t1 t2)*/ * from t1,t2 where t2.c1 > (select 1);
select /*+use_nl(t2) leading(t1 t2)*/ * from t1,t2 where t1.c1 > (select 1);
### 测试hint对spf的有效性
select /*+use_nl(t1) leading(t2 t1)*/ * from t1,t2 where t2.c1 > (select 1);
select /*+use_nl(t1) leading(t2 t1)*/ * from t1,t2 where t1.c1 > (select 1);

### 左枝是spf，且spf左枝是join
select * from t1,t2,t3 where (t2.c1 + t1.c1) > (select 1);
### 左枝是spf，且spf左枝和右枝都是join
select * from t1,t2,t3 where (t2.c1 + t1.c1) > (select t4.c1 from t4, t5);

## 多个subquey
select 1 from t1 inner join t1 t2 using(c1) where t1.c1 < (select t3.c1 from t3) and t1.c1 > (select t4.c1 from t4);
select 1 from t1 inner join t1 t2 using(c1) where t1.c1 < (select t3.c1 from t3) order by (select t4.c1 from t4);

## subquery中含有join
select 1 from t1 inner join t1 t2 using(c1) where t1.c1 < (select t4.c1 from t3, t4);

## test qb_name, explain outline中不应该显示qb_name
select /*+qb_name(select_1)*/* from t4;
select /*+qb_name(select_1) qb_name(select_2)*/* from t4;

#################################### test for left deep tree #############################
### not left deep tree ####
select t20.c1 from t20 join t0 where t0.c1 in (select t7.c1 from t7 join t8);
select t20.c1 from t20 join t0 join (t7 left join t8 on t7.c1 = t8.c1);
select t20.c1 from t20 left join (t7 left join t8 on t7.c1 = t8.c1) on t20.c1 = t7.c1 join t0;

### subplan filter ###
select t20.c1 from t20 join t0 where t0.c1 in (select max(t7.c1) from t7 join t8);
select t20.c1 from t20 join t0 where t0.c1 in (select max(t7.c1) from t7 join t8) and t0.c1 in (select max(t7.c2) from t7);
select t20.c1 from t20 join t0 where 1 in (select t7.c1 from t7 join t8);
