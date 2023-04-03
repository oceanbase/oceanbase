################################## Index Hint ##################################
#测试主键index和普通index的情况
select /*+FULL(t4)*/* from t4;
select /*+INDEX(t4 idx_t4_c2_c3)*/* from t4;
select /*+INDEX(t4 idx_t4_c2)*/* from t4;
select /*+INDEX(t4 idx_t4_c3)*/* from t4;
select /*+INDEX(t_pt t_pt_idx_1)*/* from t_pt where (c2,c1) > (300,200);
select /*+INDEX(t_pt primary)*/* from t_pt where (c2,c1) > (300,200);
select /*+INDEX(t_pt primary)*/* from t_pt where (c1,c2) > (200,300);

#测试无效index的情况
select /*+INDEX(t4 idx_t4_invalid)*/* from t4;

#测试同一个table 对应多个可选index
select /*+INDEX(t4 idx_t4_c3) INDEX(t4 idx_t4_c2) INDEX(t4 idx_t4_c2_c3) */* from t4;
select * from t4 use index(idx_t4_c3, idx_t4_c2, idx_t4_c2_c3);
select * from t4 force index(idx_t4_c3, idx_t4_c2, idx_t4_c2_c3);
select /*+INDEX(t4 idx_t4_c3) FULL(t4) */* from t4;

#测试指定多个table index的情况
select /*+INDEX(t4 idx_t4_c3) INDEX(t5 idx_t5_c2)*/* from t4,t5;

#测试alias的情况
select /*+INDEX(t4 idx_t4_c3)*/* from t4 as tt;
select /*+INDEX(tt idx_t4_c3)*/* from t4 as tt;

#测试index list为空的情况
select * from t4 use index();

#测试use_index与ignore_index混用的情况
select /*+INDEX(t4 idx_t4_c3) */* from t4 ignore index(idx_t4_c3);

##### 测试ingore的情况#####
select * from t4 ignore index(idx_t4_c3);
select * from t4 ignore index(idx_t4_c2);

# 同时制定多个ingore, 当同时编写多个ingore时，只有第一个生效
select * from t4 ignore index(idx_t4_c3, idx_t4_c2);
select * from t4 ignore index(idx_t4_c3) ignore index(idx_t4_c2);

# alias
select * from t4 tt ignore index(idx_t4_c3, idx_t4_c2);
select * from t4 tt ignore index(idx_t4_c3) ignore index(idx_t4_c2);

# primary
select * from t4 tt ignore index(primary, idx_t4_c2);
select * from t4 tt ignore index(primary) ignore index(idx_t4_c3);
select * from t4 tt ignore index(primary) ignore index(idx_t4_c2_);


################################## Join Type Hint ##################################

# normal
select * from t4,t5;
select /*+leading(t4 t5) use_nl(t5)*/* from t4,t5;

# alias
select /*+leading(tt4 tt5) use_nl(tt5)*/* from t4 tt4, t5 tt5;

# multi hint
select /*+leading (t4 t5) use_nl(t5) use_nl(t5) use_nl(t5)*/* from t4,t5;
select /*+use_nl(t4) use_nl(t5)*/* from t4,t5;

# outer join
select /*+leading (t4 t5) use_nl(t5)*/* from t4 left join t5 on t4.c1 = t5.c1;
select /*+leading (t5 t4) use_nl(t5)*/* from t4 left join t5 on t4.c1 = t5.c1;
select /*+leading (t4 t5) use_nl(t5)*/* from t4 right join t5 on t4.c1 = t5.c1;
select /*+leading (t5 t4) use_nl(t5)*/* from t4 right join t5 on t4.c1 = t5.c1;
select /*+leading (t4 t5) use_nl(t5)*/* from t4 full join t5 on t4.c1 = t5.c1;
select /*+leading (t5 t4) use_nl(t5)*/* from t4 full join t5 on t4.c1 = t5.c1;
select /*+leading (t4 t5) use_merge(t5)*/* from t4 left join t5 on t4.c1 = t5.c1;
select /*+leading (t4 t5) use_merge(t5)*/* from t4 right join t5 on t4.c1 = t5.c1;
select /*+leading (t4 t5) use_merge(t5)*/* from t4 full join t5 on t4.c1 = t5.c1;

# subplan_scan
select /*+leading(tt t5) use_nl(t5)*/* from (select max(c1) from t1) tt, t5;
select /*+leading(t5 tt) use_nl(tt)*/* from (select max(c1) from t1) tt, t5;

# test for different join type
select /*+leading(t4 t5) use_bnl(t5)*/* from t4,t5;
select /*+leading(t4 t5) use_bnl(t5)*/* from t4,t5;

# multi join tree
select /*+leading(tt t5) use_nl(t5)*/* from (select /*+leading(t4 t7) use_nl(t7)*/max(t4.c1) from t4,t7) tt, t5;
select /*+leading(t5 tt) use_nl(tt)*/* from (select /*+leading(t7 t4) use_nl(t4)*/max(t4.c1) from t4,t7) tt, t5;

# test for leading
select /*+ordered*/* from t4,t5;
select /*+ordered*/* from t5,t4;
select /*+ordered*/* from t5;
select /*+ordered*/* from t4 left join t5 on t4.c1=t5.c1;
select /*+ordered*/* from t4 right join t5 on t4.c1=t5.c1;
select /*+ordered*/* from t4 full join t5 on t4.c1=t5.c1;

################################## test for late materialization ##################################
select /*+index(t_normal_idx idx)*/* from t_normal_idx order by c3 limit 1;
select /*+use_late_materialization index(t_normal_idx idx)*/* from t_normal_idx order by c3 limit 1;
select /*+no_use_late_materialization index(t_normal_idx idx)*/* from t_normal_idx where c2=1 order by c4 limit 1;

################################## test for qb_name ##################################
select * from t4;
select /*+qb_name(select_1)*/* from t4;
select /*+qb_name(select_1) qb_name(select_2)*/* from t4;
select /*+qb_name(select_1) */* from t4 where t4.c1 > any(select /*+qb_name(select_2)*/c1 from t5);
select /*+qb_name(select_1) */* from t4 where t4.c1 > any(select /*+qb_name(select_1)*/c1 from t5);

################################## 测试不应该显示的hint  ##################################
select /*+topk(1 100) hotspot max_concurrent(1)*/* from t4;

################################## 测试指定即生效的hint  #################################
select /*+read_consistency("weak")*/* from t1;
select /*+query_timeout(100)*/ * from t1;
select /*+frozen_version(1)*/ * from t1;
select /*+use_plan_cache(none)*/ * from t1;
select /*+use_plan_cache(default)*/ * from t1;
select /*+use_plan_cache(nothing)*/ * from t1;
select /*+no_rewrite*/ * from t1;
select /*+trace_log*/ * from t1;
select /*+log_level('INFO')*/ * from t1;
