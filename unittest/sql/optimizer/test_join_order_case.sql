# case 1 join
select t1.c1 from t1, t2 limit 100;

select c1 from t1 group by c1;

select distinct t4.c1, t4.c2, t2.c2 from t4, t2 where t4.c1 = t2.c1;
# case 26 join with predicate
select t1.c1 from t1, t2 where t1.c1=t2.c1 limit 100;

# case expresions from both branches
select t1.c2 + t2.c1 from t1, t2 where t1.c1 = t2.c2 and t1.c1 and t1.c1 = 1 and t2.c1 = 2;

# case 27 3-table join
select t1.c1
from t1, t2, t3
where t1.c1=t2.c1
      and t1.c2>t2.c2
      and t2.c3=t3.c3
      and t3.c1>10;

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

# test for merge join. Make sure that the sort operator use the correct sort keys
select /*+ ordered, use_merge(t3) */ * from t2, t3
where t2.c2 = t3.c1
and t2.c1 = 0
and t3.c1 = 0;
select /*+ use_merge(t3) */ * from t2, t3
where t2.c1 = t3.c2
and t2.c1 = 0
and t3.c1 = 0;

#case join and index hint to cover func add_table_by_hint
select /*+ index(t5 idx_t5_c2) */ t5.c2 from t5, t1 where t5.c2 = t1.c2;
select c1, c2 from t1 ignore index (idx_t1_c2) where c1 =2 or c2 = 5;
select t5.c2 from t5, t1 ignore index (idx_t5_c2) where t5.c2 = t1.c2;
#case  virtal table  case for create_virtual_access_path()
#case virious join type
select * from t1 right join t2 on t1.c1=t2.c1 and t1.c1>1;
select * from t1 full join t2 on t1.c1=t2.c1 and t1.c1>1;
#case LEFT_SEMI_JOIN
select * from t1 where c1 in (select c1 from t2);
select c1, c2 from t1 where exists (select * from t2 where  t2.c1 = t1.c1);
#case LEFT_ANTI_SEMI_JOIN
select * from t1 where c1 not in (select c1 from t2);
select c1, c2 from t1 where not exists (select * from t2 where  t2.c1 = t1.c1);
#case
select t1.c1 from t1, t2 where t1.c1 + t2.c1 = 5;
#case test extract_nl_params
select /*+ use_nl(t2)*/ * from t1 , t2 where t1.c1= t2.c1;
select /*+ use_nl(t2)*/ * from t1 join t2 on t2.c1 = 1 and t1.c1= t2.c1;
select /*+ use_nl(t2)*/ * from t1 , t2 where t1.c1 + t2.c1 = t2.c2;
select /*+ use_nl(t2), use_nl(t3)*/ * from t1 , t2, t3 where t1.c1 + t2.c1 = t2.c2 and t1.c1 + t2.c1 = t3.c1;
#case geneate_outer_baserel
select t1.c1 from t1 left join t2 on t1.c1 = t2.c1 where exists (select c1 from t3 limit 1);
#case push_down_oj_qual
select t1.c1 from t1 left join t2 on t2.c1 = 5;
select /*+no_use_px*/ t1.c1 from t1 left join t2 tt2  on t1.c1 = tt2.c1, t3 tt3 left join t4 on  tt3.c1=t4.c1 , t2 left join t3 on  t3.c1 = 5 ;
#select tt4.c1 from t3 tt3 left join t4 tt4 on  tt3.c1=tt4.c1 , t3 ttt3 left join t5 on  ttt3.c1=t5.c1 , t1 left join t2 tt2  on t1.c1 = tt2.c1, t2 left join t3 on  t3.c1 = 5 ;
#select t1.c1 from t1, t2 left join t3 on t1.c1 = t3.c1 and t2.c3 = t3.c3 and  t3.c2 = 5;
#select t1.c1 from t1, (select * from t2 where c2>1 order by c1 limit 10) as t where t1.c1=t.c1
select t1.c1 from t1 left join  (select t2.c1 as a1, t3.c1 as a2 from t2 left join  t3 on t3.c1 = 5) as a on  a.a1 = 3;
select t1.c1 from t1 left join (t2 left join t3 on t2.c1 = t3.c1) on t2.c1 = 1;
select /*+leading(t1 t2)  use_mj(t1 t2)*/ t1.c1, t2.c1 from tr t1 inner join tr t2 on t1.c1 = t2.c1 order by t1.c1 desc;
#this should put at last
use oceanbase;
select * from __all_virtual_zone_stat where zone='zone1';
select /*+leading(t2 t1) use_nl(t1 t2)*/ t1.c1, t2.c1 from opt.t4 t1 inner join  opt.t2 as t2 on t1.c1 = t2.c1;
select /*+leading(opt.t1 opt.t2) use_nl(t1 t2)*/ t1.c1, t2.c1 from  opt.t1  as t1 inner join  opt.t2 as t2 on t1.c1 = t2.c2 order by t1.c1 desc, t2.c1 desc;
select /*+use_nl(opt.t1 opt.t2 opt.t3)*/ * from opt.t1 as t1 left join opt.t2 as t2 on t1.c1 = t2.c1 and t1.c1 = 1 and t2.c1 = 1 left join opt.t3 as t3 on t2.c1 = t3.c1 and t3.c1 in (1, 2) order by t1.c1;
