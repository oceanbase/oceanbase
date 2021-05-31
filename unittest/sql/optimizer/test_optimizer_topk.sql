## ------------------case that not match the rules to use topk---------------------------##
#without order by
select /*+topk(20 6)*/ avg(c1), avg(c2), avg(c3) from t1_topk where c2 <10 group by c2  limit 5;
#without limit
select /*+topk(20 6)*/ avg(c1), avg(c2), avg(c3) from t1_topk  where c2 <10 group by c2 order by avg(c1);
#without group by
select /*+topk(20 6)*/ avg(c1), avg(c2), avg(c3) from t1_topk  where c2 <10 order by avg(c1) limit 5;
#is select  for update
select /*+topk(20 6)*/ avg(c1), avg(c2), avg(c3) from t1_topk  where c2 <10 group by c2 order by avg(c1) limit 5 for update nowait;
#without topk hint
select avg(c1), avg(c2), avg(c3) from t1_topk  where c2 <10 group by c2 order by avg(c1) limit 5;
#with found_rows
select /*+topk(20 6)*/ sql_calc_found_rows avg(c2), avg(c3)  from t1_topk where c2 <10 group by c2 order by avg(c1) limit 5;
#with subquery
select /*+topk(20 6)*/  avg(c1), avg(c2), avg(c3)  from t1_topk where c2 <10 group by c2 order by (select c1  from t1_topk where c2 = 1)  limit 5;
#with distinct
select /*+topk(20 6)*/ distinct avg(c1), avg(c2), avg(c3)  from t1_topk where c2 <10 group by c2 order by avg(c1) limit 5;
#with group_concat will not use topk
select /*+topk(20 6)*/ sum(c1), avg(c2), group_concat(c3, c1)  from t1_topk where c2 <10 group by c2 order by avg(c1) limit 5;
#not based table
select /*+topk(20 6)*/  avg(c1), avg(c2), avg(c3) from (select *  from t1_topk where c2 <10) as a  group by a.c2 order by avg(a.c1) limit 5;
# join table
select /*+topk(20 6)*/ distinct avg(t1_topk.c1), avg(t1_topk.c2), avg(t1_topk.c3)  from t1_topk , t2_topk group by t1_topk.c2 order by avg(t1_topk.c1) limit 5;

##-----------------case that match rules of topk-----------------------##
##hash agg + topk_sort
select /*+topk(1 2) use_hash_aggregation*/ c3, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <10 group by c2 order by c3 limit 1;
##merge agg + sort +topk
select /*+topk(1 1)*/ c3, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <10 group by c2 order by c3 limit 1;
## with offset
select /*+topk(1 1)*/ c3, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <12 group by c2 order by c3 limit 1 offset 4;
select /*+topk(1 1)*/ c3, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <12 group by c2 order by c3 limit 1 offset 1000;
##test limit 0
select /*+topk(1 1)*/ c3, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <10 group by c2 order by c3 limit 0;
##complex  order by item
select /*+topk(1 1)*/ c2, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <10 group by c2 order by avg(c3), sum(c1)/sum(c2) limit 5;
select /*+topk(1 1)*/ c1, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <10 group by c2 order by avg(c3) desc, sum(c1)/sum(c2)  limit 5;
select /*+topk(1 1)*/ c2, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <10 group by c2 order by avg(c3) desc, sum(c1)/sum(c2) + avg(c2)  limit 5;
##different topk params
select /*+topk(0 0)*/ c2, sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <12 group by c2 order by avg(c3) desc, sum(c1)+sum(c2)  limit 5;
select /*+topk(50 0)*/ c2, sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <12 group by c2 order by avg(c3) desc, sum(c1)+sum(c2)  limit 5;
select /*+topk(10000 1)*/ c2, sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <12 group by c2 order by avg(c3) desc, sum(c1)+sum(c2)  limit 5;
select /*+topk(1 10000)*/ c2, sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1)  from t1_topk where c2 <12 group by c2 order by avg(c3) desc, sum(c1)+sum(c2)  limit 5;



##topk is in subquery
select * from (select /*+topk(0 4)*/ avg(c1)  from t1_topk where c2 <10 group by c2 order by sum(c2) limit 5) as a;
select c1  from t1_topk where c1 < any (select /*+topk(0 4)*/ avg(c1)  from t1_topk where c2 <10 group by c2 order by sum(c2)  limit 5);
select /*+topk(0 4), no_use_px*/ avg(c1)  from t1_topk where c2 <10 group by c2 order by sum(c2) limit 5 union (select /*+topk(0 10)*/ sum(c1)  from t1_topk where c2 <10 group by c2 order by sum(c2) limit 7);

select c1  from t1_topk where (select /*+topk(0 10)*/ avg(c1)  from t1_topk where c2 <10 group by c2 order by sum(c2) limit 1) > 1 ;

##TODO:this is not supported
##select  (select  /*+topk(10 5)*/ avg(c1)  from t1_topk  where c2 =2 group by a.c2 order by sum(c2) limit 5)   from t2_topk as a;
#select (select  /*+topk(10 5)*/ avg(c1) from t1_topk  where c2 =2 group by c2 order by sum(a.c2 + c2) limit 5)  from t2_topk as a ;

select * from (select /*+topk(10 5)*/ c3, c1, avg(c1), sum(c2), sum(c1) + sum(c2), count(c3), min(c3), max(c1), sum(c2)/count(c1), sum(c3)/sum(c1) from t1_topk group by c2 order by c3 limit 7) as a left join t2_topk on a.c1 = t2_topk.c1 left join t3_topk on t3_topk.c1 = a.c1;
