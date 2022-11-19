######################## test for hierarchical query #######################################
#### test for from item ###
## 一基表 #
#select t1.c1 from t1 connect by prior t1.c1 = c2;
#
## 两张基表 #
#select t1.c1 from t1, t2  connect by prior t1.c1 = t2.c1;
#select t1.c1 from t1 join t2 connect by prior t1.c1 = t2.c1;
#select t1.c1 from t1 left join t2 on t1.c1 = t2.c1 connect by prior t1.c1 = t2.c1;
#select t1.c1 from t1 right join t2 on t1.c1 = t2.c1 connect by prior t1.c1 = t2.c1;
#
## 一个基表，一个view #
#select c1 from (select * from t1) as tt connect by prior c1 = c2;
#select tt.c1 from (select * from t1) as tt , t2 connect by prior tt.c1 = t2.c2;
#select tt.c1 from (select * from t1) as tt join t2 on tt.c1 = t2.c1 connect by prior tt.c1 = t2.c2;
#select tt.c1 from (select * from t1) as tt left join t2 on tt.c1 = t2.c1 connect by prior tt.c1 = t2.c2;
#select tt.c1 from (select * from t1) as tt right join t2 on tt.c1 = t2.c1 connect by prior tt.c1 = t2.c2;
#
## 三个表 #
#select t1.c1 from t1, t2, t3 connect by prior t1.c1 = t2.c1;
#select t1.c1 from (t1 join t2 on t1.c1 = t2. c1) , t3 connect by prior t1.c1 = t2.c1;
#select t1.c1 from (t1 join t2 on t1.c1 = t2. c1) join t3 on t2.c3 = t3.c3 connect by prior t1.c1 = t2.c1;
#
## 四个表 #
#select t1.c1 from t1, t2, t3, t4 connect by prior t1.c1 = t2.c1;
#select t1.c1 from t1 join t2 on t1.c1 = t2.c1, t3, t4 connect by prior t1.c1 = t2.c1;
#select t1.c1 from t1 join t2 on t1.c1 = t2.c1, t3 join t4 on t3.c3 = t4.c3 connect by prior t1.c1 = t2.c1;
#select t1.c1 from (t1 join t2 on t1.c1 = t2.c1) join t3 on t2.c3 = t3.c3, t4 connect by prior t1.c1 = t2.c1;
#select t1.c1 from ((t1 join t2 on t1.c1 = t2.c1) join t3 on t2.c3 = t3.c3) join t4 on t3.c3 = t3.c2 connect by prior t1.c1 = t2.c1;
#
#
#### test for rewrite start with filter column expr ###
#
## 单表 #
#
#select t1.c1 from t1 start with t1.c1 connect by prior 1 = 1;
#select t1.c1 from t1 start with t1.c1 = 1 connect by prior 1 = 1;
#
#select t1.c1 from t1 start with t1.c1 = 1 connect by prior t1.c1 = c2;
#select t1.c1 from t1 where t1.c1 > 1 start with t1.c1 = 1 connect by prior t1.c1 = c2;
#select t1.c1 from t1 start with c1 + c2 = 1 connect by prior t1.c1 = c2;
#select t1.c1 from t1 start with c1 > (select c2 from t3) connect by prior t1.c1 = c2;
#select t1.c1 from t1 start with c1 > (select c2 from t3) + c2 connect by prior t1.c1 = c2;
### 暂时不支持相关子查询!!! ###
###  select t1.c1 from t1 start with c1 > (select c2 from t3 where t3.c3 > t1.c1) connect by prior t1.c1 = c2;
###  select t1.c1 from t1 start with c1 > (select c2 from t3 where t1.c1) connect by prior t1.c1 = c2;
###  select t1.c1 from t1 start with c1 > (select c2 from (select * from t3 where t3.c3 > t1.c1) as tt) connect by prior t1.c1 = c2;
#
## 多表 #
#select t1.c1 from t1 join t2 start with t1.c1 + t2.c2 = 1 connect by prior t1.c1 = t1.c2;
###  select t1.c1 from t1 join t2 start with t1.c1 > (select c2 from t3 where t3.c2 > t2.c2) connect by prior t1.c1 = t1.c2;
#
## alias table #
#select c1 from t1 as tt start with tt.c1 = 1 connect by prior c1 = c2;
#
## view #
#select * from (select * from t1) as tt start with tt.c1 = 1 connect by prior c1 = c2;
#
#### test for rewrite prior expr ###
#
## 测试对不同位置表达处理 #
#select t1.c1 from t1 start with t1.c1 connect by prior c1 = c2;
#select prior c2 from t1 start with t1.c1 connect by prior 1 = 1;
#select prior c2 from t1 where prior c2 > 1 start with t1.c1 connect by prior 1 = 1;
#select prior c2 from t1 where prior c2  + 1 > 1 start with t1.c1 connect by prior 1 = 1;
#select c1 from t1 where prior c1 > 1 start with t1.c1 connect by prior 1 = 1;
#select c1 from t1 where prior (c1 + c2) > 1 start with t1.c1 connect by prior 1 = 1;
###select max(c1) from t1 start with t1.c1 connect by prior 1 = 1 group by prior c2;
###select max(c1) from t1 start with t1.c1 connect by prior 1 = 1 group by c1 having prior c2;
###select c1 from t1 start with t1.c1 connect by prior 1 = 1 order by prior c2;
#
## 测试prior const expr #
#select prior 1 from t1 start with t1.c1 connect by prior 1 = 1;
## 测试subquery #
### select c2 from t1 where c1 > (select c1 from t2 where prior t1.c1 > t2.c2 ) start with t1.c1 connect by prior 1 = 1;
### select c2 from t1 where c1 > (select c1 from t2 where prior t1.c1) start with t1.c1 connect by prior 1 = 1;
### select c2 from t1 where c1 > (select c1 from (select * from t2 where prior t1.c1 > t2.c2 ) as tt) start with t1.c1 connect by prior 1 = 1;
#
## alias column #
#
##select (prior c2) as cc from t1 start with t1.c1 connect by prior 1 = 1 order by cc;
###select (prior c2 + c1) as cc from t1 start with t1.c1 connect by prior 1 = 1 order by cc;
#
## alias table #
#
## 测试copy origin prior expr #
#select prior (c1 + c2)  from t1 start with t1.c1 connect by prior c1 = c2;
#select prior (abs(c1) + c2)  from t1 start with t1.c1 connect by prior c1 = c2;
#select prior ((abs(c1) + c2) * 3)  from t1 start with t1.c1 connect by prior c1 = c2;
#
#### test for partition table ###
#select c1 from pt2 connect by prior 1 = 1;
#select c1 from st1 connect by prior c1 = c2;
#select c1,level, connect_by_isleaf, connect_by_iscycle, connect_by_root c1, sys_connect_by_path(c1, '|') from st1 connect by nocycle prior c1 = c2;

# distinct消除冗余表
select distinct a from pjt1,pjt2;
select distinct c from pjt1,pjt2,pjt3;
select distinct a,d,g from pjt1,pjt2,pjt3,pjt4;
select distinct a from pjt1,pjt2,pjt3 where b=d;
select distinct a,g from pjt1,pjt2,pjt3,pjt4 where b=d;
select distinct c from pjt1,pjt2,pjt3,pjt4 where a=e;
select * from pjt1, (select distinct c from pjt2,pjt3) where a=c;
select * from pjt1, (select distinct c from pjt2,pjt3,pjt4 where d=g) where a=c;
select distinct a from pjt1, (select distinct c from pjt2,pjt3,pjt4 where d=g) where a=1;
select distinct a from pjt1 where b in (select distinct c from pjt1,pjt2 where d>1);
select distinct a from pjt1 where exists (select distinct c from pjt1,pjt2,pjt3 where d>1 and f=d);
select distinct a from pjt1,pjt2 where b in (select distinct c from pjt2,pjt3 where d in (select distinct e from pjt3,pjt4));
(select distinct a from pjt1,pjt2) union (select distinct c from pjt2 where exists (select distinct e from pjt3,pjt4));
(select distinct a from pjt1,pjt2) intersect (select distinct c from pjt2,(select distinct e from pjt3,pjt4) where c > 1); 

select max(a) from pjt1,pjt2;
select max(a), min(c) from pjt1,pjt2,pjt3;
select count(distinct g), max(d), min(a) from pjt1,pjt2,pjt3,pjt4;
select sum(distinct a) from pjt1,pjt2,pjt3 where b=d;
select max(a) from pjt1,pjt2,pjt3,pjt4 where b=d;
select avg(distinct c) from pjt1,pjt2,pjt3,pjt4 where a=e;
select * from pjt1, (select min(d) as x from pjt2,pjt3) where a=x;
select * from pjt1, (select count(distinct h)as x from pjt2,pjt3,pjt4 where d=g) where a=x;
select min(b) from pjt1, (select count(distinct h)as x from pjt2,pjt3,pjt4 where d=g) where a=1;
select distinct a from pjt1 where b>(select sum(distinct c) from pjt1,pjt2 where d>1);
select distinct a from pjt1 where b>(select avg(distinct d) from pjt1,pjt2,pjt3 where d>1 and f=d);
select max(a) from pjt1,pjt2 where b > (select min(c) from pjt2,pjt3 where d < (select sum(distinct e) from pjt3,pjt4));
(select count(distinct a) from pjt1,pjt2) union (select max(c) from pjt2 where d>(select avg(distinct e) from pjt3,pjt4));

(select max(a) from pjt1, pjt2) minus (select distinct e from pjt3,pjt4);
select sum(distinct b) from pjt1, (select distinct c from pjt2, pjt3) where a = c;
select distinct a from pjt1, pjt2 where b > (select max(e) from pjt3, pjt4);
select count(distinct a) from pjt1,pjt2 where b in (select distinct c from pjt2,pjt3 where d < (select max(e) from pjt3,pjt4));

select distinct a, c from pjt1,pjt2;
select distinct a from pjt1,pjt2,pjt3 where c=e;
select distinct a + d - e from pjt1,pjt2,pjt3;
select avg(b) from pjt1, pjt2;
select max(a), sum(b) from pjt1, pjt2;
select max(a+c) from pjt1, pjt2;
select distinct a, e from ((select a from pjt1) union (select c from pjt2)), pjt3;
select distinct a from pjt3, pjt1 join pjt2 on b=d;

# 测试 order by 非 UNIQUE KEY 的消除
## test on pk
select sum(c1) from t1 group by c1, c2;
select sum(c1) from t1 group by c2, c1;
## test on uniq index
select uniq_c2, sum(uniq_c2) from pullup1 group by uniq_c2, pk;

# test row uniq
select sum(c1) from test_simp group by c1, c2 ,c3;
select sum(c1) from test_simp group by c2 ,c3;
# test row uniq index
select sum(c1) from test_simp group by c2 ,c3, c4;
