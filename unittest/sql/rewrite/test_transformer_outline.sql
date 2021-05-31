#case
select * from t1, t2 where t1.c1 > 10 and t1.c1 in (select c2 from t2);

#case
select * from t1 where c1 >ANY (select c2 from t2);

##case
select * from t1, t2 where t1.c1 In (select c1 from t2);

#case 1 exists without correlated exprs 
select * from t1 where Exists (select c1 from t2);

#case 2 ANY without correlated exprs 
select * from t1 where c1 In (select c1 from t2);

select * from t1 where c1 =ANY (select c1 from t2);

select * from t1 where c1 >ANY (select c1 from t2);

#case 3 ALL without correlated exprs
#can be pulled up
select * from t1 where c1 <ALL (select c1 from t2);
#can not be pulled up
select * from t1 where c1 <ALL (select c1 from t2 limit 1);

#case 4 subquery-expr without correlated vars
#can be pulled up
select * from t1 where 1 < ANY(select c1 from t2);
#can not be pulled up
select * from t1 where 1 < ANY(select c1 from t2 limit 1);

#case 5 multi-level subquery without correlated expr
select * from t1 where Exists (select c1 from t2 where c2 >ALL (select c1 from t3));

#case 6 subquery with correlated exprs
#can be pulled up
select * from t1 where Exists (select c1 from t2 where t1.c2=t2.c2);
select * from t1 where c1 <ALL (select c1 from t2 where t1.c2=t2.c2);
#can not be pulled up
select * from t1 where c1 !=ANY (select c1 from t2 where t1.c2=t2.c2 limit 1);

#case 7 Scalar-subquery Expr
select * from t1 where c1 < (select c1 from t2);

#case 8 add [limit 1] to [exists subquery]
select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1);
select * from t1 where exists (select c1 from t2);
select * from t1 where exists (select c1 from t2 limit 10, 2);
select * from t1 where exists (select c1 from t2 group by c1);

#case for pullup subquery with lefthand T_OP_ROW
select * from t1 where (t1.c1, t1.c2) in (select c1,c2 from t2 where c3 = 1);
select * from t1 where (t1.c1, t1.c2) not in (select c1,c2 from t2 where c3 = 1);
select * from t1 where (t1.c1, t1.c2) = ANY(select c1,c2 from t2 where c3 = 1);
select * from t1 where (t1.c1, t1.c2) != ANY(select c1,c2 from t2 where c3 = 1);
select * from t1 where (t1.c1, t1.c2) = ALL(select c1,c2 from t2 where c3 = 1);
select * from t1 where (t1.c1, t1.c2) != ALL(select c1,c2 from t2 where c3 = 1);
#with correlated query
select * from t1 where (t1.c1, t1.c2) != ANY(select c1,c2 from t2 where t1.c2 = t2.c2);
select * from t1 where (t1.c1, t1.c2) = ALL(select c1,c2 from t2 where t1.c1 > t2.c1);
select * from t3 where (t3.c1, t3.c2, t3.c3) in (select c1,c2,c3 from t2 where t3.c3 <> t2.c3);

#case keep expression subquery in [not] exists
select * from t1 where exists (select 1, round(1.1) from dual);
select * from t1 where not exists (select 1, round(1.1) from dual);

#case eliminate [not] exists subquery withnot groupby and having
select * from t1 where exists (select max(c1) from t2);
select * from t1 where not exists (select max(c1) from t2);
select * from t1 where exists (select max(c1), sum(c1), count(c1)  from t2);
select * from t1 where not exists (select max(c1), sum(c1), count(c1)  from t2);
select * from t1, (select * from t2 where exists(select sum(c1) from t3)) tx where t1.c1 = tx.c1;
select * from t1 where t1.c1 in (select c1 from t2 where exists(select count(c1) from t3));

#case eliminate select list and group by and add limit 1 to [not] exists subquery 
select * from t1 where exists (select group_concat(c1, c2) from t2 group by c1) ;
select * from t1 where exists (select max(c1), sum(c1), count(c1) from t2 group by c1) ;
select * from t1 where exists (select max(c1), sum(c1), count(c1) from t2 group by c1 having c1 > 1);
select * from t1 where exists (select max(c1), sum(c1), count(c1) from t2 group by c1 having sum(c1) > 1);
select * from t1 where exists (select max(c1), sum(c1), count(c1) from t2 group by c1 having sum(c1) > 1 and count(c1) > 0); 
select * from t1 where not exists (select group_concat(c1, c2) from t2 group by c1) ;
select * from t1 where not exists (select max(c1), sum(c1), count(c1) from t2 group by c1) ;
select * from t1 where not exists (select max(c1), sum(c1), count(c1) from t2 group by c1 having c1 > 1);
select * from t1 where not exists (select max(c1), sum(c1), count(c1) from t2 group by c1 having sum(c1) > 1);
select * from t1 where not exists (select max(c1), sum(c1), count(c1) from t2 group by c1 having sum(c1) > 1 and count(c1) > 0);
select * from t1 where exists(select max(c1) from t2 group by c1 having max(c1) > 1);
select * from t1, (select * from t2 having c1 > 1) as tx where t1.c1 = tx.c1;
select * from t1, (select * from t2 where exists(select sum(c1)  from t3 group by c1 having c1 > 1)) as tx where t1.c1 = tx.c1;
select * from t1 where exists (select max(c1) from t2 group by c1 having exists (select * from t3 where t3.c1 > max(t2.c1)));
select * from t1 where exists (select sum(c1) from t2 group by c1 having exists (select * from t3 where t3.c1 > sum(t2.c1)));

#case eliminate [not]exists subquery in condition exprs params
select * from t1 where c2 > exists(select c2 from t2 where c2 > exists(select c2 from t3));
select * from t1 where c2 > exists(select c1 from t2 where c2 > exists(select sum(c1) from t3));
select * from t1 where c2 > exists(select sum(c1) from t2 where c2 > exists(select sum(c1) from t3));


# remove distinct in simplify
SELECT c1 FROM pullup1 WHERE EXISTS(SELECT DISTINCT c1 FROM pullup2);
# remove distinct in simplify
SELECT c1 FROM pullup1 WHERE EXISTS(SELECT DISTINCT c1 FROM pullup2 LIMIT 1);
# cannot remove distinct
SELECT c1 FROM pullup1 WHERE EXISTS(SELECT DISTINCT c1 FROM pullup2 LIMIT 1 OFFSET 2);


# case pull up sub-query to inner join: base case 
# [TO INNER]pk is primary key of pullup2
# [TO INNER]pk is primary key of pullup2, sub-query contains uncorrelated condition(s)
# [TO INNER]pk is primary key of pullup2, sub-query contains correlated condition(s)
# [TO INNER]pk is primary key of pullup2, sub-query contains correlated condition(s)
# [TO INNER]pk is primary key of pullup2, sub-query contains order by
# [TO INNER]uniq_c2 is unique key of pullup2
# [TO INNER]uniq_c2 is unique key of pullup2, sub-query contains uncorrelated condition(s)
# [TO INNER]uniq_c2 is unique key of pullup2, sub-query contains correlated condition(s)
# [TO INNER]uniq_c2 is unique key of pullup2, sub-query contains correlated condition(s)
# [TO INNER]uniq_c2 is unique key of pullup2, sub-query contains order by
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 WHERE pullup2.c3 = pullup1.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 WHERE pullup2.c1 = pullup2.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 WHERE pullup2.c1 = pullup1.c3 AND pullup2.c1 = pullup2.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 ORDER BY pullup2.c3);

SELECT c1 FROM pullup1 WHERE c1 IN (SELECT uniq_c2 FROM pullup2);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT uniq_c2 FROM pullup2 WHERE pullup2.c3 = pullup1.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT uniq_c2 FROM pullup2 WHERE pullup2.c1 = pullup2.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT uniq_c2 FROM pullup2 WHERE pullup2.c1 = pullup1.c3 AND pullup2.c1 = pullup2.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT uniq_c2 FROM pullup2 ORDER BY pullup2.c3);

# case pull up sub-query to inner join: base case
# [TO INNER  ]sub-query contains distinct
# [TO INNER  ]sub-query contains distinct, sub-query contains uncorrelated condition(s)
# [NO REWRITE]sub-query contains distinct, sub-query contains correlated condition(s)
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT DISTINCT c1 FROM pullup2);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT DISTINCT c1 FROM pullup2 WHERE pullup2.c1 = pullup2.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT DISTINCT c1 FROM pullup2 WHERE pullup2.c3 = pullup1.c3);

# case pull up sub-query to inner join: cannot be pulled up:
# [NO REWRITE] has limit
# [NO REWRITE] has group by
# [NO REWRITE] has set-op
# [NO REWRITE] left operator of this IN is a constant
# [TO SEMI] select item of sub-query is a constant
# [TO SEMI] select item of sub-query is not a regular column
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 LIMIT 1);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 GROUP BY pullup2.c1);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM pullup2 UNION ALL SELECT pk FROM pullup3);
SELECT c1 FROM pullup1 WHERE 1 IN (SELECT c1 FROM pullup2);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT 1 FROM pullup2);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT floor(pk) FROM pullup2);

# case pull up sub-query to inner join: cannot be pulled up:
# [TO SEMI   ] not unique key
# [TO SEMI   ] more than one FromItem
# [TO SEMI   ] FromItem is not base table: generated table
# [TO SEMI   ] FromItem is not base table: joined table
# [NO REWRITE] DISTINCT with correlated condition(s)
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT c1 FROM pullup2);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pullup2.pk FROM pullup2, pullup3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pk FROM (select * from pullup2 limit 1));
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT pullup2.pk FROM pullup2 JOIN pullup3 on pullup2.c3 = pullup3.c3);
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT DISTINCT pullup2.c1 FROM pullup2 WHERE pullup1.c3 = pullup2.c3);

# case pull up sub-query to inner join: more complicated upper statement
# [TO INNER  ]
# [TO INNER  ]
SELECT c1 FROM pullup1 WHERE c1 IN (SELECT c1 FROM pullup2) ORDER BY c3;
SELECT c1 FROM pullup1 GROUP BY c1 HAVING c1 IN (SELECT c1 FROM pullup2);
# [TO INNER  ]
# pk is primary key and can be removed by simplify.
# after that we can do pullup
SELECT * FROM pullup2 WHERE pk IN (SELECT pullup2.pk FROM pullup2 GROUP BY pullup2.pk DESC);
###TODO case first sql pull up sub query in where fisrtly and then do view merge, finally again where pull up
##question is after merge operation there maybe new table item and where pull up entry exist error
###select * from (select * from t1 where c1 >ANY (select c2 from t2)) as v where v.c1 in (select c1 from t2);
###select * from t1 where c1 > ANY(select c2 from t2) and t1.c1 in (select c1 from t2);

#select c1 from t1 where c1 not in(select c1 from t2 where c2 not in (select c2 from t2));
#select c1 from t1 where c1 not in(select c1 from t2 where c2 not in (select c2 from t2));
#select * from t1 where c1 in (select c2 from t2 where c1 in (select 1 + 1));
#select * from t1 where c1 in (select c2 from t2 where c1 in (select 1 + 1));

#case sub query in sub view cannot be pulled up
select * from (select * from t1 where c1 > ANY(select c1 from t2 where c1 > 0 group by c2)) as v where v.c1 in (select c1 from t2);
select * from (select * from t1 where c1 > ANY(select c1 from t2 where c1 > 0 group by c2)) as v where v.c1 in (select c1 from t2);

#case add origin join item in parent stmt
select * from t1 left join t2 on t1.c1 = t2.c1, (select * from t3) as v3;
select * from t1 left join t2 on t1.c1 = t2.c1, t3;

#case add origin multi join in parent stmt
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2, (select * from t3) as v3;
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2, t3 as v3;

#case
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 join t4 on t4.c1=t3.c1 join t5 on t3.c1 = t5.c2, (select * from t3) as v3, (select * from t2) as v2;
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2 join t4 on t3.c2 = t4.c2 join t5 on t4.c1=t5.c1, t3 as v3;

#case add multi join in sub view
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1, (select * from t3) as v3, (select * from t2) as v2;
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1, t3 as v3, t2 as v2;

#case add multi join in sub view
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 join t4 on t4.c1=t3.c1 join t5 on t3.c1 = t5.c2, (select * from t3) as v3, (select * from t2) as v2;
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1, t3 as v3, t2 as v2;

#case outer join in parent and in sub view
#select * from (select * from t3 left join t2 on t3.c1 = t2.c1) as v;
#select * from (select * from t3 left join t2 on t3.c1 = t2.c1) as v;

#case join in parent and in sub view
select t1.c1 from t1 join t2 on t1.c1 = t2.c1, (select t2.c1 from (t2 join t3 on t2.c1 = t3.c1)) as v;
select t1.c1 from t1, t2, t3 where t1.c1 = t2.c1 and t2.c1 = t3.c1;

#case pull up part expr in child join
select * from (select t1.c1 from t1 join t2 on t1.c1 = t2.c1) as v;
select t1.c1 from t1, t2 where t1.c1 = t2.c1;

#case none spj table inside view
select * from (select * from t1 where c1 > 0 group by c2) as v;

#case join table inside view
select * from (select t2.c1 from t1, t2 join t3 on t2.c1 = t3.c1 where t1.c1 > 0) as v;
select t2.c1 from t1, t2 join t3 on t2.c1 = t3.c1 where t1.c1 > 0;

#case cannot be eliminated left join
select * from t1 left join (select * from t2) as v on v.c1 = t1.c1;

#case can be eliminated left join
select * from t1 left join (select * from t2) as v on v.c2 = t1.c2 where v.c2 is not null;

#case
select * from t1 left join (select * from t2) as v on v.c2 = t1.c2 where v.c2 is not null;

#case
select * from t1 as v where v.c1 > 0;

#case
select * from (select * from t1) as v;

#case
select * from (select * from t1) as v left join t3 on v.c1=t3.c1 where t3.c2 is not null;

#case
select * from t1 join (select * from t2) as v on t1.c1 = v.c1 left join t3 on t1.c1 = t3.c1 where t3.c2 is not null;
select * from t1, t2, t3 where t1.c1 = t2.c1 and t1.c1 = t3.c1 and t3.c2 is not null;

##case
select * from t1 join (select * from t2) as v2 on t1.c1 = v2.c1;
select * from t1, t2 where t1.c1 = t2.c1;

#case
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c2 = t3.c2;

#case
select * from t1 inner join t2 on t1.c1 = t2.c1;

##case alias TODO resolver not support now
##select k from (select (c1 + 1) as k, (c2 + 1) as k2 from t1 where c1 = 1) as v group by k2 having v.k > 0;
##select c1 + 1 from t1 where c1 = 1 group by (c2 + 1) having (c1 + 1) > 0;
########################################################################################
##case from item contain sub query
####case test merge subquery in view
select * from (select * from t1 where c1 > 1 + (select avg(c2) from t2)) as v;
select * from t1 where c1 > 1 + (select avg(c2) from t2);

##case
select * from (select * from t1 where c1 > 1 + (select avg(c2) from t2)) as v, t3;
select * from t1, t3 where t1.c1 > 1 + (select avg(c2) from t2);

#case
select * from (select * from t1 where c1 > 1 + (select avg(c2) from t2)) as v join t3 on v.c1=t3.c1;
select * from t1, t3 where t1.c1 > 1 + (select avg(c2) from t2);

#case
#select * from (select * from t1, t2 where t1.c1 In (select c1 from t2)) as v;
#select * from (select * from t1, t2 where t1.c1 In (select c1 from t2)) as v;

##TODO case problem when doing where pull up
##select * from (select * from (select * from t1 where c1 >ANY (select c2 from t2)) as v where v.c1 in (select c1 from t2)) as v1;
##select * from t1 where c1 >ANY (select c1 from t2) and c1 in (select c1 from t2);

#case
select * from (select * from t1 where c1 in (select c1 from t2 where t1.c1=t2.c1)) as v where v.c1 > 0;
select * from t1 where c1 in (select c1 from t2 where t1.c1=t2.c1) and c1 > 0;

#case
select * from (select * from t1 where c1 in (select c1 from t2 where t1.c1=t2.c1)) as v where v.c1 in (select c1 from t2);
select * from t1 where c1 in (select c1 from t2 where t1.c1=t2.c1) and c1 in (select c1 from t2);


#case
select * from (select * from t1 where Exists (select c1 from t2)) as v where v.c1 = 1;
select * from t1 where Exists (select c1 from t2) and t1.c1 = 1;

#case
select * from (select * from t1 where c1 In (select c1 from t2)) as v where v.c1 = 1;
select * from t1 where c1 In (select c1 from t2) and t1.c1 = 1;

#case
select * from (select * from t1 where c1 =ANY (select c1 from t2)) as v where v.c1 + 1 = 2;
select * from t1 where c1 =ANY (select c1 from t2) and c1 + 1 = 2;

#case
select * from (select * from t1 where c1 >ANY (select c1 from t2)) as v where v.c1 - 10 < 5;
select * from t1 where c1 >ANY (select c1 from t2) and t1.c1 - 10 < 5;

#case
select * from (select * from t1 where c1 >ANY (select c1 from t2)) as v where v.c1 < 10;
select * from t1 where c1 >ANY (select c1 from t2) and t1.c1 < 10;

#case
select * from (select * from t1 where c1 <ALL (select c1 from t2)) as v where v.c1 > 10;
select * from t1 where c1 <ALL (select c1 from t2) and t1.c1 > 10;

#case
select * from (select * from t1 where c1 <ALL (select c1 from t2 limit 1)) as v where v.c1 > 10;
select * from t1 where c1 <ALL (select c1 from t2 limit 1) and t1.c1 > 10;

#case
select * from (select * from t1 where 1 < ANY(select c1 from t2)) as v where v.c1 - 10 = 10;
select * from t1 where 1 < ANY(select c1 from t2) and t1.c1 - 10 = 10;

#case
select * from (select * from t1 where 1 < ANY(select c1 from t2 limit 1)) as v where v.c1 = 1;
select * from t1 where 1 < ANY(select c1 from t2 limit 1) and t1.c1 = 1;

#case from view multi-level subquery without correlated expr
select * from (select * from t1 where Exists (select c1 from t2 where c2 >ALL (select c1 from t3))) as v where v.c1 > 0;
select * from t1 where Exists (select c1 from t2 where c2 >ALL (select c1 from t3)) and t1.c1 > 0;

#case from view with correlated exprs
select * from (select * from t1 where Exists (select c1 from t2 where t1.c2=t2.c2)) as v where v.c1 > 0;
select * from t1 where Exists (select c1 from t2 where t1.c2=t2.c2) and t1.c1 > 0;

#case
select * from (select (c1 + 1) as k from t1 where c1 <ALL (select c1 from t2 where t1.c2=t2.c2)) as v where v.k > 0;
select (c1 + 1) as k from t1 where c1 <ALL (select c1 from t2 where t1.c2=t2.c2) and t1.c1 > 0;

#case
select * from (select (c1 + 1) as k from t1 where c1 !=ANY (select c1 from t2 where t1.c2=t2.c2 limit 1)) as v where v.k > 0;
select (c1 + 1) as k from t1 where c1 !=ANY (select c1 from t2 where t1.c2=t2.c2 limit 1) and t1.c1 > 0;

#case from view Scalar-subquery Expr
select * from (select * from t1 where c1 < (select c1 from t2)) as v where v.c1 > 0;
select * from t1 where c1 < (select c1 from t2) and t1.c1 > 0;
####################################################################################
#case multi join in sub view
#select * from (select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c2 = t3.c2) as v;
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c2 = t3.c2;

#case
select c1 from (select c1 from t1) as s1 ;
select c1 from t1;

#case
select * from (select * from (select * from t1) as s1) as s2, t2;
select * from t1, t2;

#case
select * from (select * from (select * from t1) as s1) as s2, t2 where s2.c1=t2.c1;
select * from t1, t2 where t1.c1=t2.c1;

#case
select * from (select * from (select * from t1) as s1) as s2, (select c1 from (select * from t2) as k1 ) as k2 where s2.c1=k2.c1;
select t1.c1, t1.c2, t2.c1 from t1, t2 where t1.c1=t2.c1;

#case
select * from (select (k + 1) as kk from (select (c1+1) as k from t1) as s1) as s2, (select (m + 1) as mm from (select (c2+1) as m from t2) as v1) as v2 where s2.kk+1=v2.mm;
select t1.c1+1+1, t2.c2+1+1 from t1, t2 where t1.c1+1+1+1=t2.c2+1+1;

#case
select * from t1, t2 left join t3 on t2.c1=t3.c1 where t1.c1=t3.c2;

#case
select c1 from t1 where c1=1;

#case alias case
select k + 1, t2.c2 from (select (c1 + 2) as k from t1) as s1, t2 where s1.k = t2.c1;
select t1.c1 +2+1, t2.c2 from t1, t2 where t1.c1+2 = t2.c1;

#case alias case
select k + 1, t2.c2 from t2, (select (c1 + 2) as k from t1) as s1 where k = t2.c1;
select t1.c1 +2+1, t2.c2 from t2, t1 where t1.c1+2 = t2.c1;

#case
select * from t1, (select * from t2) as t where t1.c1=t.c1 group by t.c2;
select * from t1, t2 where t1.c1=t2.c1 group by t2.c2;

##case 2 problem resolver two where cond? TODO
select c1 as alias_name from t1 where c1 = 7;

#case
select distinct c1 as ali_name from t1;

####case
###select c1, 1 + 1 from t1 order by c1 limit 100;
###select c1, 1 + 1 from t1 order by c1 limit 100;

###case where Scalar
select * from t1 inner join t2 on t1.c1=t2.c1 where false;

#case
select * from t1 left join t2 on t1.c1>1 where t2.c1 is not null and t2.c2>1;

#case
select c1, c1+c2 from t1 where c1 > 100 limit 1, 10;

#case
select * from t1 left join t2 on t1.c1>1 where t2.c1 is null and t2.c2>1;

#case
select c1, c1+c2 from t1 where c1 > 100 order by c2 + c1;

#case
select t1.c1 from t1, t2, t3 where t1.c1=t2.c1 and t1.c2>t2.c2 and t2.c3=t3.c3 and t3.c1>10;

#case
select t1.c1, t2.c2, t2.c3 from t1,t2 where t1.c1 = t2.c1;

#case
select * from t1, (select c1, c2, c3 from t2) as ss where ss.c1 = t1.c1;
select t1.c1, t1.c2, t2.c1, t2.c2, t2.c3 from t1, t2 where t2.c1 = t1.c1;

#case
select t1.c1 from t1 right join t2 on t1.c1 = t2.c1;

#case
select s1.c1, s2.c3 from (select c1 from t1) as s1, (select * from t2) as s2;
select t1.c1, t2.c3 from t1, t2;

#case
select t1.c1, t1.c2, ss.c1, ss.c2 from t1, (select * from t2)as ss where t1.c1 = ss.c1;
select t1.c1, t1.c2, t2.c1, t2.c2 from t1, t2 where t1.c1 = t2.c1;

#case
select t1.c1, ss.c3 from t1, (select c1, c2, c3 from t2) as ss;
select t1.c1, t2.c3 from t1, t2;

#case
select t1.c2, ss.c3, ss.c2 from t1, (select c3, c2 from t2)as ss where t1.c1 = ss.c3;
select t1.c2, t2.c3, t2.c2 from t1, t2 where t1.c1 = t2.c3;

#case
select tt.c1, tt.c2, t1.c2 from t1, (select * from t2) as tt where tt.c1 = t1.c1 ;
select t2.c1, t2.c2, t1.c2 from t1, t2 where t1.c1 = t2.c1;

#case
select s1.c1, s2.c2  from (select * from t1) as s1, (select c1, c2, c3 from t2) as s2 where s1.c1 = s2.c1;
select t1.c1, t2.c2  from t1, t2 where t1.c1 = t2.c1;

#case
select s1.c2, s2.c1, s3.c3 from (select * from t1) as s1, (select c1, c2 from t2) as s2, (select c2, c3 from t3) as s3 where s1.c1 = s2.c1 and s2.c2 = s3.c2;
select t1.c2, t2.c1, t3.c3 from t1, t2, t3 where t1.c1 = t2.c1 and t2.c2 = t3.c2;

#case
select t1.c1, s2.c3, t3.c2 from t1, (select * from t2) as s2, t3 where t1.c1 = s2.c1 and s2.c3 = t3.c3;
select t1.c1, t2.c3, t3.c2 from t1, t2, t3 where t1.c1 = t2.c1 and t2.c3 = t3.c3;

#case
select s1.c1, t2.c2, s3.c3 from (select c1 from t1) as s1, t2, (select * from t3) as s3 where s1.c1 = t2.c1 and t2.c3 = s3.c3;
select t1.c1, t2.c2, t3.c3 from t1, t2, t3 where t1.c1 = t2.c1 and t2.c3 = t3.c3;

#case  test merge conditions in sub query
select ss.c1, ss.c2 from (select * from t1 where c2 > 0) as ss;
select t1.c1, t1.c2 from t1 where t1.c2 > 0;

#case  test where conditions in sub query
select s1.c1, s2.c2 from (select * from t1 where c2 > 0)as s1, (select * from t2 where c2 > 0)as s2 where s1.c2 = s2.c2;
select t1.c1, t2.c2 from t1, t2 where t1.c2 > 0 and t2.c2 > 0 and t1.c2 = t2.c2;

#case
select s2.c1 from (select * from t2 where c2 > 0) as s2;
select c1 from t2 where c2 > 0;

#case
select * from (select * from t1 where c1 > 0) as s1, t2 where s1.c1 = t2.c2;
select * from t1, t2 where t1.c1 > 0 and t1.c1 = t2.c2;

#case
select * from (select (c1+1) as k from t1) as s1;
select c1 +1 from t1;

#case
select k+1 from (select (c1+1) as k from t1) as s1;
select c1+1 from t1;

#case
select * from (select (c1+2) as k from t1) as s1 where k=3;
select c1+2 from t1 where c1+2=3;

#case
select * from (select (c1+2) as k from t1) as s1, t2 where k=c2;
select t1.c1+2, t2.c1, t2.c2, t2.c3 from t1, t2 where t1.c1+2=t2.c2;

#case
select * from (select (c1+2) as k from t1) as s1, t2 where k+3=c2;
select t1.c1+2, t2.c1, t2.c2, t2.c3 from t1, t2 where t1.c1+2+3=t2.c2;

#case
select k + 1, t2.c2 from (select (c1 + 2) as k from t1) as s1, t2 where k  = t2.c1;
select t1.c1 + 3, t2.c2 from t1, t2 where t1.c1 + 4 = t2.c1;

#case
select k + 1, t2.c2 from t2, (select (c1 + 2) as k from t1) as s1 where k + 2 = t2.c1;
select t1.c1 + 3, t2.c2 from t1, t2 where t1.c1 + 4 = t2.c1;

#case
select * from (select (c1 + 1) as k1 from t1) as s1, (select (c1 - 1) as k2 from t2) as s2 where k1 = k2;
select t1.c1 + 1, t2.c1 - 1 from t1, t2 where t1.c1+1=t2.c1 -1;

#case
select * from (select (c1 + 1) as k1 from t1 where c1 > 1) as s1, (select (c1 - 1) as k2 from t2) as s2 where k1 = k2;
select t1.c1 + 1, t2.c1 - 1 from t1, t2 where t1.c1 > 1 and t1.c1 + 1= t2.c1 - 1;

#case
select * from (select (c1 + 1) as k1, (c2 + 1) as k2  from t1 where c1  > 0) as s1, (select (c1 - 1) as d1, (c2 - 1) as d2  from t2) as s2 where k1 = d1 and k2=d2;
select t1.c1 + 1, t2.c2 + 1, t2.c1 - 1, t2.c2 - 1 from t1, t2 where t1.c1 + 1 = t2.c1 - 1 and t1.c2 + 1 = t2.c2 - 1;

#case
select k from (select (c1+1) as k from t1) as s1, t2 where k + 10 = 2;
select k + 1, t2.c2 from (select (c1 + 2) as k from t1) as s1, t2 ;

#case
select t1.c1 + 3, t2.c2 from t1, t2 where t1.c1 +2+2 =t2.c1;

#case
select (k + 1) as mmm from (select (c1 + 1) as k from t1 where c1 < 1) as s1, (select (c1 + 1) as d from t2 ) as s2 where k+2=d;
select t1.c1 + 1 + 1 from t1, t2 where t1.c1 + 1 + 2 = t2.c1 + 1 and t1.c1 < 1;

#case
select (k + 1) as mmm from (select (c1 + 1) as d from t2 ) as s2, (select (c1 + 1) as k from t1 where c1 < 1) as s1 where k+2=d;
select t1.c1 + 1 + 1 from t1, t2 where t1.c1 + 1 + 2 = t2.c1 + 1 and t1.c1 < 1;

###case when then sql
select case t1.c1 when 1 then (2) else (3) end from t1;

###case complex case when then
select case k1 when 1 then (2) else (3) end from (select (c1 + 1) as k1, (c2 + 1) as k2  from t1 where c1  > 0) as s1, (select (c1 - 1) as d1, (c2 - 1) as d2  from t2) as s2 where k1 = d1 and k2=d2 group by k1;
select t1.c1 + 1, t2.c2 + 1, t2.c1 - 1, t2.c2 - 1 from t1, t2 where t1.c1 + 1 = t2.c1 - 1 and t2.c2 + 1 = t2.c2 - 1;

##group by alias case
select * from (select (c1 + 1) as k1, (c2 + 1) as k2  from t1 where c1  > 0) as s1, (select (c1 - 1) as d1, (c2 - 1) as d2  from t2) as s2 where k1 = d1 and k2=d2 group by k1;
select t1.c1 + 1, t2.c2 + 1, t2.c1 - 1, t2.c2 - 1 from t1, t2 where t1.c1 + 1 = t2.c1 - 1 and t2.c2 + 1 = t2.c2 - 1;

##group by alias case
select max(k1) from (select (c1 + 1) as k1, (c2 + 1) as k2  from t1 where c1  > 0) as s1, (select (c1 - 1) as d1, (c2 - 1) as d2  from t2) as s2 where k1 = d1 and k2=d2 group by k1;
select t1.c1 + 1, t2.c2 + 1, t2.c1 - 1, t2.c2 - 1 from t1, t2 where t1.c1 + 1 = t2.c1 - 1 and t2.c2 + 1 = t2.c2 - 1;

##sys func alias case
select lnnvl(k1) from (select (c1 + 1) as k1, (c2 + 1) as k2  from t1 where c1  > 0) as s1, (select (c1 - 1) as d1, (c2 - 1) as d2  from t2) as s2 where k1 = d1 and k2=d2 group by k1;
select t1.c1 + 1, t2.c2 + 1, t2.c1 - 1, t2.c2 - 1 from t1, t2 where t1.c1 + 1 = t2.c1 - 1 and t2.c2 + 1 = t2.c2 - 1;

###joined table
select * from (select t1.c1 from t1 join t2 on t1.c1 = t2.c1, t3, t1 as tt) as view;

###joined table
select * from (select t1.c1 from t1 left join t2 on t1.c1 = t2.c1, t3, t1 as tt) as view;

###joined table
select * from (select t1.c1 from t1 join t2 on t1.c1 = t2.c1, t1 as tt, t1 as tt2 join t3 as tt3 on tt2.c1 = tt3.c2) as view;

#########################################################limit combine case TODO
###limit case
###select * from (select * from t1 limit 2, 90) as s1 limit 1, 10;
###select * from t1 limit 3, 10;
