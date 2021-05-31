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
