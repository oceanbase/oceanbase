# simple test
select max(c1) from t1;

select min(c1) from t1;

# select alias test
select max(c1) as max from t1;
select min(c1) as min from t1;

# index test
select max(c2) from t9;
select max(c3) from t10;
select * from t9 where c2 = (select max(c2) from t9);

# table alias test
select max(c1) from t1 as tmp;
select c1 from t1 where c1 > (select max(c1) from t1);

# having test
select max(c1) as max from t1 having max(c1) > 1;
select min(c1) as min from t1 having min(c1) > 1;
select max(c1) as max from t1 having max(c1);
select min(c1) as min from t1 having true;
select max(c1) from t7 having false;

# distribute test
select max(c1) from pt2;
select max(c1) as max from pt2;
select min(c1) from pt2;
select min(c1) as min from pt2;
select * from (select max(c1) from pt2) as tmp_table;
select * from t2 where t2.c1 > (select max(c1) from pt2);
select * from (select min(c1) as min from pt2) as tmp_table;
select * from t2 where t2.c1 > (select min(c1) as min from pt2);

# test subquery
select * from (select max(c1) from t1) as tmp_table;
select * from t2 where t2.c1 > (select max(c1) from t1);
select * from (select max(c1) as max from t1) as tmp_table;
select * from t2 where t2.c1 > (select max(c1) as max from t1);
select * from (select min(c1) from t1) as tmp_table;
select * from t2 where t2.c1 > (select min(c1) from t1);
select * from (select min(c1) as min from t1) as tmp_table;
select * from t2 where t2.c1 > (select min(c1) as min from t1);


#do not transform max/min
select max(c1) from t7;
select min(c1) from t7;
select max(c1 + 1) + 1 from t1 limit 1;
select max(c1) from t1 limit 1;
select min(c1) from t1 limit 1;
select min(c1) as min from t1 limit 1;
select min(c1 + 1) + 1 from t1 limit 1;
select max(c1) as max from t1 limit 1;
select max(c1 + 1) + 1 as max from t1 limit 1;
select min(c1 + 1) + 1 as min from t1 limit 1;
select max(c1) as max from t1 having min(c1) > 1;
select min(c1) as min from t1 having max(c1) > 1;
select max(c1) from t1 having 1 in (select c1 from t2);
select * from (select * from t1 where c1 + c2 in (select c2 from t3) and not exists(select c1 from t2 where c1 > 5 group by c1)) as v where v.c1 in (select c3 from t2);
select max(c4) from t9;

# max/min(const) simplify
select min(999) from t7 having max(c1) > 0;
select min(999) from t7 group by c1;
select max(999+abs(-1)) from t7 group by c2;
select max(1+2) from t7 limit 10;
select max(1+2) from t7;
select min(1+2) from t7 where c2 = 1;
select max(1+2), 3 from t7;
select max(1+2) from t7,t8 where t7.c1 = t8.c1;
select min(1+2) from t7 inner join t8 on t7.c1=t8.c1;
select max(1+2) from t7 left join t8 using(c1);
select * from t7 where exists(select max(t7.c2) from t8);
select * from t7 where exists(select max(t7.c2 + 1) from t8);
select * from t7 where exists(select min(999+1) from t8 group by t8.c2);
select * from t7 where exists(select min(999+1) from t8 where t8.c1 = 1 and t8.c2 = 1 group by t8.c2);
select * from t7 where c1 in (select max(999+1) from t8);
select * from t7 where c1 in (select max(999+1) from t8 where t8.c1=2 and t8.c2=3);
select * from t7 where c1 > any(select min(999+1) from t8 group by t8.c2);
select max(1) from t7 order by 1;
select max(1+2) from t7 order by c1;
select max(1+2) from t7 order by c2,c3;
select max(1 + 1) from t7 group by c1 order by 1;
select min(1 + 1) from t7 group by c1 order by 1;

# test for any/all
select c1 from t1 where c1 > any (select c1 from t2);
select c1 from t1 where c1 < some (select c1 from t2);
select c1 from t1 where c1 > all (select c1 from t2);
select c1 from t1 where c1 < all (select c1 from t2);
select c1 from t1 where c1 in (select c1 from t2 where c2 >= some(select c1 from t3 where t1.c2=t3.c1));
select c1 from t1 where c1 > any (select c1 from t2 where c2 >= some(select c1 from t3 where t1.c2=t3.c1));
select c1 from t1 group by c1 > any (select c1 from t2);
select c1 from t1 having c1 < any (select c1 from t2);
select c1 from t1 order by c1 > all (select c1 from t2);
select c1 from t1 order by c1 < all (select c1 from t2);

# test for complex expr
select c1 from t1 where c1 + 1 > any (select c1 from t2);
select c1 from t1 where abs(c1) < some (select c1 from t2);
select c1 from t1 where c1 + (1 < any (select c1 from t2));

# test for any, column may be NULL
select c1 from t1 where c1 > any (select c2 from t9);
select c1 from t1 where c1 < any (select c2 from t9);

# transform prefix const column in index
select max(c3) from t9 where c2 = 1;
select max(c3) from t9 where 1 = c2;
select max(c3) from t9 where c2 is TRUE;
select max(c3) from t9 where c2 = 1 and c1 = 1;
select max(c3) from t9 where c2 != 1;
select max(c3) from t9 where c2 >1;
select max(c3) from t9 where c2 <1;

# do not transform any/all
select c1 from t1 where c1 > any (select c2 from t9);
select c1 from t1 where c1 > any (select c1 + 1 from t2);
select c1 from t1 where c1 > any (select c1 from t2 having c1 > 1);
select c1 from t1 where c1 > any (select max(c1) from t2 group by c1);
select c1 from t1 where c1 > any (select c1 from t2 union select c1 from t2);
select c1 from t1 where c1 > all (select c2 from t9);
select c1 from t1 where c1 < all (select c2 from t9);

SELECT MIN(1) FROM agg_t2 ORDER BY (SELECT AVG(c2) FROM agg_t1);

SELECT MIN(DISTINCT OUTR . `col_varchar_10`) AS X FROM B AS OUTR WHERE (OUTR . `col_varchar_10_key` , OUTR . `col_varchar_20_key`) IN (SELECT DISTINCT INNR . `col_varchar_10` AS X , INNR . `col_varchar_10_key` AS Y FROM CC AS INNR2 LEFT JOIN CC AS INNR ON (INNR2 . `col_int` > INNR . `pk`) WHERE INNR . `pk` = INNR . `col_int_key` AND OUTR . `col_datetime` IS NOT NULL) AND OUTR . `col_varchar_20` = 'b' HAVING X = NULL ORDER BY OUTR . `col_date_key` , OUTR . `pk`;


# transform with some constant expressions
select 5, max(c2) from t9;
select max(c2), 5 from t9;
select 5, min(c2) from t9;
select min(c2), 5 from t9;
select 5, 5, min(c2), 7 + 5, floor(5) from t9;
select abs(-55), max(c2), 3 + 2, 5 from t9;

# do not transform with more than one non-constant
select 5, max(c2), c1 from t9;
select c1, min(c2), 233333 from t9;

#bug
select * from (select c1 as col from t1) as L where col = (select max(c1) from t1 where L.col > 5);
select * from (select a.acctnbr as l1, a.timeuniqueextn as l2  from cb_loan_acctacctstathist_11 a left join cb_loan_acctloaninfotemp_11 b on a.acctnbr = b.acctnbr where a.acctstatcd not in ('CLS', 'DENI') and b.acctnbr is null) as l where l.l2 = (select max(a2.timeuniqueextn) from cb_loan_acctacctstathist_11 a2 where a2.acctnbr = l.l1 and a2.postdate <= '2018-11-10');
