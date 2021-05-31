##### test for replace is NULL condition #####
select * from t1 where c1 is NULL;
select * from t10 where c3 is NULL;
select * from t1 where c2 = 2 and c1 is NULL;
select * from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);
select * from t1 inner join t3 on (t1.c1 = t3.c1) where t1.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);
## replace IS NULL condition after view merge
select * from (select * from pt3)  as tt where c6 = (select t2.c1 from t2 where c6 is NULL limit 1);
## replace IS NULL conditin after eliminator outer join
select * from t1 right join t3 on (t1.c1 = t3.c1) where t1.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);
select * from t3 left join t1 on (t1.c1 = t3.c1) where t1.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);
select * from t1 where c1 is NOT NULL;
## test for bug #8093018
select * from t1 as a inner join (select * from t1) as b using(c1) where a.c1 is not null;
select * from t1 as a inner join (select * from t1) as b using(c1) where a.c1 is not null;

## should not replace ##
select * from t11 where c3 is NULL;
select * from t11 where c2 is NULL;
select * from t1 where c2 = 2 or c1 is NULL;
select * from t3 left join t1 on (t1.c1 = t3.c1) where t1.c1 is NULL;
select * from t1 right join t3 on (t1.c1 = t3.c1) where t1.c1 is NULL;
select * from t1 full outer join t3 on (t1.c1 = t3.c1) where t1.c1 is NULL;
select * from (select * from t4) as tt where c1 is NULL;
select * from t1 right join t3 on (t1.c1 = t3.c1) where t3.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);
select * from t3 left join t1 on (t1.c1 = t3.c1) where t3.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);
select * from t3 left join (t1 inner join t4 on (t1.c2 = t4.c1)) on (t1.c2 = t3.c1) where t3.c1 = (select t2.c1 from t2 where t1.c1 is NULL limit 1);




##### test for simplify transform for subquery #####
select * from (select c1 from t1 union all select c1 from t2 limit 1) as tt;
select * from t2 where t2.c2 in (select c1 from t1 union all select c1 from t2 limit 1);
select * from ((select c1 from t1 order by c2) union (select c1 from t2)) as tt;
select * from t2 where t2.c2 in ((select c1 from t1 order by c2) union (select c1 from t2));
select * from (select c1 from t1 order by c1, c1) as tt;
select * from t2 where c2 in (select c1 from t1 order by c1, c1);
select * from t2 where c2 = (select c1 from t1 order by c1, c1 limit 1);
select * from (select c1 from t1 group by c1 desc) as tt;
select * from t2 where c2 in (select c1 from t1 group by c1 desc);

##### test for add limit to union all #####
## should be add
select c1 from t1 union all select c1 from t2 limit 1;
select c1 from t1 union all select c1 from t2 order by c1 limit 1;
select c2 from t2 union all select c2 from t3 order by c2 limit 1;
(select c2, c3 from t2 limit 1) union all (select c2, c3 from t3 order by c3) limit 1;
(select c2, c3 from t2 order by c3) union all (select c2, c3 from t3 order by c2) limit 1;
(select c2, c3 from t2) union all (select c2, c3 from t3 order by c3) order by c2 limit 1;
## should not be add
select c1 from t1 union all select c1 from t2;
select c1 from t1 union select c1 from t2 limit 1;
select SQL_CALC_FOUND_ROWS c1 from t1 union all select c1 from t2 limit 1;
select SQL_CALC_FOUND_ROWS c1 from t1 union all select c1 from t2 union all select c1 from t2 limit 1;
select c1 from t1 union all select c1 from t2 limit 1, 10;
select c1 from t1 union distinct select c1 from t2 order by c1;

########################################################
##
##case sub query in sub view cannot be pulled up
## TODO merge view firstly, and then pull up not right
##select * from (select * from t1 where c1 > ANY(select c1 from t2 where c1 > 0 group by c2)) as v, (select * from t2) as v2;
##select * from (select * from t1 where c1 > 0) as v;
##select * from (select * from t1 where c1 > 0) as v;
##select * from (select * from t1 where c1 > 0) as v where v.c1 in (select c1 from t2);
##select * from (select * from t1 where c1 > 0) as v where v.c1 in (select c1 from t2);
##select * from (select * from t1 where c1 > ANY(select c1 from t2 where c1 > 0 group by c2)) as v where v.c1 in (select c1 from t2);
##
##case alias used in parent order by clause
##select * from ( select c1 as b from t1) as t2 order by b;
##select * from ( select c1 as b from t1) as t2 order by b;
##select * from ( select c1 from t1) as t2 order by c1;
##select * from ( select c1 from t1) as t2 order by c1;
##select * from t1 order by c1;
##select * from t1 order by c1;
##select count(*) from (select count(*) from t1 ) t2;
##select count(*) from (select count(*) from t1 ) t2;
##select count(*) from (select count(*) from t1 ) t2;
##select count(*) from (select count(*) from t1 ) t2;
##select * from tt1, (select * from tt1) as v1 where tt1.a=v1.a and tt1.b=1;
##select * from tt1, (select * from tt1) as v1 where tt1.a=v1.a and tt1.b=1;
##select * from t1,v1 where a=va and vb=(select max(vb) from v1);
##
#TODO where pull up plan not right
select c1 from t1 where c1 not in(select c1 from t2 where c2 not in (select c2 from t2));
##select c1 from t1 where c1 not in (select c1 from t2);

##select sum(c1 * c2) as cc1 from t1 where c1 > (select 1+1);
##select sum(c1 * c2) as cc1 from t1 where c1 > (select 1+1);
##case multi table in where sub query, test semi join order pull up fail ?
##select * from t1 where t1.c1 > any(select t3.c2 from t2, t3 where t2.c1 > any(select t4.c1 from t4, t5 where exists(select c1 from t1)));
##select * from t1 where t1.c1 > any(select t3.c2 from t2, t3 where t2.c1 > any(select t4.c1 from t4, t5 where exists(select c1 from t1)));

##case test pull up part in child join
select * from (select * from pt2 left join pt3 on pt2.c1 = pt3.c4) as v;
select * from pt2 left join pt3 on pt2.c1 = pt3.c4;

##case rel id set, alias table item
select t1.c1 from t1, (select c2 from t1 where 1 = any(select t3.c2 from t3 where t3.c2 > t1.c1)) as v;

##case change the right outerjoin to left outer
SELECT X.c1, X.c2, Y.c2  FROM t1 AS X RIGHT OUTER JOIN t1 AS Y ON X . c2 = Y . c2 WHERE X . c1 < -10;

##case change the right outerjoin to left outer
SELECT X.c1, X.c2, Y.c2  FROM t1 AS X RIGHT OUTER JOIN t1 AS Y ON X . c2 = Y . c2 RIGHT OUTER JOIN t1 as Z on Y.c1 = Z.c1 WHERE X . c1 < -10;

##case adjust ambiguis table in subquery, and adjust table
select c from v2 where exists (select * from tt2 where a=2 and b=c);

##case adjust expr copy with all_exprs_ container
select first.col2 from nv1 first where first.col2 in (select second.c2 from t1 second where first.col1 and first.col2);

##case adjust expr copy with all_exprs_ container
select first.col2 from nv1 first where first.col2 in (select second.c2 from t1 second where first.col1 + first.col2);

##case adjust expr copy with all_exprs_ container
select first.col2 from nv1 first where first.col2 in (select second.c2 from t1 second where first.col1);

##case multi table in where sub query, test semi join order
select * from t1, t2  where t1.c1 > any(select t3.c2 from t2, t3 where t2.c1 > any(select t4.c1 from t4, t5));

##case multi table in where sub query, test semi join order
select * from t1, (select * from (select * from t2) as v where v.c1 in (select c2 from t2)) as vv where t1.c1 in (select c2 from t2);

##case multi table in where sub query, test semi join order
select * from t1 where t1.c1 > any(select t3.c2 from t2, t3 where t2.c1 > any(select t4.c1 from t4, t5));
##case multi table in where sub query, test semi join orde

##case multi table in where sub query, test semi join order
select * from t1 where t1.c1 > any(select t3.c2 from t2, t3);

##case multi table in where sub query, test semi join order
select * from t1 where t1.c1 > any(select t3.c2 from t2, t3, t1 as v);

##case order by alias column in from sub query
select t1.c1 from t1, (select * from t2) as v order by v.c1;
select t1.c1 from t1, t2 order by t2.c1;

##case merge and together
#select * from t1, (select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end) as v where v.c1 = t1.c1;
#select * from t1, (select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end) as v where v.c1 = t1.c1;

##case when logical plan generate
select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;

##case when logical plan generate
select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case t2.c2 when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;

##case when logical plan generate
select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;

##case when logical plan generate
select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 > case t2.c2 when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;

##case test alias same table
select * from (select tt.c1 from (select t2.c1 from t2 join t1 on t1.c1 = t2.c1) tt join t2 on tt.c1 = t2.c1) as v1 join t2 on v1.c1 = t2.c1;

##case set the alias new id correctly
select tt.c1 from (select t2.c1 from t2 join t1 on t1.c1 = t2.c1) tt join t2 on tt.c1 = t2.c1;

##case case outerjoin should NOT be eliminated
SELECT * FROM t1 LEFT JOIN t2 ON t1.c1=t2.c2 WHERE (lnnvl(t1.c1=30 and t2.c2=1));

##case case outerjoin should NOT be eliminated
SELECT * FROM t1 LEFT JOIN t2 ON t1.c1=t2.c2 WHERE not(0+(t1.c1=30 and t2.c2=1));

##case case outerjoin should NOT be eliminated
SELECT * FROM t1 LEFT JOIN t2 ON t1.c2<>0 WHERE t1.c1=1 AND t2.c1<=>NULL;

##case case outerjoin should NOT be eliminated
SELECT * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.c1 WHERE t1.c1 NOT BETWEEN t2.c2 AND t1.c2;

####case when logical plan generate
##select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##
####case when logical plan generate
##select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case t2.c2 when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case t2.c2 when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##
####case when logical plan generate
##select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##
####case when logical plan generate
##select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 > case t2.c2 when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;
##select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 > case t2.c2 when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end;

##cases test insert stmts having column ref
INSERT INTO bt1 VALUES(SUBSTR(f1, 1, 3),1);

##case const expr adjust
select * from (select 'a' as c1 from t1 ) as t2 where  c1 > 'b';

##case const expr adjust
select * from (select 1 as c1 from t1 ) as t2 where  c1 > 0;

##case sub query in select view
select * from (select c1, c2, c1 in (select c1+2 from t1), c1 = all(select c1 from t1) from t1) as v;
select c1, c2, c1 in (select c1+2 from t1), c1 = all(select c1 from t1) from t1;

##case
select * from vv1;


##case same table id merged to parent stmt
select * from (select * from t1) as v1, (select * from t1) as v2 where v1.c1 = v2.c2;
select * from t1 as v1, t1 as v2 where v1.c1 = v2.c2;

##case
select * from (select * from t2) as v where v.c1 in (select c2 from t2);

##case
select * from t1, (select * from (select * from t2) as v where v.c1 in (select c2 from t2)) as vv;

##case
select * from t1, (select * from (select * from t2) as v where v.c1 in (select c2 from t2)) as vv where t1.c1 in (select c2 from t2);

##TODO case where pull up same table entry exist error
select * from t1, (select * from t2) as v where t1.c1 > any (select c1 from t2);
select * from t1, t2 where t1.c1 > any (select c2 from t2);

##TODO case where pull up same table entry exist error
select * from t1, (select * from t2) as v where exists(select c2 from t2 where t2.c1 = t1.c1);
select * from t1, t2 where t1.c1 > exists(select c2 from t2 where t2.c1 = t1.c1);

##TODO case where pull up same table entry exist error
select * from t1, (select * from t2) as v where exists(select c2 from t2 where t2.c1 = t1.c1);
select * from t1, t2 where t1.c1 > exists(select c2 from t2 where t2.c1 = t1.c1);

##TODO case where pull up same table entry exist error
select * from t1, (select * from t2) as v where exists(select c1 from t2 where t2.c1 = t1.c1);
select * from t1, t2 where t1.c1 > exists(select c1 from t2 where t2.c1 = t1.c1);

#################################################################################################

##case
select * from t1, t2 where not exists(select c1 from t3 where c1 > 5 group by c1);

##case that has sub query cannot be pull up
select * from (select * from t1 where c1 + c2 in (select c2 from t3) and not exists(select c1 from t2 where c1 > 5 group by c1)) as v where v.c1 in (select c3 from t2);
select * from t1 where c1 + c2 in (select c2 from t3) and not exists(select c1 from t2 where c1 > 5 group by c1);

##case same table exist in multi level
select * from (select k * 2 as mm from (select c1 + c2 as k from t1 where c2 > 0) as v inner join t1 on v.k = t1.c1  where k < 10) as vvv where mm = 9;
select t1.c1 + t1.c2 * 2 from t1, t1 as v1 where t1.c1 + t1.c2  = v1.c1 and t1.c1 + t1.c2 < 10 and t1.c1 + t1.c2 * 2 = 9;

#case alias in sub-view used in order by clause
select * from ( select c1 as b from t1) as t2 order by b;
select * from t1 order by c1;

#case
select max(k) from (select c1 * c2 as k from t2 where c2 in (select c2 from t3)) as v;
select max(c1 * c2) from t2 where c2 in (select c2 from t3);

#case
select * from t1, (select * from t3 where c2 > 0) as v1 where v1.c1 in (select c1 from t2);
select * from t1, t3 where t3.c1 in (select c1 from t2);

##case that same table exist in parent stmt
select * from t1, (select * from t1) as v where t1.c1>v.c1;
select * from t1, t1 as v where t1.c1 > v.c1;

###this is the case that need to pull up semi info
select * from (select * from t1 where c1 >all(select c2 from t2)) as v;
select * from t1 where c1 >all(select c2 from t2);

####case test merge table partition info
###select * from (select * from t1 partition (p1)) as v;
###select * from t1 partition (p1);

##case test simplify operation on stmt, eg remove duplicates order items
select * from t1 order by c1 desc, c1;
select * from t1 order by c1 desc;

##case test simplify operation on stmt, eg remove duplicates order items
select * from t1 order by c1 desc, c1 asc;
select * from t1 order by c1 asc;

##case test simplify operation on stmt, eg remove duplicates order items
select * from t1 order by c1 desc, c2, c1;
select * from t1 order by c1 desc, c2;

###case
###select * from t1, (select * from t2) as v where t1.c1 > any (select c1 from t2);
###select * from t1, t2 where t1.c1 > any (select c2 from t2);


#case
select * from t1, (select * from t1 as v) as vv;
select * from t1, t1 as v;

#case
select * from t1, t1 as v inner join t2 on v.c1 = t2.c1;
select * from t1, t1 as v, t2 where v.c1 = t2.c1;

#case
select * from t1, (select * from t1 ) as v1;
select * from t1, t1 as v1;

#case
#select * from t1, t2, (select * from t1, t2) as v;
select * from t1, t2, t1 as v1, t2 as v2;

###############
##case
select * from t1 join (select * from t2) as v on t1.c1 = v.c1 left join t3 on t1.c1 = t3.c1 where t3.c2 is not null;
select * from t1, t2, t3 where t1.c1 = t2.c1 and t1.c1 = t3.c1 and t3.c2 is not null;

##case
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2 where t3.c1 is not null and t2.c3 is not null;
select * from t1, t2, t3 where t3.c1 is not null and t1.c2 = t3.c2 and t2.c3 is not null;

#case
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2 where t3.c1 is not null;
select * from t1 left join t2 on t1.c1 = t2.c1, t3 where t3.c1 is not null and t1.c2 = t3.c2;

#case
select * from t1 join t2 on t1.c1 = t2.c1;
select * from t1, t2 where t1.c1 = t2.c1;

#case can be eliminate
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1;
select * from t1, t2, t3 where t1.c1 = t2.c1 and t2.c1 = t3.c1;

#case
select * from t1 left join t2 on t1.c1 = t2.c1 where t2.c1 is not null;
select * from t1, t2 where t2.c1 is not null and t1.c1 = t2.c1;


#case
select * from t1 left join t2 on t1.c1>1 where t2.c1 is not null;
select * from t1, t2 where t1.c1 > 1 and t2.c1 is not null;

#case
select t1.c1 from t1 left join t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2 where t2.c2 is not null;
select t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c2 = t2.c2 and t2.c2 is not null;

#case
select * from t1 inner join t2 on t1.c1>1 where t2.c3 is not null;
select * from t1, t2 where t1.c1 > 1 and t2.c3 is not null;

#case
select * from t1 inner join t2 on t1.c1>1 where t2.c2 is not null;
select * from t1, t2 where t1.c1 > 1 and t2.c2 is not null;

#case
select * from t1 inner join t2 on t1.c1 = t2.c1 where t2.c1 is not null;
select * from t1, t2 where t1.c1=t2.c1 and t2.c1 is not null;

#case
select t1.c1 from t1 inner join t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2 where t2.c2 is not null;
select t1.c1 from t1, t2 where t1.c1=t2.c1 and t1.c2=t2.c2 and t2.c2 is not null;

#case
select * from t1 inner join t2 on t1.c1>1 where t2.c1 is not null;
select * from t1, t2 where t1.c1 > 1 and t2.c1 is not null;

#case
select * from t1 inner join t2 on t1.c1>1 where t2.c2 is not null;
select * from t1, t2 where t1.c1 > 1 and t2.c2 is not null;

#case
select * from t1 inner join t2 on t1.c1>1 where t2.c3 is not null;
select * from t1, t2 where t1.c1 > 1 and t2.c3 is not null;
################################################################################################################
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

##bug6518874 alias
select alias from (select c1 as alias, c2 from t1) a where alias in (select c1 from t1);

select alias from (select lnnvl(c1) as alias, c2 from t1) a where alias in (select c1 from t1);

select alias from (select floor(c1 + c2) as alias, c2 from t1) a where alias in (select c1 from t1);

select alias from (select floor(c1+c2) as alias, c2 from t1 where c2 > (select 1) ) a where alias in (select c1 from t1);

##case norewrite hint test
select /*+ NO_REWRITE */ * from (select * from t1) as v;

select /*+ NO_REWRITE*/ * from t1 where c1 In (select c1 from t2);


###add partition view test
select * from part_view where part_view.k1 > 1;

select * from part_view where part_view.k1 + 1 > 1;

select k1 as newk from part_view where k1 > 0;

select k1 as newk from part_view where k1=2;

select k1 as newk from part_view where k1=3;

###
select nn as newk from (select k1 as nn from part_view) as tv where nn=3;

##with subquery partion key
select nn as newk from (select k1 as nn from part_view) as tv where exists(select c1 from t2 where t2.c1 = nn) and nn = 2;

##
select * from part_view as v1, part_view as v2 where v1.k1 = 1 and v2.k1 = 2;
select * from (select * from part_view) as v1, (select * from part_view) as v2 where v1.k1 = 1 and v2.k1 = 2;

##
select * from t1 where c2="1" order by c2;
select /*+leading(t1, t2) use_nl(t1, t2)*/ * from t1,t2 where t1.c2=t2.c2 order by t1.c2, t2.c2;

## Do simplification for where v.a = ?; In this situation, v.a can be replaced with const
select * from t2 where c3 = 'abc';
select c2 from t2 where c3 = 'abc' group by c2, c3 order by c3, c2;
select c2 from t2 where c3 = 'abc' group by c2, c3 order by c3, c2, c3, c3 desc;
select c2 from t2 where c3 = 'abc' and c1 = 4 group by c2, c3 order by c3, c2;
select c2 from t2 where c3 = 'abc' and c1 = 4 and c2 = 5 and c2 = 6 and c2 = 7 group by c2, c3 order by c3, c2;
select c1,c2,c3,c2+1 from t2 where c3 = 'abc' and c1 = 4 and c2 = 5 and c2 = 6 and c2 = 7 group by c2, c3 order by c3, c2;
select c2 from t2 where c3 = 'abc' and (c1 = 4 or c3 = 'abc') group by c2, c3 order by c3, c2;
select c2 from t2 where c3 = 'abc' and c1 = 4 group by c2, c3 having count(c2) > 0 order by c3, c2;
select c2 from t2 where c3 = 'abc' and c1 = 4 group by c2, c3 having c2 > 0 order by c3, c2;
select c2 from t2 where c3 = 'abc' and c1 = 4 group by c2+1, c3 order by c3+1, c2;
select c2 from t2 where c3 = 'abc' and c1 = 4 or c2 = 5 group by c2, c3 order by c3, c2;
select c2, c1, c2, c1 from t2 where c3 = 'abc' and c1 = 4 or c2 = 5 group by c2, c3, c2, c2 desc, c3 order by c3, c2, c3, c3 desc, c2;
select * from t2, (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;
select * from (select c2 from t3 where c3 = 'abc' group by c2, c3 order by c3, c2) v2, t2, (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v where v.c3 > 0 and v2.c2 < 5 order by v.c3, v.c3+1, t2.c1;
select * from t2, (select c3,c1 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;
select * from t2 inner join (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v on t2.c2 = v.c3 where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;
select * from t2 inner join (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v using(c3) where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;
select * from t2 left join (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v on t2.c2 = v.c3 where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;
select * from t2 right join (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v on t2.c2 = v.c3 where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;
select t3.c2, v2.c3 from t3, (select t2.c2, v.c3 from t2, (select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v where v.c3 > 0 group by t2.c2, v.c3 order by t2.c2, v.c3) v2 where t3.c1 = v2.c3 group by v2.c3, t3.c2 having v2.c3 > 0 order by v2.c3, t3.c2;
select * from t2 right join (select c2, c3 from t5 where c3 = 'abc' and c1 > 10 and c1 = 15 group by c2, c3 order by c3, c2) v on t2.c2 = v.c3 where v.c3 > 0 and v.c3 < 10 order by v.c3, v.c3+1, t2.c1;
select * from t2, (select c2, c3 from t5 where c3 = 'abc' and c1 > 10 and c1 = 15 group by c2, c3 order by c3, c2) v, t3 where t2.c2 = v.c3 and v.c3 > 0 and v.c3 < 10 order by v.c3, v.c3+1, t2.c1;
select * from (select c2, c3 from t5 where c3 = 'abc' and c1 > 10 and c1 = 15 group by c2, c3 order by c3, c2) v left join t2 on t2.c2 = v.c3 where v.c3 > 0 and v.c3 < 10 order by v.c3, v.c3+1, t2.c1;
select t4.c2, v2.c3 from (select t2.c2, v.c3 from t2, (select c2, c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v, t3 where v.c3 > 0 group by t3.c2, v.c3 order by t3.c2, v.c3) v2, t4 where t4.c1 = v2.c3 group by t4.c2, v2.c3 having v2.c3 > 0 order by t4.c2, v2.c3;
select * from t2 inner join (select c3 as a1, c3 as a2 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v on t2.c2 = v.a1 where v.a1 > 0 group by v.a1, v.a2, t2.c2 order by v.a1, v.a1+1, v.a2, t2.c1;
select * from t2 left join ((select c3 from t5 where c3 = 'abc' group by c2, c3 order by c3, c2) v left join t3 on v.c3 = t3.c3) on t2.c3 = v.c3 where v.c3 > 0 order by v.c3, v.c3+1, t2.c1;

select * from t12 where c1=c2 and c1=cast('2010-10-10 00:00:00' as datetime);

select * from t1 where c2>(select c2 from t1 order by c2);

select * from t1 where c2>(select c2 from (select * from t1 order by c2) v);

select * from t1 where c2>(select c2 from t1 order by c2, (select c2 from t1));

select * from (select * from t1 order by c2) v;

select * from (select * from (select * from t1 order by c2) v) vv;

select * from t1 having c2>(select c2 from t1 order by c2);

select * from t1 having c2>(select c2 from (select c2 from t1 order by c2) v);

select * from t1 order by (select c2 from t1 order by c2);

select * from t1 order by (select c2 from (select c2 from t1 order by c2) v);

select * from t1 group by (select c2 from t1 order by c2);

select * from t1 group by (select c2 from (select c2 from t1 order by c2) v);

select * from t1 where c2>(select c2 from t1 order by c2 limit 1);

select * from (select c1 + c2 as c1 from t1 limit 5) a where exists (select c1 from t1 b where a.c1 = b.c1);
