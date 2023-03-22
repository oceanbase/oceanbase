##case arithmatic and basic compare
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (v.c2 not between -10 and 10);

##case arithmatic and basic compare
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (v.c2 <=> null);

##case arithmatic and basic compare
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and ((v.c2 + 2 > 1) + 2 < 0);

##case should not be eliminated
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (not(v.c2 and false));

##case should not be eliminated
#select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (not((1 + 2 > v.c2) and false));
#select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (not((1 + 2 > v.c2) and false));

##case should not be eliminated
# TODO shengle pre calc expr extract and calc bug:
# select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (((1 + 2 > v.c2) and false) + 2 > -2);

##case arithmatic operator
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (1 + 2 > v.c2);

##case
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (1 + 2 > (v.c2 is not null));

##case
select * from t2 left join (select * from t1) as v  on t2.c1 = v.c2 where (v.c2 is null) and (1 + 2 > (v.c2 is not null) and (v.c1 is not null));

##case
select * from t1 left join t2 using(c1) where (t2.c1 is not null)=0;

##case
select * from t1 left join t2 using(c1) where (t2.c1 is not null) and (t2.c1 is null);

##case
select * from t1 left join t2 using(c1) where (t2.c1 is null) and (t2.c1 > 0);

##case null and false     and parent is not and
select * from t1 left join t2 using(c1) where (t2.c1 and false) < 1 + 1;

##case
select * from t1 left join t2 on t1.c1 > t2.c1 where lnnvl(t2.c2) and 1 > 0;

##case
select * from t1 left join t2 on t1.c1 > t2.c1 where lnnvl(t2.c2 + 2 - 1) and 1 > 0;

##case
select * from t1 left join t2 on t1.c1 > t2.c1 where t2.c1 + 1 > all(select c1 from t3 group by c2);

##case
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2 where t2.c1 > 0;

##case
select * from t1 left join t2 on t1.c1 = t2.c1 left join t3 on t1.c2 = t3.c2 where (case t2.c2 when 0 then 1 end) = 1;

## case
select * from t1, t1 as v join t2 on v.c1 = t2.c1;

##case
select * from t1 right join t2 on t1.c1 = t2.c1 right join t3 on t2.c2 = t3.c2;

#case
select * from t1 right join t2 on t1.c1 = t2.c1, t2 as v, t3 where t3.c2 is not null;

##case resolver only support one joined table
#select * from t1 right join t2 on t1.c1 = t2.c1, t2 as v left join t3 on v.c2 = t3.c2 where t3.c2 is not null;
#select * from t1 right join t2 on t1.c1 = t2.c1, t2 as v, t3 where v.c2 = t3.c2 and t3.c2 is not null;

##case same table alias twice
select * from t1 as table1 left join t1 as table2 on table1.c2 = table2.c1 where table1.c1 < table2.c1 or table1.c1 = table2.c2;

##case same table alias twice
select * from t1 as table1 left join t1 as table2 on table1.c2 = table2.c1 where table1.c1 < table2.c1 and table1.c1 = table2.c2;

########################################################################
### null-reject satisfied

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
#case
select * from t1 left join t2 on t1.c1=t2.c1 left join t3 on t1.c1=t2.c1;

#case
select t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2 left join t3 on t2.c1=t3.c1, t1 tt where t1.c1 is not null;

#case
select t1.c1 from t1,t2,t3, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;

#case
select t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2 left join t3 on t2.c1=t3.c1;

#case
select t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2 left join t3 on t2.c1=t3.c1, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;

#case
select t1.c1 from t1, t2 where t1.c1=t2.c1 limit 100;

#case
select sum(c1), count(*) from t1 group by c2 order by c2 limit 100;

#case
select c1 from t1 as t order by c1;

#case
select * from t2 left join t3 on t2.c1=t3.c1 and t2.c2<t3.c2 where t2.c3>t3.c3;

#case
select t1.c1 from t1,t2,t3 where t1.c1=t3.c1 and t1.c2=t2.c2 and t2.c3=t3.c3;

#case
select t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2 left join t3 on t2.c1=t3.c1 where t1.c1=t3.c1;

#case
select * from t1 left join t2 on t1.c1=t2.c1 where false;

#case
select * from t1 left join t2 on t1.c1>1 where t2.c1 is null and t2.c2>1;

#case
select * from t1 right join t2 on t1.c1 = t2.c2;

#case
select * from t1 inner join t2 on t1.c1 = t2.c2;

## case
select c1 from t1 where c1 > 0 and c2 < 0 order by c2 limit 100;

## case  join
select t1.c1 from t1, t2 limit 100;

## case  join with predicate
select t1.c1 from t1, t2 where t1.c1=t2.c1 limit 100;

#case
select * from t1 left join t2 on t1.c1 = t2.c1 ;

#case  join with predicate
select t1.c1 from t1, t2 where t1.c1=t2.c1 limit 100;

#case  3-table join
select t1.c1 from t1, t2, t3 where t1.c1=t2.c1 and t1.c2>t2.c2 and t2.c3=t3.c3 and t3.c1>10;

#case
select * from t2 left join t3 on t2.c1=t3.c1 and t2.c2<t3.c2 where t2.c3>t3.c3;

#case
select t1.c1 from t1 left join t2 on t1.c1=t2.c1 where t2.c2>1;

#case
select * from t1 left join t2 on t1.c1=t2.c1 and t1.c1>1;

#case
select * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c2 = t3.c2;

##case right join to left bug6621936
select * from t1 right join t2 on t1.c1 = t2.c1 right join t3 on t3.c2 = t1.c2 where t2.c2 = 1;

##case right join to left bug6621936
select * from t1 right join t2 on t1.c1 = t2.c1 right join t3 on t3.c2 = t2.c2 where t2.c2 = 1;

##case right join to left bug6621936
select * from t1 right join t2 on t1.c1 = t2.c1 right join t3 on t3.c2 = t2.c2 where t1.c2 = 1;

## case in bug6564603
select t1.c1 as a1, t2.c1 as a2 from t1 left join t2 on t1.c1 != t2.c1 where t2.c2 in (1, 2, 3);
select t1.c1 as a1, t2.c1 as a2 from t1 left join t2 on t1.c1 != t2.c1 where t2.c2 not in (1, 2, 3);
# should not do rewrite
select t1.c1 as a1, t2.c1 as a2 from t1 left join t2 on t1.c1 != t2.c1 where t1.c2 in (1, 2, t2.c2);
select t1.c1 as a1, t2.c1 as a2 from t1 left join t2 on t1.c1 != t2.c1 where t2.c2 in (1, 2, NULL);
select t1.c1 as a1, t2.c1 as a2 from t1 left join t2 on t1.c1 != t2.c1 where t2.c2 not in (1, 2, NULL);

## case multi outer join bug11493667
# left-deep
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 is NULL;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 = 0;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 is NULL;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 = 0;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 is NULL;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 = 0;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 IS NULL;
select * from t1 left join t2 on t1.c1 != t2.c1 left join t3 on t1.c1 != t3.c1 left join t4 on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 = 0;
# bushy
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 is NULL;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 = 0;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 is NULL;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 = 0;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 is NULL;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 = 0;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 IS NULL;
select * from (t1 left join t2 on t1.c1 != t2.c1) left join (t3 left join t4 on t4.c1 != t3.c1) on t1.c1 != t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 = 0;

## case full outer join el
# case
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 is NULL;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 = 0;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 is NULL;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 = 0;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 is NULL;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 = 0;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 IS NULL;
select * from (t1 full join t2 on t1.c1 = t2.c1) left join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 = 0;
# case
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 is NULL;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 is NULL and t4.c1 = 0;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 is NULL;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 is NULL and t3.c1 = 0 and t4.c1 = 0;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 is NULL;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 is NULL and t4.c1 = 0;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 IS NULL;
select * from (t1 left join t2 on t1.c1 = t2.c1) full join (t3 left join t4 on t4.c1 = t3.c1) on t1.c1 = t4.c1 where t2.c1 = 0 and t3.c1 = 0 and t4.c1 = 0;
