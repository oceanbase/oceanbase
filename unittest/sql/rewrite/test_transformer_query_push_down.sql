####################
# Part 1
#
# Expect to rewrite
####################

## case 1 ##

## expect to rewrite to peel of the top-level stmt
select * from (select * from t1 order by c2) a limit 1;
select * from t1 order by c2 limit 1;

## case 2 ##

## with limit + offset
select * from (select * from t1 order by c2) a limit 1, 10;
select * from t1 order by c2 limit 1, 10;

## case 3 ##

## with desc
select * from (select * from t1 order by c2 desc) a limit 1, 10;
select * from t1 order by c2 limit 1, 10;


## case 4 ##

## with no-column predicate
# TODO shengle, core in pre calc expr, mysqltest not core, may unitest problem
#
# select * from (select * from t1 order by c2 desc) a where 1=1 limit 1, 10;
select * from t1 order by c2 limit 1, 10;

## case 5 ##

## with no-column predicate
# TODO shengle, core in pre calc expr, mysqltest not core, may unitest problem
#
# select * from (select * from t1 order by c2 desc) a where (1=1) and (0=0) limit 1, 10;
select * from t1 order by c2 limit 1, 10;

## case 6 ##

## with no-column predicate
select * from (select * from t1 order by c2 desc) a where 1 limit 1, 10;
select * from t1 order by c2 limit 1, 10;


## case 7 ##

## muiltiple-level
select * from (select * from (select * from (select * from (select * from (select * from (select * from t1 order by c2) a) b) c) d) e) f limit 1;

## case 8 ##

## muiltiple-level, with some alias table names in select list
select * from (select * from (select d.* from (select c.* from (select * from (select * from (select * from t1 order by c2) a) b) c) d) e) f limit 1;


###########################
# Part 1
#
# Expect NOT to rewrite
###########################

## case 1 ##

# with top-level order-by, no rewrite
select * from (select * from t1) a order by a.c2 limit 1, 10;
select * from t1 order by c2 limit 1, 10;

## case 2 ##

# with top-level group-by, no rewrite
select count(*) from (select * from t1) a group by a.c2 limit 1, 10;

## case 3 ##

# non-star select, no rewrite
select a.c1 from (select * from t1) a group by a.c2 limit 1, 10;

## case 4 ##

# no top-level limit, no rewrite
select a.c2 from (select * from t1) a;

## case 5 ##

# more than 1 table, no rewrite
select * from (select * from t1) a, t1 where a.c2 = t1.c2 limit 10;

## case 6 ##

# column-related predicates, no rewrite
select * from (select * from t1) a where a.c2 = 1 limit 1;

## case 7 ##

# with distinct, no rewrite
select distinct a.c1 from (select * from t1) a limit 1;

## case 8 ##

# not top-level, no rewrite
select b.c2 from (select * from (select * from t1) a limit 10) b;

## case 9 ##

# subquery in set, no rewrite
(select * from (select * from t1) a limit 10) union all (select * from (select * from t1) b limit 10);

## case 10 ##

# subquery in condition, no rewrite
select * from (select * from t1 order by c2) a where exists (select * from t1) limit 10;

#has other functions, so no write
select now(), b.* from (select c2 from t1 group by c2) as b;

select * from tl0 union distinct select * from tr0;
select * from tl0 union distinct select * from tr1;
select * from tl0 union distinct select * from tr2;
select * from tl0 union distinct select * from tr3;

select * from tl1 union distinct select * from tr0;
select * from tl1 union distinct select * from tr1;
select * from tl1 union distinct select * from tr2;
select * from tl1 union distinct select * from tr3;

select * from tl2 union distinct select * from tr0;
select * from tl2 union distinct select * from tr1;
select * from tl2 union distinct select * from tr2;
select * from tl2 union distinct select * from tr3;

select * from tl3 union distinct select * from tr0;
select * from tl3 union distinct select * from tr1;
select * from tl3 union distinct select * from tr2;
select * from tl3 union distinct select * from tr3;
