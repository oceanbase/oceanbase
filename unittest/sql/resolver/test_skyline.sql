select * from t1 group by c1;
select distinct(c1) from t1;
select * from t1 order by c1;

select * from t1 group by c2;
select distinct(c2) from t1;
select * from t1 order by c2;

select * from t1 group by c1, c2;
select * from t1 group by c2, c1;
select distinct(c1), c2 from t1;
select distinct(c2), c1 from t1;
select * from t1 order by c1, c2;
select * from t1 order by c2, c1;

## order by
select a from t_equal_prefix where b = 1 order by c desc limit 9;
select b, c from t_equal_prefix where a = 1 order by b, c;
select a from t_equal_prefix where b = 1 and a = 2 order by c;
select a from t_equal_prefix where b = 1 or b = 2 order by a;
select a from t_equal_prefix where b = 1 and c = 2 order by a;
select a from t_equal_prefix where b = 1 order by a, c desc;
select a from t_equal_prefix where b > 1 order by c desc;
select a from t_equal_prefix where b = 1 and a > 2 order by c;
select a from t_equal_prefix where b = 1 and d = 2 order by e, a; 
select a from t_equal_prefix where b = 1 and d = 2 order by e, c;
select a from t_equal_prefix where e = 1 and c = 2 order by b, d, a;

## group by
select a from t_equal_prefix where b = 1 group by c limit 9;
select b, c from t_equal_prefix where a = 1 group by b, c;
select a from t_equal_prefix where b = 1 and a = 2 group by c;
select a from t_equal_prefix where b = 1 or b = 2 group by a;
select a from t_equal_prefix where b = 1 and c = 2 group by a;
select a from t_equal_prefix where b = 1 group by a, c;
select a from t_equal_prefix where b > 1 group by c;
select a from t_equal_prefix where b = 1 and a > 2 group by c;
select a from t_equal_prefix where b = 1 and d = 2 group by e, a; 
select a from t_equal_prefix where b = 1 and d = 2 group by e, c;
select a from t_equal_prefix where e = 1 and c = 2 group by b, d, a;


## distinct
select distinct c from t_equal_prefix where b = 1 and a > 0 limit 9;
select distinct b, c from t_equal_prefix where a = 1;
select distinct c from t_equal_prefix where b = 1 and a = 2;
select distinct a from t_equal_prefix where b = 1 or b = 2;
select distinct a from t_equal_prefix where b = 1 and c = 2;
select distinct a, c from t_equal_prefix where b = 1;
select distinct c from t_equal_prefix where b > 1;
select distinct c from t_equal_prefix where b = 1 and a > 2;
select distinct e, a from t_equal_prefix where b = 1 and d = 2;
select distinct e, c from t_equal_prefix where b = 1 and d = 2;
select distinct b, d, a from t_equal_prefix where e = 1 and c = 2;

## union

