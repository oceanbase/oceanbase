## 
# distinct 下压(有才limit 下压)
select c1 from t1 union select c2 from t2;
# 下压
select c1 from t1 union select c2 from t2 limit 2;
select c1 from t1 minus select c2 from t2 limit 2;
select c1 from t1 limit 2 union select c2 from t2;

# limit 下压(只有union all下压)
select c1 from t1 union all select c2 from t2 limit 2;
select c1 from t1 limit 5 union all select c2 from t2 limit 2;
select c1 from t1 intersect select c2 from t2 limit 2;

# order by 下压
select c1 from t1 union all select c2 from t2 order by c1;
select c1 from t1 union all select c2 from t2 order by c1 limit 2;
select c1, c2 from t1 order by c2 union all select c1, c2 from t2 order by c1 limit 2;
select c1, c2 from t1 limit 5 union all select c1, c2 from t2 order by c1 limit 2;
select c1 from t1 minus select c2 from t2 order by c1 limit 2;

## complex test
select * from v limit 10 union select * from v limit 10 union select * from v limit 10 union select * from v limit 10 union select * from v limit 10 union select * from v order by c1 limit 10;

select * from v order by c1 union select * from v order by c1 union select * from v order by c1 union select * from v order by c1 union select * from v order by c1 union select * from v order by c1 limit 10;

select * from v order by c1 union all select * from v order by c1 union all select * from v order by c1 union all select * from v order by c1 union all select * from v order by c1 union select * from v order by c1 limit 10;

(select c2 from t1 order by c2 intersect select * from v order by c2 limit 10)union (select c1 from t1 minus select * from v order by c1 limit 11) order by c2 limit 3;
