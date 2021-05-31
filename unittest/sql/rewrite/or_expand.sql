#case
select * from t1 where c1 > 0 or c2 < 0;
select * from t1 where c1 > 0 union all select * from t1 where c2 < 0 and lnnvl(c1 > 0);

#case 2
select * from t1 where c1 > 0 or c2 < 0 or c1 = 1;
select * from t1 where c1 > 0 union all select * from t1 where c2 < 0 and lnnvl(c1 > 0) union all select * from t1 where c1 = 1 and lnnvl(c1 > 0 or c2 < 0);

#case 3
select * from t1 where c1 > 0 or c2 < 0;
select * from t1 where c1 > 0 union all select * from t1 where c2 < 0 and lnnvl(c1 > 0);

#case 4
select * from t1 where c1 = 1 or c1 = 2 or c1 = 3;
select * from t1 where c1 = 1 union all select * from t1 where c1 = 2 and lnnvl(c1 = 1) union all select * from t1 where c1 = 3 and lnnvl(c1 = 1 or c1 = 2);

#case 5
select c2 from t1 where c1 = 1 or c1 = 2 or c1 = 3;
select c2 from t1 where c1 = 1 union all select c2 from t1 where c1 = 2 and lnnvl(c1 = 1) union all select c2 from t1 where c1 = 3 and lnnvl(c1 = 1 or c1 = 2);

#case 6
select * from t1 where c1 > 0 or c2 < 0;
select * from t1 where c1 > 0 union all select * from t1 where c2 < 0 and lnnvl(c1 > 0);
