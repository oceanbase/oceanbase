
explain extended select * from t1 order by c1,c2 limit 100;

explain extended select c2, sum(c1) from t1 group by c2;

explain extended select t1.c1 from t1,t2,t3, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;

explain extended select t1.c1 from t1, (select * from t2 where c2>1 order by c1 limit 10) as t where t1.c1=t.c1;

explain extended select t1.c1 from t1 left join t2 t on t1.c1=t.c1,t2,t3, t1 tt where t1.c1=t3.c1 and t1.c2=tt.c2 and t1.c1+t2.c1=tt.c1;

