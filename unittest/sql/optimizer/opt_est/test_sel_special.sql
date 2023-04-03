# 有效行数的占比A1 = (10000-1000)/10000, A2 = (5000-1000)/5000
# 等值条件选择率B1 = A1*1/1000, B2 = A2*1/1000  
#table1 B1
#table2 B2
select * from t1,t2 where t1.c1 = t2.c1;
#table1  B1 + A1 * 1 - B1*A1
#table2 B2 + A1 * 1 - B2*A1
select * from t1,t2 where t1.c1 = t2.c1 or t1.c1 > 1;
select * from t1,t2 where t1.c1 = t2.c1 and t1.c1 > 1;
#DEFAULT_INEQ_SEL
select * from t1,t2 where t1.c1 < t2.c1; 
select * from t1,t2 where t1.c1 > t2.c1;
select * from t1,t2 where t1.c1 != t2.c1;
