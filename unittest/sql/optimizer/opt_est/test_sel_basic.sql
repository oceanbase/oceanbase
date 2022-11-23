#select c1 from t1 where c1 between 10000 and 1000;
#select c1 from t1 where c1 not in (500, 1000)
#select c1 from t1 where (c1, c2) = (2000, 10000);
#select c1 from t1 where c1 = (select c2 from t1 where c2 >= 10000);
#select c1 from t1 where c1 = 1000;
select 1.
select c1 from t1 where c1 > 1000 or c1 > 2000 and c1 < 3000;
# const
select 1;
select c1 from t1 where 1;
select 1;
select c1 from t1 where 1 > 0;
select 0;
select c1 from t1 where 0;
select 0;
select c1 from t1 where null;
# distinct(1000), min(500), max(10000), nullnum(100)
#equal a = 1
select 1/1000*0.95;
select c1 from t1 where c1 = 999;
select 1/1000*0.95;
select c1 from t1 where c1 = 10001;
select 1/1000*0.95;
select c1 from t1 where c1 = 1000;
select 500/10000
select c1 from t1 where c1 <=> null;
select 0
select c1 from t1 where c1 = null;
select 1/1000 * 0.95
select c1 from t1 where c1 + 1 = 1000;
select 1/1000 * 0.95
select c1 from t1 where c1 * 2 = 1000;
select 0.005(DEFAULT_EQ_SEL)
select c1 from t1 where c1 / 2 = 1000;
select 1/1000 * 0.95
select c1 from t1 where c1 - 100 = 1000;
# range cond
select 0;
select c1 from t1 where c1 > null;
select 1;
select c1 from t1 where c1 > null or 1 > 0;
select 0;
select c1 from t1 where c1 > null or 1 > 2;
select 0;
select c1 from t1 where c1 < null;
select 0.95;
select c1 from t1 where c1 > 30;
select 2*1/1000 * 0.95;
select c1 from t1 where c1 < 50;
select 1.0/3.0 as not calc
select c1 from t1 where c1 + 1 > 5;
select (10000-2000)/(10000-1000) * 0.95;
select c1 from t1 where c1 > 2000;
select 0.95;
select c1 from t1 where c1 > 1000 or c1 > 2000 and c1 < 3000;
select (10000-2000)/(10000-1000) * 0.95 + 1/1000 * 0.95;
select c1 from t1 where c1 >= 2000;
select (5000-1000)/(10000-1000) * 0.95;
select c1 from t1 where c1 < 5000;
select (5000-1000)/(10000-1000) * 0.95 + 1/1000 * 0.95;
select c1 from t1 where c1 <= 5000;
select 20*1/1000 * 0.95(min_distinct 20);
select c1 from t1 where c1 <= 500;
select 20*1/1000 * 0.95;
select c1 from t1 where c1 >= 10000;
#T_OP_NE 未做处理
select 1 - 1/1000*0.95 - 100/10000
select c1 from t1 where c1 != 500;
select 1 - 1/1000*0.95 - 100/10000
select c1 from t1 where c1 != 50;
select 1 - 1/1000*0.95 - 100/10000
select c1 from t1 where c1 != 50;
select 1 - 1/1000*0.95 - 100/10000
select c1 from t1 where c1 + 1 != 50;
select 1 - 1/1000*0.95 - 100/10000
select c1 from t1 where c1 * 2 != 50;
select 0.5
select c1 from t1 where c1 / 2 != 50;
# and && or 混合 A and B: A*B, A OR B : A + B - A*B
select (10000-2000)/(10000-1000)*0.95;
select c1 from t1 where c1 > 2000;
select (3000-1000)/(10000-1000)*0.95;
select c1 from t1 where c1 < 3000;
select 2*1/1000 * 0.95;
select c1 from t1 where c1 > 10000;
select 0.95;
select c1 from t1 where c1 > 500;
select 2*1/1000 * 0.95;
select c1 from t1 where c1 < 500;
select 2*1/1000 * 0.95;
select c1 from t1 where c1 < 1000;
select (10000-9000)/(10000-1000)*0.95;
select c1 from t1 where c1 > 9000;
select (8000-1000)/(10000-1000)*0.95;
select c1 from t1 where c1 < 8000;
#以上为所需单个条件的选择率
select (3000-2000)/(10000-1000)*0.95;
select c1 from t1 where c1 > 2000 and c1 < 3000;
select 0.019;
select c1 from t1 where c1 > 10000 and c1 > 500;
select 0.95;
select c1 from t1 where c1 < 10000 and c1 > 500;
select 0.019*2;
select c1 from t1 where c1 < 500 or c1 > 10000;
select (10000-2000)/(10000-1000)*0.95 + 0.019;
select c1 from t1 where c1 > 2000 or c1 < 1000;
select (10000-2000)/(10000-1000)*0.95 + 0.019;
select c1 from t1 where c1 > 2000 or c1 < 1000 or c1 < 500;
select (10000-2000)/(10000-1000)*0.95 + 0.0095;
select c1 from t1 where c1 > 2000 or c1 < 1000 and c1 > 500;
select 0.0095;
select c1 from t1 where c1 < 1000 and c1 > 500;
select 0.019;
select c1 from t1 where c1 < 1000 or c1 > 9000 and c1 > 2000 and c1 < 8000;
select 0.019;
select c1 from t1 where c1 < 500 or c1 > 10000 and c1 > 10001 and c1 < 499;
#T_OP_IS
select 500/10000;
select c1 from t1 where c1 is null;
select 1/1000 * 0.95;
select c1 from t1 where c1 is false;
select 1 * 0.95 - 1/1000(suppose the ratio of false);
select c1 from t1 where c1 is true;
#T_OP_NOT
select 1 - 500/10000;
select c1 from t1 where c1 is not null;
select 1 - 1/1000 * 0.95;
select c1 from t1 where c1 is not false;
select 1 - 0.95;
select c1 from t1 where c1 is not true;
select 0;
select * from t1 where not 1;
select 1;
select * from t1 where not 0;
select 0.95 - (10000-2000)/(10000-1000)*0.95;
select * from t1 where not c1 > 2000;
select 0.95 - (3000-2000)/(10000-1000)*0.95;
select * from t1 where not (c1 > 2000 and c1 < 3000);
select 0.95 - 1/1000*0.95*3;
select * from t1 where not c1 in (2000, 3000);
#T_OP_IN
#T_OP_NOT_IN.
select 1/1000*0.95 * 3;
select c1 from t1 where c1 in (500, 8000, 10000);
select square(1/1000*0.95) * 2
select c1 from t1 where (c1,c2) in ((500, 8000), (8000, 10000));
select 0.95 - 0.95*2;
select c1 from t1 where c1 not in (500, 1000);
# const in const
select 1;
select c1 from t1 where (200) in (100, 200);
select 1;
select c1 from t1 where (200) in (100, 300, 400, 200);
select 0;
select c1 from t1 where (200) in (100, 300, 400, 500);
select 1/1000*0.95;
select c1 from t1 where (200) in (c1, 300, 400, 500);
select 0.95;
select c1 from t1 where (c1) in (c1, 300, 400, 500);
select 1;
select c1 from t1 where 200 in ('200', c2, 300, 400, 500);
select 1;
select c1 from t3 where 'a' in ('A', c1, '300', '400', '500');
select 1/80*(10000 - 10) | str(distinct 80, null:10);
select c1 from t3 where c1 = 'a';
select 1/80*(10000 - 10) | str(distinct 80, null:10);
select c1 from t3 where 'a' in ('A' collate 'utf8mb4_bin', c1, '300', '400', '500');
select 1/1000*0.95 * 3;
select c1 from t1 where c1 in (500, 8000, 10000);
EXPECTED select 0.95 - 1/1000*0.95 * 3;
select c1 from t1 where c1 not in (500, 8000, 10000);
EXPECTED select 1/1000*0.95 * 2;
select c1 from t1 where c1 in (500, 1000);
EXPECTED select 0.95 - 1/1000*0.95 * 2;
select c1 from t1 where c1 not in (500, 1000);
EXPECTED select 1/1000*0.95 * 1/1000*0.95*2;
select c1 from t1 where (c1,c2) in ((500, 8000), (8000, 10000));
EXPECTED select 0.95 - 1/1000*0.95 * 1/1000*0.95*2;
select c1 from t1 where (c1,c2) not in ((500, 8000), (8000, 10000));
#BTW, NOT BTW
select 0.95;
select c1 from t1 where c1 between 1000 and 10000;
select 20/1000*.95 + 20/1000*0.95,20:minest of half open range
select c1 from t1 where c1 not between 1000 and 10000;
select 1/1000*0.95;
select c1 from t1 where c1 between 1000 and 1000;
select 0.95;
select c1 from t1 where c1 not between 1000 and 1000;
select 0;考虑设最低选择率;#bug 8551829 :6;
select c1 from t1 where c1 between 10000 and 1000;
select 1;
select c1 from t1 where c1 not between 10000 and 1000;
select 0.018*0.018;
select c1 from t1 where c1 >= 10000 and c1<=1000;
select 1/1000*0.95;
select c1 from t1 where 1000 between c1 and 1000;
select 1 - 1/1000*0.95;
select c1 from t1 where 1000 not between c1 and 1000;
select (2000-1000)/(10000-1000)*0.95;
select c1 from t1 where 2000 between c1 and 10000;
select (10000-8000)/(10000-1000)*0.95;
select c1 from t1 where 2000 not between c1 and 10000;
#btw的最小选择率暂时未设置，这里会直接为0
select 0;
select c1 from t1 where 1000 between 10000 and c1;
select (10000-2000)/(10000-1000)*0.95;
select c1 from t1 where 2000 between 1000 and c1;
#常量这种情况暂时未做计算 #bug 8551829 :5;
select 1;
select c1 from t1 where 1 between 0 and 1;
select 0;
select c1 from t1 where 100 between 0 and 1;
#JOIN COND
# min(basic_sel1, basic_sel2)
select 1/1000*((2000 - 500)/2000) table2: row count = 2000;
select t1.c1 from t1,t2 where t1.c1 = t2.c1;
select 1/1000*0.75 table2: row count = 2000;
select t1.c1 from t1,t2 where t1.c1 + 1 = t2.c1;
select 1/1000*0.75 table2: row count = 2000;
select t1.c1 from t1,t2 where t1.c1 + 1 = t2.c1 + 1;
select 1/1000*0.75 table2: row count = 2000;
select t1.c1 from t1,t2 where t1.c1 * 2 = t2.c1 + 1;
select 1/1000*0.75 table2: row count = 2000;
select t1.c1 from t1,t2 where t1.c1 / 2 = t2.c1 + 1;
select 1/1000*0.75 * 1/3;
select t1.c1 from t1,t2 where t1.c1 = t2.c1 and t1.c2 > t2.c2;
select 1/3;
select t1.c1 from t1, t2 where t1.c1 < t2.c2;
select 0.5;
select t1.c1 from t1, t2 where t1.c1 != t2.c2;
# IS_WITH_SUBQUERY
select 1/1000*0.95
select c1 from t1 where c1 = (select c2 from t1 where c2 >= 10000);
select 1.0 / 3.0 DEFAULT_INEQ_SEL
select c1 from t1 where c1 > (select c2 from t1 where c2 >= 10000);
todo. 1 - 1/1000*0.95 - 0.05
select c1 from t1 where c1 != (select c2 from t1 where c2 >= 10000);
todo. DEFAULT_SEL
select c1 from t1 where c1 in (select c2 from t1 where c2 = 10000);
todo. DEFAULT_SEL
select c1 from t1 where c1 = ANY (select c2 from t1 where c2 > 1000);
todo. DEFAULT_SEL
select c1 from t1 where c1 != ANY (select c2 from t1 where c2 > 1000);
todo. DEFAULT_SEL
select c1 from t1 where c1 = SOME (select c2 from t1 where c2 > 1000);
todo. DEFAULT_SEL
select c1 from t1 where c1 != SOME (select c2 from t1 where c2 > 1000);
todo. DEFAULT_SEL
select c1 from t1 where c1 = ALL (select c2 from t1 where c2 > 1000);
todo. DEFAULT_SEL
select c1 from t1 where c1 != ALL (select c2 from t1 where c2 > 1000);
#ROW
select 1/1000 * 0.95 * (1/1000 * 0.95)
select c1 from t1 where (c1, c2) = (2000, 10000);
todo. T_OP_ROW != T_OP_ROW
select c1 from t1 where (c1, c2) != (2000, 10000);
select (10000 - 2000)/(10000 - 1001) * 0.95
select c1 from t1 where (c1, c2) > (2000, 10000);
# test decimal
AC: not null = 0.95
select c1 from t5 where c1 < 10001;
AC: not null = 0.95
select c1 from t5 where c1 > 1000;
AC: not null = 0.95
select c1 from t5 where c1 <= 10000;
AC: not null = 0.95
select c1 from t5 where c1 >= 1001;
AC: not null / 2 near 0.47
select c1 from t5 where c1 > 5500;
AC: not null / 2 near 0.47
select c1 from t5 where c1 < 5500;
