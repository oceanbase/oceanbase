# 目前只针对OR有检查独立性的操作，且只针对包含单列的列
#define p_nn = (10-5)/10 = 0.5
#define p_e = 1/5*0.5 = 0.1
p_nn = 0.5
select c1 from t1 where c1 is not NULL;
p_e = 0.1
select c1 from t1 where c1 = 1;
p_nn = 0.5
select c1 from t1 where c1 > 0;
# basic
p_e + p_e = 0.2
select c1 from t1 where c1 = 1 or c1 = 2;

# 不受影响
p_e = 0.1
select c1 from t1 where c1 = 1;
p_e + p_e - p_e * p_e = 0.19
select c1 from t1 where c1 = 1 or c2 = 1;
p_e + p_nn - p_nn * p_e = 0.55
select c1 from t1 where c1 > 0 or c1 = 1;

# complex
# 这个还是不太准, 不过符合预期
# (p_e + p_e) and (p_e + p_e - p_e * p_e) = 4 * p_e ^ 2 - 2 * p_e ^ 3 = 0.038
p_e * p_e + p_e - p_e * p_e * p_e = 0.109
select c1 from t1 where (c1 = 1 and c2 = 1) or c1 = 2;
# not mutex
p_e ^ 2 + p_e * p_nn - p_e ^ 3 * p_nn = 0.0595
select c1 from t1 where (c1 = 1 and c2 = 1) or (c1 = 2 and c2 > 0);

# left c1 c2 is not mutex, other OR is mutex
(2 * p_e - p_e * p_e) + (p_e * p_nn) - (2 * p_e - p_e * p_e) * (p_e * p_nn)  = 0.2305
select c1 from t1 where (c1 = 1 or c2 = 1) or (c1 = 2 and c2 > 0);
# all OR is not mutex
p_e ^ 2 + (p_e + p_nn - p_e * p_nn) - (p_e ^ 2 * (p_e + p_nn - p_e * p_nn)) = 0.5545
select c1 from t1 where (c1 = 1 and c2 = 1) or (c1 = 2 or c2 > 0);

p_e + p_e + p_e + p_e = 0.4
select c1 from t1 where c1 = 1 or c1 = 2 or c1 = 3 or c1 = 4;
(p_e + p_e + p_e) + p_e - p_e * (p_e * 3) = 0.37
select c1 from t1 where c1 = 1 or c1 = 2 or c1 = 3 or c2 = 4;

((p_e + p_e) + p_e - p_e * (p_e + p_e)) + p_e - p_e * (3 * p_e - p_e * (p_e + p_e)) = 0.352
select c1 from t1 where c1 = 1 or c1 = 2 or c2 = 3 or c1 = 4;
