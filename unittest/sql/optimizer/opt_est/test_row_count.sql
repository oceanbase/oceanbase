select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c1 = 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c1 > 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t2.c1 = 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t2.c1 > 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c1 = 1 and t2.c1 = 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c1 > 1 and t2.c1 = 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c1 = 1 and t2.c1 > 1;
select /*+ LEADING(t1, t2) USE_HASH(t1, t2) */ t1.c1 from t1, t2 where t1.c1 = t2.c1 and t1.c1 > 1 and t2.c1 > 1;
# multi join
select /*+ LEADING(a, b, c) USE_HASH(a, b, c) */ c.c1 from t1 a, t1 b, t1 c where b.c1 = c.c1;
select /*+ LEADING(a, b, c) USE_HASH(a, b, c) */ c.c1 from t1 a, t1 b, t1 c where b.c1 = c.c1 and c.c1 = 1;
select /*+ LEADING(a, b, c) USE_HASH(a, b, c) */ c.c1 from t1 a, t1 b, t1 c where b.c1 = c.c1 and c.c1 > 1;
select /*+ LEADING(a, b, c) USE_HASH(a, b, c) */ c.c1 from t1 a, t1 b, t1 c where b.c1 = c.c1 and a.c1 = 1 and b.c1 = 1 and c.c1 = 1;
