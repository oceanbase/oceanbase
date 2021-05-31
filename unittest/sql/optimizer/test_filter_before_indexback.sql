#filter before index back
select /*+index(z1 z1_b)*/ * from z1 where a > 1 and b > 1 and c > 1 and d > 1 and ceil(b) > 1;
select /*+index(z1 primary)*/ * from z1 where a > 1 and b > 1 and c > 1 and d > 1 and ceil(b) > 1;

select /*+index(z1 z1_b)*/ * from z1 where a + b > 1 and a + c > 1 and b + c > 1 and a + b > c;
select /*+index(z1 primary)*/ * from z1 where a + b > 1 and a + c > 1 and b + c > 1 and a + b > c;

select /*+index(z1 z1_b)*/ * from z1 where a + b > 1 and 1 + a > b and 1 + b > a and b + 1 > a;
select /*+index(z1 primary)*/ * from z1 where a + b > 1 and 1 + a > b and 1 + b > a and b + 1 > a;

#two tables and nl pushed down filters
select /*+leading(z1,z2),use_nl(z2)*/ * from z1,z2 where z1.b = z2.b;
select /*+leading(z1,z2),use_nl(z2)*/ * from z1,z2 where z1.b = z2.b and z1.a > 1 and z1.b > 1 and z1.c > 1;
select /*+leading(z1,z2),use_nl(z2)*/ * from z1,z2 where z1.b = z2.b and z2.a > 1 and z2.b > 1 and z2.c > 1;
select /*+leading(z1,z2),use_nl(z2)*/ * from z1,z2 where z1.b = z2.b and z1.a + z1.b > 1 and z1.a + z1.b > z1.c and z1.a + z1.b > z2.c and z1.a + z2.b > z1.c;






