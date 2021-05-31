select c1 from t3 where c1 like '|||%';
select c1 from t3 where c1 like '11';
#range cond
select c1 from t3 where c1 > '|||||||||||||||||||2';
select c1 from t3 where c1 = '11';
select c1 from t3 where c1 != '11';
select c1 from t3 where c1 > '11';
select c1 from t3 where c1 < '11';
select c1 from t3 where c1 < '|||||||||||||||||||2';
select c1 from t3 where c1 > '|||||||||||||||||||2';
select c1 from t3 where c1 > '||||||||||||||||||||';

select c1 from t3 where c1 > '20150305123445555';
select c1 from t3 where c1 > '20150305123445555';
#like 
select c1 from t3 where c1 like '11';
select c1 from t3 where c1 like 'abc';
select c1 from t3 where c1 like '|||%';
select c1 from t3 where c1 like 123;
select c1 from t3 where c1 like 's%123' escape 's';
select c1 from t3 where c1 like '123';
select c1 from t3 where c1 like '%123';
select c1 from t3 where c1 like '%%';
select c1 from t3 where c1 like '%||||%';
select c1 from t3 where c1 like '%|||';
# test datetime
datetime
select c1 from t3 where c1 > '20150305123445555';
select c1 from t3 where c1 < '20150305123445555';
select c1 from t3 where c1 < '99991231235959999';
select c1 from t3 where c1 > '99991231235959999';
select c1 from t3 where c1 < '10000101000000111';
select c1 from t3 where c1 > '10000101000000111';

select c1 from t3 where c1 > '10000101000000111' and c1 < '99991231235959999';
select c1 from t3 where c1 > '20150305123445555' and c1 < '10000101000000111';
select c1 from t3 where c1 < '20150305123445555' and c1 > '99991231235959999';
select c1 from t3 where c1 > '20150305123445555' and c1 < '50120805123445666';
select c1 from t3 where c1 > '20150305123445555' and c1 < '20150305123446666';
select c1 from t3 where c1 = '20150305123445555';
select c1 from t3 where c1 != '20150305123445555';
select c1 from t3 where c1 like '20150305123445555';
select c1 from t3 where c1 like '1000%';
select c1 from t3 where c1 like '9999%';
select c1 from t3 where c1 like '1%';
select c1 from t3 where c1 like '99%';
