set ob_query_timeout=1000000000;
--disable_warnings
drop database if exists px_test;
--enable_warnings
create database px_test;
use px_test;

# 场景：一个年级 6 个班，修了2门课，英语和数学

create table stu (
  sid     int,
  name    varchar(32),
  cls     int,
  primary key (cls, sid)
) partition by hash(sid) partitions 6;

create table teacher (
  tid        int,
  name       varchar(32),
  subject    varchar(4),
  primary key (tid)
) partition by hash(tid) partitions 8;

create table score (
  sid     int,
  subject varchar(4),
  score   int,
  primary key (sid, subject)
) partition by hash(sid) partitions 6;

insert into stu values
(11, 'a1', 1),
(12, 'b1', 1),
(13, 'c1', 1),

(21, 'a2', 2),
(22, 'b2', 2),

(31, 'a3', 3),

(41, 'a4', 4),
(42, 'b4', 4),

(51, 'a5', 5),
(52, 'b5', 5),

(61, 'a6', 6),
(62, 'b6', 6),
(63, 'c6', 6),
(64, 'd6', 6);

insert into teacher values
(1, 'Miss Zhang', 'EN'),
(2, 'Mr Wang', 'MA');


insert into score values
(11, 'EN', 60),
(12, 'EN', 70),
(13, 'EN', 80),

(21, 'EN', 58),
(22, 'EN', 90),

(31, 'EN', 80),

(41, 'EN', 80),
(42, 'EN', 90),

(51, 'EN', 89),
(52, 'EN', 99),

(61, 'EN', 100),
(62, 'EN', 90),
(63, 'EN', 99),
(64, 'EN', 87);


insert into score values
(11, 'MA', 60),
(12, 'MA', 70),
(13, 'MA', 80),

(21, 'MA', 58),
(22, 'MA', 90),

(31, 'MA', 80),

(41, 'MA', 80),
(42, 'MA', 90),

(51, 'MA', 89),
(52, 'MA', 99),

(61, 'MA', 100),
(62, 'MA', 90),
(63, 'MA', 99),
(64, 'MA', 87);

--disable_query_log
--disable_result_log
select * from stu;
select * from teacher;
select * from score;
--enable_query_log
--enable_result_log
