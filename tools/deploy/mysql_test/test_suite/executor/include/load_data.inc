
--source mysql_test/include/check_ob_or_mysql.inc

create table t_h3_01_20(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 3;
create table t_h3_05_24(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 3;
create table t_h3_09_28(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 3;
create table t_h5_01_20(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 5;
create table t_h5_05_24(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 5;
create table t_h5_09_28(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 5;
create table t_r4_01_20(a int, b int, c datetime, primary key (a,b))
partition by range columns(a)
(
  partition p6 values less than (6),
  partition p11 values less than (11),
  partition p16 values less than (16),
  partition pm values less than (MAXVALUE)
);
create table t_r4_05_24(a int, b int, c datetime, primary key (a))
partition by range columns(a)
(
  partition p10 values less than (10),
  partition p15 values less than (15),
  partition p20 values less than (20),
  partition pm values less than (MAXVALUE)
);
create table t_r4_09_28(a int, b int, c datetime, primary key (a))
partition by range columns(a)
(
  partition p14 values less than (14),
  partition p19 values less than (19),
  partition p24 values less than (24),
  partition pm values less than (MAXVALUE)
);
create table t_r5_01_20(a int, b int, c datetime, primary key (a))
partition by range columns(a)
(
  partition p5 values less than (5),
  partition p9 values less than (9),
  partition p13 values less than (13),
  partition p17 values less than (17),
  partition pm values less than (MAXVALUE)
);
create table t_r5_05_24(a int, b int, c datetime, primary key (a))
partition by range columns(a)
(
  partition p9 values less than (9),
  partition p13 values less than (13),
  partition p17 values less than (17),
  partition p21 values less than (21),
  partition pm values less than (MAXVALUE)
);
create table t_r5_09_28(a int, b int, c datetime, primary key (a,b))
partition by range columns(a)
(
  partition p13 values less than (13),
  partition p17 values less than (17),
  partition p21 values less than (21),
  partition p25 values less than (25),
  partition pm values less than (MAXVALUE)
);
#--disable_query_log
if ($cluster_is_ob == 1) {
  create table t_h3_r4_01_20(a int, b int, c int, d int, e datetime, primary key (a, b, c))
  partition by hash(a)
  subpartition by range(b)
  subpartition template (
    subpartition p6 values less than (6),
    subpartition p11 values less than (11),
    subpartition p16 values less than (16),
    subpartition pm values less than (MAXVALUE)
  )
  partitions 3;
  create table t_h3_r5_09_28(a int, b int, c int, d int, e datetime, primary key (a, b))
  partition by hash(a)
  subpartition by range(b)
  subpartition template (
    subpartition p13 values less than (13),
    subpartition p17 values less than (17),
    subpartition p21 values less than (21),
    subpartition p25 values less than (25),
    subpartition pm values less than (MAXVALUE)
  )
  partitions 3;
  create table t_h5_r4_01_20(a int, b int, c int, d int, e datetime, primary key (a, b))
  partition by hash(a)
  subpartition by range(b)
  subpartition template (
    subpartition p6 values less than (6),
    subpartition p11 values less than (11),
    subpartition p16 values less than (16),
    subpartition pm values less than (MAXVALUE)
  )
  partitions 5;
  create table t_h5_r5_09_28(a int, b int, c int, d int, e datetime, primary key (a, b, c))
  partition by hash(a)
  subpartition by range(b)
  subpartition template (
    subpartition p13 values less than (13),
    subpartition p17 values less than (17),
    subpartition p21 values less than (21),
    subpartition p25 values less than (25),
    subpartition pm values less than (MAXVALUE)
  )
  partitions 5;
}
if ($cluster_is_mysql == 1) {
  create table t_h3_r4_01_20(a int, b int, c int, d int, e datetime, primary key (a, b, c));
  create table t_h3_r5_09_28(a int, b int, c int, d int, e datetime, primary key (a, b));
  create table t_h5_r4_01_20(a int, b int, c int, d int, e datetime, primary key (a, b));
  create table t_h5_r5_09_28(a int, b int, c int, d int, e datetime, primary key (a, b, c));
}

create table t_refered(a int, aa bigint, b int unsigned, bb bigint unsigned, c datetime, d date, e timestamp,  primary key (a))  partition by hash(a) partitions 3;

create table t_h5_int(a int, b int, c datetime, primary key (a)) partition by hash(a) partitions 5;

create table t_r4_int(a int, b int, c date, primary key (a,b))
partition by range columns(a)
(
  partition p6 values less than (6),
  partition p11 values less than (11),
  partition p16 values less than (16),
  partition pm values less than (MAXVALUE)
);

create table t_r4_date(a int, b int, c date, primary key (b,c))
partition by range columns(c)
(
  partition p11 values less than ("20161101"),
  partition p12 values less than ("20161201"),
  partition p171 values less than ("20170101"),
  partition pm values less than (MAXVALUE)
);

create table t_r4_datetime(a int, b int, c datetime, primary key (b,c))
partition by range columns(c)
(
  partition p11 values less than ("20161101"),
  partition p12 values less than ("20161201"),
  partition p171 values less than ("20170101"),
  partition pm values less than (MAXVALUE)
);

create table t_r4_timestamp(a int, b int, c timestamp, primary key (a))
partition by range columns(a)
(
  partition p11 values less than (10),
  partition p12 values less than (100),
  partition p171 values less than (1000),
  partition pm values less than (MAXVALUE)
);

create table t_h5_r4_int_int(a int, b int, c datetime, primary key (a, b))
partition by hash(a)
subpartition by range columns(b)
subpartition template (
  subpartition p6 values less than (6),
  subpartition p11 values less than (11),
  subpartition p16 values less than (16),
  subpartition pm values less than (MAXVALUE)
);

create table t_r4_h3_date_int(a date, b int, c int, primary key (a, b))
partition by range columns(a) subpartition by hash(b) subpartitions 3
(
  partition p11 values less than ("20161101"),
  partition p12 values less than ("20161201"),
  partition p171 values less than ("20170101"),
  partition pm values less than (MAXVALUE)
);

create table t_r4_h2_date_int(a date, b int, c int, primary key (a, b))
partition by range columns(a) subpartition by hash(b) subpartitions 2
(
  partition p11 values less than ("20161101"),
  partition p12 values less than ("20161201"),
  partition p171 values less than ("20170101"),
  partition pm values less than (MAXVALUE)
);

create table t_r4_h3_date_bigint(a date, b bigint, c int, primary key (a, b))
partition by range columns(a) subpartition by hash(b) subpartitions 3
(
  partition p11 values less than ("20161101"),
  partition p12 values less than ("20161201"),
  partition p171 values less than ("20170101"),
  partition pm values less than (MAXVALUE)
);


create table t_h5_r4_int_int_datetime(a int, b int, c datetime, primary key (a, b, c))
partition by hash(a)
subpartition by range columns(b, c)
subpartition template (
  subpartition p6 values less than (6, "20170510"),
  subpartition p11 values less than (11, "20170510"),
  subpartition p16 values less than (16, "20170510"),
  subpartition pm values less than (MAXVALUE, MAXVALUE)
);

create table tt (a int, b int, c date, primary key(a, b));



#--enable_query_log

insert into t_h3_01_20 values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104),
                              ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120);
insert into t_h3_05_24 values ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124);
insert into t_h3_09_28 values ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124),
                              (25, 25, 20161125), (26, 26, 20161126), (27, 27, 20161127), (28, 28, 20161128);
insert into t_h5_01_20 values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104),
                              ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120);
insert into t_h5_05_24 values ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124);
insert into t_h5_09_28 values ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124),
                              (25, 25, 20161125), (26, 26, 20161126), (27, 27, 20161127), (28, 28, 20161128);
insert into t_r4_01_20 values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104),
                              ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120);
insert into t_r4_05_24 values ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124);
insert into t_r4_09_28 values ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124),
                              (25, 25, 20161125), (26, 26, 20161126), (27, 27, 20161127), (28, 28, 20161128);
insert into t_r5_01_20 values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104),
                              ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120);
insert into t_r5_05_24 values ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124);
insert into t_r5_09_28 values ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124),
                              (25, 25, 20161125), (26, 26, 20161126), (27, 27, 20161127), (28, 28, 20161128);
insert into t_h3_r4_01_20 values ( 1,  1,  1,  1, 20161101), ( 2,  2,  2,  2, 20161102), ( 3,  3,  3,  3, 20161103), ( 4,  4,  4,  4, 20161104),
                                 ( 5,  5,  5,  5, 20161105), ( 6,  6,  6,  6, 20161106), ( 7,  7,  7,  7, 20161107), ( 8,  8,  8,  8, 20161108),
                                 ( 9,  9,  9,  9, 20161109), (10, 10, 10, 10, 20161110), (11, 11, 11, 11, 20161111), (12, 12, 12, 12, 20161112),
                                 (13, 13, 13, 13, 20161113), (14, 14, 14, 14, 20161114), (15, 15, 15, 15, 20161115), (16, 16, 16, 16, 20161116),
                                 (17, 17, 17, 17, 20161117), (18, 18, 18, 18, 20161118), (19, 19, 19, 19, 20161119), (20, 20, 20, 20, 20161120);
insert into t_h3_r5_09_28 values ( 9,  9,  9,  9, 20161109), (10, 10, 10, 10, 20161110), (11, 11, 11, 11, 20161111), (12, 12, 12, 12, 20161112),
                                 (13, 13, 13, 13, 20161113), (14, 14, 14, 14, 20161114), (15, 15, 15, 15, 20161115), (16, 16, 16, 16, 20161116),
                                 (17, 17, 17, 17, 20161117), (18, 18, 18, 18, 20161118), (19, 19, 19, 19, 20161119), (20, 20, 20, 20, 20161120),
                                 (21, 21, 21, 21, 20161121), (22, 22, 22, 22, 20161122), (23, 23, 23, 23, 20161123), (24, 24, 24, 24, 20161124),
                                 (25, 25, 25, 25, 20161125), (26, 26, 26, 26, 20161126), (27, 27, 27, 27, 20161127), (28, 28, 28, 28, 20161128);
insert into t_h5_r4_01_20 values ( 1,  1,  1,  1, 20161101), ( 2,  2,  2,  2, 20161102), ( 3,  3,  3,  3, 20161103), ( 4,  4,  4,  4, 20161104),
                                 ( 5,  5,  5,  5, 20161105), ( 6,  6,  6,  6, 20161106), ( 7,  7,  7,  7, 20161107), ( 8,  8,  8,  8, 20161108),
                                 ( 9,  9,  9,  9, 20161109), (10, 10, 10, 10, 20161110), (11, 11, 11, 11, 20161111), (12, 12, 12, 12, 20161112),
                                 (13, 13, 13, 13, 20161113), (14, 14, 14, 14, 20161114), (15, 15, 15, 15, 20161115), (16, 16, 16, 16, 20161116),
                                 (17, 17, 17, 17, 20161117), (18, 18, 18, 18, 20161118), (19, 19, 19, 19, 20161119), (20, 20, 20, 20, 20161120);
insert into t_h5_r5_09_28 values ( 9,  9,  9,  9, 20161109), (10, 10, 10, 10, 20161110), (11, 11, 11, 11, 20161111), (12, 12, 12, 12, 20161112),
                                 (13, 13, 13, 13, 20161113), (14, 14, 14, 14, 20161114), (15, 15, 15, 15, 20161115), (16, 16, 16, 16, 20161116),
                                 (17, 17, 17, 17, 20161117), (18, 18, 18, 18, 20161118), (19, 19, 19, 19, 20161119), (20, 20, 20, 20, 20161120),
                                 (21, 21, 21, 21, 20161121), (22, 22, 22, 22, 20161122), (23, 23, 23, 23, 20161123), (24, 24, 24, 24, 20161124),
                                 (25, 25, 25, 25, 20161125), (26, 26, 26, 26, 20161126), (27, 27, 27, 27, 20161127), (28, 28, 28, 28, 20161128);

insert into t_refered values ( 1,  1, 1, 1, 20161101, 20161101, 20161101), ( 2,  2, 2, 2, 20161101, 20161102, 20161102), ( 3,  3, 3, 3, 20161103, 20161101, 20161103), ( 4,  4, 4, 4, 20161104, 20161101, 20161104);
insert into t_h5_int values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104), ( 5,  5, 20161105);
insert into t_r4_int values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104),  ( 5,  5, 20161105);
insert into t_r4_date values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104);
insert into t_r4_datetime values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104);
insert into t_r4_timestamp values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104);
insert into t_h5_r4_int_int values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104);
insert into t_h5_r4_int_int_datetime values ( 1,  1, 20161101), ( 2,  2, 20161102), ( 3,  3, 20161103), ( 4,  4, 20161104);

insert into tt values ( 5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124),
                              (5, 6, 20161105), ( 6,  7, 20161106), ( 7,  8, 20161107), ( 8,  9, 20161108),
                              (9, 10, 20161109), (10, 11, 20161110), (11, 12, 20161111), (12, 13, 20161112),
                              (13, 14, 20161113), (14, 15, 20161114), (15, 16, 20161115), (16, 17, 20161116),
                              (17, 18, 20161117), (18, 19, 20161118), (19, 20, 20161119), (20, 21, 20161120),
                              (21, 22, 20161121), (22, 23, 20161122), (23, 24, 20161123), (24, 25, 20161124);

insert into t_r4_h2_date_int(b, c, a) values (5,  5, 20161105), ( 6,  6, 20161106), ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123), (24, 24, 20161124);

insert into t_r4_h3_date_int(b, c, a) values ( 7,  7, 20161107), ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122), (23, 23, 20161123);

insert into t_r4_h3_date_bigint(b, c, a) values  ( 8,  8, 20161108),
                              ( 9,  9, 20161109), (10, 10, 20161110), (11, 11, 20161111), (12, 12, 20161112),
                              (13, 13, 20161113), (14, 14, 20161114), (15, 15, 20161115), (16, 16, 20161116),
                              (17, 17, 20161117), (18, 18, 20161118), (19, 19, 20161119), (20, 20, 20161120),
                              (21, 21, 20161121), (22, 22, 20161122);

create table t_many_range_part(a int, b int, primary key (a))
partition by range columns(a)
(
  partition p05 values less than (5),
  partition p10 values less than (10),
  partition p15 values less than (15),
  partition p20 values less than (20),
  partition p25 values less than (25),
  partition p30 values less than (30),
  partition p35 values less than (35),
  partition p40 values less than (40),
  partition p45 values less than (45),
  partition p50 values less than (50),
  partition p55 values less than (55),
  partition p60 values less than (60),
  partition p65 values less than (65),
  partition p70 values less than (70),
  partition p75 values less than (75),
  partition p80 values less than (80),
  partition p85 values less than (85),
  partition pm values less than (MAXVALUE)
);
insert into t_many_range_part values (01, 01), (02, 02), (03, 03), (04, 04), (05, 05), (06, 06), (07, 07), (08, 08), (09, 09), (10, 10),
                                     (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20),
                                     (21, 21), (22, 22), (23, 23), (24, 24), (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30),
                                     (31, 31), (32, 32), (33, 33), (34, 34), (35, 35), (36, 36), (37, 37), (38, 38), (39, 39), (40, 40),
                                     (41, 41), (42, 42), (43, 43), (44, 44), (45, 45), (46, 46), (47, 47), (48, 48), (49, 49), (50, 50);



