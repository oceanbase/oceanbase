create database opt;
use opt;
create table pt2(c1 int primary key, c2 int, c3 varchar(32)) partition by hash (c1) partitions 3;
create table pt3(c4 int primary key, c5 int, c6 varchar(32) NOT NULL) partition by hash (c4) partitions 2;
create table t1(c1 int primary key, c2 int);
create table t2(c1 int primary key, c2 int, c3 varchar(32));
create table t3(c1 int primary key, c2 int, c3 varchar(32));
create table t4(c1 int primary key, c2 int, c3 varchar(32));
create table t5(c1 int primary key, c2 int, c3 varchar(32));
create table t6(c1 int primary key, c2 int unique key, c3 varchar(32));
create table t7(c1 int , c2 int , c3 int, primary key(c1,c2));
create table t8(c1 int , c2 int , c3 int primary key);
create table t9(c1 int , c2 int , c3 int, c4 int);
create table t10(c1 int, c2 int , c3 int not null);
create table t11(c1 int primary key, c2 datetime not NULL, c3 date not NULL);
create table t12(c1 bigint, c2 varchar(64), c3 datetime);
create index idx_t9 on t9 (c2, c3) local;
create unique index idx_t10 on t10(c3, c2);
create table tt1(a int primary key, b char(20));
create table tt2(a int primary key, b int);
create table kt1(c1 int primary key, c2 int) partition by hash (c1) partitions 5;
create table dist_t(c1 int primary key, c2 int) partition by hash (c1) partitions 5;
create view v1 as select a as va, b as vb from tt1;
create view v(c1) as select c1 from t1;
create view nv1 (col1, col2) as select c1 as col1, c2 as col2 from t1;
create view vv1 (c,d,e,f) as select c1 as c, c2 as d, c1 in (select c1 + 2 from t1) as e, c1 = all(select c1 from t1) as f from t1;
create view v2(c) as select a+1 as c from tt2 where b >= 4;
create view part_view(k1, k2) as select c1 as k1, c2 as k2 from dist_t;
create table st1(c1 int primary key, c2 int) partition by hash(c1 + 1) partitions 3;
CREATE TABLE bt1(f1 VARCHAR(100) DEFAULT 'test',id int primary key);
create table xt1(c1 int default 99, c2 int);
create table tm(money int, id int);
create table agg_t1(c1 int);
create table agg_t2(c2 int);
CREATE TABLE `B` (`col_int` int(11) DEFAULT NULL, `col_varchar_10` varchar(10) DEFAULT NULL, `col_datetime` datetime DEFAULT NULL, `col_varchar_10_key` varchar(10) DEFAULT NULL, `col_varchar_20` varchar(20) DEFAULT NULL, `pk` int(11) NOT NULL, `col_date_key` date DEFAULT NULL, `col_int_key` int(11) DEFAULT NULL, `col_date` date DEFAULT NULL, `col_varchar_20_key` varchar(20) DEFAULT NULL, `col_datetime_key` datetime DEFAULT NULL, PRIMARY KEY (`pk`)) DEFAULT CHARSET = utf8mb4 REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE;
create index idx3 on B(col_varchar_10) local;
create index idx7 on B(col_date_key) local;
create index idx1 on B(col_int_key) local;
create index idx5 on B(col_varchar_20_key) local;
create index idx9 on B(col_datetime_key) local;
CREATE TABLE `CC` (`col_datetime` datetime DEFAULT NULL, `col_datetime_key` datetime DEFAULT NULL, `col_varchar_10_key` varchar(10) DEFAULT NULL, `col_varchar_20_key` varchar(20) DEFAULT NULL, `pk` int(11) NOT NULL, `col_date_key` date DEFAULT NULL, `col_date` date DEFAULT NULL, `col_int_key` int(11) DEFAULT NULL, `col_int` int(11) DEFAULT NULL, `col_varchar_10` varchar(10) DEFAULT NULL, `col_varchar_20` varchar(20) DEFAULT NULL, PRIMARY KEY (`pk`)) DEFAULT CHARSET = utf8mb4 REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE;
create index idx9 on CC(col_datetime_key) local;
create index idx3 on CC(col_varchar_10_key) local;
create index idx5 on CC(col_varchar_20_key) local;
create index idx7 on CC(col_date_key) local;
create index idx1 on CC(col_int_key) local;
CREATE TABLE at1 (col_int_key INT DEFAULT NULL,col_time_nokey TIME DEFAULT NULL, col_varchar_key VARCHAR(1) DEFAULT NULL, col_varchar_nokey VARCHAR(1) DEFAULT NULL, KEY col_int_key (col_int_key), KEY col_varchar_key (col_varchar_key,col_int_key));
CREATE TABLE at2 ( col_int_key INT DEFAULT NULL, col_time_nokey TIME DEFAULT NULL, col_varchar_key VARCHAR(1) DEFAULT NULL, col_varchar_nokey VARCHAR(1) DEFAULT NULL, KEY col_int_key (col_int_key), KEY col_varchar_key (col_varchar_key,col_int_key));
CREATE TABLE at3 ( col_int_key INT DEFAULT NULL, col_time_nokey TIME DEFAULT NULL, col_varchar_key VARCHAR(1) DEFAULT NULL, col_varchar_nokey VARCHAR(1) DEFAULT NULL, KEY col_int_key (col_int_key), KEY col_varchar_key (col_varchar_key,col_int_key));


CREATE TABLE tl0 (c1 VARCHAR(10)); 
CREATE TABLE tl1 (c1 int); 
CREATE TABLE tl2 (c1 double); 
CREATE TABLE tl3 (c1 date);

create view tr0 as select 1;
create view tr1 as select '1';
create view tr2 as select 1.1;
create view tr3 as select date('2017-08-21');
CREATE TABLE `cb_loan_acctacctstathist_11` (`acctnbr` varchar(34) NOT NULL COMMENT '账号',  `effdatetime` date NOT NULL COMMENT '生效时间',   `acctstatcd` varchar(4) NOT NULL COMMENT '账户状态',   `postdate` date NOT NULL COMMENT '交易日期',   `mainttellerid` varchar(20) DEFAULT NULL COMMENT '维护柜员',   `maintbranchnbr` varchar(12) DEFAULT NULL COMMENT '维护机构',   `gmt_create` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '维护日期',   `gmt_modified` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '维护时间 ',   `timeuniqueextn` bigint(20) NOT NULL COMMENT '时间唯一流水号',   PRIMARY KEY (`acctnbr`, `effdatetime`, `timeuniqueextn`),   KEY `idx_acctacctstathist_acctnbr_postdate` (`acctnbr`, `postdate`) BLOCK_SIZE 16384,   KEY `idx_acctacctstathist_timeuniqueextn_acctstatcd` (`timeuniqueextn`, `acctstatcd`) BLOCK_SIZE 16384);
CREATE TABLE `cb_loan_acctloaninfotemp_11` (   `acctnbr` varchar(34) NOT NULL COMMENT '贷款账号',   `processyn` int(11) NOT NULL COMMENT '账号是否已处理',   `gmt_create` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '维护日期',   `gmt_modified` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '维护时间',   `shardingkey` varchar(4) NOT NULL COMMENT '分库分表字段',   PRIMARY KEY (`acctnbr`));
CREATE TABLE pullup1(pk int primary key, c1 int, uniq_c2 int, c3 int);
CREATE TABLE pullup2(pk int primary key, c1 int, uniq_c2 int, c3 int);
CREATE TABLE pullup3(pk int primary key, c1 int, uniq_c2 int, c3 int);
create unique index uniq on pullup1(uniq_c2) local;
create unique index uniq on pullup2(uniq_c2) local;
create unique index uniq on pullup3(uniq_c2) local;
create table pjt1(a int, b int);
create table pjt2(c int, d int);
create table pjt3(e int, f int);
create table pjt4(g int, h int);

create table test_simp (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2));
create unique index uniq on test_simp(c3, c4);


CREATE TABLE `lineitem` (  `l_orderkey` bigint(20) NOT NULL,  `l_partkey` bigint(20) NOT NULL,  `l_suppkey` bigint(20) NOT NULL,  `l_linenumber` bigint(20) NOT NULL,  `l_quantity` bigint(20) NOT NULL,  `l_extendedprice` decimal(10,2) NOT NULL,  `l_discount` decimal(10,2) NOT NULL,  `l_tax` decimal(10,2) NOT NULL,  `l_returnflag` char(1) DEFAULT NULL,  `l_linestatus` char(1) DEFAULT NULL,  `l_shipdate` date DEFAULT NULL,  `l_commitdate` date DEFAULT NULL,  `l_receiptdate` date DEFAULT NULL,  `l_shipinstruct` char(25) DEFAULT NULL,  `l_shipmode` char(10) DEFAULT NULL,  `l_comment` varchar(44) DEFAULT NULL,  PRIMARY KEY (`l_orderkey`, `l_linenumber`),  KEY `i_l_orderkey` (`l_orderkey`) BLOCK_SIZE 16384);

create table orders(    o_orderkey           bigint NOT NULL ,    o_custkey            bigint NOT NULL ,    o_orderstatus        char(1) ,    o_totalprice         number(10,2) ,    o_orderdate          date ,    o_orderpriority      char(15) ,    o_clerk              char(15) ,    o_shippriority       bigint ,    o_comment            varchar(79),    primary key(o_orderkey),    index(o_orderkey) local    );

create table partsupp(    ps_partkey           bigint NOT NULL ,    ps_suppkey           bigint NOT NULL ,    ps_availqty          bigint ,    ps_supplycost        number(10,2) ,    ps_comment           varchar(199) ,    primary key (PS_PARTKEY, PS_SUPPKEY) ,    unique index ps_pkey_skey(ps_partkey,ps_suppkey) local    );

CREATE TABLE `part` (  `p_partkey` bigint(20) NOT NULL,`p_name` varchar(55) DEFAULT NULL,`p_mfgr` char(25) DEFAULT NULL,`p_brand` char(10) DEFAULT NULL,`p_type` varchar(25) DEFAULT NULL,`p_size` bigint(20) DEFAULT NULL,`p_container` char(10) DEFAULT NULL,`p_retailprice` decimal(10,2) DEFAULT NULL,`p_comment` varchar(23) DEFAULT NULL, PRIMARY KEY (`p_partkey`));

create table customer(    c_custkey            bigint NOT NULL ,    c_name               varchar(25) ,    c_address            varchar(40) ,    c_nationkey          bigint ,    c_phone              char(15) ,    c_acctbal            number(10,2) ,    c_mktsegment         char(10) ,    c_comment            varchar(117),    primary key(C_CUSTKEY),    unique index i_c_custkey(c_custkey) local);

create table supplier(    s_suppkey            bigint NOT NULL ,    s_name               char(25) ,    s_address            varchar(40) ,    s_nationkey          bigint ,    s_phone              char(15) ,    s_acctbal            bigint ,    s_comment            varchar(101) ,    primary key (S_SUPPKEY)    );

create table nation(    n_nationkey          bigint NOT NULL ,    n_name               char(25) ,    n_regionkey          bigint ,    n_comment            varchar(152) ,    primary key (N_NATIONKEY) );

create table region(    r_regionkey          bigint ,    r_name               char(25) ,    r_comment            varchar(152) ,    primary key (R_REGIONKEY)    );

