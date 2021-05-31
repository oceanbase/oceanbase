create database test;
use test;
create table t1(c1 int primary key, c2 int) partition by hash (c1) partitions 5
create table t2(c1 int primary key, c2 int, c3 varchar(32)) partition by hash (c1) partitions 3
create table t3(c1 int, c2 int, c3 varchar(32), primary key(c1,c2)) partition by hash (c2) partitions 2
create index idx_t1_c2 on t1(c2) local;
create view vt as select * from t1 where c1 = 0;

CREATE TABLE `lineitem` (  `l_orderkey` bigint(20) NOT NULL,  `l_partkey` bigint(20) NOT NULL,  `l_suppkey` bigint(20) NOT NULL,  `l_linenumber` bigint(20) NOT NULL,  `l_quantity` bigint(20) NOT NULL,  `l_extendedprice` decimal(10,2) NOT NULL,  `l_discount` decimal(10,2) NOT NULL,  `l_tax` decimal(10,2) NOT NULL,  `l_returnflag` char(1) DEFAULT NULL,  `l_linestatus` char(1) DEFAULT NULL,  `l_shipdate` date DEFAULT NULL,  `l_commitdate` date DEFAULT NULL,  `l_receiptdate` date DEFAULT NULL,  `l_shipinstruct` char(25) DEFAULT NULL,  `l_shipmode` char(10) DEFAULT NULL,  `l_comment` varchar(44) DEFAULT NULL,  PRIMARY KEY (`l_orderkey`, `l_linenumber`),  KEY `i_l_orderkey` (`l_orderkey`) local) partition by hash(l_orderkey) partitions 5;

CREATE TABLE `part` (  `p_partkey` bigint(20) NOT NULL,`p_name` varchar(55) DEFAULT NULL,`p_mfgr` char(25) DEFAULT NULL,`p_brand` char(10) DEFAULT NULL,`p_type` varchar(25) DEFAULT NULL,`p_size` bigint(20) DEFAULT NULL,`p_container` char(10) DEFAULT NULL,`p_retailprice` decimal(10,2) DEFAULT NULL,`p_comment` varchar(23) DEFAULT NULL, PRIMARY KEY (`p_partkey`)) partition by hash(p_partkey) partitions 5;
