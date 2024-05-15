# 什么是向量数据库
非结构化数据呈爆炸式增长，而我们可以通过机器学习模型，将非结构化数据转化为 embedding 向量，随后处理分析这些数据。在此过程中，向量数据库应运而生。向量数据库是一套全托管的非结构化数据处理解决方案，可用于存储、索引、检索 embedding 向量。
# OceanBase 提供哪些向量数据库能力
Oceanbase vector_search分支版本将基于 Vector 类型存储、索引、检索 embedding 向量，提供语义搜索能力。OceanBase 提供的向量数据库能力包括：新的向量数据类型、向量索引以及向量搜索 SQL 操作符等几项功能，它使得 OceanBase 数据库能够将文档、图像及其他非结构化数据的语义内容存储为 embedding 向量，并使用这些 embedding 向量快速进行相似性查询。
OceanBase 向量数据库能力支持将您的向量数据与其他数据一起存储。目前的核心能力包括：

- 支持向量类型的存储
- 支持精确搜索、近似最近邻搜索
- 支持计算L2距离、内积和余弦距离
- 支持HNSW、IVF_FLAT索引
- 支持最大 16,000 维的向量存储
# 如何使用OceanBase的向量数据库功能
下面介绍如何使用OceanBase的向量数据库功能，包括基本SQL语法以及目前版本的一些功能约束和注意事项。
请注意，**该特性目前迭代中的功能，建议只用做个人学习或功能验证**。
## 操作示例（省流版）
```sql
# 建表语句，向量作为一个单独的列
create table t1 (c1 vector(3), c2 int, c3 float, primary key (c2));
# 分区表
create table t2 (c1 vector(3), c2 int, c3 float, primary key (c2)) partition by key(c2) partitions 2;

# 数据导入，使用标准SQL语法导入
insert into t1 values ('[1.1, 2.2, 3.3]', 1, 1.1), ('[ 9.1, 3.14, 2.14]', 2, 2.43);
insert into t1 values ('[7576.42, 467.23, 2913.762]', 3, 54.6);

insert into t2 values ('[7576.42, 467.23, 2913.762]', 3, 54.6);

# DML 操作和普通表一致
select * from t1;
delete from t1 where c2=1;

# 使用DDL语法创建向量索引，可以指定向量索引类型和距离算法，不建议同时创建两种类型的向量索引
## l2: 距离算法，欧式距离; hnsw: 索引类型
CREATE INDEX vidx1_c1_t1 on t1 (c1 l2) using hnsw;

## cosine: 距离算法，余弦距离; hnsw: 索引类型
CREATE INDEX vidx2_c1_t1 on t1 (c1 cosine) using hnsw;

## inner_product: 距离算法，内积; hnsw: 索引类型
CREATE INDEX vidx3_c1_t1 on t1 (c1 inner_product) using hnsw;

## l2: 距离算法，欧式距离; ivfflat: 索引类型
CREATE INDEX vidx4_c1_t1 on t1 (c1 l2) using ivfflat;
CREATE INDEX vidx1_c1_t2 on t1 (c1 l2) using ivfflat;

## cosine: 距离算法，余弦距离; ivfflat: 索引类型; lists: 聚簇中心数，建议值sqrt(主表数据行数）
CREATE INDEX vidx5_c1_t1 on t1 (c1 cosine) using ivfflat with (lists=2);
CREATE INDEX vidx2_c1_t2 on t1 (c1 cosine) using ivfflat with (lists=3);

# 支持增量更新
insert into t2 values ('[1.2, 5.4, 6.3]', 4, 10.3);

# 带有向量计算的SQL，优化器会自动使用向量索引
select *,c1 <-> '[3,1,2]' as dis from t1 order by c1 <-> '[3,1,2]' limit 2;
select c3 from t1 order by c1 <-> '[3,1,2]' limit 2;

select * from t2 order by c1 <-> '[3,1,2]' limit 3;
```
## 包含向量列的建表语句
```sql
create table table_name (……, id int, vector_column_name vector(vector_length),……, primary key (id));
```

- 建立包含向量列的表建议设置非向量列的主键，以便在建立向量索引之后获得良好的回表查询性能；
- 非向量主键列建议使用简单的int类型id值，虽然支持复杂类型主键以及多列主键，但是建立向量索引时会带来更多的内存占用和内存拷贝操作，导致一定的性能回退；
- 目前不支持不定长向量长度，插入等操作会根据指定vector_length进行检查，不相同的vector长度不允许操作；
- 目前支持将包含向量列的表创建为分区表。推荐使用HASH或KEY分区。
### 示例
```sql
obclient> create table item (c1 vector(3), c2 int, c3 float, primary key (c2));
Query OK, 0 rows affected (0.34 sec)

obclient> desc item;
+-------+-----------+------+-----+---------+-------+
| Field | Type      | Null | Key | Default | Extra |
+-------+-----------+------+-----+---------+-------+
| c1    | vector(3) | YES  |     | NULL    |       |
| c2    | int(11)   | NO   | PRI | NULL    |       |
| c3    | float     | YES  |     | NULL    |       |
+-------+-----------+------+-----+---------+-------+
3 rows in set (0.03 sec)
```
## 包含向量列的DML操作
### 插入操作
```sql
insert into table_name values (……, '[float0, float1, ……]', ……),……
```

- 向量常量格式为'[float0, float1, ……]'；
- 插入会对vector长度做检查；
- 支持对已有ivfflat索引的表进行插入
#### 示例
```sql
obclient> insert into item values ('[1.1, 2.2, 3.3]', 1, 1.1), ('[  9.1, 3.14, 2.14]', 2, 2.43);
Query OK, 2 rows affected (0.01 sec)
Records: 2  Duplicates: 0  Warnings: 0

obclient> select * from item;
+------------------------------+----+------+
| c1                           | c2 | c3   |
+------------------------------+----+------+
| [1.100000,2.200000,3.300000] |  1 |  1.1 |
| [9.100000,3.140000,2.140000] |  2 | 2.43 |
+------------------------------+----+------+
2 rows in set (0.01 sec)
```
### 删除操作
```sql
delete from table_name where 非向量列条件
```

- 目前条件中不支持向量列；
- 支持对已有ivfflat索引的表进行删除
#### 示例
```sql
obclient> delete from item where c2=1;
Query OK, 1 row affected (0.00 sec)

obclient> select * from item;
+------------------------------+----+------+
| c1                           | c2 | c3   |
+------------------------------+----+------+
| [9.100000,3.140000,2.140000] |  2 | 2.43 |
+------------------------------+----+------+
1 row in set (0.00 sec)
```
### 更新操作

- 目前更新对象为向量列、更新条件为向量列的update操作行为未定义，不建议使用；
## 向量运算操作
### 向量&标量乘法操作
```sql
obclient> select c1, c1 * 2, 3 * c1 from item limit 3;
+--------------------------------------+---------------------------------------+----------------------------------------+
| c1                                   | c1 * 2                                | 3 * c1                                 |
+--------------------------------------+---------------------------------------+----------------------------------------+
| [1.100000,2.200000,3.300000]         | [2.200000,4.400000,6.600000]          | [3.300000,6.600000,9.900000]           |
| [9.100000,3.140000,2.140000]         | [18.200000,6.280000,4.280000]         | [27.300000,9.420000,6.420000]          |
| [7576.420000,467.230000,2913.762000] | [15152.840000,934.460000,5827.524000] | [22729.260000,1401.690000,8741.286000] |
+--------------------------------------+---------------------------------------+----------------------------------------+
3 rows in set (0.01 sec)
```
### 向量&向量距离操作

   1. <->：欧式距离
```sql
obclient> select c1<->'[1,2,3]' from item limit 3;
+--------------------+
| c1<->'[1,2,3]'     |
+--------------------+
| 0.3741657386773941 |
|  8.224913373404002 |
|  8128.712231955564 |
+--------------------+
3 rows in set (0.01 sec)
```

   2. <@>：向量内积

**注意⚠️**：<@> 返回负内积，因为默认进行 ASC 顺序扫描，内积越大近似度越大
```sql
obclient> select c1<@>'[1,2,3]' from item limit 3;
+--------------------+
| c1<@>'[1,2,3]'     |
+--------------------+
| -15.399999999999999 |
| -21.799999999999997 |
|          -17252.166 |
+--------------------+
3 rows in set (0.01 sec)
```

   3. <~>: 余弦距离
```sql
obclient> select c1<~>'[1,2,3]' from item limit 3;
+--------------------+
| c1<~>'[1,2,3]'     |
+--------------------+
|                  0 |
| 0.4091877967874197 |
| 0.4329197210753687 |
+--------------------+
3 rows in set (0.01 sec)
```
## 包含向量列的DDL操作
### 向量索引建立
#### HNSW索引
```sql
create index index_table_name on table_name (vector_column_name distance_function_type) using hnsw;
```

- 向量索引目前不支持多列索引；
- 索引列vector_column_name必须是向量列；
- distance_function_type是向量索引使用的距离函数，包含以下函数：
   - l2：欧式距离；
   - inner_product：向量内积；
   - cosine：余弦距离；
##### 相关参数
```sql
alter system set vector_hnsw_m=xxx;
alter system set vector_hnsw_ef_construction=xxx;
```

- vector_hnsw_m：每个向量的最大连接度，大于0层最大连接为vector_hnsw_m，0层连接为2 * vector_hnsw_m，默认值16；推荐范围【5，48】，越大召回率越高，构建索引速度越慢；
- vector_hnsw_ef_construction：HNSW索引构建时每个向量备选邻居的搜索规模，默认值200；推荐范围【200，400】，越大召回率越高，构建索引速度越慢；
##### 示例
```sql
obclient> CREATE INDEX vidx1  on item (c1 l2) using hnsw;
Query OK, 0 rows affected (0.77 sec)

obclient> select * from __idx_500006_vidx1;
+--------------------------------------+----------+-------+---------+-------------+-------------+
| _c1                                  | base_pk0 | level | ref_pk0 | n_neighbors | distance    |
+--------------------------------------+----------+-------+---------+-------------+-------------+
| [1.100000,2.200000,3.300000]         |        1 |     0 |       1 |           0 |           0 |
| [1.100000,2.200000,3.300000]         |        1 |     0 |       2 |           1 |    8.138132 |
| [1.100000,2.200000,3.300000]         |        1 |     0 |       3 |           1 | 8128.500173 |
| [1.100000,2.200000,3.300000]         |        1 |     0 |       4 |           1 |    2.596151 |
| [1.100000,2.200000,3.300000]         |        1 |     0 |       5 |           1 |     65.5761 |
……
```
#### IVF_FLAT索引
```sql
create index index_table_name on table_name (vector_column_name distance_function_type) using ivfflat [with (lists=cluster_count)];
```

- 包含HNSW索引的所有要求；
- cluster_count：指定ivfflat聚簇中心数量, 默认为128，建议手动指定；聚簇中心数越多，建索引时间越长。为达到最优召回率，建议值：小于等于100万行，lists=rows/1000；大于100万行，lists=sqrt(rows)。
##### 相关参数
```sql
alter system set vector_ivfflat_iters_count=xxx;
alter system set vector_ivfflat_elkan = 'True';
SET SESSION _FORCE_PARALLEL_DDL_DOP = 2;
```

- vector_ivfflat_iters_count: 设置kmeans最大迭代次数，默认200。
- vector_ivfflat_elkan：设置kmeans算法为elkan kmeans。建议在测试欧式距离索引时开启，可以加快索引构建速度。默认值为'True'。
- vector_ivfflat_sample_count：构建索引时样本采样数量。默认值为10000。建索引时样本数取值为`MAX（list * 50, 10000)`。
##### 示例
```sql
obclient> create table t (c1 int, c2 int, c3 int, c4 vector(3), primary key (c1,c2));
Query OK, 0 rows affected (0.58 sec)

obclient> insert into t values(1,2,3,'[1,2,3]'),(2,3,4,'[2,3,4]'),(4,5,6,'[4,5,6]'),(10,11,12,'[10,11,12]'),(11,12,13,'[11,12,13]'),(25,26,27,'[25,26,27]'),(31,32,33,'[31,32,33]'),(50,51,52,'[50,51,52]'),(55,56,57,'[55,56,57]');
Query OK, 9 rows affected (0.11 sec)
Records: 9  Duplicates: 0  Warnings: 0

obclient> CREATE INDEX idx_ivfflat_t  on t (c4 l2) using ivfflat with(lists=3);
Query OK, 0 rows affected (1.29 sec)

# 分区表
create table ivfflat_test (c1 int, c2 vector(3), c3 int, c4 int, primary key(c1,c3)) partition by hash(c3) partitions 2;
insert into ivfflat_test values(1,'[1,2,3]',2,3),(2,'[2,3,4]',3,4),(4,'[4,5,6]',5,6),(10,'[10,11,12]',11,12),(11,'[11,12,13]',12,13),(25,'[25,26,27]',26,27),(31,'[31,32,33]',32,33),(50,'[50,51,52]',51,52),(55,'[55,56,57]',56,57);

create index ivfflat_idx1 on ivfflat_test (c2 l2) using ivfflat with(lists=3);
```
##### 注意事项
建ivfflat分区索引时，可以通过修改`_FORCE_PARALLEL_DDL_DOP`，或者在`create index`中增加`parallel` hint来提高建索引并行度。
### 向量索引删除
```sql
drop index index_table_name on table_name; 
```
与普通索引表删除一致，不做赘述
### 向量查询
#### 最邻近向量查询
```sql
select column_name1,column_name2…… from table_name order by vector_column_name <-> '[float0,float1,……]' limit K;
```

- order by子句表达式是一个向量列距离运算操作，且目前只支持向量列与常量向量；
- limit子句必须是一个整数常量表达式；
- 在未建立向量索引时，走全表扫描后top-k排序计划：
```sql
obclient> select c1,c2 from item order by c1 <-> '[3,1,2]' limit 2;
+------------------------------+----+
| c1                           | c2 |
+------------------------------+----+
| [3.000000,1.000000,2.000000] |  4 |
| [3.100000,1.500000,2.120000] |  6 |
+------------------------------+----+
2 rows in set (0.01 sec)

obclient> explain select c1,c2 from item order by c1 <-> '[3,1,2]' limit 2;
+------------------------------------------------------------------------------------------------------------------------------+
| Query Plan                                                                                                                   |
+------------------------------------------------------------------------------------------------------------------------------+
| =================================================                                                                            |
| |ID|OPERATOR         |NAME|EST.ROWS|EST.TIME(us)|                                                                            |
| -------------------------------------------------                                                                            |
| |0 |TOP-N SORT       |    |2       |3           |                                                                            |
| |1 |└─TABLE FULL SCAN|item|20      |3           |                                                                            |
| =================================================                                                                            |
| Outputs & filters:                                                                                                           |
| -------------------------------------                                                                                        |
|   0 - output([item.c1], [item.c2]), filter(nil), rowset=256                                                                  |
|       sort_keys([(T_OP_VECTOR_L2_DISTANCE, item.c1, cast('[3,1,2]', (-1, -1))), ASC]), topn(2)                               |
|   1 - output([item.c2], [item.c1], [(T_OP_VECTOR_L2_DISTANCE, item.c1, cast('[3,1,2]', (-1, -1)))]), filter(nil), rowset=256 |
|       access([item.c2], [item.c1]), partitions(p0)                                                                           |
|       is_index_back=false, is_global_index=false,                                                                            |
|       range_key([item.c2]), range(MIN ; MAX)always true                                                                      |
+------------------------------------------------------------------------------------------------------------------------------+
14 rows in set (0.01 sec)
```

- **建立向量索引后，满足以下条件，走ANN查询计划：**
   - **只有一个基表，无join等操作；**
   - **存在order by子句和limit子句；**
   - **order by 必须升序；**
   - **order by后面是一个vector distance表达式，且引用基表的向量列；**
   - **该向量列上建有向量索引；**
   - **特殊：**
      - **分区表可以额外在where子句中增加，且只能增加分区键；**
      - **分区表可开启并行查询进行加速，并行度建议为可被分区数整除的数值。**
```sql
obclient> select c1,c2 from item order by c1 <-> '[3,1,2]' limit 2;
+------------------------------+----+
| c1                           | c2 |
+------------------------------+----+
| [3.000000,1.000000,2.000000] |  4 |
| [3.100000,1.500000,2.120000] |  6 |
+------------------------------+----+
2 rows in set (0.04 sec)

obclient> explain select c1,c2 from item order by c1 <-> '[3,1,2]' limit 2;
+-------------------------------------------------------------------------------------------------------+
| Query Plan                                                                                            |
+-------------------------------------------------------------------------------------------------------+
| ======================================================                                                |
| |ID|OPERATOR       |NAME       |EST.ROWS|EST.TIME(us)|                                                |
| ------------------------------------------------------                                                |
| |0 |TABLE FULL SCAN|item(vidx1)|381     |13          |                                                |
| ======================================================                                                |
| Outputs & filters:                                                                                    |
| -------------------------------------                                                                 |
|   0 - output([item.c1], [item.c2]), filter(nil), rowset=256                                           |
|       access([item.c2], [item.c1]), partitions(p0)                                                    |
|       is_index_back=false, is_global_index=false,                                                     |
|       range_key([item.c2], [item.level], [item.ref_pk0]), range(MIN,MIN,MIN ; MAX,MAX,MAX)always true |
+-------------------------------------------------------------------------------------------------------+
11 rows in set (0.01 sec)

// 带分区键条件
obclient> select * from ivfflat_test where c3=4 order by c2 <-> '[3,1,2]' limit 3;
+----+---------------------------------+----+------+
| c1 | c2                              | c3 | c4   |
+----+---------------------------------+----+------+
|  1 | [1.000000,2.000000,3.000000]    |  2 |    3 |
| 11 | [11.000000,12.000000,13.000000] | 12 |   13 |
+----+---------------------------------+----+------+

obclient> explain select * from ivfflat_test where c3=4 order by c2 <-> '[3,1,2]' limit 3;
+------------------------------------------------------------------------------------------------------------------------------------+
| Query Plan                                                                                                                         |
+------------------------------------------------------------------------------------------------------------------------------------+
| =====================================================================                                                              |
| |ID|OPERATOR       |NAME                      |EST.ROWS|EST.TIME(us)|                                                              |
| ---------------------------------------------------------------------                                                              |
| |0 |TABLE FULL SCAN|ivfflat_test(ivfflat_idx1)|1       |8           |                                                              |
| =====================================================================                                                              |
| Outputs & filters:                                                                                                                 |
| -------------------------------------                                                                                              |
|   0 - output([ivfflat_test.c1], [ivfflat_test.c2], [ivfflat_test.c3], [ivfflat_test.c4]), filter([ivfflat_test.c3 = 4]), rowset=16 |
|       access([ivfflat_test.c1], [ivfflat_test.c3], [ivfflat_test.c2], [ivfflat_test.c4]), partitions(p0)                           |
|       is_index_back=true, is_global_index=false, filter_before_indexback[true],                                                    |
|       range_key([ivfflat_test.center_idx], [ivfflat_test.c1], [ivfflat_test.c3]), range(MIN,MIN,MIN ; MAX,MAX,MAX)always true      |
+------------------------------------------------------------------------------------------------------------------------------------+
11 rows in set (0.01 sec)

// 带并行hint
obclient> select /*+parallel(2)*/* from ivfflat_test order by c2 <-> '[3,1,2]' limit 3;
+----+------------------------------+----+------+
| c1 | c2                           | c3 | c4   |
+----+------------------------------+----+------+
|  1 | [1.000000,2.000000,3.000000] |  2 |    3 |
|  2 | [2.000000,3.000000,4.000000] |  3 |    4 |
|  4 | [4.000000,5.000000,6.000000] |  5 |    6 |
+----+------------------------------+----+------+
3 rows in set (0.17 sec)

obclient> explain select /*+parallel(2)*/* from ivfflat_test order by c2 <-> '[3,1,2]' limit 3;
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Query Plan                                                                                                                                                      |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ===================================================================================                                                                             |
| |ID|OPERATOR                     |NAME                      |EST.ROWS|EST.TIME(us)|                                                                             |
| -----------------------------------------------------------------------------------                                                                             |
| |0 |LIMIT                        |                          |3       |24          |                                                                             |
| |1 |└─PX COORDINATOR MERGE SORT  |                          |3       |24          |                                                                             |
| |2 |  └─EXCHANGE OUT DISTR       |:EX10000                  |3       |22          |                                                                             |
| |3 |    └─TOP-N SORT             |                          |3       |20          |                                                                             |
| |4 |      └─PX PARTITION ITERATOR|                          |9       |20          |                                                                             |
| |5 |        └─TABLE FULL SCAN    |ivfflat_test(ivfflat_idx1)|9       |20          |                                                                             |
| ===================================================================================                                                                             |
| Outputs & filters:                                                                                                                                              |
| -------------------------------------                                                                                                                           |
|   0 - output([ivfflat_test.c1], [ivfflat_test.c2], [ivfflat_test.c3], [ivfflat_test.c4]), filter(nil), rowset=16                                                |
|       limit(3), offset(nil)                                                                                                                                     |
|   1 - output([ivfflat_test.c1], [ivfflat_test.c2], [ivfflat_test.c3], [ivfflat_test.c4]), filter(nil), rowset=16                                                |
|       sort_keys([(T_OP_VECTOR_L2_DISTANCE, ivfflat_test.c2, cast('[3,1,2]', VECTOR(-1, -1))), ASC])                                                             |
|   2 - output([ivfflat_test.c1], [ivfflat_test.c2], [ivfflat_test.c3], [ivfflat_test.c4], [(T_OP_VECTOR_L2_DISTANCE, ivfflat_test.c2, cast('[3,1,2]', VECTOR(-1, |
|        -1)))]), filter(nil), rowset=16                                                                                                                          |
|       dop=2                                                                                                                                                     |
|   3 - output([ivfflat_test.c1], [ivfflat_test.c2], [ivfflat_test.c3], [ivfflat_test.c4], [(T_OP_VECTOR_L2_DISTANCE, ivfflat_test.c2, cast('[3,1,2]', VECTOR(-1, |
|        -1)))]), filter(nil), rowset=16                                                                                                                          |
|       sort_keys([(T_OP_VECTOR_L2_DISTANCE, ivfflat_test.c2, cast('[3,1,2]', VECTOR(-1, -1))), ASC]), topn(3)                                                    |
|   4 - output([ivfflat_test.c1], [ivfflat_test.c3], [ivfflat_test.c2], [ivfflat_test.c4], [(T_OP_VECTOR_L2_DISTANCE, ivfflat_test.c2, cast('[3,1,2]', VECTOR(-1, |
|        -1)))]), filter(nil), rowset=16                                                                                                                          |
|       force partition granule                                                                                                                                   |
|   5 - output([ivfflat_test.c1], [ivfflat_test.c3], [ivfflat_test.c2], [ivfflat_test.c4], [(T_OP_VECTOR_L2_DISTANCE, ivfflat_test.c2, cast('[3,1,2]', VECTOR(-1, |
|        -1)))]), filter(nil), rowset=16                                                                                                                          |
|       access([ivfflat_test.c1], [ivfflat_test.c3], [ivfflat_test.c2], [ivfflat_test.c4]), partitions(p[0-1])                                                    |
|       is_index_back=true, is_global_index=false,                                                                                                                |
|       range_key([ivfflat_test.center_idx], [ivfflat_test.c1], [ivfflat_test.c3]), range(MIN,MIN,MIN ; MAX,MAX,MAX)always true                                   |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
30 rows in set (0.03 sec)

```
##### 最邻近向量查询参数

1. HNSW索引
```sql
alter system set vector_hnsw_ef_search=xxx;
```

- vector_hnsw_ef_search：HNSW索引查询时每个向量备选邻居的搜索规模，默认值20；推荐范围【1，1000】，越大召回率越高，查询耗时越大；
2. IVF_FLAT索引
```sql
// 设置session系统变量
set @@vector_ivfflat_probes = xx;

// 设置global系统变量
set global vector_ivfflat_probes = xx;

// 使用hint
select /*+probes(xx)*/ * from t1 order by c1 <-> '[3,1,2]' limit 2;

```

- vector_ivfflat_probes：设置查询时访问的聚簇中心数，默认为1；该值需要小于索引创建时指定的聚簇中心数，建议值：数量量小于等于100万行，probes=lists/10；数据量大于100万行，probes=sqrt(lists)。理论上，该值越大，召回率越高，查询耗时越大。
##### 注意事项

1. **在同一个向量列上同时建立IVF_FLAT以及HNSW索引，在查询时采用哪一个索引目前是未定义的；**
2. **当前版本推荐使用IVF_FLAT；**
3. **目前不支持索引表动态更新，即建立索引之后对数据表的更新操作不会同步到索引表，请导入数据后再建索引；**
