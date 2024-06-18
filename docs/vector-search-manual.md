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
下面介绍如何部署并使用OceanBase的向量数据库，包括基本SQL语法以及目前版本的一些功能约束和注意事项。
请注意，**该特性目前迭代中的功能，建议只用做个人学习或功能验证**。
## 部署单机版OceanBase向量数据库
拉取并启动OceanBase向量数据库单机docker镜像：
```shell
docker run -p 2881:2881 --name obvec -d oceanbase/oceanbase-ce:vector
```
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
## vector_l2_ops: 距离算法，欧式距离; hnsw: 索引类型
CREATE VECTOR INDEX vidx1_c1_t1 on t1 (c1 vector_l2_ops) with(type=hnsw);

## vector_cosine_ops: 距离算法，余弦距离; hnsw: 索引类型
CREATE VECTOR INDEX vidx2_c1_t1 on t1 (c1 vector_cosine_ops) with(type=hnsw);

## vector_ip_ops: 距离算法，内积; hnsw: 索引类型
CREATE VECTOR INDEX vidx3_c1_t1 on t1 (c1 vector_ip_ops) with(type=hnsw);

## vector_l2_ops: 距离算法，欧式距离; ivfflat: 索引类型
CREATE VECTOR INDEX vidx4_c1_t1 on t1 (c1 vector_l2_ops) with(type=ivfflat);

## vector_cosine_ops: 距离算法，余弦距离; ivfflat: 索引类型; lists: 聚簇中心数，建议值sqrt(主表数据行数）
CREATE VECTOR INDEX vidx5_c1_t1 on t1 (c1 vector_cosine_ops) with (type=ivfflat, lists=2);

# 支持增量更新
insert into t2 values ('[1.2, 5.4, 6.3]', 4, 10.3);

# 带有向量计算的SQL，通过approximate来指定走向量索引
select *, l2_distance(c1, '[3,1,2]') as dis from t1 order by l2_distance(c1,'[3,1,2]') approximate limit 2;
select c3 from t1 order by l2_distance(c1, '[3,1,2]') approximate limit 2;
```
## 包含向量列的建表语句
```sql
create table table_name (……, id int, vector_column_name vector(vector_dims),……, primary key (id));
```

- 建立包含向量列的表建议设置非向量列的主键，以便在建立向量索引之后获得良好的回表查询性能；
- 非向量主键列建议使用简单的int类型id值，虽然支持复杂类型主键以及多列主键，但是建立向量索引时会带来更多的内存占用和内存拷贝操作，导致一定的性能回退；
- 目前不支持不定长向量维度，插入等操作会根据指定vector_dims进行检查，不相同的vector维度不允许操作；
- 目前支持将包含向量列的表创建为分区表。推荐使用HASH或KEY分区。
### 示例
```sql
obclient> create table item (c1 vector(3), c2 int, c3 float, primary key (c2));

obclient> desc item;
+-------+-----------+------+-----+---------+-------+
| Field | Type      | Null | Key | Default | Extra |
+-------+-----------+------+-----+---------+-------+
| c1    | vector(3) | YES  |     | NULL    |       |
| c2    | int(11)   | NO   | PRI | NULL    |       |
| c3    | float     | YES  |     | NULL    |       |
+-------+-----------+------+-----+---------+-------+
```
## 包含向量列的DML操作
### 插入操作
```sql
insert into table_name values (……, '[float0, float1, ……]', ……),……
```

- 向量常量格式为'[float0, float1, ……]'；
- 插入会对vector维度做检查；
- 支持对已有ivfflat索引的表进行插入
#### 示例
```sql
obclient> insert into item values ('[1.1, 2.2, 3.3]', 1, 1.1), ('[  9.1, 3.14, 2.14]', 2, 2.43), ('[7576.420000,467.230000,2913.762000]', 3, 4.33);

obclient> select * from item;
+--------------------------------------------+----+------+
| c1                                         | c2 | c3   |
+--------------------------------------------+----+------+
| [1.10000002,2.20000005,3.29999995]         |  1 |  1.1 |
| [9.10000038,3.14000010,2.14000010]         |  2 | 2.43 |
| [7576.41992188,467.23001099,2913.76196289] |  3 | 4.33 |
+--------------------------------------------+----+------+
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

obclient> select * from item;
+--------------------------------------------+----+------+
| c1                                         | c2 | c3   |
+--------------------------------------------+----+------+
| [9.10000038,3.14000010,2.14000010]         |  2 | 2.43 |
| [7576.41992188,467.23001099,2913.76196289] |  3 | 4.33 |
+--------------------------------------------+----+------+
```
### 更新操作

- 目前更新对象为向量列、更新条件为向量列的update操作行为未定义，不建议使用；
## 向量运算操作
### 向量&标量乘法操作
```sql
obclient> select c1, c1 * 2, 3 * c1 from item;
+--------------------------------------+---------------------------------------+----------------------------------------+
| c1                                   | c1 * 2                                | 3 * c1                                 |
+--------------------------------------+---------------------------------------+----------------------------------------+
| [9.100000,3.140000,2.140000]         | [18.200000,6.280000,4.280000]         | [27.300000,9.420000,6.420000]          |
| [7576.420000,467.230000,2913.762000] | [15152.840000,934.460000,5827.524000] | [22729.260000,1401.690000,8741.286000] |
+--------------------------------------+---------------------------------------+----------------------------------------+
```
### 向量&向量距离操作

   1. l2_distance：欧式距离
```sql
obclient> select l2_distance(c1, '[1,2,3]') from item;
+----------------------------+
| l2_distance(c1, '[1,2,3]') |
+----------------------------+
|          8.224913752651519 |
|          8128.712146488757 |
+----------------------------+
```

   2. inner_product：向量内积

**注意⚠️**：inner_product 返回负内积，因为默认进行 ASC 顺序扫描，内积越大近似度越大
```sql
obclient> select inner_product(c1, '[1,2,3]') from item;
+------------------------------+
| inner_product(c1, '[1,2,3]') |
+------------------------------+
|           -21.80000066757202 |
|          -17252.166076660156 |
+------------------------------+
```

   3. cosine_distance: 余弦距离
```sql
obclient> select cosine_distance(c1, '[1,2,3]') from item;
+--------------------------------+
| cosine_distance(c1, '[1,2,3]') |
+--------------------------------+
|             0.4091877985702831 |
|             0.4329197185927741 |
+--------------------------------+
```
## 包含向量列的DDL操作
### 向量索引建立
```sql
create VECTOR index index_table_name on table_name (vector_column_name distance_function_type) with(vector_index_parameters);
```

- VECTOR为关键字，表示创建向量索引；
- 向量索引目前不支持多列索引；
- 单表多向量索引的场景下路径选择行为未定义，不建议使用；
- 要求索引列vector_column_name必须是向量列；
- distance_function_type是向量索引使用的索引排序标准，包含：
   - vector_l2_ops：欧式距离；
   - vector_ip_ops：向量内积；
   - vector_cosine_ops：余弦距离；
- vector_index_parameters为可选索引参数，使用key=value的形式指定，包括：
   - type：索引类型，仅支持ivfflat和hnsw，如type=ivfflat。不指定情况下默认为ivfflat。
   - lists：ivfflat索引使用，表示聚簇中心数量，如lists=3。不指定情况下默认为128.
   - m：hnsw索引使用，表示每个向量的最大连接度，如m=16。不指定情况下默认为16.
   - ef_construction：hnsw索引使用，表示索引构建时每个向量备选邻居的搜索规模，如ef_construction=200。不指定情况下默认为200。
#### HNSW索引
```sql
create VECTOR index index_table_name on table_name (vector_column_name distance_function_type) with(type=hnsw[, m=xx][, ef_construction=xx]);
```

##### 参数详解
- m：每个向量的最大连接度，大于0层最大连接为m，0层连接为2 * m，默认值16；推荐范围【5，48】，越大召回率越高，构建索引速度越慢；
- ef_construction：HNSW索引构建时每个向量备选邻居的搜索规模，默认值200；推荐范围【200，400】，越大召回率越高，构建索引速度越慢；
##### 示例
```sql
obclient> CREATE VECTOR INDEX vidx1 on item (c1 vector_l2_ops) with(type=hnsw);

obclient> drop INDEX vidx1 on item;
```
#### IVF_FLAT索引
```sql
create index index_table_name on table_name (vector_column_name distance_function_type) with (type=ivfflat[, lists=cluster_count]);
```

##### 参数详解
- cluster_count：指定IVF_FLAT聚簇中心数量, 默认为128，建议手动指定；聚簇中心数越多，建索引时间越长。为达到最优召回率，建议值：小于等于100万行，lists=rows/1000；大于100万行，lists=sqrt(rows)。
  - 特殊情况：当指定中心数小于数据行数时，会采用数据行数作为实际聚簇中心数。即实际取值为`MIN（row_count, cluster_count)`。
- vector_ivfflat_iters_count: 设置kmeans最大迭代次数，默认200。
- vector_ivfflat_elkan：设置kmeans算法为elkan kmeans。建议在测试欧式距离索引时开启，可以加快索引构建速度。默认值为'True'。
- vector_ivfflat_sample_count：构建IVF_FLAT索引时样本采样数量。默认值为10000。建索引时样本数取值为`MAX（lists * 50, 10000)`。

其中后三个参数为租户级配置项，使用如下方式进行修改：
```sql
alter system set vector_ivfflat_iters_count = 100;
alter system set vector_ivfflat_elkan = 'True';
alter system set vector_ivfflat_sample_count = 50000;
```
##### 示例
```sql
obclient> CREATE VECTOR INDEX vidx2 on item (c1 vector_l2_ops) with(type=ivfflat,lists=3);
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
select column_name1,column_name2…… from table_name order by vector_distance_func(vector_column_name, '[float0,float1,……]') [approximate|approx] limit K;
```

- vector_distance_func是向量距离表达式，包括：
  - l2_distance：欧式距离
  - inner_product：内积
  - cosine_distance：余弦距离
- 可选关键字approximate或approx用来在有向量索引的情况下强制走向量索引。目前即使不使用该关键字同样强制走向量索引。
- 要求order by子句表达式必须是一个向量距离表达式，且目前只支持向量列与常量向量；
- 要求limit子句必须是一个整数常量表达式；
- 在未建立向量索引时，走全表扫描后top-k排序计划；

##### 示例
```sql
obclient> select c1,c2 from item order by l2_distance(c1,'[3,1,2]') approx limit 2;
+------------------------------------+----+
| c1                                 | c2 |
+------------------------------------+----+
| [9.10000038,3.14000010,2.14000010] |  2 |
+------------------------------------+----+
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

##### 示例
```sql
obclient> create table ivfflat_test (c1 int, c2 vector(3), c3 int, primary key(c1,c3)) partition by hash(c3) partitions 100;

obclient> insert into ivfflat_test values (1, '[1.1, 2.2, 3.3]', 124), (2, '[2.1, 3.2, 4.3]', 124), (3, '[3.1, 4.2, 5.3]', 124), (2, '[  9.1, 3.14, 2.14]', 243), (3, '[7576.42, 467.23, 2913.762]', 546), (4, '[3,1,2]', 467), (5, '[42.4,53.1,5.23]', 4232);

obclient> create index ivfflat_idx1 on ivfflat_test (c2 vector_l2_ops) with(type=ivfflat, lists=128);

# 带分区键条件
obclient> select /*+probes(2)*/* from ivfflat_test where c3=124 order by l2_distance(c2,'[3,1,2]') approx limit 3;
+----+------------------------------------+-----+
| c1 | c2                                 | c3  |
+----+------------------------------------+-----+
|  1 | [1.10000002,2.20000005,3.29999995] | 124 |
|  2 | [2.09999990,3.20000005,4.30000019] | 124 |
+----+------------------------------------+-----+

# 带并行hint
obclient> select /*+parallel(2)*/* from ivfflat_test order by l2_distance(c2, '[3,1,2]') approx limit 3;
+----+------------------------------------+-----+
| c1 | c2                                 | c3  |
+----+------------------------------------+-----+
|  4 | [3.00000000,1.00000000,2.00000000] | 467 |
|  1 | [1.10000002,2.20000005,3.29999995] | 124 |
|  2 | [2.09999990,3.20000005,4.30000019] | 124 |
+----+------------------------------------+-----+
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
select /*+probes(xx)*/ * from t1 order by l2_distance(c1, '[3,1,2]') approx limit 2;
```

- vector_ivfflat_probes：设置查询时访问的聚簇中心数，该值需要小于索引创建时指定的聚簇中心数，默认值为0。建议值：数据量小于等于100万行，probes=lists/10；数据量大于100万行，probes=sqrt(lists)。理论上，该值越大，召回率越高，查询耗时越大。实际取值规则为hint probes优先，如果hint probes为0则取系统变量vector_ivfflat_probes值，如果系统变量为0则取sqrt(lists)，如果最后仍然为0则取1。
##### 注意事项

1. **在同一个向量列上同时建立IVF_FLAT以及HNSW索引，在查询时采用哪一个索引目前是未定义的；**
2. **当前版本推荐使用IVF_FLAT；**
3. **目前仅IVF_FLAT索引表支持动态更新，建立索引之后对数据表的插入和删除操作会同步到IVF_FLAT索引表，但索引表聚簇中心不会变化，长时间增量会导致聚簇中心偏移。建议导入数据后再建索引，不建议使用场景存在大量增量数据；**
