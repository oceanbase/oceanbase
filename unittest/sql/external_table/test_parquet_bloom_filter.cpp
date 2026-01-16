/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
// TODO: @liushang: please remove this macro after moving OZ from ob_parquet_table_row_iter.h
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_parquet_table_row_iter.h"

namespace oceanbase {
namespace sql {
namespace unittest {

class TestObParquetBloomFilter : public ::testing::Test {
public:
  TestObParquetBloomFilter()
  {}
  virtual ~TestObParquetBloomFilter()
  {}

protected:
  void load_file(std::string path);
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::shared_ptr<parquet::FileMetaData> file_meta_;
  static const ObExternalTablePushdownFilter::PushdownLevel LEVEL = ObExternalTablePushdownFilter::PushdownLevel::ROW_GROUP;

  void test_bloom_filter(
      ObParquetTableRowIterator::ParquetBloomFilterParamBuilder& builder,
      const ObLakeTableParquetReaderMetrics& metrics,
      const ObDatumMeta& meta,
      const ObDatum& not_exist_v,
      const ObDatum& exist_v,
      const int64_t no_bf_col,
      const int64_t has_bf_col) {
    bool is_contain = true;
    int prev_load_cnt = metrics.load_bloom_filter_count_;
    int prev_skip_cnt = metrics.skip_bloom_filter_count_;

    {
      // column without bloomfilter => may contain, bloom filter won't load/skip
      ObExternalTablePushdownFilter::BloomFilterItem item(no_bf_col, &meta,
                                                          &not_exist_v);
      EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
      EXPECT_TRUE(is_contain);
      EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_cnt);
      EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_cnt);
    }

    // column with bloomfilter
    {
      // value exists in bloom filter => may contain, load = true, skip = false
      ObExternalTablePushdownFilter::BloomFilterItem item(has_bf_col, &meta,
                                                          &exist_v);
      EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
      EXPECT_TRUE(is_contain);
      EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_cnt + 1);
      EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_cnt);
    }
    {
      // value does not exist in bloom filter => won't contain, load = true,
      // skip = true
      ObExternalTablePushdownFilter::BloomFilterItem item(has_bf_col, &meta,
                                                          &not_exist_v);
      EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
      EXPECT_FALSE(is_contain);
      EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_cnt + 1);
      EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_cnt + 1);
    }
  }

  void test_bloom_filter_nyi(
      ObParquetTableRowIterator::ParquetBloomFilterParamBuilder& builder,
      const ObLakeTableParquetReaderMetrics& metrics,
      const ObDatumMeta& meta,
      const ObDatum& not_exist_v,
      const ObDatum& exist_v,
      const int64_t no_bf_col,
      const int64_t has_bf_col) {
    bool is_contain = true;
    int prev_load_cnt = metrics.load_bloom_filter_count_;
    int prev_skip_cnt = metrics.skip_bloom_filter_count_;

    for (int col : {no_bf_col, has_bf_col}) {
      for (const ObDatum &v : {not_exist_v, exist_v}) {
        ObExternalTablePushdownFilter::BloomFilterItem item(col, &meta, &v);
        EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
        EXPECT_TRUE(is_contain);
        EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_cnt);
        EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_cnt);
      }
    }
  }
};

void TestObParquetBloomFilter::load_file(std::string path)
{
  file_reader_ = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/false);
  file_meta_ = file_reader_->metadata();
}

/**
OceanBase(admin@test)>CREATE EXTERNAL TABLE `smoking_test` (
    -> `i_no_bf` bigint,
    -> `i_has_bf` bigint,
    -> `s_no_bf` text,
    -> `s_has_bf` text
    -> )
    -> LOCATION='file:///tmp/gen_parquet_test_data_20250716_163335/bloom_filter_smoking_test'
    -> FORMAT (
    ->   TYPE = 'PARQUET'
    -> ) DEFAULT CHARSET = utf8mb4;
Query OK, 0 rows affected (0.405 sec)

OceanBase(admin@test)>select * from smoking_test;
+---------+----------+---------+----------+
| i_no_bf | i_has_bf | s_no_bf | s_has_bf |
+---------+----------+---------+----------+
|       1 |        1 | uno     | uno      |
|       2 |        2 | dos     | dos      |
|       4 |        4 | cuatro  | cuatro   |
+---------+----------+---------+----------+
 3 rows in set (0.024 sec)
 **/
TEST_F(TestObParquetBloomFilter, smoking_test)
{
  load_file("./bloom_filter_smoking_test.parquet");
  auto &bloom_filter_reader = file_reader_->GetBloomFilterReader();
  auto bf_rg_0 = bloom_filter_reader.RowGroup(0);
  auto rg_0 = file_reader_->RowGroup(0);

  ObLakeTableParquetReaderMetrics metrics;
  ObParquetTableRowIterator::ParquetBloomFilterParamBuilder builder(rg_0, bf_rg_0, file_meta_, &metrics, -1);
  builder.set_tz_offset(0);

  ObDatumMeta int64_meta(common::ObIntType, common::CS_TYPE_INVALID, 0);
  const int64_t i[] = {3, 2};
  ObDatum not_exist_i(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
  ObDatum exist_i(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);

  ObDatumMeta text_meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
  ObDatum not_exist_str("tres", 4, false);
  ObDatum exist_str("dos", 3, false);
  bool is_contain;

  test_bloom_filter(builder, metrics, int64_meta, not_exist_i, exist_i, 0, 1);
  test_bloom_filter(builder, metrics, text_meta, not_exist_str, exist_str, 2, 3);
  metrics.dump_metrics();
}

TEST_F(TestObParquetBloomFilter, corner_test)
{
  load_file("./bloom_filter_smoking_test.parquet");
  auto &bloom_filter_reader = file_reader_->GetBloomFilterReader();
  auto bf_rg_0 = bloom_filter_reader.RowGroup(0);
  auto rg_0 = file_reader_->RowGroup(0);

  ObLakeTableParquetReaderMetrics metrics;
  ObParquetTableRowIterator::ParquetBloomFilterParamBuilder builder(rg_0, bf_rg_0, file_meta_, &metrics, -1);
  builder.set_tz_offset(0);
  int64_t i_no_bf_col = 0, i_has_bf_col = 1, s_no_bf_col = 2, s_has_bf_col = 3;

  ObDatumMeta int64_meta(common::ObIntType, common::CS_TYPE_INVALID, 0);
  const int64_t i[] = {3, 2};
  ObDatum not_exist_i(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
  ObDatum exist_i(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);

  ObDatumMeta text_meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
  ObDatum not_exist_str("tres", 4, false);
  ObDatum exist_str("dos", 3, false);
  bool is_contain;

  // 1. normal case
  EXPECT_EQ(metrics.load_bloom_filter_count_, 0);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 0);

  ObExternalTablePushdownFilter::BloomFilterItem item_i(i_has_bf_col, &int64_meta, &not_exist_i);
  EXPECT_EQ(builder.may_contain(LEVEL, item_i, is_contain), OB_SUCCESS);
  EXPECT_FALSE(is_contain);

  EXPECT_EQ(metrics.load_bloom_filter_count_, 1);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 1);

  ObExternalTablePushdownFilter::BloomFilterItem item_s(s_has_bf_col, &text_meta, &not_exist_str);
  EXPECT_EQ(builder.may_contain(LEVEL, item_s, is_contain), OB_SUCCESS);
  EXPECT_FALSE(is_contain);

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);

  // 2. level != ROW GROUP
  EXPECT_EQ(
      builder.may_contain(ObExternalTablePushdownFilter::PushdownLevel::FILE, item_i, is_contain),
      OB_SUCCESS);
  EXPECT_TRUE(is_contain);
  EXPECT_EQ(
      builder.may_contain(ObExternalTablePushdownFilter::PushdownLevel::PAGE, item_i, is_contain),
      OB_SUCCESS);
  EXPECT_TRUE(is_contain);
  EXPECT_EQ(builder.may_contain(
                ObExternalTablePushdownFilter::PushdownLevel::ENCODING, item_i, is_contain),
            OB_SUCCESS);
  EXPECT_TRUE(is_contain);

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 0);
  metrics.dump_metrics();

  // 3. bloom filter size > non-bloom filter size
  // TODO: bloom_filter_length won't set, don't know why
  ObLakeTableParquetReaderMetrics metrics2;
  load_file("./bloom_filter_smoking_test_pyspark4.0.parquet");
  bloom_filter_reader = file_reader_->GetBloomFilterReader();
  bf_rg_0 = bloom_filter_reader.RowGroup(0);
  rg_0 = file_reader_->RowGroup(0);
  ObParquetTableRowIterator::ParquetBloomFilterParamBuilder builder2(rg_0, bf_rg_0, file_meta_, &metrics2, 20);
  builder2.set_tz_offset(0);
  EXPECT_EQ(builder2.may_contain(LEVEL, item_i, is_contain), OB_SUCCESS);
  EXPECT_TRUE(is_contain);

  EXPECT_EQ(metrics2.load_bloom_filter_count_, 0);
  EXPECT_EQ(metrics2.skip_bloom_filter_count_, 0);
  EXPECT_EQ(metrics2.skip_load_bloom_filter_count_, 1);

  EXPECT_EQ(builder2.may_contain(LEVEL, item_s, is_contain), OB_SUCCESS);
  EXPECT_TRUE(is_contain);

  EXPECT_EQ(metrics2.load_bloom_filter_count_, 0);
  EXPECT_EQ(metrics2.skip_bloom_filter_count_, 0);
  EXPECT_EQ(metrics2.skip_load_bloom_filter_count_, 2);

  metrics2.dump_metrics();
}

/***
 CREATE EXTERNAL TABLE `bloom_filter_all_types` (
 `int8_no_bf` tinyint,
 `int8_has_bf` tinyint,
 `int16_no_bf` smallint,
 `int16_has_bf` smallint,
 `int32_no_bf` integer,
 `int32_has_bf` integer,
 `int64_no_bf` bigint,
 `int64_has_bf` bigint,
 `float_no_bf` FLOAT,
 `float_has_bf` float,
 `double_no_bf` double,
 `double_has_bf` double,
 `short_deciaml_no_bf` decimal(10,3),
 `short_deciaml_has_bf` decimal(10,3),
 `long_deciaml_no_bf` decimal(20,3),
 `long_deciaml_has_bf` decimal(20,3)
)
LOCATION='file:///path_to_dir'
FORMAT (
 TYPE = 'PARQUET'
) DEFAULT CHARSET = utf8mb4

 select * from bloom_filter_all_types;
+----------+-----------+-----------+------------+-----------+------------+-----------+------------+-----------+------------+------------+-------------+-------------------+--------------------+------------------+-------------------+
|int8_no_bf|int8_has_bf|int16_no_bf|int16_has_bf|int32_no_bf|int32_has_bf|int64_no_bf|int64_has_bf|float_no_bf|float_has_bf|double_no_bf|double_has_bf|short_deciaml_no_bf|short_deciaml_has_bf|long_deciaml_no_bf|long_deciaml_has_bf|
+----------+-----------+-----------+------------+-----------+------------+-----------+------------+-----------+------------+------------+-------------+-------------------+--------------------+------------------+-------------------+
|         1|          1|          1|           1|          1|           1|          1|           1|        1.0|         1.0|         1.0|          1.0|              1.000|               1.000|             1.000|              1.000|
|         2|          2|          2|           2|          2|           2|          2|           2|        2.0|         2.0|         2.0|          2.0|              2.000|               2.000|             2.000|              2.000|
|         4|          4|          4|           4|          4|           4|          4|           4|        4.0|         4.0|         4.0|          4.0|              4.000|               4.000|             4.000|              4.000|
+----------+-----------+-----------+------------+-----------+------------+-----------+------------+-----------+------------+------------+-------------+-------------------+--------------------+------------------+-------------------+
 */
TEST_F(TestObParquetBloomFilter, all_numeric_types)
{
  load_file("./bloom_filter_all_numerics.parquet");
  auto &bloom_filter_reader = file_reader_->GetBloomFilterReader();
  auto bf_rg_0 = bloom_filter_reader.RowGroup(0);
  auto rg_0 = file_reader_->RowGroup(0);
  bool is_contain;
  ObLakeTableParquetReaderMetrics metrics;
  ObParquetTableRowIterator::ParquetBloomFilterParamBuilder builder(rg_0, bf_rg_0, file_meta_, &metrics, -1);
  builder.set_tz_offset(0);

  // start check tinyint
  // [0]`int8_no_bf` tinyint,
  // [1]`int8_has_bf` tinyint,
  {
    ObDatumMeta meta(common::ObTinyIntType, common::CS_TYPE_INVALID, 0);
    const int8_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int8_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int8_t), false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 0, 1);
  }
  // end check tinyint

  // start check smallint
  // [2]`int16_no_bf` smallint,
  // [3]`int16_has_bf` smallint,
  {
    ObDatumMeta meta(common::ObSmallIntType, common::CS_TYPE_INVALID, 0);
    const int16_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int16_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int16_t), false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 2, 3);
  }
  // end check smallint

  // start check integer
  // [4]`int32_no_bf` integer,
  // [5]`int32_has_bf` integer,
  {
    ObDatumMeta meta(common::ObInt32Type, common::CS_TYPE_INVALID, 0);
    const int32_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int32_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int32_t), false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 4, 5);
  }
  // end check integer

  // start check bigint
  // [6]`int64_no_bf` bigint,
  // [7]`int64_has_bf` bigint,
  {
    ObDatumMeta meta(common::ObIntType, common::CS_TYPE_INVALID, 0);
    const int64_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 6, 7);
  }
  // end check bigint

  // start check float
  // [8]`float_no_bf` FLOAT,
  // [9]`float_has_bf` float,
  {
    ObDatumMeta meta(common::ObFloatType, common::CS_TYPE_INVALID, 0);
    const float i[] = {3.0, 2.0};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(float), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(float), false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 8, 9);
  }
  // start check float

  // start check double
  // [10]`double_no_bf` double,
  // [11]`double_has_bf` double,
  {
    ObDatumMeta meta(common::ObDoubleType, common::CS_TYPE_INVALID, 0);
    const double i[] = {3.0, 2.0};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(double), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(double), false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 10, 11);
  }
  // end check double

  // start check short decimal: bloom filter NYI
  // [12]`short_deciaml_no_bf` decimal(10,3),
  // [13]`short_deciaml_has_bf` decimal(10,3),
  {
    ObDatumMeta meta(common::ObDecimalIntType, common::CS_TYPE_INVALID, 0);
    meta.scale_ = 3;
    meta.precision_ = 10;
    const int64_t i[] = {3000L, 2000L};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);
    test_bloom_filter_nyi(builder, metrics, meta, not_exist_v, exist_v, 12, 13);
  }
  // end check short decimal: bloom filter NYI

  // start check long decimal: bloom filter NYI
  // [14]`long_deciaml_no_bf` decimal(20,3),
  // [15]`long_deciaml_has_bf` decimal(20,3)
  {
    ObDatumMeta meta(common::ObDecimalIntType, common::CS_TYPE_INVALID, 0);
    meta.scale_ = 3;
    meta.precision_ = 20;
    const int128_t i[] = {3000LL, 2000LL};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int128_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int128_t), false);
    test_bloom_filter_nyi(builder, metrics, meta, not_exist_v, exist_v, 14, 15);
  }
  // end check long decimal: bloom filter NYI

  EXPECT_EQ(metrics.load_bloom_filter_count_, 6);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 6);
  metrics.dump_metrics();
}

/**
OceanBase(admin@test)>CREATE EXTERNAL TABLE `bloom_filter_all_types` (
    -> `binary_no_bf` binary(10),
    -> `binary_has_bf` binary(10),
    -> `str_no_bf` text,
    -> `str_has_bf` text,
    -> `bool_no_bf` BOOL,
    -> `bool_has_bf` BOOL,
    -> `date_no_bf` DATE,
    -> `date_has_bf` DATE,
    -> `ts_no_bf` timestamp
    -> )
    -> LOCATION='file:///data/jsl/third/spark-3.5.6-bin-hadoop3/bloom_filter_all_types'
    -> FORMAT (
    ->   TYPE = 'PARQUET'
    -> ) DEFAULT CHARSET = utf8mb4;
Query OK, 0 rows affected (0.453 sec)

OceanBase(admin@test)>select * from bloom_filter_all_types;
+--------------+---------------+-----------+------------+------------+-------------+------------+-------------+---------------------+
| binary_no_bf | binary_has_bf | str_no_bf | str_has_bf | bool_no_bf | bool_has_bf | date_no_bf | date_has_bf | ts_no_bf
|
+--------------+---------------+-----------+------------+------------+-------------+------------+-------------+---------------------+
| hola         | hola          | hola      | hola       |          1 |           1 | 1970-01-01 | 1970-01-01  |
1970-01-01 00:00:00 | | bonjour      | bonjour       | bonjour   | bonjour    |          1 |           1 | 1969-12-31 |
1969-12-31  | 1969-12-31 23:59:59 | | ciao         | ciao          | ciao      | ciao       |          1 |           1 |
2025-07-16 | 2025-07-16  | 2025-07-16 09:00:00 |
+--------------+---------------+-----------+------------+------------+-------------+------------+-------------+---------------------+
3 rows in set (0.051 sec)
 */
TEST_F(TestObParquetBloomFilter, all_other_types)
{
  load_file("./bloom_filter_all_other_types.parquet");
  auto &bloom_filter_reader = file_reader_->GetBloomFilterReader();
  auto bf_rg_0 = bloom_filter_reader.RowGroup(0);
  auto rg_0 = file_reader_->RowGroup(0);
  bool is_contain;
  ObLakeTableParquetReaderMetrics metrics;
  ObParquetTableRowIterator::ParquetBloomFilterParamBuilder builder(rg_0, bf_rg_0, file_meta_, &metrics, -1);
  builder.set_tz_offset(0);

  // start check binary
  // ->[0] `binary_no_bf` binary(10),
  // ->[1] `binary_has_bf` binary(10),
  {
    ObDatumMeta meta(common::ObCharType, common::CS_TYPE_BINARY, 0);
    ObDatum not_exist_v("aaa", 3, false);
    ObDatum exist_v("hola", 4, false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 0, 1);
  }
  // end check binary

  // start check text
  // ->[2] `str_no_bf` text,
  // ->[3] `str_has_bf` text,
  {
    ObDatumMeta meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
    ObDatum not_exist_v("aaa", 3, false);
    ObDatum exist_v("hola", 4, false);
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, 2, 3);
  }
  // end check text

  // start check bool: NYI
  // ->[4] `bool_no_bf` BOOL,
  // ->[5] `bool_has_bf` BOOL,
  {
    ObDatumMeta meta(common::ObTinyIntType, common::CS_TYPE_INVALID, 0);
    const int64_t i[] = {0, 1};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);
    test_bloom_filter_nyi(builder, metrics, meta, not_exist_v, exist_v, 4, 5);
  }
  // end check bool

  // start check date: NYI
  // ->[6] `date_no_bf` DATE,
  // ->[7] `date_has_bf` DATE,
  {
    ObDatumMeta meta(common::ObMySQLDateType, common::CS_TYPE_INVALID, 0);
    ObMySQLDate not_exist_d;
    not_exist_d.year_ = 2025;
    not_exist_d.month_ = 7;
    not_exist_d.day_ = 15;
    ObMySQLDate exist_d;
    exist_d.year_ = 2025;
    exist_d.month_ = 7;
    exist_d.day_ = 16;
    ObDatum not_exist_v(reinterpret_cast<const char *>(&not_exist_d), sizeof(int32_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&exist_d), sizeof(int32_t), false);
    test_bloom_filter_nyi(builder, metrics, meta, not_exist_v, exist_v, 6, 7);
  }
  // end check date: NYI

  // start check timestamp: NYI
  // ->[8] `ts_no_bf` timestamp
  {
    ObDatumMeta meta(common::ObTimestampType, common::CS_TYPE_INVALID, 0);
    const int64_t i[] = {10, 0};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int32_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int32_t), false);

    ObExternalTablePushdownFilter::BloomFilterItem item(8, &meta, &not_exist_v);
    EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
    EXPECT_TRUE(is_contain);
    item.datum_ = &exist_v;
    EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
    EXPECT_TRUE(is_contain);
  }
  // end check timestamp: NYI

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);
  metrics.dump_metrics();
}

/**
+---------+----------+-------------+-----------+-------------------+
|int_no_bf|int_has_bf|double_has_bf|text_has_bf|      binary_has_bf|
+---------+----------+-------------+-----------+-------------------+
|      204|       204|        204.0|       0204|[30 32 30 34 61 35]|
|     2618|      2618|       2618.0|       2618|[32 36 31 38 31 37]|
|     1132|      1132|       1132.0|       1132|[31 31 33 32 35 37]|
+---------+----------+-------------+-----------+-------------------+

Note: Spark won't generate bloom filter when NDV is low.

row_group |  min |  max | note                     | has_bf
-----------------------------------------------------------
        0 |    0 | 3998 | even numbers 0, 2, 4...  |      Y
        1 |    1 | 3999 | odd numbers 1, 3, 5..    |      Y
        2 | 1110 | 1114 | NDV=3: 1110,1111,1114    |      N
        3 |    0 | 1999 | sequence numbers         |      Y
        4 | 4000 | 5999 | sequence numbers         |      Y
        5 | 1110 | 1114 | NDV=3: 1110,1111,1114    |      N

 **/
TEST_F(TestObParquetBloomFilter, multi_stripes_with_simple_types) {
  load_file("./bloom_filter_multi_stripes_with_simple_types.parquet");
  auto &bloom_filter_reader = file_reader_->GetBloomFilterReader();
  bool is_contain;
  ObLakeTableParquetReaderMetrics metrics;

  /**
row_group |  min |  max | note                    | has_bf | 1111 in bf | 1112 in bf
------------------------------------------------------------------------------------
       0 |    0 | 3998 | even numbers 0, 2, 4...  |      Y |           N |         Y
       1 |    1 | 3999 | odd numbers 1, 3, 5..    |      Y |           Y |         N
       2 | 1110 | 1114 | NDV=3: 1110,1111,1114    |      N |           - |         -
       3 |    0 | 1999 | sequence numbers         |      Y |           Y |         Y
       4 | 4000 | 5999 | sequence numbers         |      Y |           N |         N
       5 | 1110 | 1114 | NDV=3: 1110,1111,1114    |      N |           - |         -
    **/

  constexpr int ROW_GROUP_NUM = 6;
  bool has_bf[ROW_GROUP_NUM] = {true, true, false, true, true, false};
  bool bf_contains_1111[ROW_GROUP_NUM] = {false, true, true, true, false, true};
  bool bf_contains_1112[ROW_GROUP_NUM] = {true, false, true, true, false, true};

  // int_has_bf
  ObDatumMeta int32_meta(common::ObInt32Type, common::CS_TYPE_INVALID, 0);
  const int32_t i[] = {1111, 1112};
  ObDatum int32_1111(reinterpret_cast<const char *>(&i[0]), sizeof(int32_t),
                     false);
  ObDatum int32_1112(reinterpret_cast<const char *>(&i[1]), sizeof(int32_t),
                     false);
  // double_has_bf
  ObDatumMeta double_meta(common::ObDoubleType, common::CS_TYPE_INVALID, 0);
  const double d[] = {1111.0, 1112.0};
  ObDatum double_1111(reinterpret_cast<const char *>(&d[0]), sizeof(double), false);
  ObDatum double_1112(reinterpret_cast<const char *>(&d[1]), sizeof(double), false);
  // text_has_bf
  ObDatumMeta text_meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
  ObDatum text_1111("1111", 4, false);
  ObDatum text_1112("1112", 4, false);
  // binary_has_bf
  ObDatumMeta binary_meta(common::ObCharType, common::CS_TYPE_BINARY, 0);
  ObDatum binary_1111("1111b5", 6, false);
  ObDatum binary_1112("111220", 6, false);

  constexpr int COL_NUM = 4;
  int col_ids[COL_NUM] = {1, 2, 3, 4};
  ObDatumMeta metas[COL_NUM] = {
      int32_meta, double_meta, text_meta, binary_meta};
  ObDatum datum_1111[COL_NUM] = {
      int32_1111, double_1111, text_1111, binary_1111};
  ObDatum datum_1112[COL_NUM] = {
      int32_1112, double_1112, text_1112, binary_1112};

  int assert_times = 0;
  for (int i = 0; i < ROW_GROUP_NUM; ++ i) {
    auto bf_rg = bloom_filter_reader.RowGroup(i);
    auto rg = file_reader_->RowGroup(i);
    ObParquetTableRowIterator::ParquetBloomFilterParamBuilder builder(rg, bf_rg, file_meta_, &metrics, -1);
    builder.set_tz_offset(0);
    for (int j = 0; j < COL_NUM; ++ j) {
      int prev_load_cnt = metrics.load_bloom_filter_count_;

      ObExternalTablePushdownFilter::BloomFilterItem item1111(col_ids[j], &metas[j], &datum_1111[j]);
      EXPECT_EQ(builder.may_contain(LEVEL, item1111, is_contain), OB_SUCCESS);
      if (!is_contain) {
        EXPECT_FALSE(bf_contains_1111[i]) << "unexpected 1111 result " << i << "," << j;
        assert_times ++;
      }

      ObExternalTablePushdownFilter::BloomFilterItem item1112(col_ids[j], &metas[j], &datum_1112[j]);
      EXPECT_EQ(builder.may_contain(LEVEL, item1112, is_contain), OB_SUCCESS);
      if (!is_contain) {
        EXPECT_FALSE(bf_contains_1112[i]) << "unexpected 1112 result " << i << "," << j;
        assert_times ++;
      }
      if (has_bf[i]) {
        EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_cnt + 1) << "expect load +1 for " << i << "," << j << col_ids[j];
      }
    }
  }
  EXPECT_EQ(assert_times, metrics.skip_bloom_filter_count_);
  EXPECT_GT(assert_times, 10); // at least should be 10
  metrics.dump_metrics();
}

}  // namespace unittest
}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
