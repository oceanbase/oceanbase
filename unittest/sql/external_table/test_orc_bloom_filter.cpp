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
#include "sql/engine/table/ob_orc_table_row_iter.h"

namespace oceanbase {
namespace sql {
namespace unittest {

class TestObOrcBloomFilter : public ::testing::Test {
public:
  TestObOrcBloomFilter()
  {}
  virtual ~TestObOrcBloomFilter()
  {}

protected:
  class UtOrcReaderAdapter : public ObOrcTableRowIterator::OrcReaderAdapter {
  public:
    UtOrcReaderAdapter(std::string path) {
      reader_ = orc::createReader(
          orc::readFile(path, readerOpts.getReaderMetrics()), readerOpts);
    }
    orc::Reader *getReaderPtr() { return reader_.get(); }

    int getType(const int64_t ext_tbl_col_id, const orc::Type*& type) {
      // orc col_id starts from 1
      const orc::Type& rootType = reader_->getType();
      type = rootType.getSubtype(ext_tbl_col_id - 1);
      return OB_SUCCESS;
    }

  private:
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader_;
  };

  static const ObExternalTablePushdownFilter::PushdownLevel LEVEL = ObExternalTablePushdownFilter::PushdownLevel::PAGE;

  void test_bloom_filter(
      ObOrcTableRowIterator::OrcBloomFilterParamBuilder& builder,
      const ObLakeTableORCReaderMetrics& metrics,
      const ObDatumMeta& meta,
      const ObDatum& not_exist_v,
      const ObDatum& exist_v,
      const int64_t no_bf_col,
      const int64_t has_bf_col) {
    bool is_contain = true;
    int prev_load_bloom_filter_count_ = metrics.load_bloom_filter_count_;
    int prev_skip_bloom_filter_count_ = metrics.skip_bloom_filter_count_;

    {
      // column without bloomfilter => may contain, bloom filter won't load/skip
      ObExternalTablePushdownFilter::BloomFilterItem item(no_bf_col, &meta,
                                                          &not_exist_v);
      EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
      EXPECT_TRUE(is_contain);
      EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_bloom_filter_count_);
      EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_bloom_filter_count_);
    }

    // column with bloomfilter
    {
    // value exists in bloom filter => may contain, load = true, skip = false
    ObExternalTablePushdownFilter::BloomFilterItem item(has_bf_col, &meta, &exist_v);
    EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain),
              OB_SUCCESS);
    EXPECT_TRUE(is_contain);
    EXPECT_EQ(metrics.load_bloom_filter_count_,
              prev_load_bloom_filter_count_ + 1);
    EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_bloom_filter_count_);
  }
  {
    // value does not exist in bloom filter => won't contain, load = true, skip = true
    ObExternalTablePushdownFilter::BloomFilterItem item(has_bf_col, &meta, &not_exist_v);
    EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain),
              OB_SUCCESS);
    EXPECT_FALSE(is_contain)
     << has_bf_col;
    EXPECT_EQ(metrics.load_bloom_filter_count_, prev_load_bloom_filter_count_ + 1);
    EXPECT_EQ(metrics.skip_bloom_filter_count_, prev_skip_bloom_filter_count_ + 1);
    }
  }
};

/**
OceanBase(admin@test)>CREATE EXTERNAL TABLE `smoking_test` (
    -> `i_no_bf` bigint,
    -> `i_has_bf` bigint,
    -> `s_no_bf` text,
    -> `s_has_bf` text)
    -> location = '/tmp/gen_orc_test_data_20250722_091840/bloom_filter_smoking_test',
    -> FORMAT ( type = 'ORC');
Query OK, 0 rows affected (0.454 sec)

OceanBase(admin@test)>select * from smoking_test;
+---------+----------+---------+----------+
| i_no_bf | i_has_bf | s_no_bf | s_has_bf |
+---------+----------+---------+----------+
|       1 |        1 | uno     | uno      |
|       2 |        2 | dos     | dos      |
|       4 |        4 | cuatro  | cuatro   |
+---------+----------+---------+----------+
3 rows in set (0.027 sec)
**/
TEST_F(TestObOrcBloomFilter, smoking_test) {
  UtOrcReaderAdapter reader_adapter =
      UtOrcReaderAdapter("./bloom_filter_smoking_test.orc");
  ObLakeTableORCReaderMetrics metrics;
  ObOrcTableRowIterator::OrcBloomFilterParamBuilder builder(&reader_adapter, 0,
                                                            0, &metrics, -1);
  builder.set_tz_offset(0);
  bool is_contain;

  ObDatumMeta int64_meta(common::ObIntType, common::CS_TYPE_INVALID, 0);
  const int64_t i[] = {3, 2};
  ObDatum not_exist_i(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t),
                      false);
  ObDatum exist_i(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t),
                  false);
  int64_t i_no_bf_col = 1, i_has_bf_col = 2, s_no_bf_col = 3, s_has_bf_col = 4;

  ObDatumMeta text_meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
  ObDatum not_exist_str("tres", 4, false);
  ObDatum exist_str("dos", 3, false);

  test_bloom_filter(builder, metrics, int64_meta, not_exist_i, exist_i, i_no_bf_col, i_has_bf_col);
  test_bloom_filter(builder, metrics, text_meta, not_exist_str, exist_str, s_no_bf_col, s_has_bf_col);

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 0);
  metrics.dump_metrics();
}

TEST_F(TestObOrcBloomFilter, corner_case_test)
{
  UtOrcReaderAdapter reader_adapter = UtOrcReaderAdapter("./bloom_filter_smoking_test.orc");
  ObLakeTableORCReaderMetrics metrics;
  ObOrcTableRowIterator::OrcBloomFilterParamBuilder builder(&reader_adapter, 0, 0, &metrics, -1);
  builder.set_tz_offset(0);
  int64_t i_no_bf_col = 1, i_has_bf_col = 2, s_no_bf_col = 3, s_has_bf_col = 4;
  bool is_contain;

  ObDatumMeta int64_meta(common::ObIntType, common::CS_TYPE_INVALID, 0);
  const int64_t i[] = {3, 2};
  ObDatum not_exist_i(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
  ObDatum exist_i(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);

  ObDatumMeta text_meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
  ObDatum not_exist_str("tres", 4, false);
  ObDatum exist_str("dos", 3, false);

  // 1. normal case
  EXPECT_EQ(metrics.load_bloom_filter_count_, 0);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 0);

  {
    ObExternalTablePushdownFilter::BloomFilterItem item(
        i_has_bf_col, &int64_meta, &not_exist_i);
    EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
    EXPECT_FALSE(is_contain);
  }

  EXPECT_EQ(metrics.load_bloom_filter_count_, 1);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 1);

  {
    ObExternalTablePushdownFilter::BloomFilterItem item(
        s_has_bf_col, &text_meta, &not_exist_str);
    EXPECT_EQ(builder.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
    EXPECT_FALSE(is_contain);
  }

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);

  // 2. level != PAGE
  {
    ObExternalTablePushdownFilter::BloomFilterItem item(
        i_has_bf_col, &int64_meta, &not_exist_i);
    EXPECT_EQ(
        builder.may_contain(ObExternalTablePushdownFilter::PushdownLevel::FILE,
                            item, is_contain),
        OB_SUCCESS);
    EXPECT_TRUE(is_contain);
    EXPECT_EQ(builder.may_contain(
                  ObExternalTablePushdownFilter::PushdownLevel::ROW_GROUP,
                  item, is_contain),
              OB_SUCCESS);
    EXPECT_TRUE(is_contain);
    EXPECT_EQ(builder.may_contain(
                  ObExternalTablePushdownFilter::PushdownLevel::ENCODING, item
                  , is_contain),
              OB_SUCCESS);
    EXPECT_TRUE(is_contain);
  }

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 0);

  // 3. bloom filter size > non-bloom filter size
  ObOrcTableRowIterator::OrcBloomFilterParamBuilder builder2(&reader_adapter, 0, 0, &metrics, 20);
  builder2.set_tz_offset(0);
  {
    ObExternalTablePushdownFilter::BloomFilterItem item(
        i_has_bf_col, &int64_meta, &not_exist_i);
    EXPECT_EQ(builder2.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
    EXPECT_TRUE(is_contain);
  }

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 1);

  {
    ObExternalTablePushdownFilter::BloomFilterItem item(
        s_has_bf_col, &text_meta, &not_exist_str);
    EXPECT_EQ(builder2.may_contain(LEVEL, item, is_contain), OB_SUCCESS);
    EXPECT_TRUE(is_contain);
  }

  EXPECT_EQ(metrics.load_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 2);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 2);
}


/**
OceanBase(admin@test)>CREATE EXTERNAL TABLE `all_numerics` (
    -> int8_no_bf tinyint,
    -> int8_has_bf tinyint,
    -> int16_no_bf smallint,
    -> int16_has_bf smallint,
    -> int32_no_bf int,
    -> int32_has_bf int,
    -> int64_no_bf bigint,
    -> int64_has_bf bigint,
    -> float_no_bf float,
    -> float_has_bf float,
    -> double_no_bf double,
    -> double_has_bf double,
    -> short_deciaml_no_bf decimal(10,3),
    -> short_deciaml_has_bf decimal(10, 3),
    -> long_deciaml_no_bf decimal(20,3),
    -> long_deciaml_has_bf decimal(20,3))
    -> LOCATION='/tmp/gen_orc_test_data_20250722_091840/bloom_filter_all_numerics/'
    -> FORMAT (
    ->   TYPE = 'ORC'
    -> ) DEFAULT CHARSET = utf8mb4;
Query OK, 0 rows affected (1.018 sec)

OceanBase(admin@test)>select * from all_numerics;
+------------+-------------+-------------+--------------+-------------+--------------+-------------+--------------+-------------+--------------+--------------+---------------+---------------------+----------------------+--------------------+---------------------+
    | int8_no_bf | int8_has_bf | int16_no_bf | int16_has_bf | int32_no_bf | int32_has_bf | int64_no_bf | int64_has_bf | float_no_bf | float_has_bf | double_no_bf | double_has_bf | short_deciaml_no_bf | short_deciaml_has_bf | long_deciaml_no_bf | long_deciaml_has_bf |
    +------------+-------------+-------------+--------------+-------------+--------------+-------------+--------------+-------------+--------------+--------------+---------------+---------------------+----------------------+--------------------+---------------------+
    |          1 |           1 |           1 |            1 |           1 |            1 |           1 |            1 |           1 |            1 |            1 |             1 |               1.000 |                1.000 |              1.000 |               1.000 |
    |          2 |           2 |           2 |            2 |           2 |            2 |           2 |            2 |           2 |            2 |            2 |             2 |               2.000 |                2.000 |              2.000 |               2.000 |
    |          4 |           4 |           4 |            4 |           4 |            4 |           4 |            4 |           4 |            4 |            4 |             4 |               4.000 |                4.000 |              4.000 |               4.000 |
    +------------+-------------+-------------+--------------+-------------+--------------+-------------+--------------+-------------+--------------+--------------+---------------+---------------------+----------------------+--------------------+---------------------+
 **/
TEST_F(TestObOrcBloomFilter, all_numeric_types)
{
  UtOrcReaderAdapter reader_adapter = UtOrcReaderAdapter("./bloom_filter_all_numerics.orc");
  ObLakeTableORCReaderMetrics metrics;
  ObOrcTableRowIterator::OrcBloomFilterParamBuilder builder(&reader_adapter, 0, 0, &metrics, -1);
  builder.set_tz_offset(0);
  bool is_contain;

  // start check tinyint
  // [1]`int8_no_bf` tinyint,
  // [2]`int8_has_bf` tinyint,
  {
    ObDatumMeta meta(common::ObTinyIntType, common::CS_TYPE_INVALID, 0);
    const int8_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int8_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int8_t), false);
    int64_t no_bf_col = 1, has_bf_col = 2;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check tinyint

  // start check smallint
  // [3]`int16_no_bf` smallint,
  // [4]`int16_has_bf` smallint,
  {
    ObDatumMeta meta(common::ObSmallIntType, common::CS_TYPE_INVALID, 0);
    const int16_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int16_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int16_t), false);
    int64_t no_bf_col = 3, has_bf_col = 4;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check smallint

  // start check integer
  // [5]`int32_no_bf` integer,
  // [6]`int32_has_bf` integer,
  {
    ObDatumMeta meta(common::ObInt32Type, common::CS_TYPE_INVALID, 0);
    const int32_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int32_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int32_t), false);
    int64_t no_bf_col = 5, has_bf_col = 6;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check integer

  // start check bigint
  // [7]`int64_no_bf` bigint,
  // [8]`int64_has_bf` bigint,
  {
    ObDatumMeta meta(common::ObIntType, common::CS_TYPE_INVALID, 0);
    const int64_t i[] = {3, 2};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);
    int64_t no_bf_col = 7, has_bf_col = 8;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check bigint

  // start check float
  // [9]`float_no_bf` FLOAT,
  // [10]`float_has_bf` float,
  {
    ObDatumMeta meta(common::ObFloatType, common::CS_TYPE_INVALID, 0);
    const float i[] = {3.0, 2.0};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(float), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(float), false);
    int64_t no_bf_col = 9, has_bf_col = 10;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // start check float

  // start check double
  // [11]`double_no_bf` double,
  // [12]`double_has_bf` double,
  {
    ObDatumMeta meta(common::ObDoubleType, common::CS_TYPE_INVALID, 0);
    const double i[] = {3.0, 2.0};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(double), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(double), false);
    int64_t no_bf_col = 11, has_bf_col = 12;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check double

  // start check short decimal
  // [13]`short_deciaml_no_bf` decimal(10,3),
  // [14]`short_deciaml_has_bf` decimal(10,3),
  {
    ObDatumMeta meta(common::ObDecimalIntType, common::CS_TYPE_INVALID, 0);
    meta.scale_ = 3;
    meta.precision_ = 10;
    const int64_t i[] = {3000L, 2000L};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t), false);
    int64_t no_bf_col = 13, has_bf_col = 14;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check short decimal

  // start check long decimal
  // [15]`long_deciaml_no_bf` decimal(20,3),
  // [16]`long_deciaml_has_bf` decimal(20,3)
  {
    ObDatumMeta meta(common::ObDecimalIntType, common::CS_TYPE_INVALID, 0);
    meta.scale_ = 3;
    meta.precision_ = 20;
    const int128_t i[] = {3000LL, 2000LL};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int128_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int128_t), false);
    int64_t no_bf_col = 15, has_bf_col = 16;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check long decimal

  EXPECT_EQ(metrics.load_bloom_filter_count_, 8);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 8);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 0);
  metrics.dump_metrics();
}

/**
OceanBase(admin@test)>CREATE EXTERNAL TABLE `all_other_types` (
    -> `binary_no_bf` binary(10),
    -> `binary_has_bf` binary(10),
    -> `str_no_bf` text,
    -> `str_has_bf` text,
    -> `bool_no_bf` BOOL,
    -> `bool_has_bf` BOOL,
    -> `date_no_bf` DATE,
    -> `date_has_bf` DATE,
    -> `ts_no_bf` timestamp,
    -> `ts_has_bf` timestamp)
    -> LOCATION='/tmp/gen_orc_test_data_20250722_091840/bloom_filter_all_other_types/'
    -> FORMAT (
    ->   TYPE = 'ORC'
    -> ) DEFAULT CHARSET = utf8mb4;
Query OK, 0 rows affected (0.325 sec)

OceanBase(admin@test)>select * from all_other_types;
+--------------+---------------+-----------+------------+------------+-------------+------------+-------------+---------------------+---------------------+
| binary_no_bf | binary_has_bf | str_no_bf | str_has_bf | bool_no_bf | bool_has_bf | date_no_bf | date_has_bf | ts_no_bf            | ts_has_bf           |
+--------------+---------------+-----------+------------+------------+-------------+------------+-------------+---------------------+---------------------+
| hola         | hola          | hola      | hola       |          1 |           1 | 1970-01-01 | 1970-01-01  | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 |
| bonjour      | bonjour       | bonjour   | bonjour    |          1 |           1 | 1969-12-31 | 1969-12-31  | 1969-12-31 23:59:59 | 1969-12-31 23:59:59 |
| ciao         | ciao          | ciao      | ciao       |          1 |           1 | 2025-07-16 | 2025-07-16  | 2025-07-16 09:00:00 | 2025-07-16 09:00:00 |
+--------------+---------------+-----------+------------+------------+-------------+------------+-------------+---------------------+---------------------+
3 rows in set (0.024 sec)
*/
TEST_F(TestObOrcBloomFilter, all_other_types)
{
  UtOrcReaderAdapter reader_adapter = UtOrcReaderAdapter("./bloom_filter_all_other_types.orc");
  ObLakeTableORCReaderMetrics metrics;
  ObOrcTableRowIterator::OrcBloomFilterParamBuilder builder(&reader_adapter, 0, 0, &metrics, -1);
  builder.set_tz_offset(0);
  bool is_contain;

  // start check binary
  // ->[1] `binary_no_bf` binary(10),
  // ->[2] `binary_has_bf` binary(10),
  {
    ObDatumMeta meta(common::ObCharType, common::CS_TYPE_BINARY, 0);
    ObDatum not_exist_v("aaa", 3, false);
    ObDatum exist_v("hola", 4, false);
    int64_t no_bf_col = 1, has_bf_col = 2;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check binary

  // start check text
  // ->[3] `str_no_bf` text,
  // ->[4] `str_has_bf` text,
  {
    ObDatumMeta meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
    ObDatum not_exist_v("aaa", 3, false);
    ObDatum exist_v("hola", 4, false);
    int64_t no_bf_col = 3, has_bf_col = 4;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check text

  // start check bool: NYI
  // ->[5] `bool_no_bf` BOOL,
  // ->[6] `bool_has_bf` BOOL,
  {
    ObDatumMeta meta(common::ObTinyIntType, common::CS_TYPE_INVALID, 0);
    const int64_t i[] = {0, 1};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int64_t),
                        false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int64_t),
                    false);
    EXPECT_TRUE(not_exist_v.is_false());
    EXPECT_TRUE(exist_v.is_true());

    ObExternalTablePushdownFilter::BloomFilterItem item1(5, &meta,
                                                         &not_exist_v);
    EXPECT_EQ(builder.may_contain(LEVEL, item1, is_contain), OB_SUCCESS);
    EXPECT_TRUE(is_contain);
    // NYI
    ObExternalTablePushdownFilter::BloomFilterItem item2(6, &meta, &exist_v);
    EXPECT_EQ(builder.may_contain(LEVEL, item2, is_contain), OB_SUCCESS);
    EXPECT_TRUE(is_contain);
  }
  // end check bool

  // start check date
  // ->[7] `date_no_bf` DATE,
  // ->[8] `date_has_bf` DATE,
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
    int64_t no_bf_col = 7, has_bf_col = 8;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check date

  // start check timestamp
  // ->[9] `ts_no_bf` timestamp
  // ->[10] `ts_has_bf` timestamp
  {
    ObDatumMeta meta(common::ObTimestampType, common::CS_TYPE_INVALID, 0);
    // >>> (datetime.datetime(2025,7,16,9) - datetime.datetime(1970,1,1)).total_seconds()
    // 1752656400.0
    const int64_t i[] = {10'000L, 1752656400'000'000L};
    ObDatum not_exist_v(reinterpret_cast<const char *>(&i[0]), sizeof(int32_t), false);
    ObDatum exist_v(reinterpret_cast<const char *>(&i[1]), sizeof(int32_t), false);
    int64_t no_bf_col = 9, has_bf_col = 10;
    test_bloom_filter(builder, metrics, meta, not_exist_v, exist_v, no_bf_col, has_bf_col);
  }
  // end check timestamp

  EXPECT_EQ(metrics.load_bloom_filter_count_, 4);
  EXPECT_EQ(metrics.skip_bloom_filter_count_, 4);
  EXPECT_EQ(metrics.skip_load_bloom_filter_count_, 0);
  metrics.dump_metrics();
}

/**
 +---------+----------+-------------+--------------+-----------+-----------+-------------------+
|int_no_bf|int_has_bf|double_has_bf|decimal_has_bf|text_has_bf|date_has_bf|   timestamp_has_bf|
+---------+----------+-------------+--------------+-----------+-----------+-------------------+
|     2710|      2710|       2710.0|      2710.123|       2710| 2007-06-03|2007-06-03 01:23:59|
|     1828|      1828|       1828.0|      1828.123|       1828| 2005-01-02|2005-01-02 01:23:59|
|     2280|      2280|       2280.0|      2280.123|       2280| 2006-03-30|2006-03-30 01:23:59|
+---------+----------+-------------+--------------+-----------+-----------+-------------------+
file layout: total 4 strips, each has 2 index.
strip_no | page_no |  min |  max | note                     |
-------------------------------------------------------------
       0 |       0 |    0 | 3998 | even numbers 0, 2, 4...  |
         |       1 |    1 | 3999 | odd numbers 1, 3, 5..    |
       1 |       0 | 2000 | 3999 | sequence numbers         |
         |       1 | 1110 | 1114 | NDV=3: 1110,1111,1114    |
       2 |       0 | 4000 | 5999 | sequence numbers         |
         |       1 |    0 | 1999 | sequence numbers         |
       3 |       0 | 1110 | 1114 | NDV=3: 1110,1111,1114    |
         |       1 | 1110 | 1114 | NDV=3: 1110,1111,1114    |
**/
TEST_F(TestObOrcBloomFilter, multi_stripes_with_simple_types) {
  UtOrcReaderAdapter reader_adapter =
      UtOrcReaderAdapter("./bloom_filter_multi_stripes_with_simple_types.orc");
  ObLakeTableORCReaderMetrics metrics;
  bool is_contain;
/**
rg_no | page_no |  min |  max | note                     | 1111 in bf | 1112 in bf?
---------------------------------------------------------------------------------------
    0 |       0 |    0 | 3998 | even numbers 0, 2, 4...  |          N |         Y
      |       1 |    1 | 3999 | odd numbers 1, 3, 5..    |          Y |         N
    1 |       0 | 2000 | 3999 | sequence numbers         |          N |         N
      |       1 | 1110 | 1114 | NDV=3: 1110,1111,1114    |          N |         N
    2 |       0 | 4000 | 5999 | sequence numbers         |          N |         N
      |       1 |    0 | 1999 | sequence numbers         |          Y |         Y
    3 |       0 | 1110 | 1114 | NDV=3: 1110,1111,1114    |          Y |         N
      |       1 | 1110 | 1114 | NDV=3: 1110,1111,1114    |          Y |         N
 */
 {
  constexpr int PAGE_NUM = 8;
  int strip_no[PAGE_NUM] = {0, 0, 1, 1, 2, 2, 3, 3};
  int page_no[PAGE_NUM] = {0, 1, 0, 1, 0, 1, 0, 1};
  bool bf_contains_1111[PAGE_NUM] = {false, true, false, false, false, true, true, true};
  bool bf_contains_1112[PAGE_NUM] = {true, false, false, false, false, true, false, false};

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
  // decimal_has_bf
  ObDatumMeta decimal_meta(common::ObDecimalIntType, common::CS_TYPE_INVALID, 0);
  decimal_meta.scale_ = 3;
  decimal_meta.precision_ = 20;
  const int128_t de[] = {1111123LL, 1112123LL};
  ObDatum decimal_1111(reinterpret_cast<const char *>(&de[0]), sizeof(int128_t), false);
  ObDatum decimal_1112(reinterpret_cast<const char *>(&de[1]), sizeof(int128_t), false);
  // text_has_bf
  ObDatumMeta text_meta(common::ObLongTextType, common::CS_TYPE_UTF8MB4_BIN, 0);
  ObDatum text_1111("1111", 4, false);
  ObDatum text_1112("1112", 4, false);

  // date_has_bf
  ObDatumMeta date_meta(common::ObMySQLDateType, common::CS_TYPE_INVALID, 0);
  ObMySQLDate md_1111;
  md_1111.year_ = 2003;
  md_1111.month_ = 1;
  md_1111.day_ = 16;
  ObMySQLDate md_1112;
  md_1112.year_ = 2003;
  md_1112.month_ = 1;
  md_1112.day_ = 17;
  ObDatum date_1111(reinterpret_cast<const char *>(&md_1111), sizeof(int32_t), false);
  ObDatum date_1112(reinterpret_cast<const char *>(&md_1112), sizeof(int32_t), false);

  // timestamp_has_bf
  ObDatumMeta ts_meta(common::ObTimestampType, common::CS_TYPE_INVALID, 0);
  const int64_t ts[] = {1042680239'000000L, 1042766639'000000L};
  ObDatum ts_1111(reinterpret_cast<const char *>(&ts[0]), sizeof(int32_t), false);
  ObDatum ts_1112(reinterpret_cast<const char *>(&ts[1]), sizeof(int32_t), false);

  constexpr int COL_NUM = 6;
  int col_ids[COL_NUM] = {2, 3, 4, 5, 6, 7};
  ObDatumMeta metas[COL_NUM] = {
      int32_meta, double_meta, decimal_meta, text_meta, date_meta, ts_meta};
  ObDatum datum_1111[COL_NUM] = {
      int32_1111, double_1111, decimal_1111, text_1111, date_1111, ts_1111};
  ObDatum datum_1112[COL_NUM] = {
      int32_1112, double_1112, decimal_1112, text_1112, date_1112, ts_1112};

  int assert_times = 0;
  for (int i = 0; i < PAGE_NUM; ++i) {
    ObOrcTableRowIterator::OrcBloomFilterParamBuilder builder(
        &reader_adapter, strip_no[i], page_no[i], &metrics, -1);
    builder.set_tz_offset(0);
    for (int j = 0; j < COL_NUM; ++j) {
      int prev_load_bloom_filter_count_ = metrics.load_bloom_filter_count_;

      ObExternalTablePushdownFilter::BloomFilterItem item1111(
          col_ids[j], &metas[j], &datum_1111[j]);
      EXPECT_EQ(builder.may_contain(LEVEL, item1111, is_contain), OB_SUCCESS);
      if (!is_contain) {
        EXPECT_FALSE(bf_contains_1111[i])
            << "unexpected 1111 result " << i << "," << j;
        assert_times++;
      }

      ObExternalTablePushdownFilter::BloomFilterItem item1112(
          col_ids[j], &metas[j], &datum_1112[j]);
      EXPECT_EQ(builder.may_contain(LEVEL, item1112, is_contain), OB_SUCCESS);
      if (!is_contain) {
        EXPECT_FALSE(bf_contains_1112[i])
            << "unexpected 1112 result " << i << "," << j;
        assert_times++;
      }

      // equal or +1
      EXPECT_LE(metrics.load_bloom_filter_count_,
                prev_load_bloom_filter_count_ + 1);
    }
  }
  EXPECT_EQ(assert_times, metrics.skip_bloom_filter_count_);
  metrics.dump_metrics();
 }

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
