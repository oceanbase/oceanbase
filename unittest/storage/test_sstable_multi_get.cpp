/**
 * Copyright (c) 2021 OceanBase
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
#include "ob_sstable_test.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {

class TestSSTableMultiGet : public ObSSTableTest {
public:
  TestSSTableMultiGet();
  void test_normal(const bool is_reverse_scan, const int64_t limit);
  void test_border(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_small_io(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_big_continue_io(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_big_discrete_io(const bool is_reverse_scan, const int64_t limit);
  void test_one_case(const ObIArray<int64_t>& seeds, const int64_t hit_mode, const bool is_reverse_scan);
  virtual ~TestSSTableMultiGet();
};

TestSSTableMultiGet::TestSSTableMultiGet() : ObSSTableTest("multi_get_sstable")
{}

TestSSTableMultiGet::~TestSSTableMultiGet()
{}

void TestSSTableMultiGet::test_one_case(
    const ObIArray<int64_t>& seeds, const int64_t hit_mode, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  UNUSED(is_reverse_scan);
  ObStoreRowIterator* getter = NULL;
  ObArray<ObStoreRowkey> rowkeys;
  ObArray<ObExtStoreRowkey> ext_rowkeys;
  ObStoreRowkey rowkey;
  ObStoreRow row;
  const ObStoreRow* prow = NULL;

  // prepare rowkeys
  ObStoreRowkey mget_rowkeys[TEST_MULTI_GET_CNT];
  ObObj mget_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mget_rows[TEST_MULTI_GET_CNT];

  for (int64_t i = 0; i < seeds.count(); ++i) {
    mget_rows[i].row_val_.assign(mget_cells[i], TEST_COLUMN_CNT);
    mget_rowkeys[i].assign(mget_cells[i], TEST_ROWKEY_COLUMN_CNT);
    ret = row_generate_.get_next_row(seeds.at(i), mget_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = 0; i < seeds.count(); ++i) {
    ret = rowkeys.push_back(mget_rowkeys[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  if (hit_mode == HIT_PART) {
    ObArray<ObStoreRowkey> part_rowkeys;
    ObArray<ObStoreRowkey> tmp_rowkeys;
    ObArray<ObExtStoreRowkey> part_ext_rowkeys;

    ret = tmp_rowkeys.assign(rowkeys);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::random_shuffle(tmp_rowkeys.begin(), tmp_rowkeys.end());

    part_rowkeys.reset();
    for (int64_t i = 0; i < tmp_rowkeys.count() / 3; ++i) {
      ret = part_rowkeys.push_back(tmp_rowkeys.at(i));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    if (part_rowkeys.count() > 0) {
      convert_rowkey(part_rowkeys, part_ext_rowkeys, allocator_);
      ret = sstable_.multi_get(param_, context_, part_ext_rowkeys, getter);
      ASSERT_EQ(OB_SUCCESS, ret);
      for (int64_t i = 0; i < part_rowkeys.count(); ++i) {
        ret = getter->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = getter->get_next_row(prow);
      ASSERT_EQ(OB_ITER_END, ret);
    }
  }
  if (nullptr != getter) {
    getter->~ObStoreRowIterator();
    getter = nullptr;
  }

  // in io
  convert_rowkey(rowkeys, ext_rowkeys, allocator_);
  ret = sstable_.multi_get(param_, context_, ext_rowkeys, getter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < seeds.count(); ++i) {
    ret = getter->get_next_row(prow);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (seeds.at(i) >= row_cnt_) {
      ASSERT_TRUE(prow->flag_ == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    } else {
      ASSERT_TRUE(mget_rows[i].row_val_ == prow->row_val_);
    }
  }
  ret = getter->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  if (nullptr != getter) {
    getter->~ObStoreRowIterator();
    getter = nullptr;
  }

  // in cache
  if (hit_mode == HIT_ALL) {
    ret = sstable_.multi_get(param_, context_, ext_rowkeys, getter);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < seeds.count(); ++i) {
      ret = getter->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (seeds.at(i) >= row_cnt_) {
        ASSERT_TRUE(prow->flag_ == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
      } else {
        ASSERT_TRUE(mget_rows[i].row_val_ == prow->row_val_);
      }
    }
    ret = getter->get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
  }
  if (nullptr != getter) {
    getter->~ObStoreRowIterator();
    getter = nullptr;
  }
}

void TestSSTableMultiGet::test_border(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* getter = NULL;
  ObArray<ObStoreRowkey> rowkeys;
  ObArray<ObExtStoreRowkey> ext_rowkeys;
  ObStoreRowkey rowkey;
  ObStoreRow row;
  ObArray<int64_t> seeds;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // empty rowkey
  ext_rowkeys.reuse();
  ret = ext_rowkeys.push_back(ObExtStoreRowkey(rowkey));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_.multi_get(param_, context_, ext_rowkeys, getter);
  ASSERT_NE(OB_SUCCESS, ret);

  // uinited sstable
  ObSSTable sstable;
  ext_rowkeys.reuse();
  ret = rowkeys.push_back(rowkey);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ext_rowkeys.push_back(ObExtStoreRowkey(rowkey));
  ret = sstable.multi_get(param_, context_, ext_rowkeys, getter);
  ASSERT_NE(OB_SUCCESS, ret);

  // the row of sstable
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, false);
  }

  // last row of sstable
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, false);
  }

  // single row not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, false);
  }

  // TEST_MULTI_GET_CNT row with the same rowkey
  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(row_cnt_ / 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiGet::test_normal(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2 rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // 10 rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // TEST_MULTI_GET_CNT rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // single row exist test
  seeds.reuse();
  ret = seeds.push_back(3);
  ASSERT_EQ(OB_SUCCESS, ret);

  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i + (i % 2 ? row_cnt_ : 0));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // 2 row not exist test
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // 10 rows not exist test
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // TEST_MULTI_GET_CNT rows not exist test
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiGet::test_multi_block_read_small_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  GCONF.multiblock_read_size = 10 * 1024;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, HIT_NONE, is_reverse_scan);

  destroy_all_cache();
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, HIT_NONE, is_reverse_scan);
  destroy_query_param();
}

void TestSSTableMultiGet::test_multi_block_read_big_continue_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  GCONF.multiblock_read_size = 10 * 1024;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, HIT_NONE, is_reverse_scan);

  // gap_size < multiblock_gap_size
  GCONF.multiblock_read_gap_size = 5 * 1024;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, HIT_NONE, is_reverse_scan);

  // gap_size > multiblock_read_gap_size
  GCONF.multiblock_read_gap_size = 0;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, HIT_NONE, is_reverse_scan);
  destroy_query_param();
}

void TestSSTableMultiGet::test_multi_block_read_big_discrete_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  GCONF.multiblock_read_size = 10 * 1024;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, HIT_NONE, is_reverse_scan);

  // gap_size < multiblock_gap_size
  GCONF.multiblock_read_gap_size = 5 * 1024;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, HIT_NONE, is_reverse_scan);

  // gap_size > multiblock_read_gap_size
  GCONF.multiblock_read_gap_size = 0;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, HIT_NONE, is_reverse_scan);
  destroy_query_param();
}

TEST_F(TestSSTableMultiGet, test_border)
{
  const bool is_reverse_scan = false;
  int64_t limit = -1;
  test_border(is_reverse_scan, limit);
  limit = 1;
  test_border(is_reverse_scan, limit);
}

TEST_F(TestSSTableMultiGet, test_border_reverse_scan)
{
  const bool is_reverse_scan = true;
  int64_t limit = -1;
  test_border(is_reverse_scan, limit);
  limit = 1;
  test_border(is_reverse_scan, limit);
}

TEST_F(TestSSTableMultiGet, test_normal)
{
  const bool is_reverse_scan = false;
  int64_t limit = -1;
  test_normal(is_reverse_scan, limit);
  limit = 1;
  test_normal(is_reverse_scan, limit);
}

TEST_F(TestSSTableMultiGet, test_normal_reverse_scan)
{
  const bool is_reverse_scan = true;
  int64_t limit = -1;
  test_normal(is_reverse_scan, limit);
  limit = 1;
  test_normal(is_reverse_scan, limit);
}

TEST_F(TestSSTableMultiGet, test_estimate)
{
  int ret = OB_SUCCESS;
  ObArray<ObStoreRowkey> rowkeys;
  ObArray<ObExtStoreRowkey> ext_rowkeys;
  ObStoreRowkey rowkey;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  ObPartitionEst cost_metrics;

  // prepare query param
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(0, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  rowkey.assign(cells, TEST_ROWKEY_COLUMN_CNT);
  rowkeys.reuse();
  ret = rowkeys.push_back(rowkey);
  ASSERT_EQ(OB_SUCCESS, ret);
  convert_rowkey(rowkeys, ext_rowkeys, allocator_);
  ret = sstable_.estimate_get_row_count(context_.query_flag_, table_key_.table_id_, ext_rowkeys, cost_metrics);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, cost_metrics.logical_row_count_);
}

TEST_F(TestSSTableMultiGet, test_multi_block_small_io)
{
  test_multi_block_read_small_io(false, -1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_small_io_reverse_scan)
{
  test_multi_block_read_small_io(true, -1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_small_io_limit)
{
  test_multi_block_read_small_io(false, 1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_small_io_limit_reverse_scan)
{
  test_multi_block_read_small_io(true, 1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_continue_io)
{
  test_multi_block_read_big_continue_io(false, -1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_continue_io_reverse_scan)
{
  test_multi_block_read_big_continue_io(true, -1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_continue_io_limit)
{
  test_multi_block_read_big_continue_io(false, 1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_continue_io_limit_reverse_scan)
{
  test_multi_block_read_big_continue_io(true, 1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_discrete_io)
{
  test_multi_block_read_big_discrete_io(false, -1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_discrete_io_reverse_scan)
{
  test_multi_block_read_big_discrete_io(true, -1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_discrete_io_limit)
{
  test_multi_block_read_big_discrete_io(false, 1);
}

TEST_F(TestSSTableMultiGet, test_multi_block_read_big_discrete_io_limit_reverse_scan)
{
  test_multi_block_read_big_discrete_io(true, 1);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_multi_get.log*");
  OB_LOGGER.set_file_name("test_sstable_multi_get.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable_multi_get");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
