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

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestSSTableSingleGet : public ObSSTableTest {
public:
  TestSSTableSingleGet();
  virtual ~TestSSTableSingleGet();
  void test_one_rowkey(const int64_t seed);
};

TestSSTableSingleGet::TestSSTableSingleGet() : ObSSTableTest("sstable_single_get")
{}

TestSSTableSingleGet::~TestSSTableSingleGet()
{}

void TestSSTableSingleGet::test_one_rowkey(const int64_t seed)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* getter = NULL;
  ObObj cells[TEST_COLUMN_CNT];
  ObExtStoreRowkey ext_rowkey;
  ObStoreRow row;
  ObStoreRowkey rowkey;
  const ObStoreRow* prow = NULL;

  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(seed, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  rowkey.assign(cells, TEST_ROWKEY_COLUMN_CNT);
  convert_rowkey(rowkey, ext_rowkey, allocator_);
  ret = sstable_.get(param_, context_, ext_rowkey, getter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = getter->get_next_row(prow);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(row.row_val_ == prow->row_val_);
  ret = getter->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);

  ret = sstable_.get(param_, context_, ext_rowkey, getter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = getter->get_next_row(prow);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(row.row_val_ == prow->row_val_);
  ret = getter->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestSSTableSingleGet, test_border_of_single_get)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* getter = NULL;
  ObStoreRow row;
  ObExtStoreRowkey ext_rowkey;
  ObStoreRowkey rowkey;
  const ObStoreRow* prow = NULL;

  // prepare query param and context
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // left border rowkey
  test_one_rowkey(0);

  // invalid call with unreset getter
  ret = sstable_.get(param_, context_, ext_rowkey, getter);
  ASSERT_NE(OB_SUCCESS, ret);

  // right border rowkey
  test_one_rowkey(row_cnt_ - 1);

  // non-exist rowkey
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(row_cnt_, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  rowkey.assign(cells, TEST_ROWKEY_COLUMN_CNT);
  convert_rowkey(rowkey, ext_rowkey, allocator_);
  ret = sstable_.get(param_, context_, ext_rowkey, getter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = getter->get_next_row(prow);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(prow->flag_ == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
}

TEST_F(TestSSTableSingleGet, test_normal)
{
  int ret = OB_SUCCESS;
  // prepare query param and context
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // row in first macro
  test_one_rowkey(3);

  // row in middle macro
  test_one_rowkey(row_cnt_ / 2);

  // row in last macro, in cache
  test_one_rowkey(row_cnt_ - 3);
}

TEST_F(TestSSTableSingleGet, random_test)
{
  int ret = OB_SUCCESS;
  ObRandom random;
  const int64_t test_count = 10;
  // prepare query param and context
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; OB_SUCC(ret) && i < test_count; ++i) {
    const int64_t row_number = std::abs(random.get() % row_cnt_);
    test_one_rowkey(row_number);
  }
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_single_get.log*");
  OB_LOGGER.set_file_name("test_sstable_single_get.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable_single_get");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
