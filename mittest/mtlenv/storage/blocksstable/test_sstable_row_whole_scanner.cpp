/**
 * Copyright (c) 2022 OceanBase
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
#define private public
#define protected public

#include "lib/random/ob_random.h"
#include "storage/access/ob_sstable_row_whole_scanner.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestSSTableRowWholeScanner : public TestIndexBlockDataPrepare
{
public:
  TestSSTableRowWholeScanner();
  virtual ~TestSSTableRowWholeScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();

  void generate_range(const int64_t start, const int64_t end, ObDatumRange &range);
  void prepare_query_param(const bool is_reverse_scan);
private:
  ObArenaAllocator allocator_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
};

TestSSTableRowWholeScanner::TestSSTableRowWholeScanner()
  : TestIndexBlockDataPrepare("Test sstable row scanner")
{
}

TestSSTableRowWholeScanner::~TestSSTableRowWholeScanner()
{
}

void TestSSTableRowWholeScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestSSTableRowWholeScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestSSTableRowWholeScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, TEST_COLUMN_CNT));
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestSSTableRowWholeScanner::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestSSTableRowWholeScanner::generate_range(
    const int64_t start,
    const int64_t end,
    ObDatumRange &range)
{
  ObDatumRowkey tmp_rowkey;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start, start_row_));
  tmp_rowkey.assign(start_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.start_key_, allocator_));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(end, end_row_));
  tmp_rowkey.assign(end_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.end_key_, allocator_));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
}

void TestSSTableRowWholeScanner::prepare_query_param(const bool is_reverse_scan)
{
  TestIndexBlockDataPrepare::prepare_query_param(is_reverse_scan);
  context_.query_flag_.whole_macro_scan_ = true;
}

TEST_F(TestSSTableRowWholeScanner, test_border)
{
  prepare_query_param(false);
  ObDatumRange range;
  ObSSTableRowWholeScanner scanner;
  int iter_row_cnt = 0;

  // (sstable_endkey, sstable_endkey]
  generate_range(row_cnt_ - 1, row_cnt_ - 1, range);
  range.set_left_open();
  const ObDatumRow *iter_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(iter_param_, context_, &sstable_, &range));
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(iter_row));

  // [sstable_endkey, MAX)
  scanner.reuse();
  range.set_left_closed();
  range.end_key_.set_max_rowkey();
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(iter_param_, context_, &sstable_, &range));
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(iter_row));
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(iter_row));

  // (sstable_startkey, sstable_endkey)
  scanner.reuse();
  generate_range(0, row_cnt_ - 1, range);
  range.set_left_open();
  range.set_right_open();
  iter_row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(iter_param_, context_, &sstable_, &range));
  while (iter_row_cnt < row_cnt_ - 2) {
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(iter_row)) << "iter_row_cnt:" << iter_row_cnt;
    ++iter_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(iter_row));

  // [sstable_start_key, sstable_endkey]
  scanner.reuse();
  range.set_left_closed();
  range.set_right_closed();
  iter_row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(iter_param_, context_, &sstable_, &range));
  while (iter_row_cnt < row_cnt_) {
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(iter_row)) << "iter_row_cnt:" << iter_row_cnt;
    ++iter_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(iter_row));

  // whole range
  scanner.reuse();
  range.set_whole_range();
  iter_row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(iter_param_, context_, &sstable_, &range));
  while (iter_row_cnt < row_cnt_) {
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(iter_row)) << "iter_row_cnt:" << iter_row_cnt;
    ++iter_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(iter_row));

}


}
}

int main(int argc, char **argv)
{
  system("rm -f test_sstable_row_whole_scanner.log*");
  OB_LOGGER.set_file_name("test_sstable_row_whole_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
