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
#define private public
#define protected public

#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/access/ob_sstable_row_getter.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;

namespace blocksstable
{
class TestSSTableRowGetter : public TestIndexBlockDataPrepare
{
public:
  TestSSTableRowGetter();
  virtual ~TestSSTableRowGetter();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
  void test_one_rowkey(const int64_t seed);
};

TestSSTableRowGetter::TestSSTableRowGetter()
  : TestIndexBlockDataPrepare("Test sstable row getter")
{
}

TestSSTableRowGetter::~TestSSTableRowGetter()
{
}

void TestSSTableRowGetter::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestSSTableRowGetter::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestSSTableRowGetter::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));

  prepare_query_param(true);
}

void TestSSTableRowGetter::TearDown()
{
  destroy_query_param();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestSSTableRowGetter::test_one_rowkey(const int64_t seed)
{
  ObSSTableRowGetter getter;
  ObSSTableRowGetter kv_getter;
  ObDatumRow query_row;
  ASSERT_EQ(OB_SUCCESS, query_row.init(allocator_, TEST_COLUMN_CNT));
  row_generate_.get_next_row(seed, query_row);
  ObDatumRowkey query_rowkey;
  query_rowkey.assign(query_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  STORAGE_LOG(INFO, "Query rowkey", K(query_row));
  ASSERT_EQ(OB_SUCCESS, getter.init(iter_param_, context_, &sstable_, &query_rowkey));
  ASSERT_EQ(OB_SUCCESS, kv_getter.init(iter_param_, context_, &ddl_kv_, &query_rowkey));

  const ObDatumRow *prow = nullptr;
  const ObDatumRow *kv_prow = nullptr;
  ASSERT_EQ(OB_SUCCESS, getter.inner_get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, kv_getter.inner_get_next_row(kv_prow));
  STORAGE_LOG(INFO, "debug datum row1", KPC(prow), KPC(kv_prow));
  if (seed >= row_cnt_) {
    ASSERT_TRUE(prow->row_flag_.is_not_exist());
    ASSERT_TRUE(kv_prow->row_flag_.is_not_exist());
  } else {
    ASSERT_TRUE(*prow == query_row);
    ASSERT_TRUE(*kv_prow == query_row);
  }
  ASSERT_EQ(OB_ITER_END, getter.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, kv_getter.inner_get_next_row(kv_prow));
  getter.reuse();
  kv_getter.reuse();
}

TEST_F(TestSSTableRowGetter, get)
{
  //left border rowkey
  test_one_rowkey(0);

  //end_key of left border
  test_one_rowkey(15933);

  //first_key of right border
  test_one_rowkey(64760);

  // end_key of right border
  test_one_rowkey(row_cnt_ - 1);

  // mid border rowkey
  test_one_rowkey(row_cnt_ / 2);

  // non-exist rowkey
  test_one_rowkey(row_cnt_);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_sstable_row_getter.log*");
  OB_LOGGER.set_file_name("test_sstable_row_getter.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
