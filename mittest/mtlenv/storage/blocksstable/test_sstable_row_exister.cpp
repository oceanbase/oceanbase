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
#include "storage/access/ob_sstable_row_exister.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestSSTableRowExister : public TestIndexBlockDataPrepare
{
public:
  TestSSTableRowExister();
  virtual ~TestSSTableRowExister();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
};

TestSSTableRowExister::TestSSTableRowExister()
  : TestIndexBlockDataPrepare("Test sstable row exister")
{
}

TestSSTableRowExister::~TestSSTableRowExister()
{
}

void TestSSTableRowExister::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestSSTableRowExister::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestSSTableRowExister::SetUp()
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

void TestSSTableRowExister::TearDown()
{
  destroy_query_param();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestSSTableRowExister, single_exist)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  ObStoreCtx ctx;
  ObSSTableRowExister row_exister;
  ObSSTableRowExister kv_row_exister;

  //exist check
  bool is_exist = false;
  bool is_found = false;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_cnt_ - 1, row));
  ASSERT_EQ(OB_SUCCESS, rowkey.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_exister.inner_open(iter_param_, context_, &sstable_, &rowkey));
  ASSERT_EQ(OB_SUCCESS, kv_row_exister.inner_open(iter_param_, context_, &ddl_kv_, &rowkey));
  const ObDatumRow *prow = nullptr;
  const ObDatumRow *kv_prow = nullptr;
  ASSERT_EQ(OB_SUCCESS, row_exister.inner_get_next_row(prow));
  ASSERT_TRUE(prow->row_flag_.is_exist());
  ASSERT_EQ(OB_SUCCESS, kv_row_exister.inner_get_next_row(kv_prow));
  ASSERT_TRUE(kv_prow->row_flag_.is_exist());

  row_exister.reuse();
  kv_row_exister.reuse();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_cnt_, row));
  ASSERT_EQ(OB_SUCCESS, row_exister.inner_open(iter_param_, context_, &sstable_, &rowkey));
  ASSERT_EQ(OB_SUCCESS, row_exister.inner_get_next_row(prow));
  ASSERT_TRUE(prow->row_flag_.is_not_exist());
  ASSERT_EQ(OB_SUCCESS, kv_row_exister.inner_open(iter_param_, context_, &ddl_kv_, &rowkey));
  ASSERT_EQ(OB_SUCCESS, kv_row_exister.inner_get_next_row(kv_prow));
  ASSERT_TRUE(kv_prow->row_flag_.is_not_exist());
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_sstable_row_exister.log*");
  OB_LOGGER.set_file_name("test_sstable_row_exister.log", false, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
