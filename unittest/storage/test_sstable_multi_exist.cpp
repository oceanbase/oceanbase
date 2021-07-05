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
#include "ob_sstable_test.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestSSTableMultiExist : public ObSSTableTest {
public:
  TestSSTableMultiExist();
  virtual ~TestSSTableMultiExist();
  virtual void SetUp();
  virtual void TearDown();

  transaction::ObTransService trans_service_;
  transaction::ObPartitionTransCtxMgr ctx_mgr_;
};

TestSSTableMultiExist::TestSSTableMultiExist() : ObSSTableTest()
{}

TestSSTableMultiExist::~TestSSTableMultiExist()
{}

void TestSSTableMultiExist::SetUp()
{
  ObSSTableTest::SetUp();
  ObPartitionKey pkey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
  ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().init());
  ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey));
  ObPartitionKey& part = const_cast<ObPartitionKey&>(ctx_mgr_.get_partition());
  part = pkey;
  trans_service_.part_trans_ctx_mgr_.mgr_cache_.set(pkey.hash(), &ctx_mgr_);
  ObPartitionService::get_instance().txs_ = &trans_service_;
}

void TestSSTableMultiExist::TearDown()
{
  ObSSTableTest::TearDown();
  ObPartitionService::get_instance().get_pg_index().destroy();
}

TEST_F(TestSSTableMultiExist, multi_exist)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey rowkey;
  ObObj cells[TEST_COLUMN_CNT];
  ObStoreRow row;
  ObStoreCtx ctx;
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  rowkey.assign(cells, TEST_ROWKEY_COLUMN_CNT);

  // prepare query param
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // exist check
  bool is_exist = false;
  bool is_found = false;
  ret = row_generate_.get_next_row(row_cnt_ - 1, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_.exist(ctx, table_key_.table_id_, rowkey, *param_.out_cols_, is_exist, is_found);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_exist);
  ASSERT_TRUE(is_found);

  ret = row_generate_.get_next_row(row_cnt_, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_.exist(ctx, table_key_.table_id_, rowkey, *param_.out_cols_, is_exist, is_found);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);
  ASSERT_FALSE(is_found);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_multi_exist.log*");
  OB_LOGGER.set_file_name("test_sstable_multi_exist.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable_multi_exist");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
