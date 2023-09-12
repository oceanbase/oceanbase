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

#ifndef DEBUG_FOR_MDS
#define DEBUG_FOR_MDS
#include "lib/ob_errno.h"
#include <chrono>
#include <thread>
#define TEST_MDS_TRANSACTION
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "ob_tablet_id.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/utility/serialization.h"
#include "share/cache/ob_kv_storecache.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"

namespace oceanbase
{
share::SCN mock_tablet_oldest_scn = unittest::mock_scn(1);
ObTabletID tablet_id;
namespace compaction
{
int ObMediumCompactionInfo::assign(ObIAllocator &allocator, const ObMediumCompactionInfo &medium_info)
{
  UNUSED(allocator);
  data_version_ = medium_info.data_version_;
  return OB_SUCCESS;
}
}
namespace storage
{
namespace mds
{
int ObTenantMdsTimer::get_tablet_oldest_scn_(ObTablet &tablet, share::SCN &oldest_scn)
{
  #define PRINT_WRAPPER KR(ret), K(tablet), K(tablet_oldest_scn), KPC(this)
  int ret = OB_SUCCESS;
  if (tablet.tablet_meta_.tablet_id_ == tablet_id) {// for tested tablet
    oldest_scn = mock_tablet_oldest_scn;
  } else {// for other tablet
    oldest_scn = unittest::mock_scn(1000);// FIXME: get correct min scn from oldest version tablet
  }
  return ret;
  #undef PRINT_WRAPPER
}
}
}
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace mds;
using namespace compaction;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class TestMdsTransactionTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  TestMdsTransactionTest() : ObSimpleClusterTestBase("test_mds_transaction_") {}
};

TEST_F(TestMdsTransactionTest, simple_test)
{
  common::ObMySQLProxy *sql_proxy = GCTX.ddl_sql_proxy_;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy->acquire(connection));
  observer::ObInnerSQLConnection *inner_conn = dynamic_cast<observer::ObInnerSQLConnection *>(connection);

  ASSERT_EQ(OB_SUCCESS, inner_conn->start_transaction(OB_SYS_TENANT_ID));
  constexpr int64_t LEN = 128;
  char buffer[LEN];
  int64_t pos = 0;
  int64_t magic_number = 12345;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(buffer, LEN, pos, magic_number));
  ASSERT_EQ(OB_SUCCESS, inner_conn->register_multi_data_source(OB_SYS_TENANT_ID, share::ObLSID(1), transaction::ObTxDataSourceType::TEST1, buffer, pos));
  ASSERT_EQ(OB_SUCCESS, inner_conn->commit());
}

TEST_F(TestMdsTransactionTest, write_mds_table)
{
  common::ObMySQLProxy *sql_proxy = GCTX.ddl_sql_proxy_;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy->acquire(connection));
  observer::ObInnerSQLConnection *inner_conn = dynamic_cast<observer::ObInnerSQLConnection *>(connection);

  ASSERT_EQ(OB_SUCCESS, inner_conn->start_transaction(OB_SYS_TENANT_ID));
  constexpr int64_t LEN = 128;
  char buffer[LEN];
  int64_t pos = 0;
  int64_t magic_number = 12345;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(buffer, LEN, pos, magic_number));
  ASSERT_EQ(OB_SUCCESS, inner_conn->register_multi_data_source(OB_SYS_TENANT_ID, share::ObLSID(1), transaction::ObTxDataSourceType::TEST3, buffer, pos));
  // ASSERT_EQ(OB_SUCCESS, inner_conn->commit());
  ASSERT_EQ(OB_SUCCESS, inner_conn->rollback());
  // int64_t val = 0;
  // ASSERT_EQ(OB_SUCCESS, TestMdsTable.get_snapshot<ExampleUserData1>([&val](const ExampleUserData1 &data) {
  //   val = data.value_;
  //   return OB_SUCCESS;
  // }, share::SCN::max_scn()));
  // ASSERT_EQ(magic_number, val);
}

TEST_F(TestMdsTransactionTest, test_for_each_kv_in_unit_in_tablet)
{
  int ret = OB_SUCCESS;
  ObMediumCompactionInfo data_1, data_2, data_3, data_11;
  ObMediumCompactionInfo &data_1_ref = data_1;
  data_1.data_version_ = 1;
  data_2.data_version_ = 2;
  data_3.data_version_ = 3;
  data_11.data_version_ = 11;
  auto mock_equal = [](const ObMediumCompactionInfo &lhs, const ObMediumCompactionInfo &rhs) -> bool {
    return lhs.data_version_ == rhs.data_version_;
  };
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    int64_t _;
    // 1. 新建一个tablet
    ASSERT_EQ(OB_SUCCESS, GCTX.ddl_sql_proxy_->write(OB_SYS_TENANT_ID, "create table test_mds_table(a int)", _));
    // 2. 从表名拿到它的tablet_id
    ObTabletID tablet_id;
    ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                              {"tablet_id"},
                                                              OB_ALL_TABLE_TNAME,
                                                              "where table_name = 'test_mds_table'",
                                                              tablet_id));
    // 3. 从tablet_id拿到它的ls_id
    ObLSID ls_id;
    char where_condition[512] = { 0 };
    databuff_printf(where_condition, 512, "where tablet_id = %ld", tablet_id.id());
    ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                              {"ls_id"},
                                                              OB_ALL_TABLET_TO_LS_TNAME,
                                                              where_condition,
                                                              ls_id));
    // 4. 从ls_id找到ls
    storage::ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    // 5. 从LS找到tablet结构
    storage::ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    // 6. 调用tablet接口第一次写入多源数据，提交
    MdsCtx ctx1(mds::MdsWriter(ObTransID(1)));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->set(ObMediumCompactionInfoKey(1), data_1_ref, ctx1));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->set(ObMediumCompactionInfoKey(3), data_3, ctx1));
    ctx1.single_log_commit(mock_scn(10), mock_scn(10));
    // 7. 调用tablet接口第二次写入多源数据，不提交
    MdsCtx ctx2(mds::MdsWriter(ObTransID(2)));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->set(ObMediumCompactionInfoKey(1), data_11, ctx2));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->set(ObMediumCompactionInfoKey(2), data_2, ctx2));
  }
}// ctx2 is auto aborted, but print ERROR log

TEST_F(TestMdsTransactionTest, test_mds_table_gc_and_recycle)
{
  int ret = OB_SUCCESS;
  ObTabletBindingMdsUserData data_to_write;
  data_to_write.schema_version_ = 1;
  data_to_write.data_tablet_id_ = ObTabletID(2);
  data_to_write.hidden_tablet_id_ = ObTabletID(3);
  data_to_write.lob_meta_tablet_id_ = ObTabletID(4);
  data_to_write.lob_piece_tablet_id_ = ObTabletID(5);
  auto mock_equal = [](const ObMediumCompactionInfo &lhs,
                       const ObMediumCompactionInfo &rhs) -> bool {
    return lhs.data_version_ == rhs.data_version_;
  };
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    int64_t _;
    // 1. 新建一个tablet
    ASSERT_EQ(OB_SUCCESS, GCTX.ddl_sql_proxy_->write(OB_SYS_TENANT_ID, "create table test_mds_table2(a int)", _));
    // 2. 从表名拿到它的tablet_id
    ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                              {"tablet_id"},
                                                              OB_ALL_TABLE_TNAME,
                                                              "where table_name = 'test_mds_table2'",
                                                              tablet_id));
    // 3. 从tablet_id拿到它的ls_id
    ObLSID ls_id;
    char where_condition[512] = { 0 };
    databuff_printf(where_condition, 512, "where tablet_id = %ld", tablet_id.id());
    ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                              {"ls_id"},
                                                              OB_ALL_TABLET_TO_LS_TNAME,
                                                              where_condition,
                                                              ls_id));
    // 4. 从ls_id找到ls
    storage::ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    // 5. 从LS找到tablet结构
    storage::ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    // 6. 调用tablet接口写入多源数据，提交
    MdsCtx ctx1(mds::MdsWriter(ObTransID(1)));
    share::SCN rec_scn;
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->set(data_to_write, ctx1));
    // ASSERT_EQ(OB_SUCCESS, static_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr())->mds_table_handler_.mds_table_handle_.get_rec_scn(rec_scn));
    // ASSERT_EQ(share::SCN::max_scn(), rec_scn);
    ctx1.single_log_commit(mock_scn(10), mock_scn(10000000));
    ASSERT_EQ(true, static_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr())->mds_table_handler_.mds_table_handle_.is_valid());
    // ASSERT_EQ(OB_SUCCESS, static_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr())->mds_table_handler_.mds_table_handle_.get_rec_scn(rec_scn));
    // ASSERT_EQ(mock_scn(10), rec_scn);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->mds_table_flush(share::SCN::max_scn()));
    // 7. 检查mds table的存在情况
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_EQ(true, static_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr())->mds_table_handler_.mds_table_handle_.is_valid());
    ASSERT_EQ(OB_SUCCESS, static_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr())->mds_table_handler_.mds_table_handle_.get_rec_scn(rec_scn));
    ASSERT_EQ(share::SCN::max_scn(), rec_scn);
    MDS_LOG(ERROR, "change mock_tablet_oldest_scn", K(tablet_id));
    mock_tablet_oldest_scn = unittest::mock_scn(2074916885902668817);
    std::this_thread::sleep_for(std::chrono::seconds(15));
    ASSERT_EQ(false, static_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr())->mds_table_handler_.mds_table_handle_.is_valid());
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));// 重新获取一下tablet handle
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_mds_data_from_tablet<ObTabletBindingMdsUserData>([&data_to_write](const ObTabletBindingMdsUserData &data_to_read) -> int {
      OB_ASSERT(data_to_write.schema_version_ == data_to_read.schema_version_);
      return OB_SUCCESS;
    }));
  }
}

TEST_F(TestMdsTransactionTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif