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


class TestGCTabletInOfflineTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  TestGCTabletInOfflineTest() : ObSimpleClusterTestBase("test_gc_tablet_in_offline_") {}
};

TEST_F(TestGCTabletInOfflineTest, test_gc_tablet_in_offline)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    int64_t affected_rows;
    ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_LOCK_CREATE_TABLET_FAILED, error_code = 6666, frequency = 1"));
    ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
    ASSERT_EQ(-6666, GCTX.ddl_sql_proxy_->write(OB_SYS_TENANT_ID, "create table test_mds_table(a int)", affected_rows));
    ObTabletID tablet_id(200001);
    ObLSID ls_id(1);
    storage::ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    storage::ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_TABLET_NOT_EXIST, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle, 10*1_s, ObMDSGetTabletMode::READ_WITHOUT_CHECK));


    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->offline());
    ls_handle.reset();
    tablet_handle.reset();

    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ASSERT_EQ(OB_TABLET_NOT_EXIST, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle, 10*1_s, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif
