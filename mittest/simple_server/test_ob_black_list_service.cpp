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
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "storage/tx/wrs/ob_black_list.h"
#include "env/ob_simple_cluster_test_base.h"  // ObSimpleClusterTestBase
#include "lib/mysqlclient/ob_mysql_result.h"  // ReadResult

namespace oceanbase
{
using namespace transaction;
using namespace storage;

namespace unittest
{

class TestObBlackListService : public ObSimpleClusterTestBase
{
public:
  TestObBlackListService() : ObSimpleClusterTestBase("test_ob_black_list_service_") {}
};

TEST_F(TestObBlackListService, black_list_inner_func)
{
  LOG_INFO("called", "func", test_info_->name());

  // init, bl_service already inited in ObServer::init()
  int ret = OB_SUCCESS;
  bool check = true;
  const int64_t query_timeout = 10 * 1000 * 1000; // 10s
  ObBLKey bl_key;
  ObLsInfo ls_info;
  ObBLService &bl_service = ObBLService::get_instance();

  // sql
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(BLACK_LIST_SELECT_LS_INFO_STMT));
  ASSERT_EQ(OB_SUCCESS, ret);
  SMART_VAR(ObISQLClient::ReadResult, res) {
    // do sql query
    ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, OB_SYS_TENANT_ID, sql.ptr(), nullptr, query_timeout));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);

    // iter result
    while (OB_SUCC(result->next())) {
      ASSERT_EQ(OB_SUCCESS, bl_service.get_info_from_result_(*result, bl_key, ls_info));
      ASSERT_EQ(true, bl_key.is_valid());
      ASSERT_EQ(true, ls_info.is_valid());
      LOG_INFO("get_info_from_result_ ", K(bl_key), K(ls_info));
    }

    // get max_stale_time_for_weak_consistency
    int64_t max_stale_time = bl_service.get_tenant_max_stale_time_(bl_key.get_tenant_id());
    LOG_INFO("get_tenant_max_stale_time_ ", K(bl_key), K(max_stale_time));

    // do blacklist check
    ASSERT_EQ(OB_SUCCESS, bl_service.check_in_black_list(bl_key, check));
    ASSERT_EQ(false, check);

    // query again
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, OB_SYS_TENANT_ID, sql.ptr(), nullptr, query_timeout));
    result = res.get_result();
    ASSERT_NE(nullptr, result);

    // sleep 5s, then check
    usleep(5 * 1000 * 1000);
    ASSERT_EQ(OB_SUCCESS, bl_service.do_black_list_check_(result));
    ASSERT_EQ(OB_SUCCESS, bl_service.check_in_black_list(bl_key, check));
    ASSERT_EQ(false, check); // leader won't be put into blacklist
  }
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF._enable_defensive_check = false;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
