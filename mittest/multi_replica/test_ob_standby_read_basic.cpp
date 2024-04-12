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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_trans_part_ctx.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObStandbyReadBasic

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_standby_read_basic);

namespace oceanbase
{

namespace transaction
{

static bool BLOCK_PRE_COMMIT = false;

OB_NOINLINE int ObPartTransCtx::errsim_do_pre_commit_without_root_()
{
  int ret = OB_SUCCESS;

  if (BLOCK_PRE_COMMIT) {
    ret = OB_EAGAIN;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "errsim in do pre_commit", K(ret));
  }

  return ret;
}
} // namespace transaction

namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

struct TableBasicArg
{
  uint64_t tenant_id_;

  int64_t dup_ls_id_num_;
  int64_t dup_table_id_;
  ObSEArray<int64_t, 10> dup_tablet_id_array_;

  int64_t normal_ls_id_num_;
  int64_t normal_table_id_;
  ObSEArray<int64_t, 10> normal_tablet_id_array_;

  TO_STRING_KV(K(tenant_id_),
               K(dup_ls_id_num_),
               K(dup_table_id_),
               K(normal_ls_id_num_),
               K(normal_table_id_),
               K(dup_tablet_id_array_),
               K(normal_tablet_id_array_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(TableBasicArg,
                    tenant_id_,
                    dup_ls_id_num_,
                    dup_table_id_,
                    dup_tablet_id_array_,
                    normal_ls_id_num_,
                    normal_table_id_,
                    normal_tablet_id_array_);

static TableBasicArg static_basic_arg_;

const std::string test_dup_table_name = "test_dup_1";
const std::string test_normal_table_name = "test_normal_1";
TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;

  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone3; zone2';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());

  unittest::TestEnvTool::create_table_for_test_env(
      test_conn, test_dup_table_name.c_str(), 1, true /*is_dup_table*/,
      static_basic_arg_.dup_ls_id_num_, static_basic_arg_.dup_table_id_,
      static_basic_arg_.dup_tablet_id_array_);

  unittest::TestEnvTool::create_table_for_test_env(
      test_conn, test_normal_table_name.c_str(), 1, false /*is_dup_table*/,
      static_basic_arg_.normal_ls_id_num_, static_basic_arg_.normal_table_id_,
      static_basic_arg_.normal_tablet_id_array_);

  static_basic_arg_.tenant_id_ = test_tenant_id;
  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<TableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  finish_event("CREATE_TEST_TABLE", tmp_str);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), read_with_one_pre_commit_participant)
{
  int ret = OB_SUCCESS;

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  BLOCK_PRE_COMMIT = true;
  WRITE_SQL_BY_CONN(test_conn, "set autocommit = false;");
  WRITE_SQL_BY_CONN(test_conn, "begin;");

  const int64_t DEFAULT_LOAD_ROW_CNT = 10;
  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_dup_sql_str =
        "INSERT INTO " + test_dup_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
    std::string insert_normal_sql_str =
        "INSERT INTO " + test_normal_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_dup_sql_str.c_str());
    WRITE_SQL_BY_CONN(test_conn, insert_normal_sql_str.c_str());
  }

  int64_t tx_id_num = 0;
  std::string insert_normal_sql_str =
      "INSERT INTO " + test_normal_table_name + " VALUES(" + std::to_string(1) + ", 0 , 0)";
  GET_TX_ID_FROM_SQL_AUDIT(test_conn, insert_normal_sql_str.c_str(), tx_id_num);

  {
    int res_ret = OB_SUCCESS;
    WRITE_SQL_BY_CONN_INNER(test_conn, "commit", res_ret);
    ASSERT_EQ(res_ret, OB_TIMEOUT);
  }

  std::string switch_to_standby_sql =
      "ALTER SYSTEM SWITCHOVER TO STANDBY TENANT " + std::string("tt1");
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  WRITE_SQL_BY_CONN(sys_conn, switch_to_standby_sql.c_str());
  {
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(static_basic_arg_.tenant_id_));
    RETRY_UNTIL_TIMEOUT(share::is_standby_tenant(share::ObTenantEnv::get_tenant()->get_tenant_role()), 30 * 1000 * 1000, 1 * 1000 * 1000);
  }

  std::string select_dup_sql_str = "select count(*) from " + test_dup_table_name;
  // READ_SQL_BY_CONN(test_conn, tmp_table_info_result, select_dup_sql_str.c_str());
  READ_SQL_BY_CONN(test_conn, table_info_result, select_dup_sql_str.c_str());

  GET_TX_ID_FROM_SQL_AUDIT(test_conn, select_dup_sql_str.c_str(), tx_id_num);

  ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  int64_t row_count = 0;
  const int64_t col_index = 0;
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int(col_index, row_count));
  ASSERT_EQ(DEFAULT_LOAD_ROW_CNT, row_count);


  std::string switch_to_primary_sql =
  "ALTER SYSTEM SWITCHOVER TO PRIMARY TENANT " + std::string("tt1");
  WRITE_SQL_BY_CONN(sys_conn, switch_to_primary_sql.c_str());
  {
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(static_basic_arg_.tenant_id_));
    RETRY_UNTIL_TIMEOUT(share::is_primary_tenant(share::ObTenantEnv::get_tenant()->get_tenant_role()), 30 * 1000 * 1000, 1 * 1000 * 1000);
  }

  usleep(10 * 1000 * 1000);

  WRITE_SQL_BY_CONN(sys_conn, switch_to_standby_sql.c_str());
  {
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(static_basic_arg_.tenant_id_));
    RETRY_UNTIL_TIMEOUT(share::is_standby_tenant(share::ObTenantEnv::get_tenant()->get_tenant_role()), 30 * 1000 * 1000, 1 * 1000 * 1000);
    // share::is_standby_tenant(share::ObTenantEnv::get_tenant()->get_tenant_role());
  }

  READ_SQL_BY_CONN(test_conn, table_info_result_2, select_dup_sql_str.c_str());

  GET_TX_ID_FROM_SQL_AUDIT(test_conn, select_dup_sql_str.c_str(), tx_id_num);

  ASSERT_EQ(OB_SUCCESS, table_info_result_2->next());
   row_count = 0;
  ASSERT_EQ(OB_SUCCESS, table_info_result_2->get_int(col_index, row_count));
  ASSERT_EQ(DEFAULT_LOAD_ROW_CNT, row_count);

  // usleep(1000 * 1000 * 1000);
}

} // namespace unittest
} // namespace oceanbase
