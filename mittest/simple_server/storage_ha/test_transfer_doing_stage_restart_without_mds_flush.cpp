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
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "test_transfer_common_fun.h"
#include "env/ob_simple_server_restart_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "storage/high_availability/ob_transfer_handler.h"  //ObTransferHandler
#include "lib/utility/utility.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
using namespace share::schema;
using namespace common;
using namespace share;
using namespace transaction::tablelock;
using namespace rootserver;

static uint64_t g_tenant_id;
static ObTransferPartList g_part_list;
static ObTransferPartList g_batch_part_list;

static const char *TEST_FILE_NAME = "test_transfer_doing_stage_restart_without_mds_flush";
static const char *BORN_CASE_NAME = "TestTransferHandler";
static const char *RESTART_CASE_NAME = "TestTransferRestart";

class TestTransferHandler : public unittest::ObSimpleClusterTestBase
{
public:
  TestTransferHandler() : unittest::ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  int read_sql(ObMySQLProxy &sql_proxy, const ObSqlString &sql, ObTransferPartList &part_list);
  int gen_mock_data(const ObTransferTaskID task_id, const ObTransferStatus &status, ObTransferTask &task);
};

int TestTransferHandler::read_sql(
    ObMySQLProxy &sql_proxy,
    const ObSqlString &sql,
    ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_UNLIKELY(!is_valid_tenant_id(g_tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      uint64_t table_id = OB_INVALID_ID;
      if (OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "object_id", table_id, uint64_t);
      }
      while(OB_SUCC(ret)) {
        uint64_t part_id = OB_INVALID_ID;
        ObTransferPartInfo part_info;
        if (OB_SUCC(res.next())) {
          EXTRACT_INT_FIELD_MYSQL(res, "object_id", part_id, uint64_t);
          if (OB_FAIL(part_info.init(table_id, part_id))) {
          } else if (OB_FAIL(part_list.push_back(part_info))) {
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to generate data", K(sql));
      }
    }
  }
  return ret;
}

int TestTransferHandler::gen_mock_data(const ObTransferTaskID task_id, const ObTransferStatus &status, ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  ObLSID src_ls(1001);
  ObLSID dest_ls(1002);
  share::SCN start_scn;
  share::SCN finish_scn;
  start_scn.convert_for_inner_table_field(1666844202200632);
  finish_scn.convert_for_inner_table_field(1666844202208490);
  ObCurTraceId::TraceId trace_id;
  trace_id.init(GCONF.self_addr_);
  ret = task.init(task_id, src_ls, dest_ls, ObString::make_string("500016:500014"), ObString("500030:500031"), ObString("500016:500015"),
      ObString::make_string("1152921504606846983"), ObString::make_string("1152921504606846983:0"), start_scn, finish_scn, status, trace_id, OB_SUCCESS,
      ObTransferTaskComment::EMPTY_COMMENT, ObBalanceTaskID(123), ObTableLockOwnerID(999));
  return ret;
}

class TestTransferRestart : public ObSimpleClusterTestBase
{
public:
  TestTransferRestart() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void select_existed_data();
};

void TestTransferRestart::select_existed_data()
{
  sleep(10);
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  int ret = OB_SUCCESS;
  const int MAX_TRY_SELECT_CNT = 20;
  int try_select_cnt = 0;
  bool select_succ = false;

  while (++try_select_cnt <= MAX_TRY_SELECT_CNT) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t row_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign("select count(*) row_cnt from ttt1"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        // table or schema may not exist yet
        ret = OB_SUCCESS;
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
        SERVER_LOG(INFO, "row count from test_tx_data_t : ", K(row_cnt));
      }
    }

    if (row_cnt > 0) {
      select_succ = true;
      LOG_INFO("select done", K(try_select_cnt));
      break;
    } else {
      LOG_INFO("select once", K(try_select_cnt));
      ::sleep(1);
    }
  }

  ASSERT_EQ(select_succ, true);
}

TEST_F(TestTransferHandler, prepare_valid_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObMySQLProxy &sql_proxy2 = get_curr_simple_server().get_sql_proxy2();

  //set transfer service wakup interval
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _transfer_service_wakeup_interval = '5s' tenant = sys"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _transfer_service_wakeup_interval = '5s' tenant = all_user"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _transfer_service_wakeup_interval = '5s' tenant = all_meta"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();

  // create table
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table ttt1(c1 int primary key, c2 blob) partition by hash(c1) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy2.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into ttt1 values(1, 'aaa')"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy2.write(sql.ptr(), affected_rows));
  sql.reset();

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index ttt1_idx on ttt1(c1) local"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy2.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index ttt1_global ON ttt1(c1) global partition by hash(c1) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy2.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt1'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy2, sql, g_part_list));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt1_global'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy2, sql, g_part_list));

  // create 101 partitions
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table ttt2(c1 int primary key) partition by hash(c1) partitions 101"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy2.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt2'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy2, sql, g_batch_part_list));

  //create other ls by cluster table
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table part_table_with_dup(c1 int) duplicate_scope = 'CLUSTER' partition by hash(c1) partitions 4"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy2.write(sql.ptr(), affected_rows));
  sql.reset();

  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _enable_balance_kill_transaction = true tenant = 'tt1';"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _balance_wait_killing_transaction_end_threshold = '10s' tenant = 'tt1';"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _enable_balance_kill_transaction = true tenant = 'tt1';"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

}

TEST_F(TestTransferHandler, test_transfer_1001_to_1002_with_transfer_doing_without_mds_flush)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // stuck tablet gc service flush mds
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set debug_sync_timeout = '1000s'"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("set ob_global_debug_sync = 'BEFORE_TABLET_MDS_FLUSH wait_for signal execute 10000'"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("set ob_global_debug_sync = 'SWITCH_LEADER_BETWEEN_FINISH_TRANSFER_IN_AND_OUT wait_for signal1 execute 10000'"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect

  ASSERT_EQ(4, g_part_list.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  //title: 1001 ls transfer to 1002 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, ObLSID(1001), ObLSID(1002),
      g_part_list, ObBalanceTaskID(123), task_id));
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  //check observer transfer
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(g_tenant_id, task_id, expected_status, false/*is_from_his*/, inner_sql_proxy, task));
  LOG_INFO("generate transfer task", K(task));
}

TEST_F(TestTransferRestart, observer_restart_when_transfer_doing_without_flush)
{
  // init sql proxy2 to use tenant tt1
  g_tenant_id = 1002;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "observer restart succ");
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  //check observer transfer
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task_after_restart(g_tenant_id, inner_sql_proxy));
  select_existed_data();
}


} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  int ret = 0;
  int time_sec = 0;
  ObSimpleServerRestartHelper restart_helper(argc, argv, oceanbase::storage::TEST_FILE_NAME, oceanbase::storage::BORN_CASE_NAME,
      oceanbase::storage::RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();
  return ret;
}
