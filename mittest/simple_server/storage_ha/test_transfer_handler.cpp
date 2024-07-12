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

class TestTransferHandler : public unittest::ObSimpleClusterTestBase
{
public:
  TestTransferHandler() : unittest::ObSimpleClusterTestBase("test_transfer_handler") {}
  int read_sql(ObMySQLProxy &sql_proxy, const ObSqlString &sql, ObTransferPartList &part_list);
  int gen_mock_data(const ObTransferTaskID task_id, const ObTransferStatus &status, ObTransferTask &task);
  int wait_transfer_task(
      const ObTransferTaskID task_id,
      const ObTransferStatus &expected_status,
      const bool &is_from_his,
      ObTransferTask &task);
  int wait_transfer_out_deleted_tablet_gc(
      const ObTransferTask &task);
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
  ObTableLockOwnerID owner_id;
  start_scn.convert_for_inner_table_field(1666844202200632);
  finish_scn.convert_for_inner_table_field(1666844202208490);
  owner_id.convert_from_value(999);
  ObCurTraceId::TraceId trace_id;
  trace_id.init(GCONF.self_addr_);
  uint64_t data_version = 0;
  ret = task.init(task_id, src_ls, dest_ls, ObString::make_string("500016:500014"), ObString("500030:500031"), ObString("500016:500015"),
      ObString::make_string("1152921504606846983"), ObString::make_string("1152921504606846983:0"), start_scn, finish_scn, status, trace_id, OB_SUCCESS,
      ObTransferTaskComment::EMPTY_COMMENT, ObBalanceTaskID(123), owner_id, data_version);
  return ret;
}

int TestTransferHandler::wait_transfer_task(
    const ObTransferTaskID task_id,
    const ObTransferStatus &expected_status,
    const bool &is_from_his,
    ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  task.reset();
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  const int64_t current_time = ObTimeUtil::current_time();
  const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
  const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms
  bool get_task = false;

  if (!task_id.is_valid() || !expected_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait transfer task get invalid argument", K(ret), K(task_id), K(expected_status));
  } else if (is_from_his) {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransferTaskOperator::get_history_task(inner_sql_proxy, g_tenant_id, task_id, task, create_time, finish_time))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
            LOG_WARN("cannot wait transfer task", K(ret), K(task_id), K(expected_status));
          } else {
            ret = OB_SUCCESS;
            usleep(SLEEP_INTERVAL);
          }
        } else {
          LOG_WARN("failed to get history task", K(ret), K(task_id), K(expected_status));
        }
      } else {
        break;
      }
    }
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransferTaskOperator::get_task_with_time(inner_sql_proxy, g_tenant_id, task_id, false/*for_update*/,
          task, create_time, finish_time))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          get_task = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get history task", K(ret), K(task_id), K(expected_status));
        }
      } else if (task.get_status() != expected_status) {
        get_task = false;
      } else {
        get_task = true;
        break;
      }

      if (OB_SUCC(ret) && !get_task) {
        if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
          LOG_WARN("cannot wait transfer task", K(ret), K(task_id), K(expected_status));
        } else {
          ret = OB_SUCCESS;
          usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int TestTransferHandler::wait_transfer_out_deleted_tablet_gc(
    const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(g_tenant_id) {
    const ObLSID &ls_id = task.src_ls_;
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLSService *ls_svr = nullptr;
    const int64_t current_time = ObTimeUtil::current_time();
    const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
    const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms

    if (!task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wait transfer out deleted tablet gc get invalid argument", K(ret), K(task));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be NULL", K(ret), K(task));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
    } else {
      int64_t index = 0;
      while (OB_SUCC(ret) && index < task.tablet_list_.count()) {
        ObTabletHandle tablet_handle;
        const ObTransferTabletInfo &transfer_info = task.tablet_list_.at(index);
        if (OB_FAIL(ls->get_tablet(transfer_info.tablet_id_, tablet_handle,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            index++;
          } else {
            LOG_WARN("failed to get tablet", K(ret), K(transfer_info));
          }
        } else if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait transfer out delete tablet gc timeout", K(ret), K(transfer_info));
        } else {
          usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}

TEST_F(TestTransferHandler, prepare_valid_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  usleep(5 * 1000 * 1000);

  // create table
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table ttt1(c1 int primary key, c2 blob) partition by hash(c1) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index ttt1_idx on ttt1(c1) local"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index ttt1_global ON ttt1(c1) global partition by hash(c1) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt1'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_part_list));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt1_global'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_part_list));

  // create 101 partitions
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table ttt2(c1 int primary key) partition by hash(c1) partitions 101"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt2'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_batch_part_list));

  //create other ls by cluster table
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table part_table_with_dup(c1 int) duplicate_scope = 'CLUSTER' partition by hash(c1) partitions 4"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt2'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_batch_part_list));


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

TEST_F(TestTransferHandler, test_transfer_1001_to_1002)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // stuck storage transfer
  //ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set debug_sync_timeout = '1000s'"));
  //ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  //usleep(100000); // wait for debug_sync_timeout to take effect
  //sql.reset();
  //ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("set ob_global_debug_sync = 'BEFORE_START_TRANSFER_TRANS wait_for signal execute 10000'"));
  //ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

  ASSERT_EQ(4, g_part_list.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  //title: 1001 ls transfer to 1002 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ObTransferTask transfer_task;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, ObLSID(1001), ObLSID(1002),
      g_part_list, ObBalanceTaskID(123), transfer_task));
  task_id = transfer_task.get_task_id();
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  //check observer transfer
  ObTransferStatus expected_status(ObTransferStatus::COMPLETED);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, test_transfer_1002_to_1001)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(4, g_part_list.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  //title: 1002 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ObTransferTask transfer_task;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, ObLSID(1002), ObLSID(1001),
      g_part_list, ObBalanceTaskID(123), transfer_task));
  task_id = transfer_task.get_task_id();
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  //check observer transfer
  ObTransferStatus expected_status(ObTransferStatus::COMPLETED);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
