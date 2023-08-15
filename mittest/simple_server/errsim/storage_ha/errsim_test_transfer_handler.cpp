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

namespace oceanbase
{
using namespace unittest;
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
  int generate_transfer_task(
      const ObLSID &src_ls_id,
      const ObLSID &dest_ls_id,
      ObTransferTaskID &task_id);
  int wait_error_happen(const ObTransferTask &task);
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
        if (OB_FAIL(ls->get_tablet(transfer_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
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

int TestTransferHandler::generate_transfer_task(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    ObTransferTaskID &task_id)
{
  int ret = OB_SUCCESS;
  task_id.reset();
  if (4 != g_part_list.count()
      || !is_valid_tenant_id(g_tenant_id)
      || !src_ls_id.is_valid()
      || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate transfer task get invalid argument", K(ret), K(src_ls_id), K(dest_ls_id));
  } else {
    ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
    share::ObTenantSwitchGuard tenant_guard;
    ObTenantTransferService *tenant_transfer = nullptr;
    ObMySQLTransaction trans;
    if (OB_FAIL(tenant_guard.switch_to(g_tenant_id))) {
      LOG_WARN("failed to swithc to tenant", K(ret), K(g_tenant_id));
    } else if (OB_ISNULL(tenant_transfer = MTL(ObTenantTransferService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant transfer should not be NULL", K(ret), KP(tenant_transfer));
    } else if (OB_FAIL(trans.start(&inner_sql_proxy, g_tenant_id))) {
      LOG_WARN("failed to start trans", K(ret));
    } else {
      if (OB_FAIL(tenant_transfer->generate_transfer_task(trans, src_ls_id, dest_ls_id,
          g_part_list, ObBalanceTaskID(123), task_id))) {
        LOG_WARN("failed to generate transfer task", K(ret));
      }
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int TestTransferHandler::wait_error_happen(const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait error happen get invalid argument", K(ret), K(task));
  } else if (OB_FAIL(tenant_guard.switch_to(g_tenant_id))) {
    LOG_WARN("failed to swithc to tenant", K(ret), K(g_tenant_id));
  } else {
    const ObLSID &ls_id = task.dest_ls_;
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLSService *ls_svr = nullptr;
    const int64_t current_time = ObTimeUtil::current_time();
    const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
    const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms

    if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be NULL", K(ret), K(task));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
    } else {
      while (OB_SUCC(ret)) {
        if (ls->get_transfer_handler()->retry_count_ > 0) {
          break;
        } else if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait error happen timeout", K(ret));
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
}

TEST_F(TestTransferHandler, start_lock_transfer_task_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_LOCK_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_LOCK_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();


  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));

}

TEST_F(TestTransferHandler, start_lock_transfer_member_list_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_LOCK_TRANSFER_MEMBER_LIST_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_LOCK_TRANSFER_MEMBER_LIST_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));

}

TEST_F(TestTransferHandler, start_check_start_transfer_tablet_status_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_CHECK_START_TRANSFER_STATUS_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_CHECK_START_TRANSFER_STATUS_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));

}

TEST_F(TestTransferHandler, start_check_active_trans_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_CHECK_ACTIVE_TRANS_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_CHECK_ACTIVE_TRANS_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, start_transfer_out_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_START_TRANSFER_OUT_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_START_TRANSFER_OUT_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, start_get_transfer_start_scn_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_GET_TRANSFER_START_SCN_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_GET_TRANSFER_START_SCN_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, start_wait_src_replay_to_start_scn_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_WAIT_SRC_REPALY_TO_START_SCN_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_WAIT_SRC_REPALY_TO_START_SCN_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, start_get_transfer_tablet_meta_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_GET_TRANSFER_TABLET_META_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::START);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_GET_TRANSFER_TABLET_META_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}
//
//TEST_F(TestTransferHandler, start_transfer_in_failed)
//{
//  int ret = OB_SUCCESS;
//  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
//  ObSqlString sql;
//  int64_t affected_rows = 0;
//  // inject error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_START_TRANSFER_IN_FAILED, error_code = 4023, frequency = 1"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //1001 ls transfer to 1 ls
//  // generate transfer task
//  ObTransferTaskID task_id;
//  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));
//
//  //check observer transfer status
//  ObTransferStatus expected_status(ObTransferStatus::START);
//  ObTransferTask task;
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));
//
//  //remove error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_START_TRANSFER_IN_FAILED, error_code = 4023, frequency = 0"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //check observer transfer
//  expected_status = ObTransferStatus::COMPLETED;
//  task.reset();
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, task.result_);
//  //wait 1001 ls transfer out tablet deleted gc.
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
//}
//
//TEST_F(TestTransferHandler, start_update_all_tablet_to_ls_failed)
//{
//  int ret = OB_SUCCESS;
//  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
//  ObSqlString sql;
//  int64_t affected_rows = 0;
//  // inject error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_UPDATE_ALL_TABLET_TO_LS_FAILED, error_code = 4023, frequency = 1"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //1 ls transfer to 1001 ls
//  // generate transfer task
//  ObTransferTaskID task_id;
//  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));
//
//  //check observer transfer status
//  ObTransferStatus expected_status(ObTransferStatus::START);
//  ObTransferTask task;
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));
//
//  //remove error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_UPDATE_ALL_TABLET_TO_LS_FAILED, error_code = 4023, frequency = 0"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //check observer transfer
//  expected_status = ObTransferStatus::COMPLETED;
//  task.reset();
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, task.result_);
//  //wait 1001 ls transfer out tablet deleted gc.
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
//}
//
//TEST_F(TestTransferHandler, start_update_transfer_task_failed)
//{
//  int ret = OB_SUCCESS;
//  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
//  ObSqlString sql;
//  int64_t affected_rows = 0;
//  // inject error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_UPDATE_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 1"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //1001 ls transfer to 1 ls
//  // generate transfer task
//  ObTransferTaskID task_id;
//  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));
//
//  //check observer transfer status
//  ObTransferStatus expected_status(ObTransferStatus::START);
//  ObTransferTask task;
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));
//
//  //remove error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_UPDATE_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 0"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //check observer transfer
//  expected_status = ObTransferStatus::COMPLETED;
//  task.reset();
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, task.result_);
//  //wait 1001 ls transfer out tablet deleted gc.
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
//}
//
//TEST_F(TestTransferHandler, doing_unlock_member_list_failed)
//{
//  int ret = OB_SUCCESS;
//  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
//  ObSqlString sql;
//  int64_t affected_rows = 0;
//  // inject error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_UNLOCK_TRANSFER_MEMBER_LIST_FAILED, error_code = 4023, frequency = 1"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //1 ls transfer to 1001 ls
//  // generate transfer task
//  ObTransferTaskID task_id;
//  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));
//
//  //check observer transfer status
//  ObTransferStatus expected_status(ObTransferStatus::DOING);
//  ObTransferTask task;
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));
//
//  //remove error
//  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_UNLOCK_TRANSFER_MEMBER_LIST_FAILED, error_code = 4023, frequency = 0"));
//  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
//  usleep(100000); // wait for debug_sync_timeout to take effect
//  sql.reset();
//
//  //check observer transfer
//  expected_status = ObTransferStatus::COMPLETED;
//  task.reset();
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
//  LOG_INFO("generate transfer task", K(task));
//  ASSERT_EQ(OB_SUCCESS, task.result_);
//  //wait 1001 ls transfer out tablet deleted gc.
//  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
//}

TEST_F(TestTransferHandler, doing_lock_transfer_task_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_LOCK_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_LOCK_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, doing_lock_member_list_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_LOCK_MEMBER_LIST_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_LOCK_MEMBER_LIST_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, finish_transfer_in_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_FINISH_TRANSFER_IN_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_FINISH_TRANSFER_IN_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, doing_wait_all_dest_tablet_normal_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_WAIT_ALL_DEST_TABLET_NORAML, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_WAIT_ALL_DEST_TABLET_NORAML, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, finish_transfer_out_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_FINISH_TRANSFER_OUT_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_FINISH_TRANSFER_OUT_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, doing_update_transfer_task_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_UPDATE_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1 ls transfer to 1001 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1), ObLSID(1001), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_UPDATE_TRANSFER_TASK_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));
}

TEST_F(TestTransferHandler, doing_commin_trans_failed)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // inject error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_COMMIT_TRANS_FAILED, error_code = 4023, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //1001 ls transfer to 1 ls
  // generate transfer task
  ObTransferTaskID task_id;
  ASSERT_EQ(OB_SUCCESS, generate_transfer_task(ObLSID(1001), ObLSID(1), task_id));

  //check observer transfer status
  ObTransferStatus expected_status(ObTransferStatus::DOING);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, false/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_SUCCESS, wait_error_happen(task));

  //remove error
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_DOING_COMMIT_TRANS_FAILED, error_code = 4023, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();

  //check observer transfer
  expected_status = ObTransferStatus::COMPLETED;
  task.reset();
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
