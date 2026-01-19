// owner: yilan.zyn
// owner group: storage

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
#include <gmock/gmock.h>
#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "storage/tx_storage/ob_ls_service.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"

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

#define INNER_EXE_SQL(tenant_id, ...)                                          \
  ASSERT_EQ(OB_SUCCESS, sql.assign(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(tenant_id, sql.ptr(), affected_rows));

static uint64_t g_tenant_id;
static ObTransferPartList g_part_list;
static ObTransferPartList g_batch_part_list;
constexpr int64_t PART_LIST_COUNT = 5;
constexpr int64_t GTT_SESSION_TABLE_ROW_COUNT = 3;

common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool;
common::ObMySQLProxy sql_proxy; // keep session alive for oracle gtt

class TestTransferHandler : public unittest::ObSimpleClusterTestBase
{
public:
  TestTransferHandler() : unittest::ObSimpleClusterTestBase("test_transfer_handler_oracle_gtt") {}
  int read_sql(ObMySQLProxy &sql_proxy, const ObSqlString &sql, ObTransferPartList &part_list);
  int check_result(ObMySQLProxy &sql_proxy, const ObSqlString &sql, int64_t affected_rows);
  int get_tablet_ids_and_check_ls_from_tablet_to_ls(ObMySQLProxy &sql_proxy, const ObSEArray<uint64_t, PART_LIST_COUNT> &table_ids,
      const int64_t expected_ls_id, ObSEArray<uint64_t, PART_LIST_COUNT> &tablet_ids/* out */);
  int get_tablet_ids_and_check_ls_from_session_table(ObMySQLProxy &sql_proxy, const ObSEArray<uint64_t, PART_LIST_COUNT> &table_ids,
      const int64_t expected_ls_id, ObSEArray<uint64_t, PART_LIST_COUNT> &tablet_ids/* out */);
  int wait_transfer_task(
      const ObTransferTaskID task_id,
      const ObTransferStatus &expected_status,
      const bool &is_from_his,
      ObTransferTask &task);
  int wait_transfer_out_deleted_tablet_gc(
      const ObTransferTask &task);
  int read_tablet_to_ls_infos(const ObIArray<uint64_t> &table_ids, ObIArray<ObTabletToLSInfo> &tablet_to_ls_infos);
};

// get part ids for transfer task
// if the table is a partitioned table, get all the partition ids
// if the table is not a partitioned table, get the table id
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
        EXTRACT_INT_FIELD_MYSQL(res, "OBJECT_ID", table_id, uint64_t);
      }
      bool is_partition = false;
      while(OB_SUCC(ret)) {
        uint64_t part_id = OB_INVALID_ID;
        ObTransferPartInfo part_info;
        if (OB_SUCC(res.next())) {
          EXTRACT_INT_FIELD_MYSQL(res, "OBJECT_ID", part_id, uint64_t);
          is_partition = true;
          if (OB_FAIL(part_info.init(table_id, part_id))) {
          } else if (OB_FAIL(part_list.push_back(part_info))) {
          }
        }
      }
      if (!is_partition) {
        ObTransferPartInfo part_info;
        if (OB_FAIL(part_info.init(table_id, table_id))) {
        } else if (OB_FAIL(part_list.push_back(part_info))) {
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

// check result
int TestTransferHandler::check_result(ObMySQLProxy &sql_proxy, const ObSqlString &sql, int64_t affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      while(OB_SUCC(ret)) {
        if (OB_FAIL(res.next())) {
          break;
        } else {
          count++;
        }
      }
      if (OB_ITER_END == ret) {
        if (count != affected_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("check result failed", K(ret), K(sql), K(affected_rows), K(count));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int TestTransferHandler::read_tablet_to_ls_infos(const ObIArray<uint64_t> &table_ids, ObIArray<ObTabletToLSInfo> &tablet_to_ls_infos)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "*";
  if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_ids));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
      ObSqlString sql;
      ObSqlString table_id_list;
      ARRAY_FOREACH(table_ids, idx) {
        const uint64_t table_id = table_ids.at(idx);
        if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid table_id", KR(ret), K(table_id));
        } else if (OB_FAIL(table_id_list.append_fmt(
            "%s%lu",
            0 == idx ? "" : ",",
            table_id))) {
          LOG_WARN("fail to assign sql", KR(ret), K(g_tenant_id), K(table_id));
        }
      }
      if (FAILEDx(sql.append_fmt(
          "SELECT %s FROM %s WHERE table_id IN (",
          query_column_str,
          OB_ALL_TABLET_TO_LS_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql.append(table_id_list.string()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql), K(table_id_list));
      }
      if (FAILEDx(sql.append_fmt(")"))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(inner_sql_proxy.read(result, g_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(g_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else {
        common::sqlclient::ObMySQLResult &res = *result.get_result();
        while (OB_SUCC(ret) && OB_SUCC(res.next())) {
          int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
          int64_t ls_id = ObLSID::INVALID_LS_ID;
          uint64_t table_id = OB_INVALID_ID;
          int64_t transfer_seq = 0;
          ObTabletToLSInfo info;

          EXTRACT_INT_FIELD_MYSQL(res, "tablet_id", tablet_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(res, "ls_id", ls_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "transfer_seq", transfer_seq, int64_t,
              true/*skip_null_error*/, true/*skip_column_error*/, 0/*default value*/);

          if (FAILEDx(info.init(ObTabletID(tablet_id), ObLSID(ls_id), table_id, transfer_seq))) {
            LOG_WARN("init failed", KR(ret), K(tablet_id), K(ls_id), K(table_id), K(transfer_seq));
          } else if (OB_FAIL(tablet_to_ls_infos.push_back(info))) {
            LOG_WARN("fail to push back", KR(ret), K(info));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int TestTransferHandler::get_tablet_ids_and_check_ls_from_tablet_to_ls(
    ObMySQLProxy &sql_proxy,
    const ObSEArray<uint64_t, PART_LIST_COUNT> &table_ids,
    const int64_t expected_ls_id,
    ObSEArray<uint64_t, PART_LIST_COUNT> &tablet_ids/* out */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletToLSInfo, PART_LIST_COUNT> tablet_to_ls_infos;

  if (OB_FAIL(read_tablet_to_ls_infos(table_ids, tablet_to_ls_infos))) {
    LOG_WARN("failed to get tablet to ls infos", K(ret), K(table_ids), K(tablet_to_ls_infos));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_to_ls_infos.count(); i++) {
    if (tablet_to_ls_infos.at(i).get_ls_id().id() != expected_ls_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet to ls info ls id is not expected", K(ret), K(tablet_to_ls_infos.at(i)), K(expected_ls_id));
      break;
    } else if (OB_FAIL(tablet_ids.push_back(tablet_to_ls_infos.at(i).get_tablet_id().id()))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(tablet_to_ls_infos.at(i)));
      break;
    }
    printf("table_id: %lu, tablet_id: %lu, ls_id: %d\n", tablet_to_ls_infos.at(i).get_table_id(),
      tablet_to_ls_infos.at(i).get_tablet_id().id(), tablet_to_ls_infos.at(i).get_ls_id().id());
  }
  printf("tablet to ls tablets count: %ld, tablet ids count: %ld, table ids count: %ld\n",
      tablet_to_ls_infos.count(), tablet_ids.count(), table_ids.count());
  lib::ob_sort(tablet_ids.begin(), tablet_ids.end());
  return ret;
}

int TestTransferHandler::get_tablet_ids_and_check_ls_from_session_table(
    ObMySQLProxy &sql_proxy,
    const ObSEArray<uint64_t, PART_LIST_COUNT> &table_ids,
    const int64_t expected_ls_id,
    ObSEArray<uint64_t, PART_LIST_COUNT> &tablet_ids/* out */)
{
  int ret = OB_SUCCESS;
  ObArray<storage::ObSessionTabletInfo> session_tablet_infos;
  if (OB_FAIL(ObTabletToGlobalTmpTableOperator::batch_get(*GCTX.sql_proxy_, g_tenant_id, session_tablet_infos))) {
    LOG_WARN("failed to get session tablet infos", K(ret), K(table_ids), K(session_tablet_infos));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < session_tablet_infos.count(); i++) {
    uint64_t tablet_id = session_tablet_infos.at(i).get_tablet_id().id();
    uint64_t table_id = session_tablet_infos.at(i).get_table_id();
    uint64_t ls_id = session_tablet_infos.at(i).get_ls_id().id();
    if (true || is_contain(table_ids, table_id)) { // get related tablet ids from session table
      if (ls_id != expected_ls_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session tablet info ls id is not expected", K(ret), K(session_tablet_infos.at(i)), K(expected_ls_id));
        break;
      } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(session_tablet_infos.at(i)));
        break;
      }
      printf("table_id: %lu, tablet_id: %lu, ls_id: %d\n", table_id, tablet_id, ls_id);
    }
  }
  printf("session tablets infos count: %ld, tablet ids count: %ld, table ids count: %ld\n",
      session_tablet_infos.count(), tablet_ids.count(), table_ids.count());
  lib::ob_sort(tablet_ids.begin(), tablet_ids.end());
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
  int ret = OB_SUCCESS;
  g_tenant_id = OB_INVALID_TENANT_ID;

  // create oracle tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant("oracle", "4G", "4G", true, 2));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id, "oracle"));
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));

  sql_conn_pool.set_db_param("sys@oracle", "", "SYS");
  ObConnPoolConfigParam param;
  param.sqlclient_wait_timeout_ = 1000;
  param.long_query_timeout_ = 300*1000*1000;
  param.connection_refresh_interval_ = 200*1000;
  param.connection_pool_warn_time_ = 10*1000*1000;
  param.sqlclient_per_observer_conn_limit_ = 1000;
  common::ObAddr db_addr;
  db_addr.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(), get_curr_simple_server().get_mysql_port());
  ret = sql_conn_pool.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy.init(&sql_conn_pool);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  usleep(5 * 1000 * 1000);

  // create table TTT1
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table TTT1(c1 int primary key, c2 blob) partition by hash(c1) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // create table TTT2
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table TTT2(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // create oracle gtt
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create global temporary table gtt_session_table(c1 int, c2 int) on commit preserve rows"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // create index
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index idx_gtt_session_table_c1 on gtt_session_table(c1);"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index idx_gtt_session_table_c2 on gtt_session_table(c2);"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create index idx_gtt_session_table_mix on gtt_session_table(c1, c2);"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into gtt_session_table values(1, 2),(3, 4),(5, 6)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create global temporary table gtt_session_table2(c1 int, c2 int) on commit preserve rows"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into gtt_session_table2 values(1, 2),(3, 4),(5, 6)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from gtt_session_table"));
  ASSERT_EQ(OB_SUCCESS, check_result(sql_proxy, sql, GTT_SESSION_TABLE_ROW_COUNT));

  // get part ids
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from DBA_OBJECTS where OBJECT_NAME='TTT1'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_part_list));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from DBA_OBJECTS where OBJECT_NAME='TTT2'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_part_list));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from DBA_OBJECTS where OBJECT_NAME='GTT_SESSION_TABLE'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_part_list));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select object_id from DBA_OBJECTS where OBJECT_NAME='GTT_SESSION_TABLE2'"));
  ASSERT_EQ(OB_SUCCESS, read_sql(sql_proxy, sql, g_part_list));

  //create other ls by cluster table
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table part_table_with_dup(c1 int) duplicate_scope = 'CLUSTER' partition by hash(c1) partitions 4"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _enable_balance_kill_transaction = true tenant = 'oracle';"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _balance_wait_killing_transaction_end_threshold = '10s' tenant = 'oracle';"));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set _enable_balance_kill_transaction = true tenant = 'oracle';"));
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

  ASSERT_EQ(PART_LIST_COUNT, g_part_list.count());
  ObSEArray<ObTableID, PART_LIST_COUNT> table_ids;
  for (int64_t i = 0; i < g_part_list.count(); i++) {
    table_ids.push_back(ObTableID(g_part_list.at(i).table_id()));
  }

  // get original tablet to ls infos
  ObSEArray<uint64_t, PART_LIST_COUNT> tablet_ids;
  ObSEArray<uint64_t, PART_LIST_COUNT> tablet_ids_from_session_table;
  printf("get tablet ids and check ls before transfer\n");
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_tablet_to_ls(sql_proxy, table_ids, 1001, tablet_ids));
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_session_table(sql_proxy, table_ids, 1001, tablet_ids_from_session_table));


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

  // get new tablet infos
  ObSEArray<uint64_t, PART_LIST_COUNT> new_tablet_ids;
  ObSEArray<uint64_t, PART_LIST_COUNT> new_tablet_ids_from_session_table;
  printf("get new tablet ids and check ls after transfer\n");
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_tablet_to_ls(sql_proxy, table_ids, 1002, new_tablet_ids));
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_session_table(sql_proxy, table_ids, 1002, new_tablet_ids_from_session_table));

  ASSERT_EQ(tablet_ids.count(), new_tablet_ids.count());
  for (int64_t i = 0; i < tablet_ids.count(); i++) {
    ASSERT_EQ(tablet_ids.at(i), new_tablet_ids.at(i));
  }
  ASSERT_EQ(tablet_ids_from_session_table.count(), new_tablet_ids_from_session_table.count());
  for (int64_t i = 0; i < tablet_ids_from_session_table.count(); i++) {
    ASSERT_EQ(tablet_ids_from_session_table.at(i), new_tablet_ids_from_session_table.at(i));
  }

  // check result
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from gtt_session_table"));
  ASSERT_EQ(OB_SUCCESS, check_result(sql_proxy, sql, GTT_SESSION_TABLE_ROW_COUNT));
}

TEST_F(TestTransferHandler, test_transfer_1002_to_1001)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(PART_LIST_COUNT, g_part_list.count());
  ObSEArray<ObTableID, PART_LIST_COUNT> table_ids;
  for (int64_t i = 0; i < g_part_list.count(); i++) {
    table_ids.push_back(ObTableID(g_part_list.at(i).table_id()));
  }

  // get original tablet to ls infos
  ObSEArray<uint64_t, PART_LIST_COUNT> tablet_ids;
  ObSEArray<uint64_t, PART_LIST_COUNT> tablet_ids_from_session_table;
  printf("get tablet ids and check ls before transfer\n");
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_tablet_to_ls(sql_proxy, table_ids, 1002, tablet_ids));
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_session_table(sql_proxy, table_ids, 1002, tablet_ids_from_session_table));


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
      g_part_list, ObBalanceTaskID(124), transfer_task));
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

  // get new tablet infos
  ObSEArray<uint64_t, PART_LIST_COUNT> new_tablet_ids;
  ObSEArray<uint64_t, PART_LIST_COUNT> new_tablet_ids_from_session_table;
  printf("get new tablet ids and check ls after transfer\n");
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_tablet_to_ls(sql_proxy, table_ids, 1001, new_tablet_ids));
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_session_table(sql_proxy, table_ids, 1001, new_tablet_ids_from_session_table));

  ASSERT_EQ(tablet_ids.count(), new_tablet_ids.count());
  for (int64_t i = 0; i < tablet_ids.count(); i++) {
    ASSERT_EQ(tablet_ids.at(i), new_tablet_ids.at(i));
  }

  ASSERT_EQ(tablet_ids_from_session_table.count(), new_tablet_ids_from_session_table.count());
  for (int64_t i = 0; i < tablet_ids_from_session_table.count(); i++) {
    ASSERT_EQ(tablet_ids_from_session_table.at(i), new_tablet_ids_from_session_table.at(i));
  }

  // check result
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from gtt_session_table"));
  ASSERT_EQ(OB_SUCCESS, check_result(sql_proxy, sql, GTT_SESSION_TABLE_ROW_COUNT));
}

TEST_F(TestTransferHandler, test_transfer_with_cancel_retry)
{
  int ret = OB_SUCCESS;
  const int64_t src_ls_id = 1001;
  const int64_t dest_ls_id = 1002;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // stuck storage transfer
  INNER_EXE_SQL(OB_SYS_TENANT_ID, "alter system set debug_sync_timeout = '1000s'");
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();
  INNER_EXE_SQL(OB_SYS_TENANT_ID, "set ob_global_debug_sync = 'BEFORE_TRANSFER_LOCK_TABLE_AND_PART wait_for signal_target'");

  ASSERT_EQ(PART_LIST_COUNT, g_part_list.count());
  ObSEArray<ObTableID, PART_LIST_COUNT> table_ids;
  for (int64_t i = 0; i < g_part_list.count(); i++) {
    table_ids.push_back(ObTableID(g_part_list.at(i).table_id()));
  }

  // get original tablet to ls infos
  ObSEArray<uint64_t, PART_LIST_COUNT> tablet_ids;
  ObSEArray<uint64_t, PART_LIST_COUNT> tablet_ids_from_session_table;
  printf("get tablet ids and check ls before transfer\n");
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_tablet_to_ls(sql_proxy, table_ids, src_ls_id, tablet_ids));
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_session_table(sql_proxy, table_ids, src_ls_id, tablet_ids_from_session_table));


  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  // generate transfer task
  ObTransferTaskID task_id;
  ObTransferTask transfer_task;
  ObMySQLTransaction trans;
  ObLSID src_ls(src_ls_id);
  ObLSID dest_ls(dest_ls_id);
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, src_ls, dest_ls,
      g_part_list, ObBalanceTaskID(125), transfer_task));
  task_id = transfer_task.get_task_id();
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  usleep(1000000);
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->try_cancel_transfer_task(task_id));
  INNER_EXE_SQL(OB_SYS_TENANT_ID, "set ob_global_debug_sync = 'BEFORE_TRANSFER_LOCK_TABLE_AND_PART clear'");
  INNER_EXE_SQL(OB_SYS_TENANT_ID, "set ob_global_debug_sync = 'now signal signal_target'");

  //check observer transfer
  ObTransferStatus expected_status(ObTransferStatus::CANCELED);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(task_id, expected_status, true/*is_from_his*/, task));
  LOG_INFO("generate transfer task", K(task));
  ASSERT_EQ(OB_CANCELED, task.result_);
  //wait 1001 ls transfer out tablet deleted gc.
  ASSERT_EQ(OB_SUCCESS, wait_transfer_out_deleted_tablet_gc(task));

  // get new tablet infos
  ObSEArray<uint64_t, PART_LIST_COUNT> new_tablet_ids;
  ObSEArray<uint64_t, PART_LIST_COUNT> new_tablet_ids_from_session_table;
  printf("get new tablet ids and check ls after transfer\n");
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_tablet_to_ls(sql_proxy, table_ids, src_ls_id, new_tablet_ids));
  ASSERT_EQ(OB_SUCCESS, get_tablet_ids_and_check_ls_from_session_table(sql_proxy, table_ids, src_ls_id, new_tablet_ids_from_session_table));

  ASSERT_EQ(tablet_ids.count(), new_tablet_ids.count());
  for (int64_t i = 0; i < tablet_ids.count(); i++) {
    ASSERT_EQ(tablet_ids.at(i), new_tablet_ids.at(i));
  }

  ASSERT_EQ(tablet_ids_from_session_table.count(), new_tablet_ids_from_session_table.count());
  for (int64_t i = 0; i < tablet_ids_from_session_table.count(); i++) {
    ASSERT_EQ(tablet_ids_from_session_table.at(i), new_tablet_ids_from_session_table.at(i));
  }

  // check result
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from gtt_session_table"));
  ASSERT_EQ(OB_SUCCESS, check_result(sql_proxy, sql, GTT_SESSION_TABLE_ROW_COUNT));
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
