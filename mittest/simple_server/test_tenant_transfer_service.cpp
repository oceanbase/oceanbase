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
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
using namespace unittest;
namespace rootserver
{
using namespace share::schema;
using namespace common;
using namespace share;
using namespace transaction::tablelock;

static uint64_t g_tenant_id;
static ObTransferPartList g_part_list;
static ObTransferPartList g_batch_part_list;

#define INNER_EXE_SQL(...)                                          \
  ASSERT_EQ(OB_SUCCESS, sql.assign(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL(...)                                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define GEN_PART_LIST(proxy, part_list, ...)                        \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, read_sql(proxy, sql, part_list));

class TestTenantTransferService : public unittest::ObSimpleClusterTestBase
{
public:
  TestTenantTransferService() : unittest::ObSimpleClusterTestBase("test_tenant_transfer_service") {}
  int read_sql(ObMySQLProxy &sql_proxy, const ObSqlString &sql, ObTransferPartList &part_list);
  int gen_mock_data(const ObTransferTaskID task_id, const ObTransferStatus &status, ObTransferTask &task);
  void create_hidden_table();
};

int TestTenantTransferService::read_sql(
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
      uint64_t part_id = OB_INVALID_ID;
      int64_t part_count = 0;
      if (OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "object_id", table_id, uint64_t);
      }
      while(OB_SUCC(ret)) {
        part_id = OB_INVALID_ID;
        ObTransferPartInfo part_info;
        if (OB_SUCC(res.next())) {
          ++part_count;
          EXTRACT_INT_FIELD_MYSQL(res, "object_id", part_id, uint64_t);
          if (OB_FAIL(part_info.init(table_id, part_id))) {
          } else if (OB_FAIL(part_list.push_back(part_info))) {
          }
        }
      }
      if (OB_ITER_END == ret) {
        if (0 == part_count && OB_INVALID_ID != table_id && OB_INVALID_ID == part_id) {
          ObTransferPartInfo part_info(table_id, 0);
          (void)part_list.push_back(part_info);
        }
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to generate data", K(sql));
      }
      LOG_INFO("finish read sql", K(sql), K(part_list), K(table_id), K(part_id));
    }
  }
  return ret;
}

int TestTenantTransferService::gen_mock_data(const ObTransferTaskID task_id, const ObTransferStatus &status, ObTransferTask &task)
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

TEST_F(TestTenantTransferService, prepare_valid_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  // create table
  ObSqlString sql;
  int64_t affected_rows = 0;
  EXE_SQL("create table ttt1(c1 int primary key, c2 blob) partition by hash(c1) partitions 2");
  EXE_SQL("create index ttt1_idx on ttt1(c1) local");
  EXE_SQL("create index ttt1_global ON ttt1(c1) global partition by hash(c1) partitions 2");
  GEN_PART_LIST(sql_proxy, g_part_list, "select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt1'");
  GEN_PART_LIST(sql_proxy, g_part_list, "select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt1_global'");

  // create 101 partitions
  EXE_SQL("create table ttt2(c1 int primary key) partition by hash(c1) partitions 101");
  GEN_PART_LIST(sql_proxy, g_batch_part_list, "select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='ttt2'");
}

TEST_F(TestTenantTransferService, test_service)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // stuck storage transfer
  INNER_EXE_SQL("alter system set debug_sync_timeout = '1000s'");
  usleep(100000); // wait for debug_sync_timeout to take effect
  sql.reset();
  INNER_EXE_SQL("set ob_global_debug_sync = 'AFTER_TRANSFER_PROCESS_INIT_TASK_AND_BEFORE_NOTIFY_STORAGE wait_for signal execute 10000'");

  ASSERT_EQ(4, g_part_list.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  // generate transfer task
  ObTransferTaskID task_id;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, ObLSID(1001), ObLSID(1),
      g_part_list, ObBalanceTaskID(123), task_id));
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(inner_sql_proxy, g_tenant_id, task_id, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_status().is_init_status() || task.get_status().is_start_status());
  ASSERT_TRUE(task.get_part_list().count() == g_part_list.count());
  ARRAY_FOREACH(g_part_list, idx) {
    ASSERT_TRUE(is_contain(task.get_part_list(), g_part_list.at(idx)));
  }
  LOG_INFO("generate transfer task", K(task));

  // generate tablet_list
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  sleep(10);
  task.reset();
  ObArenaAllocator allocator;
  ObString tablet_list_str;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(inner_sql_proxy, g_tenant_id, task_id, false, task, 0/*group_id*/));
  LOG_INFO("generate tablet list", K(task));
  ASSERT_TRUE(task.is_valid());
  const ObTransferTabletList &tablet_list = task.get_tablet_list();
  ASSERT_TRUE(10 == tablet_list.count()); // 2 * (primary + local_index + lob_meta + lob_piece) + 2 * global_index
  ASSERT_EQ(OB_SUCCESS, tablet_list.to_display_str(allocator, tablet_list_str));
  LOG_INFO("tablet list string", K(tablet_list_str));
  ASSERT_TRUE(0 == tablet_list_str.case_compare("200001:0,200002:0,1152921504606846977:0,1152921504606846978:0,"
      "1152921504606846979:0,1152921504606846980:0,1152921504606846981:0,1152921504606846982:0,1152921504606846983:0,1152921504606846984:0"));

  // try cancel transfer task
  ObTransferTask init_task;
  ObTransferTask aborted_task;
  ObTransferTaskID init_task_id(222);
  ObTransferTaskID aborted_task_id(333);
  ASSERT_EQ(OB_SUCCESS, gen_mock_data(init_task_id, ObTransferStatus(ObTransferStatus::INIT), init_task));
  ASSERT_EQ(OB_SUCCESS, gen_mock_data(aborted_task_id, ObTransferStatus(ObTransferStatus::ABORTED), aborted_task));
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(inner_sql_proxy, g_tenant_id, init_task));
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(inner_sql_proxy, g_tenant_id, aborted_task));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->try_cancel_transfer_task(ObTransferTaskID(555))); // task which does not exist will be canceled successfully
  ASSERT_EQ(OB_OP_NOT_ALLOW, tenant_transfer->try_cancel_transfer_task(aborted_task_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->try_cancel_transfer_task(init_task_id));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get(inner_sql_proxy, g_tenant_id, init_task_id, false, init_task, 0/*group_id*/));

  // try clear transfer task
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(inner_sql_proxy, g_tenant_id, task_id, false, task, 0/*group_id*/));
  ASSERT_EQ(OB_SUCCESS, ret);
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("update oceanbase.__all_transfer_task set status = 'COMPLETED' where task_id = %ld", task.get_task_id().id()));
  ASSERT_EQ(OB_SUCCESS, inner_sql_proxy.write(g_tenant_id, sql.ptr(), affected_rows));

  ObTransferPartList all_part_list;
  ObTransferPartList finished_part_list;
  ASSERT_EQ(OB_NEED_RETRY, tenant_transfer->try_clear_transfer_task(aborted_task_id, all_part_list, finished_part_list));
  ASSERT_TRUE(all_part_list.empty() && finished_part_list.empty());
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->try_clear_transfer_task(task_id, all_part_list, finished_part_list));
  ObString all_part_list_str;
  ASSERT_TRUE(all_part_list.count() == g_part_list.count());
  ARRAY_FOREACH(g_part_list, idx) {
    ASSERT_TRUE(is_contain(all_part_list, g_part_list.at(idx)));
  }
  ASSERT_TRUE(finished_part_list.count() == all_part_list.count());
  ARRAY_FOREACH(all_part_list, idx) {
    ASSERT_TRUE(is_contain(finished_part_list, all_part_list.at(idx)));
  }
  ObString finished_part_list_str;
  ASSERT_EQ(OB_SUCCESS, finished_part_list.to_display_str(allocator, finished_part_list_str));
  LOG_WARN("finished_part_list", K(finished_part_list_str));
  ASSERT_TRUE(0 == finished_part_list_str.case_compare("500002:500003,500002:500004,500016:500014,500016:500015"));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get(inner_sql_proxy, g_tenant_id, task_id, false, task, 0/*group_id*/));
  create_time = OB_INVALID_TIMESTAMP;
  finish_time = OB_INVALID_TIMESTAMP;
  ObTransferTask history_task;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_history_task(inner_sql_proxy, g_tenant_id, task_id, history_task, create_time, finish_time));
  ASSERT_TRUE(history_task.get_status().is_completed_status());
}

TEST_F(TestTenantTransferService, test_batch_part_list)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(101, g_batch_part_list.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  // generate transfer task in batch
  ObTransferTaskID batch_task_id;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, ObLSID(1001), ObLSID(1001),
      g_batch_part_list, ObBalanceTaskID(124), batch_task_id));
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(inner_sql_proxy, g_tenant_id, batch_task_id, false, task, 0/*group_id*/));
  ASSERT_TRUE(ObTenantTransferService::PART_COUNT_IN_A_TRANSFER == task.get_part_list().count());
  ARRAY_FOREACH(task.get_part_list(), idx) {
    ASSERT_TRUE(is_contain(g_batch_part_list, task.get_part_list().at(idx)));
  }
}

TEST_F(TestTenantTransferService, test_empty_list)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(101, g_batch_part_list.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));
  // errorsim
  ObSqlString sql;
  int64_t affected_rows = 0;
  INNER_EXE_SQL("alter system set_tp tp_name = EN_TENANT_TRANSFER_ALL_LIST_EMPTY, error_code = 4016, frequency = 1");
  // transfer
  ObTransferTaskID task_id;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, ObLSID(1001), ObLSID(1),
      g_part_list, ObBalanceTaskID(124), task_id));
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  ASSERT_EQ(OB_ERR_UNEXPECTED, tenant_transfer->process_init_task_(task_id));
}

void TestTenantTransferService::create_hidden_table()
{
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  int64_t affected_rows = 0;

  INNER_EXE_SQL("set ob_global_debug_sync = 'reset';");
  INNER_EXE_SQL("alter system set debug_sync_timeout = '1000s';");
  usleep(100000);
  INNER_EXE_SQL("set ob_global_debug_sync = 'TABLE_REDEFINITION_REPLICA_BUILD wait_for signal execute 10000'");
  usleep(100000);
  EXE_SQL("create table t_hidden_1(c1 int); ");
  EXE_SQL("alter table t_hidden_1 modify c1 char(10);");
  LOG_INFO("finish create hidden table", K(sql), K(affected_rows));
}

TEST_F(TestTenantTransferService, test_offline_ddl_hidden_table)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));
  // errorsim
  ObSqlString sql;
  int64_t affected_rows = 0;
  std::thread create_hidden_table_thread([this]() { create_hidden_table(); });
  create_hidden_table_thread.detach();
  sleep(3);
  sql.reset();
  ObTransferPartList primary_table_part_list;
  GEN_PART_LIST(inner_sql_proxy, primary_table_part_list, "select object_id from oceanbase.CDB_OBJECTS where CON_ID = %lu and OBJECT_NAME = 't_hidden_1'", g_tenant_id);
  LOG_INFO("read hidden table primary table", K(primary_table_part_list));
  ASSERT_TRUE(1 == primary_table_part_list.count());
  ASSERT_TRUE(500119 == primary_table_part_list.at(0).table_id());
  ObTransferPartList hidden_part_list;
  GEN_PART_LIST(inner_sql_proxy, hidden_part_list, "select association_table_id as object_id from oceanbase.__all_virtual_table where tenant_id = %lu and table_name = 't_hidden_1'", g_tenant_id);
  LOG_INFO("read hidden table", K(hidden_part_list));
  ASSERT_TRUE(500120 == hidden_part_list.at(0).table_id());

  ObMySQLTransaction trans;
  ObTransferPartList offline_ddl_table_part_list;
  ObTransferPartList not_exist_part_list;
  ObTransferPartList lock_conflict_part_list;
  ObDisplayTabletList table_lock_tablet_list;
  ObArray<ObTabletID> tablet_ids;
  ObTableLockOwnerID lock_owner_id;
  ASSERT_EQ(OB_SUCCESS, common::append(offline_ddl_table_part_list, primary_table_part_list));
  ASSERT_EQ(OB_SUCCESS, common::append(offline_ddl_table_part_list, hidden_part_list));
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->lock_table_and_part_(trans, ObLSID(1001), offline_ddl_table_part_list,
      not_exist_part_list, lock_conflict_part_list, table_lock_tablet_list, tablet_ids, lock_owner_id));
  LOG_INFO("lock_table_and_part", K(offline_ddl_table_part_list), K(not_exist_part_list), K(lock_conflict_part_list), K(table_lock_tablet_list), K(tablet_ids), K(lock_owner_id));
  ASSERT_TRUE(not_exist_part_list.empty() && table_lock_tablet_list.empty() && tablet_ids.empty());
  ASSERT_TRUE(2 == lock_conflict_part_list.count());
  ObArenaAllocator allocator;
  ObString lock_conflict_part_list_str;
  ASSERT_EQ(OB_SUCCESS, lock_conflict_part_list.to_display_str(allocator, lock_conflict_part_list_str));
  LOG_INFO("lock conflict hidden table", K(lock_conflict_part_list_str));
  ASSERT_TRUE(0 == lock_conflict_part_list_str.compare("500119:0,500120:0"));

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
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
