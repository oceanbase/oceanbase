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

#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"

#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_direct_load_table_guard.h"

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_tablet_memtable_mit";
static const char *BORN_CASE_NAME = "TestTabletMemtable";
static const char *RESTART_CASE_NAME = "TestTabletMemtableRestart";

ObSimpleServerRestartHelper *helper_ptr = nullptr;


namespace oceanbase {

using namespace observer;
using namespace memtable;
using namespace storage;
using namespace share;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};
TestRunCtx RunCtx;
ObLSID      LS_ID;
ObTabletID  TABLET_ID;
bool FLUSH_DISABLED;

namespace storage {

int ObDDLKV::flush(ObLSID ls_id)
{
  if (FLUSH_DISABLED) {
    return 0;
  }

  int ret = OB_SUCCESS;
  if (!ready_for_flush()) {
    return OB_ERR_UNEXPECTED;
  } else {
    ObTabletMemtableMgr *mgr = get_memtable_mgr();
    ObTableHandleV2 handle;
    EXPECT_EQ(OB_SUCCESS, mgr->get_first_frozen_memtable(handle));
    if (handle.get_table() == this) {
      mgr->release_head_memtable_(this);
      STORAGE_LOG(INFO, "flush and release one ddl kv ddl_kv", KP(this));
    }
  }
  return ret;
}

}

namespace unittest {


class TestTabletMemtable : public ObSimpleClusterTestBase {
public:
  TestTabletMemtable() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  ~TestTabletMemtable() {}

  void basic_test();
  void create_test_table(const char* table_name);

private:
  common::ObArenaAllocator allocator_; // for medium info
};

#define EXE_SQL(sql_str)                      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                              \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define WRITE_SQL_BY_CONN(conn, sql_str)      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)              \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define READ_SQL_FMT_BY_CONN(conn, res, ...)          \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res));

#define DEF_VAL_FOR_SQL      \
  int ret = OB_SUCCESS;      \
  ObSqlString sql;           \
  int64_t affected_rows = 0; \
  sqlclient::ObISQLConnection *connection = nullptr;

void TestTabletMemtable::create_test_table(const char* table_name)
{
  DEF_VAL_FOR_SQL
  ObString create_sql;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  WRITE_SQL_FMT_BY_CONN(connection, "create table if not exists %s (c1 int, c2 int)", table_name);

  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    int64_t int_tablet_id = 0;
    int64_t int_ls_id = 0;
    int64_t retry_times = 10;

    READ_SQL_FMT_BY_CONN(connection,
                         res,
                         "SELECT tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME = \'%s\'",
                         table_name);
    common::sqlclient::ObMySQLResult *result = res.mysql_result();
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", int_tablet_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", int_ls_id));
    TABLET_ID = int_tablet_id;
    LS_ID = int_ls_id;
    fprintf(stdout, "get table info finish : tablet_id = %ld, ls_id = %ld\n", TABLET_ID.id(), LS_ID.id());
  }
}

void TestTabletMemtable::basic_test() {
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));

  create_test_table("test_tablet_memtable");

  ObLSService *ls_svr = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ASSERT_EQ(1001, LS_ID.id());
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObLS *ls = nullptr;
  ObLSIterator *ls_iter = nullptr;
  ASSERT_NE(nullptr, ls = ls_handle.get_ls());

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(TABLET_ID, tablet_handle));
  ASSERT_NE(nullptr, tablet = tablet_handle.get_obj());

  // *********** CREATE DATA METMABLE ************
  ASSERT_EQ(OB_SUCCESS, tablet->create_memtable(1, SCN::min_scn(), false /*for_direct_load*/, false /*for_replay*/));
  ObProtectedMemtableMgrHandle *protected_handle = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet->get_protected_memtable_mgr_handle(protected_handle));
  ASSERT_NE(nullptr, protected_handle);
  ASSERT_EQ(true, protected_handle->has_memtable());

  // *********** CHECK METMABLE TYPE ************
  ObTableHandleV2 memtable_handle;
  ObITabletMemtable *memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_active_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, memtable_handle.get_tablet_memtable(memtable));
  ASSERT_EQ(ObITable::TableType::DATA_MEMTABLE, memtable->get_table_type());

  // *********** DO TABLET FREEZE ************
  ObFreezer *freezer = nullptr;
  ASSERT_NE(nullptr, freezer = ls->get_freezer());
  ASSERT_EQ(OB_SUCCESS, ls->tablet_freeze(TABLET_ID, false /* need_rewrite_meta */, true /* is_sync */, 0));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, protected_handle->get_active_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_boundary_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, memtable_handle.get_tablet_memtable(memtable));
  STORAGE_LOG(INFO, "finish freeze data memtable", KPC(memtable));

  // *********** CREATE DIRECT LOAD MEMTABLE ************
  ASSERT_EQ(OB_SUCCESS, tablet->create_memtable(1, SCN::min_scn(), true /*for_direct_load*/, false /*for_replay*/));
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_active_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, memtable_handle.get_tablet_memtable(memtable));
  ASSERT_EQ(ObITable::TableType::DIRECT_LOAD_MEMTABLE, memtable->get_table_type());
  STORAGE_LOG(INFO, "finish create direct load memtable", KPC(memtable));

  // *********** GET DIRECT LOAD MEMTABLE FOR WRITE ************
  memtable->inc_write_ref();

  // *********** CONCURRENT TABLET FREEZE ************
  int64_t freeze_start_time = ObClockGenerator::getClock();
  ASSERT_EQ(OB_SUCCESS, ls->tablet_freeze(TABLET_ID, false /*need_rewrite_meta*/, false /*is_sync*/, INT64_MAX));

  sleep(2);
  ASSERT_EQ(TabletMemtableFreezeState::ACTIVE, memtable->get_freeze_state());
  STORAGE_LOG(INFO, "waiting write finish", KPC(memtable));

  // *********** DIRECT LOAD MEMTABLE WRITE FINISH ************
  memtable->dec_write_ref();
  STORAGE_LOG(INFO, "write_ref_cnt should be zero", KPC(memtable));
  sleep(1);

  // *********** CHECK FREEZE RESULT ************
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, protected_handle->get_active_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_boundary_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, memtable_handle.get_tablet_memtable(memtable));
  STORAGE_LOG(INFO, "get boundary memtable", KPC(memtable));

  ASSERT_EQ(ObITable::TableType::DIRECT_LOAD_MEMTABLE, memtable->get_table_type());
  ASSERT_NE(TabletMemtableFreezeState::ACTIVE, memtable->get_freeze_state());
  ASSERT_EQ(true, memtable->is_in_prepare_list_of_data_checkpoint());

  // *********** CREATE ANOTHER DIRECT LOAD MEMTABLE ************
  ASSERT_EQ(OB_SUCCESS, tablet->create_memtable(1, SCN::min_scn(), true /*for_direct_load*/, false /*for_replay*/));
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_active_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, memtable_handle.get_tablet_memtable(memtable));
  STORAGE_LOG(INFO, "create a new direct load memtable", KPC(memtable));

  // *********** CONSTURCT A DIRECT LOAD TABLE GUARD ************
  SCN fake_ddl_redo_scn = SCN::plus(SCN::min_scn(), 10);
  ObDirectLoadTableGuard direct_load_guard(*tablet, fake_ddl_redo_scn, false);

  // *********** CHECK DIRECT LOAD TABLE GUARD UASBLE ************
  ObDDLKV *memtable_for_direct_load = nullptr;
  ASSERT_EQ(OB_SUCCESS, direct_load_guard.prepare_memtable(memtable_for_direct_load));
  ASSERT_NE(nullptr, memtable_for_direct_load);
  ASSERT_EQ(OB_SUCCESS, memtable_for_direct_load->set_rec_scn(fake_ddl_redo_scn));
  ASSERT_EQ(memtable, memtable_for_direct_load);
  ASSERT_EQ(1, memtable_for_direct_load->get_write_ref());
  direct_load_guard.reset();
  ASSERT_EQ(0, memtable_for_direct_load->get_write_ref());

  // *********** DO LOGSTREAM FREEZE ************
  ASSERT_EQ(OB_SUCCESS, ls->logstream_freeze(checkpoint::INVALID_TRACE_ID, true /* is_sync */));
  STORAGE_LOG(INFO, "finish logstream freeze");

  // *********** CHECK LOGSTREAM FREEZE RESULT ************
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, protected_handle->get_active_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_boundary_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, memtable_handle.get_tablet_memtable(memtable));
  ASSERT_EQ(ObITable::TableType::DIRECT_LOAD_MEMTABLE, memtable->get_table_type());

  ASSERT_NE(TabletMemtableFreezeState::ACTIVE, memtable->get_freeze_state());
  ASSERT_EQ(true, memtable->is_in_prepare_list_of_data_checkpoint());
  STORAGE_LOG(INFO, "finish check logstream freeze result", KPC(memtable));

  // *********** WAIT ALL MEMTABLES FLUSH ************
  FLUSH_DISABLED = false;
  int64_t retry_times = 0;
  while (retry_times <= 10) {
    sleep(1);
    if (protected_handle->has_memtable()) {
      ObSEArray<ObTableHandleV2, 2> handles;
      ASSERT_EQ(OB_SUCCESS, protected_handle->get_all_memtables(handles));
      STORAGE_LOG(INFO, "wait all memtable flushed", K(retry_times));
      for (int i = 0; i < handles.count(); i++) {
        ObITabletMemtable *memtable = static_cast<ObITabletMemtable*>(handles.at(i).get_table());
        STORAGE_LOG(INFO, "PRINT Table", K(memtable->key_), K(memtable->get_freeze_state()));
      }
      retry_times++;
    } else {
      break;
    }
  }

  ASSERT_FALSE(protected_handle->has_memtable());
  fprintf(stdout, "finish basic test\n");
}

TEST_F(TestTabletMemtable, observer_start) { SERVER_LOG(INFO, "observer_start succ"); }

TEST_F(TestTabletMemtable, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(TestTabletMemtable, init_config)
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *sys_conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);
  sleep(2);
}

TEST_F(TestTabletMemtable, create_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  OB_LOG(INFO, "create_table start");
  ObSqlString sql;
  int64_t affected_rows = 0;
  EXE_SQL("create table if not exists test_tablet_memtable (c1 int, c2 int, primary key(c1))");
  OB_LOG(INFO, "create_table succ");
}

TEST_F(TestTabletMemtable, basic_test)
{
  FLUSH_DISABLED = true;
  basic_test();
}

class TestTabletMemtableRestart : public ObSimpleClusterTestBase
{
public:
  TestTabletMemtableRestart() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
private:

};

TEST_F(TestTabletMemtableRestart, observer_restart)
{
  SERVER_LOG(INFO, "observer restart succ");
}

}  // namespace unittest
}  // namespace oceanbase


int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  int concurrency = 1;
  char *log_level = (char *)"DEBUG";
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
      case 't':
        time_sec = atoi(optarg);
        break;
      case 'l':
        log_level = optarg;
        oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
        break;
      case 'c':
        concurrency = atoi(optarg);
        break;
      default:
        break;
    }
  }
  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);

  int ret = 0;
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  helper_ptr = &restart_helper;
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();

  return ret;
}