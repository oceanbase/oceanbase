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
#include <stdlib.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "logservice/ob_garbage_collector.h"
#include "logservice/palf/palf_env_impl.h"
#include "storage/access/ob_rows_info.h"
#include "storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_ls_recover";
static const char *BORN_CASE_NAME = "ObLSBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObLSAfterRestartTest";

namespace oceanbase
{
using namespace oceanbase::share;
namespace palf
{
// should not check clog disk usage
bool PalfEnvImpl::check_can_create_palf_handle_impl_() const
{
  bool bool_ret = true;
  return bool_ret;
}
}
namespace logservice
{

// should not gc a ls create by ob_ls_service.cpp
int ObGarbageCollector::gc_check_ls_status_(storage::ObLS &ls,
                                            ObGCCandidateArray &gc_candidates)
{
  int ret = OB_SUCCESS;
  GCCandidate candidate;
  candidate.ls_id_ = ls.get_ls_id();
  candidate.ls_status_ = LSStatus::LS_NORMAL;
  candidate.gc_reason_ = GCReason::INVALID_GC_REASON;
  if (OB_FAIL(gc_candidates.push_back(candidate))) {
    LOG_WARN("gc_candidates push_back failed", K(ret), K(candidate));
  }
  return ret;
}

}

namespace unittest
{
using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::storage::checkpoint;
using namespace oceanbase::share;

static inline void prepare_palf_base_info(const obrpc::ObCreateLSArg &arg,
                                          palf::PalfBaseInfo &palf_base_info)
{
  palf_base_info.generate_by_default();
  palf_base_info.prev_log_info_.scn_ = arg.get_create_scn();
  if (arg.is_create_ls_with_palf()) {
    palf_base_info = arg.get_palf_base_info();
  }
}


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};

TestRunCtx RunCtx;

class ObLSBeforeRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLSBeforeRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void prepare_uncommitted_data();
  void minor_freeze();
  void wait_minor_finish();
  void minor_freeze_tx_ctx_table();
};

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define WRITE_SQL_BY_CONN(conn, sql_str)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

void ObLSBeforeRestartTest::prepare_uncommitted_data()
{
  LOG_INFO("insert data start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t i = 0;
  int64_t affected_rows = 0;
  int rollback_cnt = 0;
  ObSqlString sql;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");

  // start transaction
  // create a data in tx ctx table.
  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_ls_recover_t values(1, 1);");
  LOG_INFO("insert data finish");
}

void ObLSBeforeRestartTest::minor_freeze()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("alter system minor freeze tenant tt1;");
  LOG_INFO("minor freeze done");
}

void ObLSBeforeRestartTest::minor_freeze_tx_ctx_table()
{
  LOG_INFO("ObLSBeforeRestartTest::minor_freeze_tx_ctx_table begin");
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tguard;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSIterator *iter = NULL;
  ObLS *ls = nullptr;

  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_svr);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls_iter(guard, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, iter = guard.get_ptr());

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter->get_next(ls))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), KP(iter));
      }
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else {
      ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
      ASSERT_NE(nullptr, checkpoint_executor);
      ObTxCtxMemtable *tx_ctx_memtable
        = dynamic_cast<ObTxCtxMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
      ASSERT_EQ(OB_SUCCESS, tx_ctx_memtable->flush(share::SCN::max_scn(), 0));
      int retry_time = 0;
      while (tx_ctx_memtable->is_frozen_memtable()) {
        usleep(1000 * 1000);
        retry_time++;
      }
      ASSERT_EQ(share::SCN::max_scn(), tx_ctx_memtable->get_rec_scn());
    }
  }
  LOG_INFO("ObLSBeforeRestartTest::minor_freeze_tx_ctx_table done");
}

void ObLSBeforeRestartTest::wait_minor_finish()
{
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  while (row_cnt == 0) {
    ASSERT_EQ(OB_SUCCESS, sql.assign("select count(*) as row_cnt from oceanbase.__all_virtual_tablet_compaction_history where tenant_id=1002 and ls_id=1001 and (type='MINI_MERGE');"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000); // 100_ms
  }
  LOG_INFO("minor finished", K(row_cnt));
}

TEST_F(ObLSBeforeRestartTest, dump_uncommitted_tx)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  // create tenant
  LOG_INFO("create_tenant start");
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  // create table
  LOG_INFO("create_table start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  EXE_SQL("create table test_ls_recover_t (c1 int, c2 int, primary key(c1))");
  LOG_INFO("create_table success");

  // begin and insert
  prepare_uncommitted_data();

  // minor tx ctx table
  minor_freeze_tx_ctx_table();

  // do minor freeze
  minor_freeze();

  // wait minor finish
  wait_minor_finish();
}

TEST_F(ObLSBeforeRestartTest, create_unfinished_ls_without_disk)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSID id_100(100);
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  LOG_INFO("ObLSBeforeRestartTest::create_unfinished_ls_without 1", K(tenant_id));
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_100, arg));
  LOG_INFO("create_ls", K(arg), K(id_100));
  ASSERT_EQ(OB_SUCCESS, ls_svr->inner_create_ls_(arg.get_ls_id(),
                                                 migration_status,
                                                 ObLSRestoreStatus(ObLSRestoreStatus::NONE),
                                                 arg.get_create_scn(),
                                                 ls));
  ObLSLockGuard lock_ls(ls);
  const ObLSMeta &ls_meta = ls->get_ls_meta();
  ASSERT_EQ(OB_SUCCESS, ls_svr->add_ls_to_map_(ls));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_prepare_create_ls_slog_(ls_meta));
}

TEST_F(ObLSBeforeRestartTest, create_unfinished_ls_with_disk)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSID id_101(101);
  palf::PalfBaseInfo palf_base_info;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  LOG_INFO("ObLSBeforeRestartTest::create_unfinished_ls_with_disk 1");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_101, arg));
  LOG_INFO("create_ls", K(arg), K(id_101));
  ASSERT_EQ(OB_SUCCESS, ls_svr->inner_create_ls_(arg.get_ls_id(),
                                                 migration_status,
                                                 ObLSRestoreStatus(ObLSRestoreStatus::NONE),
                                                 arg.get_create_scn(),
                                                 ls));
  const bool unused_allow_log_sync = true;
  prepare_palf_base_info(arg, palf_base_info);
  ObLSLockGuard lock_ls(ls);
  const ObLSMeta &ls_meta = ls->get_ls_meta();
  ASSERT_EQ(OB_SUCCESS, ls_svr->add_ls_to_map_(ls));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_prepare_create_ls_slog_(ls_meta));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls(arg.get_tenant_info().get_tenant_role(),
                                      palf_base_info,
                                      arg.get_replica_type(),
                                      unused_allow_log_sync));
}

TEST_F(ObLSBeforeRestartTest, create_unfinished_ls_with_inner_tablet)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSID id_102(102);
  palf::PalfBaseInfo palf_base_info;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  LOG_INFO("ObLSBeforeRestartTest::create_unfinished_ls_with_inner_tablet 1");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_102, arg));
  LOG_INFO("create_ls", K(arg), K(id_102));
  ASSERT_EQ(OB_SUCCESS, ls_svr->inner_create_ls_(arg.get_ls_id(),
                                                 migration_status,
                                                 ObLSRestoreStatus(ObLSRestoreStatus::NONE),
                                                 arg.get_create_scn(),
                                                 ls));
  const bool unused_allow_log_sync = true;
  prepare_palf_base_info(arg, palf_base_info);
  ObLSLockGuard lock_ls(ls);
  const ObLSMeta &ls_meta = ls->get_ls_meta();
  ASSERT_EQ(OB_SUCCESS, ls_svr->add_ls_to_map_(ls));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_prepare_create_ls_slog_(ls_meta));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls(arg.get_tenant_info().get_tenant_role(),
                                      palf_base_info,
                                      arg.get_replica_type(),
                                      unused_allow_log_sync));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls_inner_tablet(arg.get_compat_mode(),
                                                   arg.get_create_scn()));
}

TEST_F(ObLSBeforeRestartTest, create_unfinished_ls_with_commit_slog)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSID id_103(103);
  palf::PalfBaseInfo palf_base_info;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  LOG_INFO("ObLSBeforeRestartTest::create_unfinished_ls_with_inner_tablet 1");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_103, arg));
  LOG_INFO("create_ls", K(arg), K(id_103));
  ASSERT_EQ(OB_SUCCESS, ls_svr->inner_create_ls_(arg.get_ls_id(),
                                                 migration_status,
                                                 ObLSRestoreStatus(ObLSRestoreStatus::NONE),
                                                 arg.get_create_scn(),
                                                 ls));
  const bool unused_allow_log_sync = true;
  prepare_palf_base_info(arg, palf_base_info);
  ObLSLockGuard lock_ls(ls);
  const ObLSMeta &ls_meta = ls->get_ls_meta();
  ASSERT_EQ(OB_SUCCESS, ls_svr->add_ls_to_map_(ls));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_prepare_create_ls_slog_(ls_meta));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls(arg.get_tenant_info().get_tenant_role(),
                                      palf_base_info,
                                      arg.get_replica_type(),
                                      unused_allow_log_sync));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls_inner_tablet(arg.get_compat_mode(),
                                                   arg.get_create_scn()));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_commit_create_ls_slog_(ls->get_ls_id()));
}

// this ls will be offlined state after restart
TEST_F(ObLSBeforeRestartTest, create_restore_ls)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSID id_104(104);
  palf::PalfBaseInfo palf_base_info;
  int64_t create_type = ObLSCreateType::RESTORE;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  LOG_INFO("ObLSBeforeRestartTest::create_unfinished_ls_with_inner_tablet 1");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_104, arg));
  LOG_INFO("create_ls", K(arg), K(id_104));
  ASSERT_EQ(OB_SUCCESS, ls_svr->inner_create_ls_(arg.get_ls_id(),
                                                 migration_status,
                                                 ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_START),
                                                 arg.get_create_scn(),
                                                 ls));
  const bool unused_allow_log_sync = true;
  prepare_palf_base_info(arg, palf_base_info);
  ObLSLockGuard lock_ls(ls);
  const ObLSMeta &ls_meta = ls->get_ls_meta();
  ASSERT_EQ(OB_SUCCESS, ls_svr->add_ls_to_map_(ls));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_prepare_create_ls_slog_(ls_meta));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls(arg.get_tenant_info().get_tenant_role(),
                                      palf_base_info,
                                      arg.get_replica_type(),
                                      unused_allow_log_sync));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls_inner_tablet(arg.get_compat_mode(),
                                                   arg.get_create_scn()));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_commit_create_ls_slog_(ls->get_ls_id()));
  ASSERT_EQ(OB_SUCCESS, ls->finish_create_ls());
  ASSERT_EQ(OB_SUCCESS, ls_svr->post_create_ls_(create_type, ls));

  // check the ls, it should be offlined.
  ASSERT_TRUE(ls->is_offline());
}

// this ls will be offline state after restart
TEST_F(ObLSBeforeRestartTest, create_rebuild_ls)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSID id_105(105);
  palf::PalfBaseInfo palf_base_info;
  int64_t create_type = ObLSCreateType::NORMAL;
  const ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  LOG_INFO("ObLSBeforeRestartTest::create_unfinished_ls_with_inner_tablet 1");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_105, arg));
  LOG_INFO("create_ls", K(arg), K(id_105));
  ASSERT_EQ(OB_SUCCESS, ls_svr->inner_create_ls_(arg.get_ls_id(),
                                                 migration_status,
                                                 ObLSRestoreStatus(ObLSRestoreStatus::NONE),
                                                 arg.get_create_scn(),
                                                 ls));
  const bool unused_allow_log_sync = true;
  prepare_palf_base_info(arg, palf_base_info);
  ObLSLockGuard lock_ls(ls);
  const ObLSMeta &ls_meta = ls->get_ls_meta();
  ASSERT_EQ(OB_SUCCESS, ls_svr->add_ls_to_map_(ls));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_prepare_create_ls_slog_(ls_meta));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls(arg.get_tenant_info().get_tenant_role(),
                                      palf_base_info,
                                      arg.get_replica_type(),
                                      unused_allow_log_sync));
  ASSERT_EQ(OB_SUCCESS, ls->create_ls_inner_tablet(arg.get_compat_mode(),
                                                   arg.get_create_scn()));
  ASSERT_EQ(OB_SUCCESS, ls_svr->write_commit_create_ls_slog_(ls->get_ls_id()));
  ASSERT_EQ(OB_SUCCESS, ls->finish_create_ls());
  ASSERT_EQ(OB_SUCCESS, ls_svr->post_create_ls_(create_type, ls));

  // make it be a rebuild ls.
  ASSERT_EQ(OB_SUCCESS, ls->set_ls_rebuild());
}

class ObLSAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLSAfterRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}

  void insert_existed_table();
};

void ObLSAfterRestartTest::insert_existed_table()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int ret = OB_SUCCESS;

  LOG_INFO("insert existed table start");
  const int MAX_TRY_SELECT_CNT = 20;
  int try_insert_cnt = 0;
  bool insert_succ = false;

  while (++try_insert_cnt <= MAX_TRY_SELECT_CNT) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign("insert into test_ls_recover_t values(2, 2)"));
    if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      ret = OB_SUCCESS;
      ::sleep(1);
    } else {
      ASSERT_GT(affected_rows, 0);
    }
  }
}

TEST_F(ObLSAfterRestartTest, observer_restart)
{
  LOG_INFO("observer restart begin");
  // init sql proxy2 to use tenant tt1
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  LOG_INFO("observer restart succ");
}

TEST_F(ObLSAfterRestartTest, insert_existed_table)
{
  LOG_INFO("insert_existed_table");
  insert_existed_table();
}

TEST_F(ObLSAfterRestartTest, check_unfinished_ls)
{
  LOG_INFO("check_unfinished_ls");
  uint64_t tenant_id = 0;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  // 1. restart success. no need check.
  // 2. check exist ls exist.
  // 3. check not exist ls not exist.
  // 4. check ls status.
  bool exist = false;
  ObLSID id_100(100);
  ObLSID id_101(101);
  ObLSID id_102(102);
  ObLSID id_103(103);
  ObLSID id_104(104); // this is a restore ls
  ObLSID id_105(105); // this is a rebuild ls

  constexpr int NOT_EXIST_NUM = 3;
  constexpr int EXIST_NUM = 3;
  constexpr int OFFLINED_NUM = 2;
  constexpr int NORMAL_NUM = 1;
  ObLSID not_exist_ls[NOT_EXIST_NUM] = {id_100, id_101, id_102};
  ObLSID exist_ls[EXIST_NUM] = {id_103, id_104, id_105};
  ObLSID offlined_ls[OFFLINED_NUM] = {id_104, id_105};
  ObLSID normal_ls[NORMAL_NUM] = {id_103};

  ObLSService* ls_svr = MTL(ObLSService*);
  LOG_INFO("check_unfinished_ls not exist ls");
  for (int i = 0; i < NOT_EXIST_NUM; i++) {
    LOG_INFO("check_unfinished_ls not exist ls", K(not_exist_ls[i]));
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(not_exist_ls[i], exist));
    ASSERT_FALSE(exist);
  }

  LOG_INFO("check_unfinished_ls exist ls");
  for (int i = 0; i < EXIST_NUM; i++) {
    LOG_INFO("check_unfinished_ls exist ls", K(exist_ls[i]));
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(exist_ls[i], exist));
    ASSERT_TRUE(exist);
  }

  LOG_INFO("check exist ls offlined status");
  for (int i = 0; i < OFFLINED_NUM; i++) {
    LOG_INFO("check_unfinished_ls offlined ls", K(offlined_ls[i]));
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(offlined_ls[i], ls_handle, ObLSGetMod::TXSTORAGE_MOD));
    ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, ls);
    LOG_INFO("ls status", KPC(ls));
    ASSERT_TRUE(ls->is_offline());
    ASSERT_TRUE(ls->get_persistent_state().is_ha_state());
  }

  LOG_INFO("check exist ls normal status");
  for (int i = 0; i < NORMAL_NUM; i++) {
    LOG_INFO("check_unfinished_ls normal ls", K(normal_ls[i]));
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(normal_ls[i], ls_handle, ObLSGetMod::TXSTORAGE_MOD));
    ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_FALSE(ls->is_offline());
    ASSERT_TRUE(ls->get_persistent_state().is_normal_state());
  }
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char *)"INFO";
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
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

  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);
  int ret = 0;
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(time_sec + 10); // sleep 10s for schema restore
  restart_helper.run();

  return ret;
}
