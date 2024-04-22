/**
 * Copyright (c) 2024 OceanBase
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
#include <thread>
#include <iostream>
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_submit_log_cb.h"


static const char *TEST_FILE_NAME = "test_memtable_new_safe_to_destroy";

namespace oceanbase
{

ObTransID qcc_tx_id;
namespace transaction
{
int ObTxLogCb::on_success()
{
  int ret = OB_SUCCESS;
  const ObTransID tx_id = trans_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTxLogCb not inited", K(ret));
  } else if (NULL == ctx_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ctx is null", K(ret), K(tx_id), KP(ctx_));
  } else {
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx *>(ctx_);
    while (qcc_tx_id == tx_id) {
      TRANS_LOG(INFO, "qcc debug", KPC(part_ctx), K(tx_id));
      fprintf(stdout, "qcc debug\n");
      usleep(1 * 1000 * 1000);
    }
    if (OB_FAIL(part_ctx->on_success(this))) {
      TRANS_LOG(WARN, "sync log success callback error", K(ret), K(tx_id));
    }
  }

  return ret;
}

}


namespace unittest
{


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

#define READ_SQL_BY_CONN(conn, sql_str)         \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));


class ObTestMemtableNewSafeToDestroy : public ObSimpleClusterTestBase
{
public:
  ObTestMemtableNewSafeToDestroy() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    ASSERT_EQ(OB_SUCCESS, create_tenant());
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    TRANS_LOG(INFO, "create_tenant end", K(tenant_id));
  }
  void prepare_sys_env()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL("alter system set debug_sync_timeout = '2000s'");
  }
  void prepare_tenant_env()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    int64_t affected_rows = 0;
    ObSqlString sql;
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    ASSERT_NE(nullptr, connection);
    WRITE_SQL_BY_CONN(connection, "set GLOBAL ob_trx_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set GLOBAL ob_trx_idle_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set GLOBAL ob_query_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "alter system set undo_retention = 0");
  }

  void get_ls(const uint64_t tenant_id,
              const share::ObLSID ls_id,
              ObLS *&ls)
  {
    ls = nullptr;
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls = handle.get_ls());
  }

  void get_memtable(const uint64_t tenant_id,
                    const share::ObLSID ls_id,
                    const ObTabletID tablet_id,
                    ObTableHandleV2 &handle)
  {
    ObLS *ls = NULL;
    get_ls(tenant_id, ls_id, ls);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    ASSERT_EQ(OB_SUCCESS, tablet->get_active_memtable(handle));
  }

  void get_tablet(const uint64_t tenant_id,
                  const share::ObLSID ls_id,
                  const ObTabletID tablet_id,
                  ObTabletHandle &tablet_handle)
  {
    ObLS *ls = NULL;
    get_ls(tenant_id, ls_id, ls);
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_tablet(tablet_id, tablet_handle));
  }

};


TEST_F(ObTestMemtableNewSafeToDestroy, test_safe_to_destroy)
{
  ObSqlString sql;
  int64_t affected_rows = 0;

  // ============================== Phase1. create tenant and table ==============================
  TRANS_LOG(INFO, "create tenant start");
  uint64_t tenant_id = 0;
  create_test_tenant(tenant_id);
  TRANS_LOG(INFO, "create tenant end");

  prepare_sys_env();

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  TRANS_LOG(INFO, "create table start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  EXE_SQL("create table qcc (a int)");
  // wait minor freeze when create table
  usleep(10 * 1000 * 1000);
  TRANS_LOG(INFO, "create_table end");

  prepare_tenant_env();

  sqlclient::ObISQLConnection *user_connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(user_connection));
  ASSERT_NE(nullptr, user_connection);

  TRANS_LOG(INFO, "start the txn");
  WRITE_SQL_BY_CONN(user_connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(user_connection, "insert into qcc values(1);");

  ASSERT_EQ(0, SSH::find_tx(user_connection, qcc_tx_id));
  ObTableHandleV2 handle;
  ObTabletID tablet_id;
  ASSERT_EQ(0, SSH::select_table_tablet(tenant_id, "qcc", tablet_id));
  get_memtable(tenant_id, share::ObLSID(1001), tablet_id, handle);
  ObIMemtable *imemtable;
  handle.get_memtable(imemtable);
  memtable::ObMemtable *memtable = dynamic_cast<memtable::ObMemtable *>(imemtable);
  TRANS_LOG(INFO, "qcc print", KPC(memtable));
  std::thread th([user_connection] () {
                   user_connection->commit();
                   TRANS_LOG(INFO, "qcc not debug");
                   fprintf(stdout, "qcc not debug\n");
                 });

  usleep(5 * 1000 * 1000);

  TRANS_LOG(INFO, "qcc print", KPC(memtable));
  bool is_safe = false;
  // EXPECT_EQ(3, memtable->ref_cnt_);
  // EXPECT_EQ(0, memtable->write_ref_cnt_);
  // EXPECT_EQ(0, memtable->unsubmitted_cnt_);

  handle.reset();

  storage::ObTabletMemtableMgr *memtable_mgr = memtable->get_memtable_mgr();
  EXPECT_EQ(OB_SUCCESS, memtable_mgr->release_memtables());

  TRANS_LOG(INFO, "qcc print2", KPC(memtable));;

  ObTabletHandle tablet_handle;
  get_tablet(tenant_id, share::ObLSID(1001), tablet_id, tablet_handle);
  tablet_handle.get_obj()->reset_memtable();

  TRANS_LOG(INFO, "qcc print3", KPC(memtable));;

  usleep(5 * 1000 * 1000);

  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(0, SSH::get_tx_ctx(tenant_id, share::ObLSID(1001), qcc_tx_id, ctx));

  EXPECT_EQ(2, ctx->mt_ctx_.trans_mgr_.callback_list_.get_length());
  EXPECT_EQ(OB_SUCCESS, memtable->safe_to_destroy(is_safe));
  EXPECT_EQ(false, is_safe);
  EXPECT_EQ(OB_SUCCESS, ctx->mt_ctx_.trans_mgr_.callback_list_.tx_print_callback());

  qcc_tx_id.reset();

  usleep(1 * 1000 * 1000);
  th.join();

}

}
}

int main(int argc, char **argv)
{
  using namespace oceanbase::unittest;

  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
