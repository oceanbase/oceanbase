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
#include <thread>
#include <iostream>
#define protected public
#define private public
#include "env/ob_simple_cluster_test_base.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ob_relative_table.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/access/ob_rows_info.h"
static int qcc = 0;
static int qcc2 = 0;
static int qcc3 = 0;
namespace oceanbase
{
namespace storage
{
int ObLSTabletService::insert_tablet_rows(
    const int64_t row_count,
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObStoreRow *rows,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &table = run_ctx.relative_table_;
  const bool check_exists = !table.is_storage_index_table() || table.is_unique_index();
  bool exists = false;
  // // 1. Defensive checking of new rows.
  // if (GCONF.enable_defensive_check()) {
  //   for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
  //     ObStoreRow &tbl_row = rows[i];
  //     if (OB_FAIL(check_new_row_legitimacy(run_ctx, tbl_row.row_val_))) {
  //       LOG_WARN("Failed to check new row legitimacy", K(ret), K_(tbl_row.row_val));
  //     }
  //   }
  // }

  // 2. Check uniqueness constraint in memetable only(active + frozen).
  // It would be more efficient and elegant to completely merge the uniqueness constraint
  // and write conflict checking, but the implementation currently is to minimize intrusion
  // into the memtable.
  // if (check_exists && OB_FAIL(tablet_handle.get_obj()->rowkeys_exists(run_ctx.store_ctx_, table,
  //                                                                     rows_info, exists))) {
  //   LOG_WARN("Failed to check the uniqueness constraint", K(ret), K(rows_info));
  // } else if (exists) {
  //   ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
  //   blocksstable::ObDatumRowkey &duplicate_rowkey = rows_info.get_conflict_rowkey();
  //   LOG_WARN("Rowkey already exist", K(ret), K(table), K(duplicate_rowkey));
  // }

  // 3. Insert rows with uniqueness constraint and write conflict checking.
  // Check write conflict in memtable + sstable.
  // Check uniqueness constraint in sstable only.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_handle.get_obj()->insert_rows(table,
                                                     run_ctx.store_ctx_,
                                                     rows,
                                                     rows_info,
                                                     check_exists,
                                                     *run_ctx.col_descs_,
                                                     row_count,
                                                     run_ctx.dml_param_.encrypt_meta_))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        blocksstable::ObDatumRowkey &duplicate_rowkey = rows_info.get_conflict_rowkey();
        TRANS_LOG(WARN, "Rowkey already exist", K(ret), K(table), K(duplicate_rowkey),
                 K(rows_info.get_conflict_idx()));
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        TRANS_LOG(WARN, "Failed to insert rows to tablet", K(ret), K(rows_info));
      }
    }
  }

  // 4. Log user error message if rowkey is duplicate.
  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && !run_ctx.dml_param_.is_ignore_) {
    int tmp_ret = OB_SUCCESS;
    char rowkey_buffer[OB_TMP_BUF_SIZE_256];
    ObString index_name = "PRIMARY";
    if (OB_TMP_FAIL(extract_rowkey(table, rows_info.get_conflict_rowkey(),
         rowkey_buffer, OB_TMP_BUF_SIZE_256, run_ctx.dml_param_.tz_info_))) {
      TRANS_LOG(WARN, "Failed to extract rowkey", K(ret), K(tmp_ret));
    }
    if (table.is_index_table()) {
      if (OB_TMP_FAIL(table.get_index_name(index_name))) {
        TRANS_LOG(WARN, "Failed to get index name", K(ret), K(tmp_ret));
      }
    } else if (lib::is_oracle_mode() && OB_TMP_FAIL(table.get_primary_key_name(index_name))) {
      TRANS_LOG(WARN, "Failed to get pk name", K(ret), K(tmp_ret));
    }
    LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, index_name.length(), index_name.ptr());
  }
  return ret;
}

int ObStorageTableGuard::refresh_and_protect_table(ObRelativeTable &relative_table)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator &iter = relative_table.tablet_iter_;
  const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
  bool need_print = false;
  if (tablet_id.id() == 200001 && store_ctx_.mvcc_acc_ctx_.tx_id_.get_id() % 2 == 0 && qcc2 == 0) {
    need_print = true;
    qcc2++;
    TRANS_LOG(INFO, "qc debug", K(store_ctx_.mvcc_acc_ctx_.tx_id_), KPC(iter.table_iter()->get_last_memtable()));
    usleep(1 * 1000 * 1000);
    TRANS_LOG(INFO, "qc debug", K(store_ctx_.mvcc_acc_ctx_.tx_id_), KPC(iter.table_iter()->get_last_memtable()));
  }
  if (tablet_id.id() == 200001 && store_ctx_.mvcc_acc_ctx_.tx_id_.get_id() % 2 == 1 && qcc3 == 0) {
    while (qcc2 == 0) {
      usleep(1000);
    }
  }
  while (OB_SUCC(ret) && need_to_refresh_table(*iter.table_iter())) {
    if (OB_FAIL(store_ctx_.ls_->get_tablet_svr()->get_read_tables(
        tablet_id,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        store_ctx_.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
        iter,
        relative_table.allow_not_ready()))) {
      TRANS_LOG(WARN, "fail to get", K(store_ctx_.mvcc_acc_ctx_.tx_id_), K(ret));
    } else {
      // no worry. iter will hold tablet reference and its life cycle is longer than guard
      tablet_ = iter.get_tablet();
      if (store_ctx_.timeout_ > 0) {
        const int64_t query_left_time = store_ctx_.timeout_ - ObTimeUtility::current_time();
        if (query_left_time <= 0) {
          ret = OB_TRANS_STMT_TIMEOUT;
        }
      }
    }
  }
  if (need_print) {
    TRANS_LOG(INFO, "qc debug", K(store_ctx_.mvcc_acc_ctx_.tx_id_), KPC(iter.table_iter()->get_last_memtable()));
  }
  if (OB_SUCC(ret)) {
    if (tablet_id.id() == 200001 && store_ctx_.mvcc_acc_ctx_.tx_id_.get_id() % 2 == 1 && qcc3 == 0) {
      qcc++;
      qcc3++;
      TRANS_LOG(INFO, "qc debug2", K(store_ctx_.mvcc_acc_ctx_.tx_id_), KPC(iter.table_iter()->get_last_memtable()));
      usleep(2 * 1000 * 1000);
      TRANS_LOG(INFO, "qc debug2", K(store_ctx_.mvcc_acc_ctx_.tx_id_), KPC(iter.table_iter()->get_last_memtable()));
    }
  }
  return ret;
}
}
namespace unittest
{
using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::memtable;
using namespace oceanbase::storage::checkpoint;
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
class ObCallbackReverseTest : public ObSimpleClusterTestBase
{
public:
  ObCallbackReverseTest() : ObSimpleClusterTestBase("callbacks_with_reverse_order", "200G", "40G") {}
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
    WRITE_SQL_BY_CONN(connection, "alter system set enable_early_lock_release = False;");
    WRITE_SQL_BY_CONN(connection, "alter system set undo_retention = 1800;");
    sleep(5);
  }
  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "20G", "100G"));
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    TRANS_LOG(INFO, "create_tenant end", K(tenant_id));
  }
  // you should use single partition when using it
  void get_tablet_id_with_table_name(const char *name,
                                     ObTabletID &tablet)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t tablet_id = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select tablet_id from oceanbase.__all_virtual_table where table_name=%s", name));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", tablet_id));
    }
    tablet = (uint64_t)tablet_id;
  }
  void minor_freeze_data()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    WRITE_SQL_BY_CONN(connection, "alter system minor freeze;");
  }
  void get_ls(uint64_t tenant_id, ObLS *&ls)
  {
    ls = nullptr;
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    share::ObLSID ls_id(1001);
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls = handle.get_ls());
  }
  void get_memtable(const ObTabletID tablet_id,
                    ObTableHandleV2 &handle)
  {
    ObLS *ls = NULL;
    get_ls(1002, ls);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    ASSERT_EQ(OB_SUCCESS, tablet->get_active_memtable(handle));
  }
private:
};
TEST_F(ObCallbackReverseTest, callback_reverse_test)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  // ============================== Phase1. create tenant and table ==============================
  TRANS_LOG(INFO, "create tenant start");
  uint64_t tenant_id = 0;
  create_test_tenant(tenant_id);
  TRANS_LOG(INFO, "create tenant end");
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  TRANS_LOG(INFO, "create table start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  EXE_SQL("create table qcc (a int primary key)");
  usleep(10 * 1000 * 1000);
  TRANS_LOG(INFO, "create_table end");
  prepare_tenant_env();
  std::thread t1(
    [this]() {
      ObSqlString sql;
      int64_t affected_rows = 0;
      common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
      sqlclient::ObISQLConnection *connection = nullptr;
      ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
      ASSERT_NE(nullptr, connection);
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_timeout = 10000000000");
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_idle_timeout = 10000000000");
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_query_timeout = 10000000000");
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_lock_timeout = 0");
      TRANS_LOG(INFO, "insert data start1");
      WRITE_SQL_BY_CONN(connection, "begin;");
      WRITE_SQL_FMT_BY_CONN(connection, "insert into qcc values(1);");
      WRITE_SQL_BY_CONN(connection, "commit;");
      TRANS_LOG(INFO, "insert data end1");
    });
  std::thread t2(
    [this]() {
      ObSqlString sql;
      int64_t affected_rows = 0;
      common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
      sqlclient::ObISQLConnection *connection = nullptr;
      ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
      ASSERT_NE(nullptr, connection);
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_timeout = 10000000000");
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_idle_timeout = 10000000000");
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_query_timeout = 10000000000");
      WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_lock_timeout = 0");
      TRANS_LOG(INFO, "insert data start2");
      WRITE_SQL_BY_CONN(connection, "begin;");
      WRITE_SQL_FMT_BY_CONN(connection, "insert into qcc values(1);");
      WRITE_SQL_BY_CONN(connection, "commit;");
      TRANS_LOG(INFO, "insert data end2");
    });
  std::thread t3(
    [this]() {
      while (qcc == 0) {
        TRANS_LOG(INFO, "qcc is not increased", K(qcc));
        usleep(100 * 1000);
      }
      minor_freeze_data();
    });
  t1.join();
  t2.join();
  t3.join();
  ASSERT_EQ(1, qcc);
}
} // namespace unittest
} // namespace oceanbase
int main(int argc, char **argv)
{
  using namespace oceanbase::unittest;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
