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
#include "src/storage/ls/ob_ls_tablet_service.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ob_relative_table.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tablet/ob_tablet.h"

static const char *TEST_FILE_NAME = "test_mvcc_gc";

namespace oceanbase
{

namespace concurrency_control
{
int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_RETRY_INTERVAL = 100_ms;
int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_EXEC_INTERVAL = 1_s;
int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_PRECISION = 1_ms;

} // namespace concurrency_control

namespace storage
{
int ObLSTabletService::table_scan(ObTableScanIterator &iter, ObTableScanParam &param)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_table_scan_begin);
  ObTabletHandle data_tablet;
  bool allow_to_read = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (FALSE_IT(allow_to_read_mgr_.load_allow_to_read_info(allow_to_read))) {
  } else if (!allow_to_read) {
    ret = OB_REPLICA_NOT_READABLE;
    STORAGE_LOG(WARN, "ls is not allow to read", K(ret), KPC(ls_));
  } else if (OB_FAIL(prepare_scan_table_param(param, *(MTL(ObTenantSchemaService*)->get_schema_service())))) {
    STORAGE_LOG(WARN, "failed to prepare scan table param", K(ret), K(param));
  } else if (OB_FAIL(get_tablet_with_timeout(param.tablet_id_, data_tablet, param.timeout_))) {
    STORAGE_LOG(WARN, "failed to check and get tablet", K(ret), K(param));
  } else if (OB_FAIL(inner_table_scan(data_tablet, iter, param))) {
    STORAGE_LOG(WARN, "failed to do table scan", K(ret), KP(&iter), K(param));
  }
  NG_TRACE(S_table_scan_end);

  if (1002 == MTL_ID() && 200001 == param.tablet_id_.id()) {
    DEBUG_SYNC(AFTER_TABLE_SCAN);
  }

  return ret;
}

int ObLSTabletService::insert_rows(
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_insert_rows_begin);
  ObTabletHandle tablet_handle;
  int64_t afct_num = 0;
  int64_t dup_num = 0;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid())
      || !ctx.is_write()
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(ctx), K(dml_param), K(column_ids), KP(row_iter));
  } else if (OB_FAIL(get_tablet_with_timeout(
      ctx.tablet_id_, tablet_handle, dml_param.timeout_))) {
    STORAGE_LOG(WARN, "failed to check and get tablet", K(ret), K(ctx.tablet_id_));
  } else {
    ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            lob_allocator,
                            ObDmlFlag::DF_INSERT);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    void *ptr = nullptr;
    ObStoreRow *tbl_rows = nullptr;
    int64_t row_count = 0;
    //index of row that exists
    int64_t row_count_first_bulk = 0;
    bool first_bulk = true;
    ObNewRow *rows = nullptr;
    ObRowsInfo rows_info;
    const ObRelativeTable &data_table = run_ctx.relative_table_;

    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      STORAGE_LOG(WARN, "failed to prepare dml running ctx", K(ret));
    }

    while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
      if (row_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "row_count should be greater than 0", K(ret));
      } else if (first_bulk) {
        first_bulk = false;
        row_count_first_bulk = row_count;
        const ObITableReadInfo &full_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
        if (OB_FAIL(rows_info.init(data_table, ctx, full_read_info))) {
          STORAGE_LOG(WARN, "Failed to init rows info", K(ret), K(data_table));
        } else if (OB_ISNULL(ptr = work_allocator.alloc(row_count * sizeof(ObStoreRow)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(ERROR, "fail to allocate memory", K(ret), K(row_count));
        } else {
          tbl_rows = new (ptr) ObStoreRow[row_count];
          for (int64_t i = 0; i < row_count; i++) {
            tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_INSERT);
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tbl_rows)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected error, tbl_rows is NULL", K(ret), KP(tbl_rows));
      } else if (OB_FAIL(insert_rows_to_tablet(tablet_handle, run_ctx, rows,
          row_count, rows_info, tbl_rows, afct_num, dup_num))) {
        STORAGE_LOG(WARN, "insert to each tablets fail", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (nullptr != ptr) {
      work_allocator.free(ptr);
    }
    lob_allocator.reset();
    if (OB_SUCC(ret)) {
      STORAGE_LOG(DEBUG, "succeeded to insert rows", K(ret));
      affected_rows = afct_num;
      EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
    }
  }
  NG_TRACE(S_insert_rows_end);

  if (1002 == MTL_ID() && 200001 == ctx.tablet_id_.id()) {
    DEBUG_SYNC(AFTER_INSERT_ROWS);
  }

  return ret;
}

} // namespace storage

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

class ObTestMvccGC : public ObSimpleClusterTestBase
{
public:
  ObTestMvccGC() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
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

  void collect_garbage_collector_info(ObIArray<concurrency_control::ObMultiVersionSnapshotInfo> &snapshots_info)
  {
    concurrency_control::ObMultiVersionGCSnapshotCollector collector(snapshots_info);

    ASSERT_EQ(OB_SUCCESS, MTL(concurrency_control::ObMultiVersionGarbageCollector *)->collect(collector));
    MVCC_LOG(INFO, "collect garbage collector info", K(snapshots_info));
  }

  void check_garbage_collector_info(concurrency_control::ObMultiVersionGCSnapshotFunctor& checker)
  {
    ASSERT_EQ(OB_SUCCESS, MTL(concurrency_control::ObMultiVersionGarbageCollector *)->collect(checker));
    MVCC_LOG(INFO, "check garbage collector info end");
  }

  void check_freeze_info_mgr(const int64_t expected_snapshot_version)
  {
    ObStorageSnapshotInfo snapshot_info;
    share::SCN snapshot_version_for_txn;
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObTenantFreezeInfoMgr *)->
              get_min_reserved_snapshot(ObTabletID(200001), 1, snapshot_info));
    snapshot_version_for_txn = MTL(concurrency_control::ObMultiVersionGarbageCollector *)->
      get_reserved_snapshot_for_active_txn();
    MVCC_LOG(INFO, "check_freeze_info_mgr", K(snapshot_info), K(expected_snapshot_version), K(expected_snapshot_version >= snapshot_info.snapshot_));
    ASSERT_EQ(TRUE, expected_snapshot_version >= snapshot_info.snapshot_);
    ASSERT_EQ(expected_snapshot_version, snapshot_version_for_txn.get_val_for_tx());
  }

  void wait_report()
  {
    // sleep 3 second
    MVCC_LOG(INFO, "start to wait");
    usleep(3 * 1000 * 1000);
    MVCC_LOG(INFO, "finish waiting");
  }

  void start_read_debug_sync(const char *name)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL_FMT("set ob_global_debug_sync = 'AFTER_TABLE_SCAN wait_for %s'", name);
  }

  void start_write_debug_sync(const char *name)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL_FMT("set ob_global_debug_sync = 'AFTER_INSERT_ROWS wait_for %s'", name);
  }

  void signal_debug_sync(const char *name)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL_FMT("set ob_global_debug_sync = 'now signal %s'", name);
  }

  void test_case1();
  void test_case2();
  void test_case3();
  void test_case4();
  void test_case5();
  void test_case6();
  void test_case7();

  static bool is_disk_almost_full_;
  static bool can_report_;
  static bool is_refresh_fail_;
};

bool ObTestMvccGC::is_disk_almost_full_ = false;
bool ObTestMvccGC::can_report_ = true;
bool ObTestMvccGC::is_refresh_fail_ = false;

void ObTestMvccGC::test_case1()
{
  // #CASE1: get snapshot with no active txn
  wait_report();
  ObArray<concurrency_control::ObMultiVersionSnapshotInfo> snapshots_info;
  snapshots_info.reset();
  collect_garbage_collector_info(snapshots_info);
  int64_t check_count = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count](const share::SCN snapshot_version,
                   const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                   const concurrency_control::ObMultiVersionGCStatus status,
                   const int64_t create_time,
                   const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_EQ(share::SCN::max_scn(), snapshot_version);
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      return OB_SUCCESS;
    });
  check_garbage_collector_info(functor);
  ASSERT_EQ(4, check_count);
}

void ObTestMvccGC::test_case2()
{
  // #CASE2: get snapshot with an ac = 0, RR txn
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObISQLClient::ReadResult read_res;

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  WRITE_SQL_BY_CONN(connection, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
  WRITE_SQL_BY_CONN(connection, "begin;");
  READ_SQL_BY_CONN(connection, "select count(*) as cnt from test_mvcc_gc");

  wait_report();
  int64_t check_count = 0;
  int64_t active_snapshot = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count,
     &active_snapshot](const share::SCN snapshot_version,
                       const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                       const concurrency_control::ObMultiVersionGCStatus status,
                       const int64_t create_time,
                       const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_NE(share::SCN::max_scn(), snapshot_version);
        active_snapshot = snapshot_version.get_val_for_tx();
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case2.1: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr));
      return OB_SUCCESS;
    });
  check_garbage_collector_info(functor);
  check_freeze_info_mgr(active_snapshot);
  ASSERT_EQ(4, check_count);


  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_mvcc_gc values(1);");

  wait_report();
  check_count = 0;
  active_snapshot = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor2(
    [&check_count,
     &active_snapshot](const share::SCN snapshot_version,
                       const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                       const concurrency_control::ObMultiVersionGCStatus status,
                       const int64_t create_time,
                       const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_NE(share::SCN::max_scn(), snapshot_version);
        active_snapshot = snapshot_version.get_val_for_tx();
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case2.2: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr));
      return OB_SUCCESS;
    });
  check_garbage_collector_info(functor2);
  check_freeze_info_mgr(active_snapshot);
  ASSERT_EQ(4, check_count);
  WRITE_SQL_BY_CONN(connection, "commit;");
}

void ObTestMvccGC::test_case3()
{
  // #CASE3: get snapshot with an ac = 0, RC txn
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObISQLClient::ReadResult read_res;

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  WRITE_SQL_BY_CONN(connection, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
  WRITE_SQL_BY_CONN(connection, "begin;");

  READ_SQL_BY_CONN(connection, "select count(*) as cnt from test_mvcc_gc");

  wait_report();
  int64_t check_count = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count](const share::SCN snapshot_version,
                   const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                   const concurrency_control::ObMultiVersionGCStatus status,
                   const int64_t create_time,
                   const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_EQ(share::SCN::max_scn(), snapshot_version);
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case3.1: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr));
      return OB_SUCCESS;
    });

  check_garbage_collector_info(functor);
  ASSERT_EQ(4, check_count);

  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_mvcc_gc values(1);");

  wait_report();
  check_count = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor2(
    [&check_count](const share::SCN snapshot_version,
                   const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                   const concurrency_control::ObMultiVersionGCStatus status,
                   const int64_t create_time,
                   const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_EQ(share::SCN::max_scn(), snapshot_version);
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case3.2: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr));
      return OB_SUCCESS;
    });
  check_garbage_collector_info(functor2);
  ASSERT_EQ(4, check_count);
  WRITE_SQL_BY_CONN(connection, "commit;");
}

void ObTestMvccGC::test_case4()
{
  // #CASE4: get snapshot with an ac = 1 txn
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObISQLClient::ReadResult read_res;

  start_read_debug_sync("test_case4_1");

  std::thread *thread = new std::thread(
    [this]() -> void {
      ObSqlString sql;
      int64_t affected_rows = 0;
      ObISQLClient::ReadResult read_res;

      common::ObMySQLProxy &sql_proxy = this->get_curr_simple_server().get_sql_proxy2();
      sqlclient::ObISQLConnection *connection = nullptr;
      ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
      ASSERT_NE(nullptr, connection);

      TRANS_LOG(INFO, "start read");
      READ_SQL_BY_CONN(connection, "select count(*) as cnt from test_mvcc_gc");
      TRANS_LOG(INFO, "end read");

      TRANS_LOG(INFO, "start write");
      WRITE_SQL_BY_CONN(connection, "insert into test_mvcc_gc values(1)");
      TRANS_LOG(INFO, "end write");
    });
  // wait thread execution
  usleep(5 * 1000 * 1000);

  wait_report();
  int64_t check_count = 0;
  int64_t active_snapshot = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count,
     &active_snapshot](const share::SCN snapshot_version,
                       const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                       const concurrency_control::ObMultiVersionGCStatus status,
                       const int64_t create_time,
                       const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_NE(share::SCN::max_scn(), snapshot_version);
        active_snapshot = snapshot_version.get_val_for_tx();
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case4.1: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr));
      return OB_SUCCESS;
    });
  check_garbage_collector_info(functor);
  check_freeze_info_mgr(active_snapshot);
  ASSERT_EQ(4, check_count);

  start_write_debug_sync("test_case4_2");
  signal_debug_sync("test_case4_1");

  // wait thread execution
  usleep(5 * 1000 * 1000);

  wait_report();
  check_count = 0;
  active_snapshot = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor2(
    [&check_count,
     &active_snapshot](const share::SCN snapshot_version,
                       const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                       const concurrency_control::ObMultiVersionGCStatus status,
                       const int64_t create_time,
                       const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_NE(share::SCN::max_scn(), snapshot_version);
        active_snapshot = snapshot_version.get_val_for_tx();
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case4.2: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr));
      return OB_SUCCESS;
    });
  check_garbage_collector_info(functor2);
  check_freeze_info_mgr(active_snapshot);
  ASSERT_EQ(4, check_count);

  signal_debug_sync("test_case4_2");
  thread->join();
}

void ObTestMvccGC::test_case5()
{
  // #CASE5: test disk is full
  is_disk_almost_full_ = true;

  wait_report();
  wait_report();
  int64_t check_count = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count](const share::SCN snapshot_version,
                   const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                   const concurrency_control::ObMultiVersionGCStatus status,
                   const int64_t create_time,
                   const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::DISABLED_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_EQ(share::SCN::max_scn(), snapshot_version);
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case5: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr), K(status));
      return OB_SUCCESS;
    });

  check_garbage_collector_info(functor);
  check_freeze_info_mgr(INT64_MAX);
  ASSERT_EQ(4, check_count);

  is_disk_almost_full_ = false;
  wait_report();
}

void ObTestMvccGC::test_case6()
{
  // #CASE5: test donot report
  can_report_ = false;
  wait_report();
  wait_report();
  int64_t check_count = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count](const share::SCN snapshot_version,
                   const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                   const concurrency_control::ObMultiVersionGCStatus status,
                   const int64_t create_time,
                   const ObAddr addr) -> int {
      check_count++;
      TRANS_LOG(INFO, "test_case6: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr), K(status));
      return OB_SUCCESS;
    });

  check_garbage_collector_info(functor);
  ASSERT_EQ(0, check_count);

  can_report_ = true;
  wait_report();
}

void ObTestMvccGC::test_case7()
{
  // #CASE5: test donot refresh
  is_refresh_fail_ = true;
  wait_report();
  int64_t check_count = 0;
  concurrency_control::ObMultiVersionGCSnapshotOperator functor(
    [&check_count](const share::SCN snapshot_version,
                   const concurrency_control::ObMultiVersionSnapshotType snapshot_type,
                   const concurrency_control::ObMultiVersionGCStatus status,
                   const int64_t create_time,
                   const ObAddr addr) -> int {
      check_count++;
      EXPECT_EQ(concurrency_control::ObMultiVersionGCStatus::NORMAL_GC_STATUS, status);
      if (concurrency_control::ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT == snapshot_type) {
        EXPECT_EQ(share::SCN::max_scn(), snapshot_version);
      } else {
        EXPECT_EQ(true, abs((snapshot_version.get_val_for_tx() / 1000) - create_time) < 1 * 1000 * 1000);
      }
      TRANS_LOG(INFO, "test_case7: check gc snapshot info", K(snapshot_version),
                K(snapshot_type), K(create_time), K(addr), K(status));
      return OB_SUCCESS;
    });

  check_garbage_collector_info(functor);
  check_freeze_info_mgr(INT64_MAX);
  ASSERT_EQ(4, check_count);

  is_refresh_fail_ = false;
  wait_report();
}

TEST_F(ObTestMvccGC, test_basic_mvcc_gc)
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
  EXE_SQL("create table test_mvcc_gc (a int)");
  // wait minor freeze when create table
  usleep(10 * 1000 * 1000);
  TRANS_LOG(INFO, "create_table end");

  prepare_tenant_env();

  // #CASE1: get snapshot with no active txn
  test_case1();

  // #CASE5: check when refresh fail too long
  test_case7();

  // #CASE2: get snapshot with an ac = 0, RR txn
  test_case2();

  // #CASE5: check when report fail too long
  test_case6();

  // #CASE3: get snapshot with an ac = 0, RC txn
  test_case3();

  // #CASE5: check when disk is full
  test_case5();

  // #CASE4: get snapshot with an ac = 1 txn
  test_case4();
}

} // namespace unittest

namespace concurrency_control
{

bool ObMultiVersionGarbageCollector::can_report()
{
  return unittest::ObTestMvccGC::can_report_;
}

bool ObMultiVersionGarbageCollector::is_refresh_fail()
{
  return unittest::ObTestMvccGC::is_refresh_fail_;
}

int ObMultiVersionGarbageCollector::is_disk_almost_full_(bool &is_almost_full)
{
  int ret = OB_SUCCESS;

  if (unittest::ObTestMvccGC::is_disk_almost_full_) {
    is_almost_full = true;
  } else {
    is_almost_full = false;
  }

  return ret;
}

} // namespace concurrency_control

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_mvcc_gc.log*");
  OB_LOGGER.set_file_name("test_mvcc_gc.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
