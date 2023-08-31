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

enum EnumTestMode : uint32_t
{
  NORMAL_TEST = 0,
  BIANQUE_TEST = 1,
  MAX_TEST_MODE
};

enum EnumDataMode : uint32_t
{
  ALL_CLEANOUT = 0,
  ALL_SAME_TXN_DELAY_CLEANOUT = 1,
  ALL_DIFF_TXN_DELAY_CLEANOUT = 2,
  MAX_DATA_MODE
};

enum EnumDataLocation : uint32_t
{
  DATA_IN_MEMORY = 0,
  DATA_ON_DISK = 1,
  MAX_DATA_LOCATION
};

enum EnumTxTableMode : uint32_t
{
  ALL_IN_MEMORY = 0,
  ALL_IN_DISK = 1,
  MAX_TX_TABLE_MODE
};

static const char *TEST_FILE_NAME = "fast_commit_report";
const int64_t DEFAULT_TOTAL_FC_ROW_COUNT = 100000;
const int64_t DEFAULT_TOTAL_FC_SESSION = 10;
const int64_t DEFAULT_PAINTING_FC_ROW_COUNT = 10000;
EnumTestMode TEST_MODE = EnumTestMode::NORMAL_TEST;
EnumDataMode DATA_MODE = EnumDataMode::ALL_CLEANOUT;
EnumDataLocation DATA_LOCATION = EnumDataLocation::DATA_IN_MEMORY;
EnumTxTableMode TX_TABLE_MODE = EnumTxTableMode::ALL_IN_MEMORY;
int64_t TOTAL_FC_ROW_COUNT = DEFAULT_TOTAL_FC_ROW_COUNT;
int64_t TOTAL_FC_SESSION = DEFAULT_TOTAL_FC_SESSION;
int64_t TOTAL_READ_TIME = 0;
int64_t READ_CNT = 0;
int64_t SELECT_PARALLEL= 1;
int64_t SLEEP_SECONDS = 0;


namespace oceanbase
{

namespace memtable
{
int ObMvccValueIterator::get_next_node(const void *&tnode)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    tnode = NULL;
    while (OB_SUCC(ret) && (NULL == tnode)) {
      bool is_lock_node = false;
      if (NULL == version_iter_) {
        ret = OB_ITER_END;
      } else if (EnumDataMode::ALL_CLEANOUT == DATA_MODE && OB_FAIL(try_cleanout_tx_node_(version_iter_))) {
        TRANS_LOG(WARN, "fail to cleanout tnode", K(ret), K(*version_iter_));
      } else if (OB_FAIL(version_iter_->is_lock_node(is_lock_node))) {
        TRANS_LOG(WARN, "fail to check is lock node", K(ret), K(*version_iter_));
      } else if (!(version_iter_->is_aborted()              // skip abort version
                   || is_lock_node)) {
        tnode = static_cast<const void *>(version_iter_);
      }

      move_to_next_node_();
    }
  }

  return ret;
}


int ObMemtable::flush(share::ObLSID ls_id)
{
  if (EnumDataLocation::DATA_IN_MEMORY == DATA_LOCATION) {
    return OB_EAGAIN;
  }
  int ret = OB_SUCCESS;

  int64_t cur_time = ObTimeUtility::current_time();
  if (is_flushed_) {
    ret = OB_NO_NEED_UPDATE;
  } else {
    if (mt_stat_.create_flush_dag_time_ == 0 &&
        mt_stat_.ready_for_flush_time_ != 0 &&
        cur_time - mt_stat_.ready_for_flush_time_ > 30 * 1000 * 1000) {
      STORAGE_LOG(WARN, "memtable can not create dag successfully for long time",
                K(ls_id), K(*this), K(mt_stat_.ready_for_flush_time_));
      compaction::ADD_SUSPECT_INFO(MINI_MERGE,
                       ls_id, get_tablet_id(),
                       ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_CREATE_DAG,
                       cur_time - mt_stat_.ready_for_flush_time_,
                       mt_stat_.ready_for_flush_time_);
    }
    compaction::ObTabletMergeDagParam param;
    param.ls_id_ = ls_id;
    param.tablet_id_ = key_.tablet_id_;
    param.merge_type_ = MINI_MERGE;
    param.merge_version_ = ObVersion::MIN_VERSION;

    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "failed to schedule tablet merge dag", K(ret));
      }
    } else {
      mt_stat_.create_flush_dag_time_ = cur_time;
      TRANS_LOG(INFO, "schedule tablet merge dag successfully", K(ret), K(param), KPC(this));
    }
  }

  return ret;
}

}

namespace storage
{
int LockForReadFunctor::operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx)
{
  int ret = OB_ERR_SHARED_LOCK_CONFLICT;
  const int64_t MAX_SLEEP_US = 1000;
  auto &acc_ctx = lock_for_read_arg_.mvcc_acc_ctx_;
  auto lock_expire_ts = acc_ctx.eval_lock_expire_ts();

  const int32_t state = ATOMIC_LOAD(&tx_data.state_);

  if (OB_ISNULL(tx_cc_ctx) && (ObTxData::RUNNING == state)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "lock for read functor need prepare version.", KR(ret));
  } else {
    for (int32_t i = 0; OB_ERR_SHARED_LOCK_CONFLICT == ret; i++) {
      if (OB_FAIL(inner_lock_for_read(tx_data, tx_cc_ctx))) {
        if (OB_UNLIKELY(observer::SS_STOPPING == GCTX.status_) ||
            OB_UNLIKELY(observer::SS_STOPPED == GCTX.status_)) {
          // rewrite ret
          ret = OB_SERVER_IS_STOPPING;
          TRANS_LOG(WARN, "observer is stopped", K(ret));
        } else if (ObTimeUtility::current_time() + MIN(i, MAX_SLEEP_US) >= lock_expire_ts) {
          ret = OB_ERR_SHARED_LOCK_CONFLICT;
          break;
        } else if (i < 10) {
          PAUSE();
        } else {
          ob_usleep((i < MAX_SLEEP_US ? i : MAX_SLEEP_US));
        }
      }
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

class ObFastCommitReport : public ObSimpleClusterTestBase
{
public:
  ObFastCommitReport() : ObSimpleClusterTestBase(TEST_FILE_NAME, "200G", "40G") {}
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
    WRITE_SQL_BY_CONN(connection, "alter system set enable_perf_event = true;");
    sleep(5);
  }

  void set_private_buffer_size(const char* size)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL_FMT("alter system set _private_buffer_size = '%s';", size);
  }

  void set_fast_commit_count(const int64_t count)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL_FMT("alter system set _fast_commit_callback_count = %ld;", count);
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

  void minor_freeze_data_and_wait()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;

    WRITE_SQL_BY_CONN(connection, "alter system minor freeze tenant sys;");
    WRITE_SQL_BY_CONN(connection, "alter system minor freeze tenant all_user;");
    WRITE_SQL_BY_CONN(connection, "alter system minor freeze tenant all_meta;");
    fprintf(stdout, "start flush user data\n");
    sleep(10);

    int retry_times = 0;
    bool freeze_success = false;
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      int64_t cnt = -1;
      while (++retry_times <= 600) {
        ASSERT_EQ(OB_SUCCESS,
                  connection->execute_read(OB_SYS_TENANT_ID,
                                           "select count(*) as cnt from oceanbase.__all_virtual_table_mgr where "
                                           "table_type=0 and is_active like '%NO%';",
                                           res));
        common::sqlclient::ObMySQLResult *result = res.mysql_result();
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
        if (0 == cnt) {
          freeze_success = true;
          break;
        }
      }
      fprintf(stdout, "waitting for data minor merge. retry times = %d\n", retry_times);
      sleep(1);
    }

    ASSERT_EQ(true, freeze_success);
  }

  void check_no_minor_freeze()
  {
    TRANS_LOG(INFO, "check no minor freeze start");
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t freeze_cnt = 0;

    ASSERT_EQ(OB_SUCCESS, sql.assign("select freeze_cnt from oceanbase.__all_virtual_tenant_memstore_info where tenant_id=1002"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("freeze_cnt", freeze_cnt));
    }

    ASSERT_EQ(0, freeze_cnt);

    TRANS_LOG(INFO, "check no minor freeze END");
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

  void insert_data_single(const int row_count)
  {
    fprintf(stdout, "start insert data, data count : %d \n", row_count);

    ObSqlString sql;
    int64_t affected_rows = 0;

    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    ASSERT_NE(nullptr, connection);

    WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_idle_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set SESSION ob_query_timeout = 10000000000");

    TRANS_LOG(INFO, "insert data start");
    const int64_t begin_time = ObTimeUtility::current_time();

    if (EnumDataMode::ALL_CLEANOUT == DATA_MODE) {
      WRITE_SQL_BY_CONN(connection, "begin;");
    } else if (EnumDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == DATA_MODE) {
      WRITE_SQL_BY_CONN(connection, "begin;");
    } else if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
      // pass
    }

    for (int i = 0; i < row_count; i++) {
      if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
        WRITE_SQL_BY_CONN(connection, "begin;");
      }
      // const int64_t single_begin_time = ObTimeUtility::current_time();
      WRITE_SQL_FMT_BY_CONN(connection, "insert into test_fast_commit values(1);");
      if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
        WRITE_SQL_BY_CONN(connection, "commit;");
      }
      // const int64_t single_end_time = ObTimeUtility::current_time();
      // TRANS_LOG(INFO, "single insert data single cost", K(single_end_time - single_begin_time));
      if ((i + 1) % DEFAULT_PAINTING_FC_ROW_COUNT == 0) {
        TRANS_LOG(INFO, "insert data single pass one round",
                  K(DATA_MODE),
                  K(TX_TABLE_MODE),
                  K(i + 1));
      }
    }

    if (EnumDataLocation::DATA_ON_DISK == DATA_LOCATION) {
      fprintf(stdout, "data location is DATA_ON_DISK\n");
      if (EnumDataMode::ALL_CLEANOUT == DATA_MODE) {
        WRITE_SQL_BY_CONN(connection, "commit;");
        sleep(10);
        minor_freeze_data_and_wait();
      } else {
        minor_freeze_data_and_wait();
        WRITE_SQL_BY_CONN(connection, "commit;");
      }
    } else if (EnumDataMode::ALL_CLEANOUT == DATA_MODE) {
      WRITE_SQL_BY_CONN(connection, "commit;");
    } else if (EnumDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == DATA_MODE) {
      WRITE_SQL_BY_CONN(connection, "commit;");
    } else if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
      // pass
    }

    const int64_t end_time = ObTimeUtility::current_time();

    TRANS_LOG(INFO, "insert data single cost", K(end_time - begin_time), K(begin_time), K(end_time));
  }

  void insert_data_parallel(const int parrallel_num,
                            const int row_count)
  {
    int single_row_count = row_count / parrallel_num;
    std::thread *threads[parrallel_num];
    const int64_t begin_time = ObTimeUtility::current_time();

    for (int i = 0; i < parrallel_num; i++) {
      threads[i] = new std::thread(&ObFastCommitReport::insert_data_single, this, single_row_count);
    }

    for (int i = 0; i < parrallel_num; i++) {
      threads[i]->join();
    }
    const int64_t end_time = ObTimeUtility::current_time();
    TRANS_LOG(INFO, "insert data parallel cost", K(end_time - begin_time), K(begin_time), K(end_time));
  }

  void insert_data_pdml(const int row_count)
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t real_inserted_rows = 0;

    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    ASSERT_NE(nullptr, connection);

    WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_idle_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set SESSION ob_query_timeout = 10000000000");

    TRANS_LOG(INFO, "pdml insert data start");
    const int64_t begin_time = ObTimeUtility::current_time();

    WRITE_SQL_BY_CONN(connection, "begin;");
    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_fast_commit values(1);");
    real_inserted_rows = 1;

    while (real_inserted_rows < row_count) {
      WRITE_SQL_FMT_BY_CONN(connection, "insert /*+ enable_parallel_dml PARALLEL(32) */ into test_fast_commit select/*+ PARALLEL(32) */ * from test_fast_commit;");
      real_inserted_rows <<= 1;
    }
    fprintf(stdout, "real inserted rows : %ld \n", real_inserted_rows);

    if (EnumDataLocation::DATA_ON_DISK == DATA_LOCATION) {
      fprintf(stdout, "data location is DATA_ON_DISK\n");
      if (EnumDataMode::ALL_CLEANOUT == DATA_MODE) {
        WRITE_SQL_BY_CONN(connection, "commit;");
        sleep(10);
        minor_freeze_data_and_wait();
      } else {
        minor_freeze_data_and_wait();
        WRITE_SQL_BY_CONN(connection, "commit;");
      }
    } else {
      WRITE_SQL_BY_CONN(connection, "commit;");
    }

    const int64_t end_time = ObTimeUtility::current_time();

    TRANS_LOG(INFO, "insert data single cost", K(end_time - begin_time), K(begin_time), K(end_time));
  }


  void read_data(const int row_count)
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t cnt = 0;
    int ret = 0;
    ObISQLClient::ReadResult read_res;

    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    ASSERT_NE(nullptr, connection);

    WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set SESSION ob_trx_idle_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set SESSION ob_query_timeout = 10000000000");

    ObLS *ls = NULL;
    get_ls(1002, ls);
    ObTxTable *tx_table = ls->get_tx_table();

    const int64_t begin_time = ObTimeUtility::current_time();
    sql.assign_fmt("SELECT/*+ enable_parallel_dml PARALLEL(%ld) */ * FROM test_fast_commit;", SELECT_PARALLEL);

    TRANS_LOG(INFO, "do select sql", K(sql));
    ASSERT_EQ(OB_SUCCESS, connection->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));
    const int64_t end_time = ObTimeUtility::current_time();
    TRANS_LOG(INFO, "read data cost", K(end_time - begin_time), K(begin_time), K(end_time));

    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      std::cout << "read data cost(total_cost=" << (end_time - begin_time)/1000 << "ms)\n";
    }

    READ_CNT++;
    if (READ_CNT > 100) {
      TOTAL_READ_TIME += (end_time - begin_time);
    }


    sqlclient::ObMySQLResult *result = read_res.get_result();
    ASSERT_NE(nullptr, result);
  }

  void check_memtable_cleanout(const bool memtable_is_all_cleanout,
                               const bool memtable_is_all_delay_cleanout,
                               const int64_t memtable_count)
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ObTableHandleV2 handle;
    get_memtable(ObTabletID(200001), handle);

    ObIMemtable *imemtable;
    handle.get_memtable(imemtable);
    ObMemtable *memtable = dynamic_cast<memtable::ObMemtable *>(imemtable);

    bool is_all_cleanout = true;
    bool is_all_delay_cleanout = true;
    int64_t count = 0;

    memtable->dump2text("/tmp/fast_commit_report/memtable.txt");
    ASSERT_EQ(OB_SUCCESS, memtable->check_cleanout(is_all_cleanout,
                                                   is_all_delay_cleanout,
                                                   count));

    ASSERT_EQ(memtable_is_all_cleanout, is_all_cleanout);
    ASSERT_EQ(memtable_is_all_delay_cleanout, is_all_delay_cleanout);
    ASSERT_EQ(memtable_count, count);
  }

  void wait_freeze_tx_table_finish(const share::SCN start_log_ts,
                                   const share::SCN end_log_ts)
  {
    bool ok = false;
    TRANS_LOG(INFO, "check tx table minor freeze finish start");
    while (!ok) {
      common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

      int ret = OB_SUCCESS;
      ObSqlString sql;
      int64_t affected_rows = 0;
      ObString state;

      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select state from oceanbase.__all_virtual_tx_data_table where tenant_id=1002 and ls_id = 1001 and start_scn = %ld and end_scn = %ld", start_log_ts.get_val_for_tx(), end_log_ts.get_val_for_tx()));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_varchar("state", state));
      }

      if (state == "MINI") {
        ok = true;
      } else {
        usleep(1 * 1000 * 1000);
      }
    }

    usleep(5 * 1000 * 1000);

    TRANS_LOG(INFO, "check tx table minor freeze finish end");
  }

  void minor_freeze_tx_data_memtable()
  {
    TRANS_LOG(INFO, "minor_freeze_tx_data_memtable begin");

    ObLS *ls = NULL;
    ObTxDataMemtable *tx_data_memtable = NULL;
    ObMemtableMgrHandle memtable_mgr_handle;
    ObIMemtableMgr *memtable_mgr = nullptr;
    ObTableHandleV2 handle;

    get_ls(1002, ls);

    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_tx_data_memtable_mgr(memtable_mgr_handle));
    memtable_mgr = memtable_mgr_handle.get_memtable_mgr();
    ASSERT_EQ(OB_SUCCESS, memtable_mgr->get_active_memtable(handle));
    ASSERT_EQ(OB_SUCCESS, handle.get_tx_data_memtable(tx_data_memtable));

    tx_data_memtable->dump2text("/tmp/fast_commit_report/tx_data_memtable.txt");
    ASSERT_EQ(OB_SUCCESS, ((ObTxDataMemtableMgr *)(memtable_mgr))->freeze());
    share::SCN start_log_ts =  tx_data_memtable->get_start_scn();
    share::SCN end_log_ts =  tx_data_memtable->get_end_scn();
    ASSERT_EQ(OB_SUCCESS, tx_data_memtable->flush());

    wait_freeze_tx_table_finish(start_log_ts, end_log_ts);

    TRANS_LOG(INFO, "minor_freeze_tx_data_memtable end");
  }
private:

};

TEST_F(ObFastCommitReport, fast_commit_report)
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
  EXE_SQL("create table test_fast_commit (a int)");
  usleep(10 * 1000 * 1000);
  TRANS_LOG(INFO, "create_table end");

  prepare_tenant_env();
  // set_freeze_trigger();
  set_private_buffer_size("2M");

  if (EnumDataMode::ALL_CLEANOUT == DATA_MODE) {
    set_fast_commit_count(TOTAL_FC_ROW_COUNT + 100);
  } else if (EnumDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == DATA_MODE) {
    set_fast_commit_count(0);
  } else if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
    set_fast_commit_count(0);
  }

  if (EnumDataMode::ALL_CLEANOUT == DATA_MODE) {
    // insert_data_single(TOTAL_FC_ROW_COUNT);
    insert_data_pdml(TOTAL_FC_ROW_COUNT);
  } else if (EnumDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == DATA_MODE) {
    // insert_data_single(TOTAL_FC_ROW_COUNT);
    insert_data_pdml(TOTAL_FC_ROW_COUNT);
  } else if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
    insert_data_parallel(TOTAL_FC_SESSION, TOTAL_FC_ROW_COUNT);
  }

  if (EnumTxTableMode::ALL_IN_DISK == TX_TABLE_MODE) {
    minor_freeze_tx_data_memtable();
  }

  TRANS_LOG(INFO, "fast commit report with arguments",
            K(DATA_MODE),
            K(TX_TABLE_MODE));

  if (EnumTestMode::BIANQUE_TEST == TEST_MODE) {
    std::cout << "Master, you should start enterring a number as you like\n";
    int n;
    std::cin >> n;
    std::cout << "Your faithful servant, qianchen is start reading data within 60 seconds\n";
    std::cout << "So prepare for command: sudo perf record -e cycles -c 100000000 -p $(pidof -s test_fast_commit_report) -g -- sleep 30\n";
  }

  while (READ_CNT < 400) {
    read_data(TOTAL_FC_ROW_COUNT);
  }

  std::cout << "Data reading has been finished, you can generate it with command: sudo perf script -i perf.data -F "
               "ip,sym -f > data.viz\n";
  std::cout << "And finally you can graph it with command: `cat data.viz | ./perfdata2graph.py` or our web based perf "
               "tool.\n";

  fprintf(stdout, "average spend time : %ld ms, total read count : %ld\n", TOTAL_READ_TIME / (READ_CNT-100) / 1000, READ_CNT);

  fprintf(stdout,
          "argument : "
          " test_mode = %d, "
          " data_mode = %d, "
          " data_location = %d, "
          " tx_table_mode = %d, "
          " row_cnt = %ld, "
          " session_cnt = %ld"
          " pdml_select_parallel = %ld\n",
          TEST_MODE,
          DATA_MODE,
          DATA_LOCATION,
          TX_TABLE_MODE,
          TOTAL_FC_ROW_COUNT,
          TOTAL_FC_SESSION,
          SELECT_PARALLEL);

  sleep(SLEEP_SECONDS);
}


} // namespace unittest
} // namespace oceanbase

void tutorial()
{
  std::cout << "./mittest/simple_server/test_fast_commit_report -m $1 -d $2 -l $3 -t $4 -r $5 -s $6\n"
            << "-m(test mode): 0 = NORMAL_MODE; 1 = BIANQUE_MODE\n"
            << "-d(data model): 0 = ALL_CLEANOUT; 1 = ALL_SAME_TXN_DELAY_CLEANOUT; 2 = ALL_DIFF_TXN_DELAY_CLEANOUT\n"
            << "-l(data location): 0 = DATA_IN_MEMORY; 1 = DATA_ON_DISK\n"
            << "-t(tx table model): 0 = ALL_IN_MEMORY; 1 = ALL_IN_DISK\n"
            << "-r(row count): n = n row that is read during benchmark\n"
            << "-s(session count): n = n session that is used during insert before benchmark\n";
}

int main(int argc, char **argv)
{
  using namespace oceanbase::unittest;

  int c = 0;
  while(EOF != (c = getopt(argc,argv,":h:m:d:l:t:r:s:p:S:"))) {
    switch(c) {
    case 'h':
      tutorial();
      return 0;
    case 'm':
      TEST_MODE = (EnumTestMode)atoi(optarg);
      break;
    case 'd':
      DATA_MODE = (EnumDataMode)atoi(optarg);
      break;
    case 'l':
      DATA_LOCATION = (EnumDataLocation)atoi(optarg);
      break;
    case 't':
      TX_TABLE_MODE = (EnumTxTableMode)atoi(optarg);
      break;
    case 'r':
      TOTAL_FC_ROW_COUNT = (int64_t)atoi(optarg);
      break;
    case 's':
      TOTAL_FC_SESSION = (int64_t)atoi(optarg);
      break;
    case 'p':
      SELECT_PARALLEL = (int64_t)atoi(optarg);
      break;
    case 'S':
      SLEEP_SECONDS = (int64_t)atoi(optarg);
      break;
    default:
      tutorial();
      return 0;
    }
  }

  if (EnumDataLocation::DATA_ON_DISK == DATA_LOCATION) {
    if (EnumDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == DATA_MODE) {
      fprintf(stdout, "Attention!! Not Support Diff Tx!!\n");
      DATA_MODE = EnumDataMode::ALL_SAME_TXN_DELAY_CLEANOUT;
    }
    TOTAL_FC_SESSION = 1;
  }

  if (TEST_MODE >= EnumTestMode::MAX_TEST_MODE || DATA_MODE >= EnumDataMode::MAX_DATA_MODE ||
      TX_TABLE_MODE >= EnumTxTableMode::MAX_TX_TABLE_MODE || DATA_LOCATION >= EnumDataLocation::MAX_DATA_LOCATION ||
      SELECT_PARALLEL <= 0 || SELECT_PARALLEL > 128) {
    fprintf(stdout, "invalid argument for fast commit report test. ");
    tutorial();
    return 0;
  }

  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
