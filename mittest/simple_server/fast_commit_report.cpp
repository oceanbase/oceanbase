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
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

static const char *TEST_FILE_NAME = "fast_commit_report";
const int64_t TOTAL_FC_ROW_COUNT = 1000000;
const int64_t TOTAL_FC_SESSION = 10;
const int64_t PAINTING_FC_ROW_COUNT = 10000;

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
      } else if (OB_FAIL(version_iter_->is_lock_node(is_lock_node))) {
        TRANS_LOG(WARN, "fail to check is lock node", K(ret), K(*version_iter_));
      } else if (!(version_iter_->is_aborted()              // skip abort version
                   || is_lock_node
                   || (NDT_COMPACT == version_iter_->type_
                       && skip_compact_))) {
        tnode = static_cast<const void *>(version_iter_);
      }

      move_to_next_node_();
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

enum FastCommitTestMode : int
{
  NORMAL_TEST = 0,
  BIANQUE_TEST = 1
};

enum FastCommitReportDataMode : int
{
  ALL_CLEANOUT = 0,
  ALL_SAME_TXN_DELAY_CLEANOUT = 1,
  ALL_DIFF_TXN_DELAY_CLEANOUT = 2
};

enum FastCommitReportTxTableMode : int
{
  ALL_IN_MEMORY = 0,
  ALL_IN_DISK = 1
};

FastCommitTestMode fast_commit_test_mode = FastCommitTestMode::NORMAL_TEST;
FastCommitReportDataMode fast_commit_data_mode = FastCommitReportDataMode::ALL_CLEANOUT;
FastCommitReportTxTableMode fast_commit_tx_table_mode = FastCommitReportTxTableMode::ALL_IN_MEMORY;
int64_t total_fc_row_count = TOTAL_FC_ROW_COUNT;
int64_t total_fc_session = TOTAL_FC_SESSION;

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

  void set_memstore_limit()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL("alter system set memstore_limit_percentage = 80;");
  }

  void set_freeze_trigger()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL("alter system set writing_throttling_trigger_percentage = 100;");
    EXE_SQL("alter system set freeze_trigger_percentage = 80;");
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

  void insert_data_single(const int row_count = oceanbase::unittest::total_fc_row_count)
  {
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

    if (FastCommitReportDataMode::ALL_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
      WRITE_SQL_BY_CONN(connection, "begin;");
    } else if (FastCommitReportDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
      WRITE_SQL_BY_CONN(connection, "begin;");
    } else if (FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
      // pass
    }
    for (int i = 0; i < row_count; i++) {
      if (FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
        WRITE_SQL_BY_CONN(connection, "begin;");
      }
      // const int64_t single_begin_time = ObTimeUtility::current_time();
      WRITE_SQL_FMT_BY_CONN(connection, "insert into test_fast_commit values(1);");
      if (FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
        WRITE_SQL_BY_CONN(connection, "commit;");
      }
      // const int64_t single_end_time = ObTimeUtility::current_time();
      // TRANS_LOG(INFO, "single insert data single cost", K(single_end_time - single_begin_time));
      if ((i + 1) % PAINTING_FC_ROW_COUNT == 0) {
        TRANS_LOG(INFO, "insert data single pass one round",
                  K(oceanbase::unittest::fast_commit_data_mode),
                  K(oceanbase::unittest::fast_commit_tx_table_mode),
                  K(i + 1));
      }
    }
    if (FastCommitReportDataMode::ALL_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
      WRITE_SQL_BY_CONN(connection, "commit;");
    } else if (FastCommitReportDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
      WRITE_SQL_BY_CONN(connection, "commit;");
    } else if (FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
      // pass
    }
    const int64_t end_time = ObTimeUtility::current_time();

    TRANS_LOG(INFO, "insert data single cost", K(end_time - begin_time), K(begin_time), K(end_time));
  }

  void insert_data_parallel(const int parrallel_num = oceanbase::unittest::total_fc_session,
                            const int row_count = oceanbase::unittest::total_fc_row_count)
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

  void read_data(const int row_count = oceanbase::unittest::total_fc_row_count)
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
    READ_SQL_BY_CONN(connection, "select count(*) as cnt from test_fast_commit");
    const int64_t end_time = ObTimeUtility::current_time();
    TRANS_LOG(INFO, "read data cost", K(end_time - begin_time), K(begin_time), K(end_time));
    std::cout << "read data cost(total_cost=" << (end_time - begin_time)
              << ", begin_time=" << begin_time
              << ", end_time=" << end_time << ")\n";

    sqlclient::ObMySQLResult *result = read_res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));

    ASSERT_EQ(cnt, row_count);
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
  set_freeze_trigger();
  set_private_buffer_size("2M");

  if (FastCommitReportDataMode::ALL_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    set_fast_commit_count(oceanbase::unittest::total_fc_row_count + 100);
  } else if (FastCommitReportDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    set_fast_commit_count(0);
  } else if (FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    set_fast_commit_count(0);
  }

  if (FastCommitReportDataMode::ALL_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    insert_data_parallel();
  } else if (FastCommitReportDataMode::ALL_SAME_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    insert_data_single();
  } else if (FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    insert_data_parallel();
  }

  check_no_minor_freeze();

  if (FastCommitReportDataMode::ALL_CLEANOUT == oceanbase::unittest::fast_commit_data_mode) {
    check_memtable_cleanout(true,  /*is_all_cleanout*/
                            false, /*is_all_delay_cleanout*/
                            oceanbase::unittest::total_fc_row_count);
  } else {
    check_memtable_cleanout(false,  /*is_all_cleanout*/
                            true, /*is_all_delay_cleanout*/
                            oceanbase::unittest::total_fc_row_count);
  }

  if (FastCommitReportTxTableMode::ALL_IN_DISK == oceanbase::unittest::fast_commit_tx_table_mode) {
    minor_freeze_tx_data_memtable();
  }

  TRANS_LOG(INFO, "fast commit report with arguments",
            K(oceanbase::unittest::fast_commit_data_mode),
            K(oceanbase::unittest::fast_commit_tx_table_mode));

  if (oceanbase::unittest::FastCommitTestMode::NORMAL_TEST == oceanbase::unittest::fast_commit_test_mode) {
    // check with 3 times
    read_data();
    read_data();
    read_data();
  } else if (oceanbase::unittest::FastCommitTestMode::BIANQUE_TEST == oceanbase::unittest::fast_commit_test_mode) {
    std::cout << "Master, you should start enterring a number as you like\n";
    int n;
    std::cin >> n;
    std::cout << "Your faithful servant, qianchen is start reading data within 60 seconds\n";
    std::cout << "So prepare for command: sudo perf record -e cycles -c 100000000 -p $(pidof -s test_fast_commit_report) -g -- sleep 30\n";
    int64_t base_ts = OB_TSC_TIMESTAMP.current_time();

    while (OB_TSC_TIMESTAMP.current_time() - base_ts <= 60 * 1000 * 1000) {
      read_data();
    }

    std::cout << "Data reading has been finished, you can generate it with command: sudo perf script -i perf.data -F ip,sym -f > data.viz\n";
    std::cout << "And finally you can graph it with command: `cat data.viz | ./perfdata2graph.py` or our web based perf tool.\n";
  }
}


} // namespace unittest
} // namespace oceanbase

void tutorial()
{
  std::cout << "./mittest/simple_server/test_fast_commit_report -m $1 -d $2 -t $3 -r $4 -s $5\n"
            << "-m(mode): 0 = NORMAL_MODE; 1 = BIANQUE_MODE\n"
            << "-d(data model): 0 = ALL_CLEANOUT; 1 = ALL_SAME_TXN_DELAY_CLEANOUT; 2 = ALL_DIFF_TXN_DELAY_CLEANOUT\n"
            << "-t(tx data model): 0 = ALL_IN_MEMORY; 1 = ALL_IN_DISK\n"
            << "-r(row count): n = n row that is read during benchmark\n"
            << "-s(session count): n = n session that is used during insert before benchmark\n";
}

int main(int argc, char **argv)
{
  int c = 0;
  while(EOF != (c = getopt(argc,argv,"h:m:d:t:r:s:"))) {
    switch(c) {
    case 'h':
      tutorial();
      return 0;
    case 'm':
      fprintf(stdout, "m : %s\n", optarg);
      oceanbase::unittest::fast_commit_test_mode = (oceanbase::unittest::FastCommitTestMode)atoi(optarg);
      break;
    case 'd':
      oceanbase::unittest::fast_commit_data_mode = (oceanbase::unittest::FastCommitReportDataMode)atoi(optarg);
      break;
    case 't':
      oceanbase::unittest::fast_commit_tx_table_mode = (oceanbase::unittest::FastCommitReportTxTableMode)atoi(optarg);
      break;
    case 'r':
      oceanbase::unittest::total_fc_row_count = (int64_t)atoi(optarg);
      break;
    case 's':
      oceanbase::unittest::total_fc_session = (int64_t)atoi(optarg);
      break;
    default:
      break;
    }
  }

  if (oceanbase::unittest::fast_commit_test_mode != oceanbase::unittest::FastCommitTestMode::NORMAL_TEST
      && oceanbase::unittest::fast_commit_test_mode != oceanbase::unittest::FastCommitTestMode::BIANQUE_TEST) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "wrong choice", K(oceanbase::unittest::fast_commit_test_mode));
    ob_abort();
  }

  if (oceanbase::unittest::fast_commit_data_mode != oceanbase::unittest::FastCommitReportDataMode::ALL_CLEANOUT
      && oceanbase::unittest::fast_commit_data_mode != oceanbase::unittest::FastCommitReportDataMode::ALL_SAME_TXN_DELAY_CLEANOUT
      && oceanbase::unittest::fast_commit_data_mode != oceanbase::unittest::FastCommitReportDataMode::ALL_DIFF_TXN_DELAY_CLEANOUT) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "wrong choice", K(oceanbase::unittest::fast_commit_data_mode));
    ob_abort();
  }

  if (oceanbase::unittest::fast_commit_tx_table_mode != oceanbase::unittest::FastCommitReportTxTableMode::ALL_IN_MEMORY
      && oceanbase::unittest::fast_commit_tx_table_mode != oceanbase::unittest::FastCommitReportTxTableMode::ALL_IN_DISK) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "wrong choice", K(oceanbase::unittest::fast_commit_tx_table_mode));
    ob_abort();
  }

  if (oceanbase::unittest::total_fc_row_count < oceanbase::unittest::total_fc_session) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "wrong choice", K(oceanbase::unittest::total_fc_row_count), K(oceanbase::unittest::total_fc_session));
    ob_abort();
  }

  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);

  TRANS_LOG(INFO, "fast commit report with arguments",
            K(oceanbase::unittest::fast_commit_test_mode),
            K(oceanbase::unittest::fast_commit_data_mode),
            K(oceanbase::unittest::fast_commit_tx_table_mode),
            K(oceanbase::unittest::total_fc_row_count),
            K(oceanbase::unittest::total_fc_session));

  return RUN_ALL_TESTS();
}
