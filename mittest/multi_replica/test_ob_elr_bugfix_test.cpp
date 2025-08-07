// owner: weixiaoxian.wxx
// owner group: transaction

/*
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

#include <cstdint>
#include <thread>
#include <unistd.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

// #include "storage/tx/ob_tx_log.h"
#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_util.h"
#include "storage/tx/ob_tx_log_adapter.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObElrBugfixTest

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_ob_elr_bugfix_);

namespace oceanbase
{

namespace transaction
{

static bool ENABLE_APPEND_LOG_ERRSIM = false;
static uint64_t TARGET_TX_ID = 0;
static ObTxLogCb *TARGET_LOG_CB_PTR = nullptr;

OB_NOINLINE int ObLSTxLogAdapter::append_log_errsim_(ObTxBaseLogCb *cb)
{
  int ret = OB_SUCCESS;

  // TRANS_LOG(INFO, "append log errsim -- ObMultiReplicaTestBase", K(ret), KPC(cb));
  if (ENABLE_APPEND_LOG_ERRSIM) {
    ObTxLogCb *tx_log_cb = static_cast<ObTxLogCb *>(cb);
    if (is_contain(tx_log_cb->get_cb_arg_array(), ObTxLogType::TX_COMMIT_LOG)) {
      TRANS_LOG(INFO, "[ObMultiReplicaTestBase] START TO BLOCK COMMIT LOG", K(ret), KPC(tx_log_cb));
      while (ENABLE_APPEND_LOG_ERRSIM) {
        usleep(1000);

        uint64_t target_id = ATOMIC_LOAD(&TARGET_TX_ID);
        if (target_id > 0 && tx_log_cb->group_ptr_->get_trans_id().get_id() != target_id) {
          TRANS_LOG(INFO, "skip block commit log", K(ret), K(target_id), KPC(tx_log_cb));
          break;
        } else if (ATOMIC_LOAD(&TARGET_LOG_CB_PTR) == nullptr) {
          ATOMIC_STORE(&TARGET_LOG_CB_PTR, tx_log_cb);
        }
      }
      TRANS_LOG(INFO, "[ObMultiReplicaTestBase] END TO BLOCK COMMIT LOG", K(ret), KPC(tx_log_cb));
    }
  }

  return ret;
}
} // namespace transaction

namespace unittest
{

struct ReplicaEnvArg
{
public:
  uint64_t tenant_id_;
  int64_t ls_id_num_;
  int64_t table_id_;
  int64_t tablet_count_;
  ObSEArray<int64_t, 10> tablet_id_array_;

  TO_STRING_KV(K(tenant_id_), K(ls_id_num_), K(table_id_), K(tablet_count_), K(tablet_id_array_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(ReplicaEnvArg,
                    tenant_id_,
                    ls_id_num_,
                    table_id_,
                    tablet_count_,
                    tablet_id_array_);

static ReplicaEnvArg ENV_ARG;

const std::string tname = "test_dup";

void async_commit(ObISQLConnection *sql_conn)
{
  int ret = OB_SUCCESS;

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] START TO ASYNC_COMMIT", K(ret));
  WRITE_SQL_BY_CONN(sql_conn, "commit;");
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] END TO ASYNC_COMMIT", K(ret));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;

  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone3; zone2';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());

  std::string create_table_sql =
      "CREATE TABLE " + tname
      + "( "
        "id_x int, "
        "id_y int, "
        "id_z int, "
        "PRIMARY KEY(id_x)"
        ") duplicate_scope='cluster' PARTITION BY hash(id_x) partitions 10;";
  std::string get_table_id_sql = "select table_id, duplicate_scope from "
                                 "oceanbase.__all_table where table_name = '"
                                 + tname + "' ";
  WRITE_SQL_BY_CONN(test_conn, create_table_sql.c_str());
  READ_SQL_BY_CONN(test_conn, table_info_result, get_table_id_sql.c_str());
  WRITE_SQL_BY_CONN(sys_conn, "alter system set _private_buffer_size = '2MB';");
  // WRITE_SQL_BY_CONN(sys_conn, "set global ob_enable_trace_log = 1;");

  ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  int64_t table_id = 0;
  int64_t dup_scope = 0;
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("table_id", table_id));
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("duplicate_scope", dup_scope));
  ASSERT_EQ(true, table_id > 0);
  ASSERT_EQ(true, dup_scope != 0);
  ENV_ARG.table_id_ = table_id;

  std::string tablet_count_sql =
      "select count(*), ls_id from oceanbase.__all_tablet_to_ls where table_id = "
      + std::to_string(table_id) + " group by ls_id order by count(*)";
  READ_SQL_BY_CONN(test_conn, tablet_count_result, tablet_count_sql.c_str());
  int64_t tablet_count = 0;
  int64_t ls_id_num = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_count_result->next());
  ASSERT_EQ(OB_SUCCESS, tablet_count_result->get_int("count(*)", tablet_count));
  ASSERT_EQ(OB_SUCCESS, tablet_count_result->get_int("ls_id", ls_id_num));
  ASSERT_EQ(10, tablet_count);
  ASSERT_EQ(true, share::ObLSID(ls_id_num).is_valid());

  ENV_ARG.ls_id_num_ = ls_id_num;
  ENV_ARG.tablet_count_ = tablet_count;

  std::string tablet_id_sql = "select tablet_id from oceanbase.__all_tablet_to_ls where table_id = "
                              + std::to_string(table_id)
                              + " and ls_id = " + std::to_string(ls_id_num);
  READ_SQL_BY_CONN(test_conn, tablet_id_reult, tablet_id_sql.c_str());
  while (OB_SUCC(tablet_id_reult->next())) {
    int64_t id = 0;
    ASSERT_EQ(OB_SUCCESS, tablet_id_reult->get_int("tablet_id", id));
    ASSERT_EQ(true, ObTabletID(id).is_valid());
    ASSERT_EQ(OB_SUCCESS, ENV_ARG.tablet_id_array_.push_back(id));
  }

  ENV_ARG.tenant_id_ = test_tenant_id;

  GET_LS(test_tenant_id, ls_id_num, ls_handle);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before wait dup tablet discover", K(ret),
             K(ls_id_num), K(tablet_count));
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
                          == tablet_count,
                      20 * 1000 * 1000, 100 * 1000);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- after wait dup tablet discover", K(ret),
             K(ls_id_num), K(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()));
  ASSERT_EQ(OB_SUCCESS, ret /*has_dup_tablet*/);

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ReplicaEnvArg>::serialize_arg(ENV_ARG, tmp_str));
  ASSERT_EQ(OB_SUCCESS, finish_event("CREATE_TEST_ENV", tmp_str));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), update_table_with_on_failure)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  WRITE_SQL_BY_CONN(test_conn, "begin;");
  std::string insert_sql = "INSERT INTO " + tname + " VALUES (111, 222, 333)";
  WRITE_SQL_BY_CONN(test_conn, insert_sql.c_str());
  WRITE_SQL_BY_CONN(test_conn, "commit;");

  WRITE_SQL_BY_CONN(test_conn, "begin;");
  std::string update_sql= "UPDATE " + tname + " SET id_x = 333";
  WRITE_SQL_BY_CONN(test_conn, update_sql.c_str());
  int64_t test_tx_id = 0;
  GET_TX_ID_FROM_SQL_AUDIT(test_conn, update_sql.c_str(), test_tx_id)

  ATOMIC_STORE(&TARGET_TX_ID, test_tx_id);

  ENABLE_APPEND_LOG_ERRSIM = true;

  std::thread t1(async_commit, test_conn);

  ObPartTransCtx *tx_ctx = nullptr;
  ObTxLogCb *tx_log_cb = nullptr;
  share::SCN commit_log_scn_1;
  {
    GET_LS(ENV_ARG.tenant_id_, ENV_ARG.ls_id_num_, ls_handle)
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_ctx(ObTransID(test_tx_id), false, tx_ctx));
    RETRY_UNTIL_TIMEOUT(ATOMIC_LOAD(&TARGET_LOG_CB_PTR) != nullptr, 30 * 1000 * 1000L, 100 * 1000);
    tx_log_cb = ATOMIC_LOAD(&TARGET_LOG_CB_PTR);

    tx_ctx->can_elr_ = true;
    tx_ctx->mt_ctx_.set_row_updated();
    ASSERT_EQ(tx_log_cb->group_ptr_->get_tx_ctx()->get_trans_id().get_id(), test_tx_id);
  }

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Start to block msg", K(ret));
  ATOMIC_STORE(&block_msg_, true);
  ENABLE_APPEND_LOG_ERRSIM = false;
  ob_usleep(10 * 1000 * 1000L);

  commit_log_scn_1 = tx_log_cb->get_log_ts();
  ObTxLogCb log_cb_backup;
  log_cb_backup.copy(*tx_log_cb);
  TRANS_LOG(INFO, "1 - print log cb stat", K(ret), K(log_cb_backup), KPC(tx_log_cb));
  tx_ctx->acquire_ctx_ref();
  tx_log_cb->on_failure();
  tx_ctx->free_cbs_.remove(tx_log_cb);
  tx_log_cb->copy(log_cb_backup);
  tx_log_cb->set_busy();
  tx_log_cb->set_callbacked();
  tx_ctx->busy_cbs_.add_last(tx_log_cb);

  TRANS_LOG(INFO, "2 -print log cb stat", K(ret), K(log_cb_backup), KPC(tx_log_cb));
  ATOMIC_STORE(&block_msg_, false);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] End to block msg", K(ret));

  RETRY_UNTIL_TIMEOUT(tx_ctx->exec_info_.state_ >= ObTxState::COMMIT, 30 * 1000 * 1000L,
                      100 * 1000);

  share::SCN commit_log_scn_2 = tx_ctx->ctx_tx_data_.get_end_log_ts();

  tx_ctx->print_trace_log();
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print tx ctx status", K(ret), K(test_tx_id),
            K(commit_log_scn_1), K(commit_log_scn_2));

  RETRY_UNTIL_TIMEOUT(tx_ctx->is_exiting_ == true, 30 * 1000 * 1000, 100 * 1000);

  EXPECT_EQ(commit_log_scn_1 < commit_log_scn_2, true);

  ASSERT_EQ(tx_ctx->ref_, 1);

  t1.join();
  // ASSERT_EQ(tx_ctx->ref_, 2);

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, finish_event("INSERT_WITH_ON_FAILURE", tmp_str));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), submit_keep_alive_on_zone2)
{
  int ret = OB_SUCCESS;

  std::string event_content;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_TEST_ENV", event_content, 5 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ReplicaEnvArg>::deserialize_arg(ENV_ARG, event_content));

  GET_LS(ENV_ARG.tenant_id_, ENV_ARG.ls_id_num_, ls_handle);
  ret = OB_EAGAIN;
  while (ret != OB_SUCCESS) {
    ret = wait_event_finish("INSERT_WITH_ON_FAILURE", event_content, 100);
    BLOCK_MSG_PROCESSOR(5 * 1000 * 1000L);
    // ls_handle.get_ls()->get_keep_alive_ls_handler()->try_submit_log(share::SCN::min_scn(),
    //                                                                 MinStartScnStatus::NO_CTX);
  }
}

} // namespace unittest

} // namespace oceanbase
