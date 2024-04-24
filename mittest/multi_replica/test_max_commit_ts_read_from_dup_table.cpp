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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_dup_table_lease.h"
#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx/ob_tx_replay_executor.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObDupTableMaxCommitTsRead

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_max_commit_ts_read_from_dup_table);

namespace oceanbase
{
namespace transaction
{

static bool STOP_TX_REPLAY = false;
static bool BLOCK_DUP_TABLE_LEADER_REVOKE = false;
static bool RETURN_NULL_GTS_CACHE = false;
static sqlclient::ObISQLConnection *static_conn = nullptr;
static sqlclient::ObMySQLResult *static_result = nullptr;
static int64_t final_row_count = 0;

int ObTxReplayExecutor::errsim_tx_replay_()
{
  int ret = OB_SUCCESS;

  if (STOP_TX_REPLAY) {
    ret = OB_EAGAIN;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "[ERRSIM] errsim tx replay in test", K(ret));
  }
  return ret;
}

int ObDupTableLSHandler::errsim_leader_revoke_()
{
  int ret = OB_SUCCESS;

  while (BLOCK_DUP_TABLE_LEADER_REVOKE) {
    usleep(1000 * 1000);
    TRANS_LOG(INFO, "[ERRSIM] errsim wait leader revoke", K(ret));
  }

  return ret;
}

} // namespace transaction

namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

struct TableBasicArg
{
  uint64_t tenant_id_;

  int64_t dup_ls_id_num_;
  int64_t dup_table_id_;
  ObSEArray<int64_t, 10> dup_tablet_id_array_;

  int64_t normal_ls_id_num_;
  int64_t normal_table_id_;
  ObSEArray<int64_t, 10> normal_tablet_id_array_;

  TO_STRING_KV(K(tenant_id_),
               K(dup_ls_id_num_),
               K(dup_table_id_),
               K(normal_ls_id_num_),
               K(normal_table_id_),
               K(dup_tablet_id_array_),
               K(normal_tablet_id_array_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(TableBasicArg,
                    tenant_id_,
                    dup_ls_id_num_,
                    dup_table_id_,
                    dup_tablet_id_array_,
                    normal_ls_id_num_,
                    normal_table_id_,
                    normal_tablet_id_array_);

static TableBasicArg static_basic_arg_;

const std::string test_dup_table_name = "test_dup_1";
const std::string test_normal_table_name = "test_normal_1";
const int64_t DEFAULT_LOAD_ROW_CNT = 10;

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;

  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());
  WRITE_SQL_BY_CONN(sys_conn, "alter system set _private_buffer_size = '1B';");
  std::string ls_id_str = std::to_string(1);
  std::string target_ip = local_ip_ + ":" + std::to_string(rpc_ports_[1]);
  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + target_ip + "' tenant='tt1';";

  WRITE_SQL_BY_CONN(sys_conn, switch_leader_sql.c_str());

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone2; zone3';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());

  unittest::TestEnvTool::create_table_for_test_env(
      test_conn, test_dup_table_name.c_str(), 10, true /*is_dup_table*/,
      static_basic_arg_.dup_ls_id_num_, static_basic_arg_.dup_table_id_,
      static_basic_arg_.dup_tablet_id_array_);

  unittest::TestEnvTool::create_table_for_test_env(
      test_conn, test_normal_table_name.c_str(), 10, false /*is_dup_table*/,
      static_basic_arg_.normal_ls_id_num_, static_basic_arg_.normal_table_id_,
      static_basic_arg_.normal_tablet_id_array_);

  GET_LS(test_tenant_id, static_basic_arg_.dup_ls_id_num_, ls_handle);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before wait dup tablet discover", K(ret),
             K(static_basic_arg_));
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
                          == static_basic_arg_.dup_tablet_id_array_.count(),
                      20 * 1000 * 1000, 100 * 1000);
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_set_count()
          >= 1,
      20 * 1000 * 1000, 100 * 1000);
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()
              ->dup_table_ls_handler_.tablets_mgr_ptr_->get_need_confirm_tablet_set_count()
          == 0,
      20 * 1000 * 1000, 100 * 1000);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- after wait dup tablet discover", K(ret),
             K(static_basic_arg_),
             K(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()));
  ASSERT_EQ(OB_SUCCESS, ret /*has_dup_tablet*/);

  WRITE_SQL_BY_CONN(test_conn, "set autocommit = false;");
  WRITE_SQL_BY_CONN(test_conn, "begin;");

  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_dup_sql_str =
        "INSERT INTO " + test_dup_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
    std::string insert_normal_sql_str =
        "INSERT INTO " + test_normal_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_dup_sql_str.c_str());
    WRITE_SQL_BY_CONN(test_conn, insert_normal_sql_str.c_str());
  }
  WRITE_SQL_BY_CONN(test_conn, "commit;");

  static_basic_arg_.tenant_id_ = test_tenant_id;
  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<TableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  finish_event("CREATE_TEST_TABLE", tmp_str);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), normal_follower_max_commit_ts_read)
{
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_TEST_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<TableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  std::string SELECT_SQL_ON_DUP_TABLE = "select count(*) from " + test_dup_table_name;

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  READ_SQL_BY_CONN(test_conn, table_info_result, SELECT_SQL_ON_DUP_TABLE.c_str());
  ASSERT_EQ(OB_SUCCESS, table_info_result->next());

  int64_t dup_table_row_count = 0;
  const int64_t col_index = 0;
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int(col_index, dup_table_row_count));
  ASSERT_EQ(dup_table_row_count, DEFAULT_LOAD_ROW_CNT);

  finish_event("NORMAL_FOLLOWER_LOCAL_READ", "");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), switch_follwer_forcedly_to_zone2_and_stop_replay)
{

  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("NORMAL_FOLLOWER_LOCAL_READ", tmp_event_val, 30 * 60 * 1000));
  // refresh location cache
  {
    std::string SELECT_SQL_ON_DUP_TABLE = "select count(*) from " + test_dup_table_name;

    common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

    ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

    READ_SQL_BY_CONN(test_conn, table_info_result, SELECT_SQL_ON_DUP_TABLE.c_str());
    ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  }

  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("PREPARE_TO_UPDATE_ON_NEW_LEADER", tmp_event_val, 30 * 60 * 1000));
  STOP_TX_REPLAY = true;
  BLOCK_DUP_TABLE_LEADER_REVOKE = true;
  block_msg_ = true;

  // usleep(6*1000*1000);
  finish_event("STOP_ZONE1", "");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), update_on_new_leader)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;

  finish_event("PREPARE_TO_UPDATE_ON_NEW_LEADER", "");
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("STOP_ZONE1", tmp_event_val, 30 * 60 * 1000));

  {
    std::string SELECT_SQL_ON_DUP_TABLE = "select count(*) from " + test_dup_table_name;

    common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

    ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

    READ_SQL_BY_CONN(test_conn, table_info_result, SELECT_SQL_ON_DUP_TABLE.c_str());
    ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  }

  {
    GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.dup_ls_id_num_, ls_handle);
    RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    ASSERT_EQ(OB_SUCCESS, ret);
    RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->ls_tx_svr_.mgr_->in_leader_serving_state(),
                        20 * 1000 * 1000, 100 * 1000);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  sleep(2);
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

    WRITE_SQL_BY_CONN(test_conn, "set autocommit = false;");
    WRITE_SQL_BY_CONN(test_conn, "set ob_trx_timeout = 1000000000;")

    WRITE_SQL_BY_CONN(test_conn, "begin;");

    int64_t tmp_tx_id = 0;
    for (int i = DEFAULT_LOAD_ROW_CNT + 1; i <= DEFAULT_LOAD_ROW_CNT + 3; i++) {
      std::string insert_dup_sql_str =
          "INSERT INTO " + test_dup_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
      // std::string insert_normal_sql_str =
      //     "INSERT INTO " + test_normal_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
      WRITE_SQL_BY_CONN(test_conn, insert_dup_sql_str.c_str());
      // GET_TX_ID_FROM_SQL_AUDIT(test_conn, insert_dup_sql_str, tmp_tx_id);
      // WRITE_SQL_BY_CONN(test_conn, insert_normal_sql_str.c_str());
    }

    // GET_RUNNGING_TRX_ID(test_conn, tmp_tx_id)
    WRITE_SQL_BY_CONN(test_conn, "commit;");
  }

  std::string SELECT_SQL_ON_DUP_TABLE = "select count(*) from " + test_dup_table_name;
  {
    ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

    READ_SQL_BY_CONN(test_conn, table_info_result, SELECT_SQL_ON_DUP_TABLE.c_str());
    ASSERT_EQ(OB_SUCCESS, table_info_result->next());

    int64_t dup_table_row_count = 0;
    const int64_t col_index = 0;
    ASSERT_EQ(OB_SUCCESS, table_info_result->get_int(col_index, dup_table_row_count));
    ASSERT_EQ(dup_table_row_count, DEFAULT_LOAD_ROW_CNT + 3);
  }

  finish_event("UPDATE_ON_NEW_LEADER", "");
  // ob_abort();
}

void read_in_new_thread()
{
  int ret = OB_SUCCESS;
  std::string SELECT_SQL_ON_DUP_TABLE = "select count(*) from " + test_dup_table_name
                                        + " where id_x=" + std::to_string(DEFAULT_LOAD_ROW_CNT + 2);
  int64_t tmp_tx_id = 0;
  {
    share::ObTenantSwitchGuard tenant_guard;
    ObTsSourceInfoGuard info_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(static_basic_arg_.tenant_id_));
    MTL(ObTxLoopWorker *)->stop();
    usleep(1 * 1000 * 1000);

    // share::SCN max_commit_ts_5_min =
    //     share::SCN::minus(MTL(ObTransService *)->get_tx_version_mgr().get_max_commit_ts(false),
    //                       3 * 1000 * 1000 * 1000L);
    // TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print max commit ts", K(ret),
    //           K(MTL(ObTransService *)->get_tx_version_mgr().get_max_commit_ts(false)),
    //           K(max_commit_ts_5_min));
    // MTL(ObTransService *)->get_tx_version_mgr().max_commit_ts_ = max_commit_ts_5_min;
    // ((ObTsMgr*)(MTL(ObTransService
    // *)->ts_mgr_))->get_ts_source_info_opt_(static_basic_arg_.tenant_id_, info_guard, true, true);
    // int64_t tmp_gts  = 0 ;
    // ASSERT_EQ(OB_SUCCESS, max_commit_ts_5_min.convert_for_gts(tmp_gts));
    // info_guard.get_ts_source_info()->get_gts_source()->gts_local_cache_.gts_ =
    // max_commit_ts_5_min.get_val_for_gts();
    //  info_guard.get_ts_source_info()->get_gts_source()->gts_local_cache_.srr_ =
    //  MonotonicTs::current_time();
    //  info_guard.get_ts_source_info()->get_gts_source()->gts_local_cache_.srr_.mts_ +=
    //  1*1000*1000;

    RETURN_NULL_GTS_CACHE = true;
    const int64_t col_index = 0;
    READ_SQL_BY_CONN(static_conn, table_info_result, SELECT_SQL_ON_DUP_TABLE.c_str());
    if (OB_FAIL(table_info_result->next())) {

      TRANS_LOG(WARN, "[ObMultiReplicaTestBase] get next in new thread failed", K(ret),
                K(final_row_count));
      final_row_count = -1;

    } else if (OB_FAIL(table_info_result->get_int(col_index, final_row_count))) {
      TRANS_LOG(WARN, "[ObMultiReplicaTestBase] get count(*) in new thread failed", K(ret),
                K(final_row_count));
      final_row_count = -1;
    }
  }
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] after read in new thread", K(ret), K(final_row_count),
            K(SELECT_SQL_ON_DUP_TABLE.c_str()));
  GET_TX_ID_FROM_SQL_AUDIT(static_conn, SELECT_SQL_ON_DUP_TABLE, tmp_tx_id);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), read_from_old_leader_zone1)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("UPDATE_ON_NEW_LEADER", tmp_event_val, 30 * 60 * 1000, 5 * 1000));
  block_msg_ = false;
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  // refresh location cache
  {
    std::string SELECT_SQL_ON_DUP_TABLE = "select count(*) from " + test_dup_table_name;

    common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

    ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

    READ_SQL_BY_CONN(test_conn, table_info_result, SELECT_SQL_ON_DUP_TABLE.c_str());
    ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  }

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] 3 - Stop blocking msg during the read operation",
            K(ret), K(final_row_count));
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  static_conn = test_conn;
  WRITE_SQL_BY_CONN(test_conn, "set ob_query_timeout=20000000;");

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] 2 - Stop blocking msg during the read operation",
            K(ret), K(final_row_count));
  std::thread read_thread(read_in_new_thread);
  std::thread::id tid = read_thread.get_id();
  uint64_t read_thread_id = *(uint64_t *)(&(tid));
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] 1 - Stop blocking msg during the read operation",
            K(ret), K(read_thread_id), K(final_row_count));

  share::ObTenantSwitchGuard tenant_guard;
  ObTsSourceInfoGuard info_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(static_basic_arg_.tenant_id_));
  int64_t start_ts = ObTimeUtility::fast_current_time();
  while (ObTimeUtility::fast_current_time() - start_ts >= 3 * 1000 * 1000) {
    share::SCN max_commit_ts_5_min =
        share::SCN::minus(MTL(ObTransService *)->get_tx_version_mgr().get_max_commit_ts(false),
                          3 * 1000 * 1000 * 1000L);
    MTL(ObTransService *)->get_tx_version_mgr().max_commit_ts_ = max_commit_ts_5_min;
    info_guard.get_ts_source_info()->get_gts_source()->gts_local_cache_.gts_ =
        max_commit_ts_5_min.get_val_for_gts();
    usleep(3 * 1000);
  }

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Stop blocking msg during the read operation", K(ret),
            K(read_thread_id), K(final_row_count));

  read_thread.join();

  ASSERT_EQ(final_row_count, 1);

  // usleep(1000 * 1000 * 1000);
}

} // namespace unittest

} // namespace oceanbase
