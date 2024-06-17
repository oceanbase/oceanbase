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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_test_base.h"
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"

#define CUR_TEST_CASE_NAME ObTxLsState

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_tx_ls_state_switch_);

namespace oceanbase
{

static bool errsim_switch_follower_ = false;
static bool errsim_apply_SWL_ = false;
static bool block_start_working_submitting_ = false;
static share::ObLSID errsim_dup_ls_id_;
static share::ObLSID errsim_normal_ls_id_;

namespace transaction
{

OB_NOINLINE int ObLSTxCtxMgr::errsim_switch_to_followr_gracefully()
{
  int ret = OB_SUCCESS;
  if (errsim_switch_follower_) {
    ret = OB_TIMEOUT;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "errsim in switch_to_follower_gracefully", K(ret), K(errsim_switch_follower_),
              K(errsim_normal_ls_id_), K(errsim_dup_ls_id_), KPC(this));
  }
  return ret;
}

OB_NOINLINE int ObLSTxCtxMgr::errsim_submit_start_working_log()
{
  int ret = OB_SUCCESS;

  TRANS_LOG(WARN, "[ObMultiReplicaTestBase] errsim for submit_start_working_log", K(ret),
            KPC(this));

  while (block_start_working_submitting_) {
    usleep(1000 * 1000);

    TRANS_LOG(WARN, "[ObMultiReplicaTestBase] errsim for submit_start_working_log", K(ret),
              K(block_start_working_submitting_), KPC(this));
  }

  return ret;
}

OB_NOINLINE int ObLSTxCtxMgr::errsim_apply_start_working_log()
{
  int ret = OB_SUCCESS;

  if (errsim_apply_SWL_) {
    ret = OB_TIMEOUT;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "errsim in  on_start_working_log_cb_succ", K(ret), K(errsim_apply_SWL_),
              K(errsim_normal_ls_id_), K(errsim_dup_ls_id_), KPC(this));
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

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;

  const std::string test_dup_table_name = "test_dup_1";
  const std::string test_normal_table_name = "test_normal_1";

  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone3; zone2';";
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

  const int64_t DEFAULT_LOAD_ROW_CNT = 10;
  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_dup_sql_str =
        "INSERT INTO " + test_dup_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
    std::string insert_normal_sql_str =
        "INSERT INTO " + test_normal_table_name + " VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_dup_sql_str.c_str());
    WRITE_SQL_BY_CONN(test_conn, insert_normal_sql_str.c_str());
  }
  WRITE_SQL_BY_CONN(test_conn, "commit;");

  errsim_apply_SWL_ = true;

  static_basic_arg_.tenant_id_ = test_tenant_id;
  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<TableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  finish_event("CREATE_TEST_TABLE", tmp_str);
}

void switch_leader_and_check(sqlclient::ObISQLConnection *test_conn,
                             const int64_t tenant_id,
                             const int64_t ls_id_num,
                             const std::string local_ip,
                             const std::string target_ip,
                             const bool is_dup_ls = false)
{
  int ret = OB_SUCCESS;

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Start switching leader to local server", K(ret),
            K(tenant_id), K(ls_id_num), K(local_ip.c_str()), K(target_ip.c_str()), K(is_dup_ls));

  GET_LS(tenant_id, ls_id_num, ls_handle);
  if (local_ip == target_ip) {
    if (is_dup_ls) {
      RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                          100 * 1000);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->ls_tx_svr_.mgr_->is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::string ls_id_str = std::to_string(ls_id_num);

  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + target_ip + "' tenant='tt1';";

  WRITE_SQL_BY_CONN(test_conn, switch_leader_sql.c_str());
  if (local_ip == target_ip) {
    if (is_dup_ls) {
      RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                          100 * 1000);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->ls_tx_svr_.mgr_->is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Finish switching leader to local server", K(ret),
            K(tenant_id), K(ls_id_num), K(local_ip.c_str()), K(target_ip.c_str()), K(is_dup_ls));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), switch_leader_to_zone2)
{
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_TEST_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<TableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  std::string target_ip = local_ip_ + ":" + std::to_string(rpc_ports_[1]);
  switch_leader_and_check(test_conn, static_basic_arg_.tenant_id_,
                          static_basic_arg_.normal_ls_id_num_, target_ip, target_ip, false);
  switch_leader_and_check(test_conn, static_basic_arg_.tenant_id_, static_basic_arg_.dup_ls_id_num_,
                          target_ip, target_ip, true);

  ASSERT_EQ(OB_SUCCESS, finish_event("SWITCH_LEADER_TO_ZONE2_GRACEFULLY", ""));
}

bool check_follower_with_lock(ObLS *ls)
{
  bool is_follower = false;
  if (ls->ls_tx_svr_.mgr_->rwlock_.try_wrlock()) {
    is_follower = ls->ls_tx_svr_.mgr_->tx_ls_state_mgr_.is_follower();
    ls->ls_tx_svr_.mgr_->rwlock_.wrunlock();
  }

  return is_follower;
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), switch_follower_failed_from_zone2_with_start_working_on_failure)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("SWITCH_LEADER_TO_ZONE2_GRACEFULLY", tmp_event_val, 30 * 60 * 1000));
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.dup_ls_id_num_, dup_ls_handle);
  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.normal_ls_id_num_, normal_ls_handle);

  errsim_dup_ls_id_ = share::ObLSID(static_basic_arg_.dup_ls_id_num_);
  errsim_normal_ls_id_ = share::ObLSID(static_basic_arg_.normal_ls_id_num_);

  // switch to follower with timeout;
  // resume leader and submit start_working;
  std::string target_ip = local_ip_ + ":" + std::to_string(rpc_ports_[0]);
  errsim_switch_follower_ = true;

  switch_leader_and_check(test_conn, static_basic_arg_.tenant_id_,
                          static_basic_arg_.normal_ls_id_num_, "", target_ip, false);
  switch_leader_and_check(test_conn, static_basic_arg_.tenant_id_, static_basic_arg_.dup_ls_id_num_,
                          "", target_ip, true);

  share::SCN normal_applyied_SWL_scn =
      normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.max_applied_start_working_ts_;
  usleep(50 * 1000);
  RETRY_UNTIL_TIMEOUT(check_follower_with_lock(normal_ls_handle.get_ls()), 10 * 1000 * 1000,
                      100 * 1000);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] zone2 can not become a follower", K(ret),
            KPC(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_));
  ASSERT_EQ(ret, OB_TIMEOUT);

  ret = OB_SUCCESS;
  ASSERT_EQ(
      normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.max_applied_start_working_ts_
          > normal_applyied_SWL_scn,
      true);

  RETRY_UNTIL_TIMEOUT(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.is_follower(),
                      30 * 1000 * 1000, 1 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);
  block_start_working_submitting_ = true;

  // block msg with a busy start_working_cb
  ATOMIC_STORE(&block_msg_, true);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Start to block msg", K(ret),
            KPC(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_));

  finish_event("BLOCK_ZONE2_MSG_PROCESS", "");

  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("BLOCK_ZONE2_MSG_PROCESS", tmp_event_val, 30 * 60 * 1000));

  usleep(1 * 1000 * 1000);
  block_start_working_submitting_ = false;

  RETRY_UNTIL_TIMEOUT(
      !normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->ls_log_writer_.start_working_cbs_.is_empty(),
      5 * 1000 * 1000, 10 * 1000);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] wait a pending start_working_cb", K(ret),
            KPC(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->is_r_pending_(), true);
  // wait election lease expired
  usleep(15 * 1000 * 1000);

  finish_event("ZONE1_SUBMIT_LAST_START_WORKING", "");

  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("ZONE1_BECOME_LEADER_IN_BLOCK_MSG", tmp_event_val, 30 * 60 * 1000));
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] The zone 1 has been a new leader", K(ret),
            KPC(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_));
  ASSERT_EQ(ret, OB_SUCCESS);

  // usleep(2 * 1000 * 1000);

  ATOMIC_STORE(&block_msg_, false);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Finish to block msg", K(ret),
            KPC(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_));

  // TODO: start_working on failure
  // RETRY_UNTIL_TIMEOUT(
  //     normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.cur_state_.state_val_.state_
  //         == ObTxLSStateMgr::TxLSState::R_SYNC_FAILED,
  //     5 * 1000 * 1000, 10 * 1000);
  // ASSERT_EQ(ret, OB_SUCCESS);

  RETRY_UNTIL_TIMEOUT(
      normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.cur_state_.state_val_.state_
          == ObTxLSStateMgr::TxLSState::F_WORKING,
      5 * 1000 * 1000, 10 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  errsim_switch_follower_ = false;

  finish_event("SWITCH_FOLLOWER_FAILED_FROM_ZONE2", "");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), switch_leader_to_zone1_with_start_working_error)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;

  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("BLOCK_ZONE2_MSG_PROCESS", tmp_event_val, 30 * 60 * 1000));
  ATOMIC_STORE(&block_msg_, true);

  finish_event("BLOCK_ZONE1_MSG_PROCESS", "");

  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("ZONE1_SUBMIT_LAST_START_WORKING", tmp_event_val, 30 * 60 * 1000));
  ATOMIC_STORE(&block_msg_, false);

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.normal_ls_id_num_, normal_ls_handle);
  // switch to leader and submit start_working log
  RETRY_UNTIL_TIMEOUT(
      normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.is_start_working_apply_pending(),
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(OB_SUCCESS, ret);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] The zone 1 has been a new leader", K(ret),
            KPC(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_));
  finish_event("ZONE1_BECOME_LEADER_IN_BLOCK_MSG", "");

  RETRY_UNTIL_TIMEOUT(normal_ls_handle.get_ls()->ls_tx_svr_.mgr_->tx_ls_state_mgr_.is_master(),
                      20 * 1000 * 1000, 1 * 1000 * 1000);
  ASSERT_EQ(OB_TIMEOUT, ret);

  finish_event("KEEP_SWL_PENDING_IN_ZONE1", "");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), transfer_with_block_normal)
{
  // TODO
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), offline_ls_with_retry)
{
  // TODO
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), gc_ls)
{
  // TODO
}

} // namespace unittest
} // namespace oceanbase
