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
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_dup_table_tablets.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_replay_executor.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObDupTableRestartCase

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

APPEND_RESTART_TEST_CASE_CLASS(2, 1);

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_dup_table_restart_);

#define DEFAULT_LOAD_ROW_CNT 30

bool STOP_TABLET_LOG_SUBMIT = false;
bool STOP_PREPARE_LOG_SUBMIT = false;
bool STOP_TX_REPLAY = false;

OB_NOINLINE int oceanbase::transaction::ObLSDupTabletsMgr::process_prepare_ser_err_test_()
{
  int ret = OB_SUCCESS;

  if (STOP_TABLET_LOG_SUBMIT) {
    ret = OB_LOG_TOO_LARGE;
  }

  if (OB_FAIL(ret)) {
    SERVER_LOG(INFO, "process prepare ser errsim test", K(ret), K(STOP_TABLET_LOG_SUBMIT));
  }
  return ret;
}

OB_NOINLINE int oceanbase::transaction::ObPartTransCtx::errism_submit_prepare_log_()
{
  int ret = OB_SUCCESS;

  if (STOP_PREPARE_LOG_SUBMIT) {
    ret = OB_EAGAIN;
  }

  if (OB_FAIL(ret)) {
    SERVER_LOG(INFO, "submit prepare log errsim test", K(ret), K(STOP_PREPARE_LOG_SUBMIT));
  }
  return ret;
}

OB_NOINLINE int oceanbase::transaction::ObTxReplayExecutor::errsim_tx_replay_()
{
  int ret = OB_SUCCESS;

  if (STOP_TX_REPLAY) {
    ret = OB_EAGAIN;
  }

  if (OB_FAIL(ret)) {
    SERVER_LOG(INFO, "stop tx replay errsim test", K(ret), K(STOP_TX_REPLAY));
  }

  return ret;
}

namespace oceanbase
{
namespace unittest
{

struct DupTableBasicArg
{
  uint64_t tenant_id_;
  int64_t ls_id_num_;
  int64_t table_id_;
  int64_t tablet_count_;
  ObSEArray<int64_t, 10> tablet_id_array_;

  TO_STRING_KV(K(tenant_id_), K(ls_id_num_), K(table_id_), K(tablet_count_), K(tablet_id_array_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(DupTableBasicArg,
                    tenant_id_,
                    ls_id_num_,
                    table_id_,
                    tablet_count_,
                    tablet_id_array_);

static DupTableBasicArg static_basic_arg_;
sqlclient::ObISQLConnection *static_test_conn_ = nullptr;

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;
  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  WRITE_SQL_BY_CONN(test_conn,
                    "CREATE TABLE test_t1( "
                    "id_x int, "
                    "id_y int, "
                    "id_z int, "
                    "PRIMARY KEY(id_x)"
                    ") duplicate_scope='cluster' PARTITION BY hash(id_x) partitions 10;");

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone3; zone2';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());

  READ_SQL_BY_CONN(test_conn, table_info_result,
                   "select table_id, duplicate_scope from "
                   "oceanbase.__all_table where table_name = 'test_t1' ");

  ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  int64_t table_id;
  int64_t dup_scope;
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("table_id", table_id));
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("duplicate_scope", dup_scope));
  ASSERT_EQ(true, table_id > 0);
  ASSERT_EQ(true, dup_scope != 0);

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

  std::string tablet_id_sql = "select tablet_id from oceanbase.__all_tablet_to_ls where table_id = "
                              + std::to_string(table_id)
                              + " and ls_id = " + std::to_string(ls_id_num);
  READ_SQL_BY_CONN(test_conn, tablet_id_reult, tablet_id_sql.c_str());
  while (OB_SUCC(tablet_id_reult->next())) {
    int64_t id = 0;
    ASSERT_EQ(OB_SUCCESS, tablet_id_reult->get_int("tablet_id", id));
    ASSERT_EQ(true, ObTabletID(id).is_valid());
    ASSERT_EQ(OB_SUCCESS, static_basic_arg_.tablet_id_array_.push_back(id));
  }
  ASSERT_EQ(tablet_count, static_basic_arg_.tablet_id_array_.count());
  ASSERT_EQ(OB_ITER_END, ret);
  ret = OB_SUCCESS;

  GET_LS(test_tenant_id, ls_id_num, ls_handle);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before wait dup tablet discover", K(ret),
             K(ls_id_num), K(tablet_count));
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
                          == tablet_count,
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
             K(ls_id_num), K(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()));
  ASSERT_EQ(OB_SUCCESS, ret /*has_dup_tablet*/);

  WRITE_SQL_BY_CONN(test_conn, "set autocommit = false;");
  WRITE_SQL_BY_CONN(test_conn, "begin;");

  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_sql_str = "INSERT INTO test_t1 VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_sql_str.c_str());
  }

  // int64_t tx_id_num;
  // GET_RUNNGING_TRX_ID(test_conn, tx_id_num);

  WRITE_SQL_BY_CONN(test_conn, "commit;");

  int64_t row_cnt = 0;
  READ_SQL_BY_CONN(test_conn, read_insert_result, "select count(*) from test_t1;");
  ASSERT_EQ(OB_SUCCESS, read_insert_result->next());
  ASSERT_EQ(OB_SUCCESS, read_insert_result->get_int("count(*)", row_cnt));
  ASSERT_EQ(row_cnt, DEFAULT_LOAD_ROW_CNT);

  ASSERT_EQ(STOP_TABLET_LOG_SUBMIT, false);
  STOP_TABLET_LOG_SUBMIT = true;

  // DupTableBasicArg static_basic_arg_;
  static_basic_arg_.tenant_id_ = test_tenant_id;
  static_basic_arg_.ls_id_num_ = ls_id_num;
  static_basic_arg_.table_id_ = table_id;
  static_basic_arg_.tablet_count_ = tablet_count;

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  finish_event("CREATE_DUP_TABLE", tmp_str);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), minor_ls_for_ckpt)
{

  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_DUP_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before wait dup tablet discover", K(ret),
             K(static_basic_arg_));
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
                          == static_basic_arg_.tablet_count_,
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
             K(static_basic_arg_));
  ASSERT_EQ(OB_SUCCESS, ret /*has_dup_tablet*/);

  share::SCN dup_table_max_applying_scn =
      ls_handle.get_ls()->dup_table_ls_handler_.interface_stat_.dup_table_max_applying_scn_;

  ASSERT_EQ(true, dup_table_max_applying_scn.is_valid_and_not_min());

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] before minor freeze ls", K(ret),
            K(dup_table_max_applying_scn), K(ls_handle.get_ls()->get_clog_checkpoint_scn()));

  MINOR_FREEZE_LS(ls_handle.get_ls());
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->get_clog_checkpoint_scn() > dup_table_max_applying_scn,
                      20 * 1000 * 1000, 100 * 1000);

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] after minor freeze ls", K(ret),
            K(dup_table_max_applying_scn), K(ls_handle.get_ls()->get_clog_checkpoint_scn()));

  ASSERT_EQ(restart_zone(2, 1), OB_SUCCESS);
}

bool refresh_and_check_ls_ts_info(ObLS *ls, const share::SCN &applied_scn)
{
  int ret = OB_SUCCESS;
  common::ObAddr target_addr;
  target_addr.set_ip_addr(ObMultiReplicaTestBase::local_ip_.c_str(),
                          ObMultiReplicaTestBase::rpc_ports_[2]);
  // ls->dup_table_ls_handler_.ts_sync_mgr_ptr_->update_all_ts_info_cache();
  oceanbase::transaction::DupTableTsInfo ts_info;
  ret = ls->dup_table_ls_handler_.ts_sync_mgr_ptr_->get_cache_ts_info(target_addr, ts_info);
  TRANS_LOG(INFO, "refresh and check ts info", K(ret), K(applied_scn), K(target_addr), K(ts_info));
  return ts_info.max_replayed_scn_ >= applied_scn;
}

TEST_F(GET_RESTART_ZONE_TEST_CLASS_NAME(2, 1), become_leader_after_restart)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_DUP_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));

  // ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  // common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  // ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  WAIT_START_SERVICE_SUCCC(30 * 1000 * 1000, 100 * 1000);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(static_basic_arg_.tenant_id_));
  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);
  RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);

  std::string ls_id_str = std::to_string(static_basic_arg_.ls_id_num_);
  std::string target_ip = local_ip_ + ":" + std::to_string(rpc_ports_[1]);

  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + target_ip + "' tenant='tt1';";

  common::ObMySQLProxy &sys_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy();
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, sys_tenant_sql_proxy);

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  ASSERT_EQ(STOP_TABLET_LOG_SUBMIT, false);
  STOP_TABLET_LOG_SUBMIT = true;

  // WRITE_SQL_BY_CONN(sys_conn, switch_leader_sql.c_str());
  RETRY_WRITE_SQL_UNTL_SUCC(sys_conn, switch_leader_sql.c_str(), 30 * 1000 * 1000);
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);

  int64_t update_tx_id = 0;
  WRITE_SQL_BY_CONN(test_conn, "begin;");
  std::string update_sql_str = "UPDATE test_t1 set id_z = 999 where id_x = 1";
  WRITE_SQL_BY_CONN(test_conn, update_sql_str.c_str());

  GET_TX_ID_FROM_SQL_AUDIT(test_conn, update_sql_str, update_tx_id);

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] execute update sql after zone2 restarted", K(ret),
            K(update_tx_id), K(update_sql_str.c_str()));

  ASSERT_EQ(STOP_PREPARE_LOG_SUBMIT, false);
  STOP_PREPARE_LOG_SUBMIT = true;

  transaction::ObPartTransCtx *tx_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->get_tx_ctx(transaction::ObTransID(update_tx_id), false, tx_ctx));
  ObTxCommitParts fake_parts;
  ASSERT_EQ(OB_SUCCESS, fake_parts.push_back(ObTxExecPart(share::ObLSID(static_basic_arg_.ls_id_num_), -1, -1)));
  tx_ctx->set_2pc_participants_(fake_parts);
  tx_ctx->submit_redo_commit_info_log_();
  RETRY_UNTIL_TIMEOUT(tx_ctx->busy_cbs_.is_empty(), 20 * 1000 * 1000, 100 * 1000);
  const share::SCN max_applied_scn = tx_ctx->exec_info_.max_applying_log_ts_;

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] commit info log has been synced", K(max_applied_scn),
            KPC(tx_ctx));

  bool res_bool = false;
  RETRY_OP_UNTIL_TIMEOUT(res_bool =
                             refresh_and_check_ls_ts_info(ls_handle.get_ls(), max_applied_scn),
                         res_bool, 20 * 1000 * 1000, 100 * 1000);

  finish_event("RESTART_ZONE2_TO_BE_LEADER", "");

  ASSERT_EQ(OB_SUCCESS, wait_event_finish("DISABLE_ZONE3_REPLAY", tmp_event_val, 30 * 60 * 1000));

  // WRITE_SQL_BY_CONN(test_conn, "commit;");
  ASSERT_EQ(STOP_PREPARE_LOG_SUBMIT, true);
  STOP_PREPARE_LOG_SUBMIT = false;

  RETRY_UNTIL_TIMEOUT(tx_ctx->ctx_tx_data_.get_state() == ObTxCommitData::COMMIT, 20 * 1000 * 1000,
                      100 * 1000);
  RETRY_UNTIL_TIMEOUT(tx_ctx->is_exiting(), 20 * 1000 * 1000, 100 * 1000);

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->revert_tx_ctx(tx_ctx));
  finish_event("UPDATE_ON_ZONE2", "");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(3), select_after_dup_table_update)
{
  int ret = OB_SUCCESS;

  ObDupTableLSLeaseMgr::DEFAULT_LEASE_INTERVAL = ObDupTableLSLeaseMgr::LEASE_UNIT * 600 * 20;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_DUP_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("RESTART_ZONE2_TO_BE_LEADER", tmp_event_val, 30 * 60 * 1000));

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);

  // ASSERT_EQ(ls_handle.get_ls()->disable_replay(), OB_SUCCESS);
  ASSERT_EQ(STOP_TX_REPLAY, false);
  STOP_TX_REPLAY = true;

  finish_event("DISABLE_ZONE3_REPLAY", "");

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  WRITE_SQL_BY_CONN(test_conn, "set ob_query_timeout = 60*1000*1000");

  int64_t select_tx_id = 0;
  int64_t id_Z = 0;
  std::string select_sql_str = "SELECT id_z from test_t1 where id_x = 1";

  // RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
  //                         == static_basic_arg_.tablet_count_,
  //                     20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ls_handle.get_ls()->dup_table_ls_handler_.is_dup_table_lease_valid(), true);
  READ_SQL_BY_CONN_WITH_RET(test_conn, tmp_select_result, select_sql_str.c_str(), ret);
  GET_TX_ID_FROM_SQL_AUDIT(test_conn, select_sql_str, select_tx_id);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] execute select sql after zone2 restarted", K(ret),
            K(select_tx_id), K(select_sql_str.c_str()));

  if (OB_FAIL(ret)) {
    ASSERT_EQ(OB_TIMEOUT, ret);
  } else {
    ASSERT_EQ(OB_SUCCESS, tmp_select_result->next());
    ASSERT_EQ(OB_SUCCESS, tmp_select_result->get_int("id_z", id_Z));
    ASSERT_EQ(id_Z, 999);
    ASSERT_EQ(ret != OB_SUCCESS, true);
  }

  ASSERT_EQ(OB_SUCCESS, wait_event_finish("UPDATE_ON_ZONE2", tmp_event_val, 30 * 60 * 1000));

  // ASSERT_EQ(ls_handle.get_ls()->enable_replay(), OB_SUCCESS);
  ASSERT_EQ(STOP_TX_REPLAY, true);
  STOP_TX_REPLAY = false;

  READ_SQL_BY_CONN_WITH_RET(test_conn, select_result, select_sql_str.c_str(), ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ls_handle.get_ls()->dup_table_ls_handler_.is_dup_table_lease_valid(), true);

  ASSERT_EQ(OB_SUCCESS, select_result->next());
  ASSERT_EQ(OB_SUCCESS, select_result->get_int("id_z", id_Z));

  ASSERT_EQ(id_Z, 999);
}

} // namespace unittest
} // namespace oceanbase
