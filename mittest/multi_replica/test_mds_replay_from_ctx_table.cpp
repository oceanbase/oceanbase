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
#include "storage/tx/ob_trans_part_ctx.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObMdsReplayFromCtxTable

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

APPEND_RESTART_TEST_CASE_CLASS(2, 1);

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_mds_replay_from_ctx_table_);

namespace oceanbase
{
namespace unittest
{

// using namespace storage;
using namespace oceanbase::storage::checkpoint;

struct RegisterSuccTxArg
{
  transaction::ObTransID tx_id_;
  share::SCN register_scn1_;
  int64_t register_no1_;
  share::SCN register_scn2_;
  int64_t register_no2_;

  TO_STRING_KV(K(tx_id_), K(register_scn1_), K(register_no1_), K(register_scn2_), K(register_no2_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(RegisterSuccTxArg,
                    tx_id_,
                    register_scn1_,
                    register_no1_,
                    register_scn2_,
                    register_no2_);

RegisterSuccTxArg register_succ_arg;

common::ObMySQLTransaction mysql_trans;
oceanbase::observer::ObInnerSQLConnection *register_mds_conn = nullptr;

void minor_freeze_tx_ctx_memtable(ObLS *ls)
{
  TRANS_LOG(INFO, "minor_freeze_tx_ctx_memtable begin");
  int ret = OB_SUCCESS;

  ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);
  ObTxCtxMemtable *tx_ctx_memtable = dynamic_cast<ObTxCtxMemtable *>(
      dynamic_cast<ObLSTxService *>(
          checkpoint_executor->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
          ->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
  ASSERT_EQ(true, tx_ctx_memtable->is_active_memtable());
  ASSERT_EQ(OB_SUCCESS, tx_ctx_memtable->flush(share::SCN::max_scn(), -1));

  // // TODO(handora.qc): use more graceful wait
  // usleep(10 * 1000 * 1000);
  // usleep(100 * 1000);

  RETRY_UNTIL_TIMEOUT(tx_ctx_memtable->is_active_memtable(), 10 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(OB_SUCCESS, ret);

  TRANS_LOG(INFO, "minor_freeze_tx_ctx_memtable end");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), register_mds_without_commit)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sys_tenant_proxy = get_curr_simple_server().get_sql_proxy();

  ASSERT_EQ(OB_SUCCESS, mysql_trans.start(GCTX.sql_proxy_, 1));

  // ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, sys_tenant_proxy);

  // ASSERT_EQ(OB_SUCCESS, mysql_trans.start_transaction(1, false));
  register_mds_conn = static_cast<observer::ObInnerSQLConnection *>(mysql_trans.get_connection());
  transaction::ObTxDesc *desc = register_mds_conn->get_session().get_tx_desc();
  ASSERT_NE(nullptr, desc);
  ASSERT_EQ(true, desc->is_valid());

  std::string test_register_str1 = "TEST REGISTER MDS 1";
  transaction::ObRegisterMdsFlag register_mds_flag1;
  register_mds_flag1.need_flush_redo_instantly_ = true;
  ASSERT_EQ(OB_SUCCESS, register_mds_flag1.mds_base_scn_.convert_for_tx(1000));
  ASSERT_EQ(OB_SUCCESS,
            register_mds_conn->register_multi_data_source(
                1, ObLSID(1), transaction::ObTxDataSourceType::DDL_TRANS,
                test_register_str1.c_str(), test_register_str1.size(), register_mds_flag1));

  ObPartTransCtx *tx_ctx = nullptr;
  GET_LS(1, 1, ls_handle);
  ASSERT_EQ(ls_handle.is_valid(), true);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_ctx(desc->get_tx_id(), false, tx_ctx));

  RETRY_UNTIL_TIMEOUT(tx_ctx->busy_cbs_.is_empty(), 5 * 1000 * 1000, 5000);
  ASSERT_EQ(ret, OB_SUCCESS);
  share::SCN register_1_scn = tx_ctx->exec_info_.max_applying_log_ts_;

  std::string test_register_str2 = "TEST REGISTER MDS 2";
  transaction::ObRegisterMdsFlag register_mds_flag2;
  register_mds_flag2.need_flush_redo_instantly_ = true;
  ASSERT_EQ(OB_SUCCESS, register_mds_flag2.mds_base_scn_.convert_for_tx(2000));
  ASSERT_EQ(OB_SUCCESS,
            register_mds_conn->register_multi_data_source(
                1, ObLSID(1), transaction::ObTxDataSourceType::DDL_TRANS,
                test_register_str2.c_str(), test_register_str2.size(), register_mds_flag2));

  RETRY_UNTIL_TIMEOUT(tx_ctx->busy_cbs_.is_empty(), 5 * 1000 * 1000, 5000);
  ASSERT_EQ(ret, OB_SUCCESS);
  share::SCN register_2_scn = tx_ctx->exec_info_.max_applying_log_ts_;

  ASSERT_EQ(true, register_2_scn > register_1_scn);
  ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_.count(), 2);

  register_succ_arg.tx_id_ = desc->get_tx_id();
  register_succ_arg.register_scn1_ = register_1_scn;
  register_succ_arg.register_no1_ = tx_ctx->exec_info_.multi_data_source_[0].get_register_no();
  register_succ_arg.register_scn2_ = register_2_scn;
  register_succ_arg.register_no2_ = tx_ctx->exec_info_.multi_data_source_[1].get_register_no();

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] register mds into tx and submit mds redo success",
            K(ret), K(register_succ_arg), K(tx_ctx->exec_info_.multi_data_source_));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->revert_tx_ctx(tx_ctx));

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<RegisterSuccTxArg>::serialize_arg(register_succ_arg, tmp_str));
  ASSERT_EQ(OB_SUCCESS, finish_event("REGISTER_MDS_SUCC", tmp_str));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), replay_mds_log_normal)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("REGISTER_MDS_SUCC", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<RegisterSuccTxArg>::deserialize_arg(register_succ_arg, tmp_event_val));

  ObPartTransCtx *tx_ctx = nullptr;
  GET_LS(1, 1, ls_handle);
  ASSERT_EQ(ls_handle.is_valid(), true);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_ctx(register_succ_arg.tx_id_, true, tx_ctx));

  RETRY_UNTIL_TIMEOUT(tx_ctx->exec_info_.max_applied_log_ts_ == register_succ_arg.register_scn2_,
                      5 * 1000 * 1000, 5000);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_.count(), 2);
  // ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_[0].get_register_no(),
  //           register_succ_arg.register_no1_);
  // ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_[1].get_register_no(),
  //           register_succ_arg.register_no2_);

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->revert_tx_ctx(tx_ctx));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), dump_tx_ctx_table)
{
  int ret = OB_SUCCESS;
  GET_LS(1, 1, ls_handle);
  ASSERT_EQ(ls_handle.is_valid(), true);

  {
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));
    minor_freeze_tx_ctx_memtable(ls_handle.get_ls());
  }

  ASSERT_EQ(restart_zone(2, 1), OB_SUCCESS);
}

TEST_F(GET_RESTART_ZONE_TEST_CLASS_NAME(2, 1), restart_zone2_from_tx_ctx_table)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("REGISTER_MDS_SUCC", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<RegisterSuccTxArg>::deserialize_arg(register_succ_arg, tmp_event_val));

  ObPartTransCtx *tx_ctx = nullptr;
  GET_LS(1, 1, ls_handle);
  ASSERT_EQ(ls_handle.is_valid(), true);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_ctx(register_succ_arg.tx_id_, true, tx_ctx));

  RETRY_UNTIL_TIMEOUT(tx_ctx->ctx_source_ == PartCtxSource::RECOVER
                   && tx_ctx->create_ctx_scn_ == register_succ_arg.register_scn2_,
                      5 * 1000 * 1000, 5000);
  // tx_ctx->print_trace_log();
  // TRANS_LOG(INFO, "after restart, print tx ctx",K(*tx_ctx));
  ASSERT_EQ(ret, OB_SUCCESS);
  RETRY_UNTIL_TIMEOUT(tx_ctx->exec_info_.max_applying_log_ts_ == register_succ_arg.register_scn2_,
                      5 * 1000 * 1000, 5000);
  ASSERT_EQ(ret, OB_SUCCESS);

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] Print mds in tx ctx after retarted", K(ret),
            K(tx_ctx->trans_id_), K(tx_ctx->exec_info_.multi_data_source_));
  ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_.count(), 2);
  ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_[0].get_register_no(),
            register_succ_arg.register_no1_);
  ASSERT_EQ(tx_ctx->exec_info_.multi_data_source_[1].get_register_no(),
            register_succ_arg.register_no2_);

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->revert_tx_ctx(tx_ctx));
}

} // namespace unittest

} // namespace oceanbase
