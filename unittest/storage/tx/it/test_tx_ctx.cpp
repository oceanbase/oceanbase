/**
 * Copyright (c) 2023 OceanBase
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
#define private public
#define protected public
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#define USING_LOG_PREFIX TRANS
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"
#include "tx_node.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share {
int ObTenantTxDataAllocator::init(const char *label)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(
                 storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ObTenantTxDataAllocator::ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}
int ObMemstoreAllocator::init()
{
  throttle_tool_ = &MTL_MEM_ALLOC_MGR.share_resource_throttle_tool();
  return arena_.init();
}
int ObMemstoreAllocator::AllocHandle::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1;
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}
};  // namespace share

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t)
{
  return OB_SUCCESS;
}
} // namespace concurrent_control

class ObTestTxCtx : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    const uint64_t tv = ObTimeUtility::current_time();
    ObCurTraceId::set(&tv);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    MTL_MEM_ALLOC_MGR.init();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  MsgBus bus_;
};

TEST_F(ObTestTxCtx, DelayAbort)
{
  ObTxPartList backup_parts;
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  PREPARE_TX(n1, tx);
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }
  ASSERT_EQ(OB_SUCCESS, backup_parts.assign(tx.parts_));
  tx.parts_.reset();

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ObPartTransCtx *tx_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx.tx_id_, false /*for_replay*/, tx_ctx));
    GCONF._private_buffer_size = 1;
    // ASSERT_EQ(OB_SUCCESS, tx_ctx->submit_log_impl_(ObTxLogType::TX_REDO_LOG));
    ASSERT_EQ(OB_SUCCESS, tx_ctx->submit_redo_after_write(false, ObTxSEQ()));
    TRANS_LOG(INFO, "[TEST] after submit redo", K(tx_ctx->trans_id_),
              K(tx_ctx->exec_info_.max_applied_log_ts_));
    n1->wait_all_redolog_applied();
    TRANS_LOG(INFO, "[TEST] after on_success redo", K(tx_ctx->trans_id_),
              K(tx_ctx->exec_info_.max_applied_log_ts_));
    ASSERT_EQ(true, tx_ctx->exec_info_.redo_lsns_.count() > 0);
    ASSERT_EQ(true, tx_ctx->exec_info_.max_applied_log_ts_.is_valid());
    ASSERT_EQ(ObTxState::INIT, tx_ctx->exec_info_.state_);
    ASSERT_EQ(false, tx_ctx->is_follower_());
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx));
  }
  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  TRANS_LOG(INFO, "add drop KEEPALIVE msg");
  n1->add_drop_msg_type(KEEPALIVE);

  ls_tx_ctx_mgr->switch_to_follower_forcedly();

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx.tx_id_, true /*for_replay*/, tx_ctx));
    ASSERT_EQ(true, tx_ctx->exec_info_.redo_lsns_.count() > 0);
    ASSERT_EQ(true, tx_ctx->exec_info_.max_applied_log_ts_.is_valid());
    ASSERT_EQ(ObTxState::INIT, tx_ctx->exec_info_.state_);
    ASSERT_EQ(true, tx_ctx->is_follower_());
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx));
  }

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
  n1->wait_all_redolog_applied();

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx.tx_id_, false /*for_replay*/, tx_ctx));
    TRANS_LOG(INFO, "[TEST] after leader takeover", K(tx_ctx->trans_id_),
              K(tx_ctx->exec_info_.state_), K(tx_ctx->exec_info_.max_applied_log_ts_));
    ASSERT_EQ(true, tx_ctx->exec_info_.redo_lsns_.count() > 0);
    ASSERT_EQ(true, tx_ctx->exec_info_.max_applied_log_ts_.is_valid());
    ASSERT_EQ(true, tx_ctx->sub_state_.is_force_abort());
    ASSERT_EQ(false, tx_ctx->sub_state_.is_state_log_submitted());
    ASSERT_EQ(ObTxState::INIT, tx_ctx->exec_info_.state_);
    ASSERT_EQ(false, tx_ctx->is_follower_());
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx));
  }

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_TRANS_KILLED, n1->write(tx, snapshot, 1110, 1120));
  }

  ASSERT_EQ(OB_SUCCESS, tx.parts_.assign(backup_parts));
  ASSERT_EQ(OB_TRANS_KILLED, n1->commit_tx(tx, n1->ts_after_ms(500)));
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t tx_id = 21533427;
  uint64_t h = murmurhash(&tx_id, sizeof(tx_id), 0);
  system("rm -rf test_tx_ctx*.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx_ctx.log", true, false,
                       "test_tx_ctx_rs.log",       // rs
                       "test_tx_ctx_election.log", // election
                       "test_tx_ctx_audit.log");   // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  TRANS_LOG(INFO, "mmhash:", K(h));
  return RUN_ALL_TESTS();
}
