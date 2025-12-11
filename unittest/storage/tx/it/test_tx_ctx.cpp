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
#define private public
#define protected public
#define USING_LOG_PREFIX TRANS
#include "test_tx_dsl.h"
#include "tx_node.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share {
int ObTenantTxDataAllocator::init(const char *label, TxShareThrottleTool* throttle_tool)
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

class ReplayLogEntryFunctor
{
public:
  ReplayLogEntryFunctor(ObTxNode *n) : n_(n) {}

  int operator()(const void *buffer,
                 const int64_t nbytes,
                 const palf::LSN &lsn,
                 const int64_t ts_ns)
  {
    return n_->replay(buffer, nbytes, lsn, ts_ns);
  }

private:
  ObTxNode *n_;
};

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

TEST_F(ObTestTxCtx, 2PC_ABORT_WITH_SWITCH_TO_FOLLOWER)
{
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 1001)

  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  // GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 120, 132));

  ASSERT_EQ(tx.parts_.count(), 2);

  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  TRANS_LOG(INFO, "add drop KEEPALIVE msg");
  n1->add_drop_msg_type(KEEPALIVE);
  n2->add_drop_msg_type(KEEPALIVE);

  FLUSH_REDO(n1);
  FLUSH_REDO(n2);

  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();

  SWITCH_TO_FOLLOWER_FORCEDLY(n2);
  SWITCH_TO_LEADER(n2);

  ObPartTransCtx * ctx_n2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, ctx_n2));
  ASSERT_EQ(ctx_n2->need_force_abort_(), true);

  ObPartTransCtx * ctx_n1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, ctx_n1));
  n1->fake_tx_log_adapter_->set_pause();

  n1->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_COMMIT_INFO_LOG);
  n1->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_PREPARE_LOG);
  COMMIT_TX(n1, tx, 120 * 1000 * 1000);

  usleep(100*1000);

  TRANS_LOG(INFO,
            "[DEBUG] print ctx_n1",
            K(ctx_n1->trans_id_),
            K(ctx_n1->ls_id_),
            K(ctx_n1->upstream_state_),
            K(ctx_n1->exec_info_.state_),
            KPC(ctx_n1->busy_cbs_.get_first()),
            KPC(ctx_n1->busy_cbs_.get_last()));
  ASSERT_EQ(ctx_n1->upstream_state_, ObTxState::ABORT);
  ASSERT_EQ(ctx_n1->busy_cbs_.get_size(), 1);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_first()->cb_arg_array_, ObTxLogType::TX_ABORT_LOG),
            true);
  n1->fake_tx_log_adapter_->clear_block_log_type();
  SWITCH_TO_FOLLOWER_GRACEFULLY(n1);
  TRANS_LOG(INFO,
            "[DEBUG] print ctx_n1 after switch_to_follower",
            K(ctx_n1->trans_id_),
            K(ctx_n1->ls_id_),
            K(ctx_n1->upstream_state_),
            K(ctx_n1->exec_info_.state_),
            KPC(ctx_n1->busy_cbs_.get_first()),
            KPC(ctx_n1->busy_cbs_.get_last()));
  ASSERT_EQ(ctx_n1->busy_cbs_.get_size(), 1);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_first()->cb_arg_array_, ObTxLogType::TX_ABORT_LOG),
            true);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_last()->cb_arg_array_, ObTxLogType::TX_COMMIT_INFO_LOG),
            false);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_last()->cb_arg_array_, ObTxLogType::TX_ACTIVE_INFO_LOG),
            false);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_last()->cb_arg_array_, ObTxLogType::TX_REDO_LOG),
            false);

  n1->fake_tx_log_adapter_->clear_pause();

  n2->revert_tx_ctx(ctx_n2);
  n1->revert_tx_ctx(ctx_n1);

  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();



  // auto n1x = new ObTxNode(1010, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  // auto n2x = new ObTxNode(1010 + 1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  // ASSERT_EQ(OB_SUCCESS, n1x->start());
  // ASSERT_EQ(OB_SUCCESS, n2x->start());
  // n1x->set_as_follower_replica(*n1);
  // n2x->set_as_follower_replica(*n2);
  //
  // ReplayLogEntryFunctor functor1(n1x);
  // ReplayLogEntryFunctor functor2(n2x);
  // n1x->fake_tx_log_adapter_->replay_all(functor1);
  // n2x->fake_tx_log_adapter_->replay_all(functor2);

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  // ASSERT_EQ(OB_SUCCESS, n1x->wait_all_tx_ctx_is_destoryed());
  // ASSERT_EQ(OB_SUCCESS, n1x->wait_all_tx_ctx_is_destoryed());
}

TEST_F(ObTestTxCtx, 2PC_ABORT_SET_EXITING)
{

  START_TWO_TX_NODE_WITH_LSID(n1, n2, 1001)

  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  // GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 120, 132));

  ASSERT_EQ(tx.parts_.count(), 2);

  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  TRANS_LOG(INFO, "add drop KEEPALIVE msg");
  n1->add_drop_msg_type(KEEPALIVE);
  n2->add_drop_msg_type(KEEPALIVE);

  FLUSH_REDO(n1);
  FLUSH_REDO(n2);

  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();

  ObPartTransCtx * ctx_n1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, ctx_n1));

  ObPartTransCtx * ctx_n2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, ctx_n2));
  // ASSERT_EQ(ctx_n2->need_force_abort_(), true);

  n1->fake_tx_log_adapter_->set_pause();
  n1->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_COMMIT_INFO_LOG);
  n1->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_PREPARE_LOG);

  COMMIT_TX(n1, tx, 120 * 1000 * 1000);
  ASSERT_EQ(ctx_n1->is_root(), true);

  usleep(10*1000);
  ASSERT_EQ(ctx_n2->exec_info_.state_, ObTxState::PREPARE);
  ASSERT_EQ(ctx_n1->exec_info_.state_, ObTxState::INIT);
  ASSERT_EQ(ctx_n1->upstream_state_, ObTxState::PREPARE);

  SWITCH_TO_FOLLOWER_FORCEDLY(n1);
  n1->fake_tx_log_adapter_->clear_block_log_type();
  n1->fake_tx_log_adapter_->clear_pause();
  n2->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_CLEAR_LOG);
  SWITCH_TO_LEADER(n1);


  ctx_n2->handle_timeout(1*1000);
  usleep(100*1000);
   TRANS_LOG(INFO,
             "[DEBUG] print ctx_n1",
             K(ctx_n1->trans_id_),
             K(ctx_n1->ls_id_),
             K(ctx_n1->upstream_state_),
             K(ctx_n1->exec_info_.state_));
   TRANS_LOG(INFO,
             "[DEBUG] print ctx_n2",
             K(ctx_n2->trans_id_),
             K(ctx_n2->ls_id_),
             K(ctx_n2->upstream_state_),
             K(ctx_n2->exec_info_.state_));
  ASSERT_EQ(ctx_n1->exec_info_.state_, ObTxState::ABORT);


  ASSERT_EQ(ctx_n2->exec_info_.state_, ObTxState::ABORT);
  ASSERT_EQ(ctx_n2->busy_cbs_.get_size(), 0);
  // ASSERT_EQ(ctx_n2->is_exiting_, true);
  SWITCH_TO_FOLLOWER_GRACEFULLY(n2);
  ASSERT_EQ(ctx_n2->is_exiting_, true);
  ASSERT_EQ(ctx_n2->is_follower_(), false);

  n2->revert_tx_ctx(ctx_n2);
  n1->revert_tx_ctx(ctx_n1);

  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t tx_id = 21533427;
  uint64_t h = murmurhash(&tx_id, sizeof(tx_id), 0);
  std::string log_file_name = "test_tx_ctx.log";
  #ifdef TX_NODE_MEMTABLE_USE_HASH_INDEX_FLAG
      log_file_name = "test_tx_ctx_no_hash_index.log";
  #endif
  system(std::string("rm -rf " + log_file_name + "*").c_str());
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_file_name.c_str(), true, false,
                       log_file_name.c_str(), // rs
                       log_file_name.c_str(), // election
                       log_file_name.c_str()); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  TRANS_LOG(INFO, "mmhash:", K(h));
  return RUN_ALL_TESTS();
}
