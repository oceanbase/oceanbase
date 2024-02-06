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
#define private public
#define protected public
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#define USING_LOG_PREFIX TRANS
#include "tx_node.h"
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag ,
                                 const int64_t ,
                                 const ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const int64_t ,
                                 const ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const int64_t )
{
  return OB_SUCCESS;
}
}

class ObTestTx : public ::testing::Test
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
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  MsgBus bus_;
};

TEST_F(ObTestTx, basic)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_TWO_TX_NODE(n1, n2);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp0);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));
  int64_t val1 = 0, val2 = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 100, val1));
  ASSERT_EQ(OB_SUCCESS, n2->read(tx, 101, val2));
  ASSERT_EQ(112, val1);
  ASSERT_EQ(113, val2);
  // rollback to savepoint
  ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp1, 1000 * 1000);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 100, val1));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n2->read(tx, 101, val2));
  // write after rollback
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 114));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 115));
  // read the write after rollback
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 100, val1));
  ASSERT_EQ(OB_SUCCESS, n2->read(tx, 101, val2));
  ASSERT_EQ(114, val1);
  ASSERT_EQ(115, val2);
  COMMIT_TX(n1, tx, 500 * 1000);
}

TEST_F(ObTestTx, start_trans_expired)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  auto guard = n1->get_tx_guard();
  ObTxDesc &tx = guard.get_tx_desc();
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000; // 1ms
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  usleep(5000); // 5ms
  // create tx ctx failed caused by trans_timeout
  ASSERT_EQ(OB_TRANS_TIMEOUT, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
}

TEST_F(ObTestTx, rollback_savepoint_with_msg_lost)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  ObTxDescGuard guard = n1->get_tx_guard();
  ObTxDesc &tx = guard.get_tx_desc();
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(5), snapshot));
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ObTxSEQ sp2;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));
  // inject link failure between scheduler to participant 2
  ASSERT_EQ(OB_SUCCESS, bus_.inject_link_failure(n1->addr_, n2->addr_));
  // rollback to savepoint should hang, because of rollback msg can't be delivered
  auto rollback_sp = [&] {
    return n1->rollback_to_implicit_savepoint(tx, sp2, n1->ts_after_ms(5000), nullptr);
  };
  auto async = test::make_async(rollback_sp);
  async.wait_started();
  ASSERT_FALSE(async.is_evaled());
  // interrupt
  ASSERT_EQ(OB_SUCCESS, n1->interrupt(tx, 101));
  {int i = 2000;
  while(!async.is_evaled() && i-- > 0) {
    usleep(1000);
  }
  ASSERT_TRUE(i > 0) << "interrupt savepoint rollback";
  }
  ASSERT_EQ(OB_ERR_INTERRUPTED, async.get());
  ASSERT_EQ(OB_SUCCESS, bus_.repair_link_failure(n1->addr_, n2->addr_));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
  ASSERT_EQ(ObTxDesc::State::ROLLED_BACK, tx.state_);
  // wait part_ctx gc
  ObPartTransCtx *part_ctx = NULL;
  ObLSID ls_id(2);
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(ls_id, tx.tx_id_, part_ctx));
  // release tx, then part_ctx can detect txn was terminated
  ASSERT_EQ(OB_SUCCESS, guard.release());
  part_ctx->last_ask_scheduler_status_ts_ = 0; // ensure check_scheduler_status will not be skipped
  ASSERT_EQ(OB_SUCCESS, part_ctx->check_scheduler_status());
  int i = 0;
  while(!part_ctx->is_exiting_ && i++ < 100) {
    usleep(500);
  }
  ASSERT_EQ(part_ctx->is_exiting_, true);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(part_ctx));
}

TEST_F(ObTestTx, rollback_savepoint_timeout)
{
  START_TWO_TX_NODE(n1, n2);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 100, 111));
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 101, 112));
  INJECT_LINK_FAILURE(n1, n2);
  ASYNC_DO(async_op, ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp2, 2000 * 1000));
  ASYNC_WAIT(async_op, 3000 * 1000, wait_ret);
  REPAIR_LINK_FAILURE(n1, n2);
  ROLLBACK_TX(n1, tx);
}

TEST_F(ObTestTx, rollback_savepoint_with_uncertain_participants)
{
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp);
  ASSERT_TRUE(sp.is_valid());
  share::ObLSArray uncertain_parts;
  ASSERT_EQ(OB_SUCCESS, uncertain_parts.push_back(share::ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_us(100000), &uncertain_parts));
  ASSERT_EQ(ObTxDesc::State::IDLE, tx.state_);
  ASSERT_EQ(0, tx.parts_.count());
}

TEST_F(ObTestTx, switch_to_follower_gracefully)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  ObTxSEQ sp1, sp2;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));

    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();

    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 113));
  }

  {
    int64_t val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 101, val2));
    ASSERT_EQ(113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_fail)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  const int64_t TX_CNT = 128;
  ObTxDesc *tx_ptr_arr[TX_CNT] = { nullptr };
  for (int64_t i = 0; i < TX_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr_arr[i]));
    ObTxDesc &tx = *tx_ptr_arr[i];
    // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000 + i, 2000 + i));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

// make switch_to_follower_gracefully_failed
  ObPartTransCtx* last_tx_ctx = nullptr;
  {
    ObLSTxCtxIterator iter;
    ObPartTransCtx* tx_ctx = nullptr;

    ASSERT_EQ(OB_SUCCESS, iter.set_ready(ls_tx_ctx_mgr));
    while (OB_SUCC(iter.get_next_tx_ctx(tx_ctx))) {
      last_tx_ctx = tx_ctx;
      iter.revert_tx_ctx(tx_ctx);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    iter.reset();
  }

  uint64_t tenant_id_backup = last_tx_ctx->tenant_id_;
// make last tx ctx fails to exec switch_to_follower_gracefully,
  last_tx_ctx->tenant_id_ = 0xbaba;

// switch_to_follower_gracefully
  n1->fake_tx_log_adapter_->set_pause();
  ASSERT_NE(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());
  n1->fake_tx_log_adapter_->clear_pause();
// reset
  last_tx_ctx->tenant_id_ = tenant_id_backup;
  n1->wait_all_redolog_applied();

// check data_complete
// TODO The maintenance of the data_completed_ variable is currently incorrect, and the
// verification will be turned on after it is fixed
  //{
  //  ObLSTxCtxIterator iter;
  //  ObPartTransCtx* tx_ctx = nullptr;
  //  ASSERT_EQ(OB_SUCCESS, iter.set_ready(ls_tx_ctx_mgr));
  //  while (OB_SUCC(iter.get_next_tx_ctx(tx_ctx))) {
  //    ASSERT_EQ(false, tx_ctx->exec_info_.data_complete_);
  //    iter.revert_tx_ctx(tx_ctx);
  //  }
  //  ASSERT_EQ(OB_ITER_END, ret);
  //  iter.reset();
  //}

  for (int64_t i = 0; i < TX_CNT; ++i) {
    ObTxDesc &tx = *tx_ptr_arr[i];
    int64_t val1 = 0;

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1000 + i, val1));
    ASSERT_EQ(2000 + i, val1);
    ObTxSEQ sp;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000 + i, 3000 + i));
  }

  for (int64_t i = 0; i < TX_CNT; ++i) {
    ObTxDesc &tx = *tx_ptr_arr[i];
    int64_t val2 = 0;

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1000 + i, val2));
    ASSERT_EQ(3000 + i, val2);
  }

  for (int64_t i = 0; i < TX_CNT; ++i) {
    ObTxDesc &tx = *tx_ptr_arr[i];
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(10000)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_then_forcedly)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  ObTxSEQ sp1;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));
    n1->wait_all_redolog_applied();
    ls_tx_ctx_mgr->switch_to_follower_forcedly();
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));
    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 113));
  }

  {
    int64_t val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 101, val2));
    ASSERT_EQ(113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_forcedly)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  n1->add_drop_msg_type(KEEPALIVE);
  {
    ls_tx_ctx_mgr->switch_to_follower_forcedly();

    int64_t val1 = 0, val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));

    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();

    ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, n1->read(snapshot, 100, val1));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, n1->write(tx, snapshot, 101, 113));
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, resume_leader)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ObTxSEQ sp1;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());

    int64_t val1 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->resume_leader());
    usleep(100 * 1000);

    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 113));
  }
  {
    int64_t val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 101, val2));
    ASSERT_EQ(113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_in_stmt_then_resume_leader)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;

  ObStoreCtx write_store_ctx;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ls_tx_ctx_mgr->switch_to_follower_gracefully();
    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1001, 1113));

    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();

    //ASSERT_EQ(OB_SUCCESS, n1->read(tx, 100, val1));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1001, 1113));
    ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val2 = 0, val3 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1000, val2));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1001, val3));

    //ASSERT_EQ(112, val1);
    ASSERT_EQ(1112, val2);
    ASSERT_EQ(1113, val3);
  }
  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

class ReplayLogEntryFunctor
{
public:
  ReplayLogEntryFunctor(ObTxNode* n) : n_(n) {}

  int operator()(const void *buffer,
               const int64_t nbytes,
               const palf::LSN &lsn,
               const int64_t ts_ns) {
    return n_->replay(buffer, nbytes, lsn, ts_ns);
  }
private:
  ObTxNode* n_;
};

TEST_F(ObTestTx, replay_basic)
{
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n1->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
    }

    ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

    {
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    }
    n1->wait_all_redolog_applied();
    int64_t retry_count = 0;
    while(ls_tx_ctx_mgr->get_tx_ctx_count() > 0)
    {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected tx ctx counts", K(ls_tx_ctx_mgr->get_ls_id()), K(ls_tx_ctx_mgr->get_tx_ctx_count()));
      ls_tx_ctx_mgr->print_all_tx_ctx(ObLSTxCtxMgr::MAX_HASH_ITEM_PRINT, true);
      ls_tx_ctx_mgr->get_retain_ctx_mgr().print_retain_ctx_info(ls_tx_ctx_mgr->get_ls_id());
      retry_count++;
      usleep(100*1000);
      if(retry_count > 10)
      {
          ob_abort();
      }
    }
    ASSERT_EQ(0, ls_tx_ctx_mgr->get_tx_ctx_count());
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  }

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;
    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n2->ts_after_ms(100),
                               snapshot));
      int64_t val1 = 0;
      ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
      ASSERT_EQ(112, val1);
    }

    {
      ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx, n2->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx));
    }
  }
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}

TEST_F(ObTestTx, replay_then_commit)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 50 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_gracefully());
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 100, 113));
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    n2->wait_all_redolog_applied();
  }

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

void do_async_read(ObTxNode* n, ObTxReadSnapshot& snapshot, int64_t key, int64_t& val)
{
  LOG_INFO("do sync commit begin");
  ASSERT_EQ(OB_SUCCESS, n->read(snapshot, key, val));
  LOG_INFO("do sync commit end");
}


void do_async_commit(ObTxNode* n, ObTxDesc& tx, int& commit_ret)
{
  LOG_INFO("do async commit begin", K(tx));
  commit_ret = n->commit_tx(tx, n->ts_after_ms(50 * 1000));
  LOG_INFO("do async commit end", K(tx), K(commit_ret));
}

TEST_F(ObTestTx, wait_commit_version_elapse_block)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_elapse_waiting_mode();
// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());

  n2->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_elapse_waiting_mode();
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_ts_mgr_().update_fake_gts(1);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObTxDesc *tx_ptr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
  ObTxDesc &tx2 = *tx_ptr2;

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, n2->read(snapshot, 100, val1));
  }

  ObTxNode::get_ts_mgr_().update_fake_gts(ls_tx_ctx_mgr2->max_replay_commit_version_.get_val_for_gts());
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
    ASSERT_EQ(113, val1);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
  }

  ObTxNode::get_ts_mgr_().elapse_callback();
  t.join();

  ASSERT_EQ(OB_SUCCESS, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, wait_commit_version_elapse_block_and_switch_to_follower_forcedly)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_elapse_waiting_mode();
// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());

  n2->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_elapse_waiting_mode();
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_ts_mgr_().update_fake_gts(1);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObTxDesc *tx_ptr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
  ObTxDesc &tx2 = *tx_ptr2;

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, n2->read(snapshot, 100, val1));
  }

  ObTxNode::get_ts_mgr_().update_fake_gts(ls_tx_ctx_mgr2->max_replay_commit_version_.get_val_for_gts());
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
    ASSERT_EQ(113, val1);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
  }

  ObTxNode::get_ts_mgr_().elapse_callback();
  t.join();

  ASSERT_EQ(OB_SUCCESS, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, get_gts_block_and_switch_to_follower_gracefully)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_get_gts_waiting_mode();
  // tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());
  n1->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_get_gts_waiting_mode();
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());

  n2->wait_all_redolog_applied();
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  ObTxNode::get_ts_mgr_().get_gts_callback();
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, get_gts_block_and_switch_to_follower_forcedly)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_get_gts_waiting_mode();
// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ObTransID fail_tx_id;
  ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id);
  n1->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());
  n1->wait_all_redolog_applied();
  t.join();
  ASSERT_EQ(OB_TRANS_KILLED, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ObTxNode::get_ts_mgr_().clear_get_gts_waiting_mode();
  ObTxNode::get_ts_mgr_().get_gts_callback();

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_in_stmt_rollback_to_last_savepoint_then_commit)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  ObStoreCtx write_store_ctx;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ls_tx_ctx_mgr->switch_to_follower_gracefully();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1001, 1113));
  ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp1, n1->ts_after_ms(100), nullptr));
  }

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 1001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1001, val2));
    ASSERT_EQ(1113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }

  n2->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_in_stmt_then_commit)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  ObStoreCtx write_store_ctx;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000, 1111));
  }

  ObTxSEQ sp2;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ls_tx_ctx_mgr->switch_to_follower_gracefully();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1001, 1113));
  ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp2, n1->ts_after_ms(100), nullptr));
  }

  ObTxSEQ sp3;
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp3));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 1001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1000, val1));
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1001, val2));
    ASSERT_EQ(1111, val1);
    ASSERT_EQ(1113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }

  n2->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}
// distributed tx
TEST_F(ObTestTx, distributed_tx_participant_switch_to_follower_gracefully_in_stmt_then_commit)
{
  int ret = OB_SUCCESS;
  GCONF._ob_trans_rpc_timeout = 5000 * 1000;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  auto n3 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);
  LOG_INFO("n3 tx_ctx_mgr", K(&n3->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n2);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000, 1111));
  }

  ObTxSEQ sp2;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 2000, 1111));
  }

  ObStoreCtx write_store_ctx;
  ObTxSEQ sp3;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp3));
    ASSERT_EQ(OB_SUCCESS, n2->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n2->write_one_row(write_store_ctx, 2000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ls_tx_ctx_mgr2->switch_to_follower_gracefully();
  n2->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n2->write_one_row(write_store_ctx, 2000, 1113));
  ASSERT_EQ(OB_SUCCESS, n2->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp3, n1->ts_after_ms(100), nullptr));
  }

  ObTxSEQ sp4;
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp4));
    ASSERT_EQ(OB_SUCCESS, n3->write(tx, snapshot, 2001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 2000, val1));
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 2001, val2));
    ASSERT_EQ(1111, val1);
    ASSERT_EQ(1113, val2);
  }

  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);
  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }
  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_gracefully_in_stmt_then_commit)
{
  int ret = OB_SUCCESS;
  GCONF._ob_trans_rpc_timeout = 5000 * 1000;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);
  LOG_INFO("n3 tx_ctx_mgr", K(&n3->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000, 1111));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 2000, 1111));
  }

  ObStoreCtx write_store_ctx;
  ObTxSEQ sp3;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp3));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ls_tx_ctx_mgr1->switch_to_follower_gracefully();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1000, 1113));
  ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp3, n1->ts_after_ms(100), nullptr));
  }

  ObTxSEQ sp4;
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp4));
    ASSERT_EQ(OB_SUCCESS, n3->write(tx, snapshot, 1001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 1000, val1));
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 1001, val2));
    ASSERT_EQ(1111, val1);
    ASSERT_EQ(1113, val2);
  }

  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);
  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }
  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
}

TEST_F(ObTestTx, distributed_tx_switch_to_follower_forcedly_in_prepare_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ASSERT_EQ(OB_SUCCESS, n2->start());
  n3->set_as_follower_replica(*n2);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));

  ObTxNode::get_ts_mgr_().set_get_gts_waiting_mode();

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  int i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::PREPARE) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(i, 100);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_follower_forcedly());
  n2->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_get_gts_waiting_mode();
  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);

  ObTxNode::get_ts_mgr_().get_gts_callback();
  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ReplayLogEntryFunctor functor_n2(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor_n2));
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_forcedly_in_prepare_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ASSERT_EQ(OB_SUCCESS, n2->start());
  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));

  n1->add_drop_msg_type(TX_2PC_PREPARE_RESP);

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n1_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, n1_ctx));
  int i = 0;
  while(!(n1_ctx->exec_info_.state_ == ObTxState::PREPARE) && i++ < 1000) {
    usleep(5000);
  }
  ASSERT_NE(i, 1001);
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(n1_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ReplayLogEntryFunctor functor_n1(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(functor_n1));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, distributed_tx_participant_switch_to_follower_forcedly_in_pre_commit_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ASSERT_EQ(OB_SUCCESS, n2->start());
  n3->set_as_follower_replica(*n2);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n2->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n2->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 112));
    }
    {
      ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx, n2->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx));
    }
    n2->wait_all_redolog_applied();
  }

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 113));

  n2->add_drop_msg_type(TX_2PC_COMMIT_REQ);
  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  int i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::PRE_COMMIT) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(101, i);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_follower_forcedly());
  n2->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  LOG_INFO("max_commit_ts after switch_to_leader",
           K(n3->txs_.tx_version_mgr_.get_max_commit_ts(false)));

  n2->del_drop_msg_type(TX_2PC_COMMIT_REQ);
  n3->add_drop_msg_type(TX_2PC_COMMIT_REQ);
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;
  int64_t val1 = 0;
  ObTxReadSnapshot snapshot3;
  {
    ASSERT_EQ(OB_SUCCESS, n3->get_read_snapshot(tx3,
                             tx_param.isolation_,
                             n3->ts_after_ms(100),
                             snapshot3));
  }
  std::thread t_read(do_async_read, n3, std::ref(snapshot3), 200, std::ref(val1));
  usleep(100 * 1000);
  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);
  n3->del_drop_msg_type(TX_2PC_COMMIT_REQ);
  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);

  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());
  t_read.join();
  ASSERT_EQ(113, val1);
  ASSERT_EQ(OB_SUCCESS, n3->release_tx(tx3));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(0, ls_tx_ctx_mgr3->get_tx_ctx_count());
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_forcedly_in_pre_commit_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500ll * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n1->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
    }
    {
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    }
    n1->wait_all_redolog_applied();
  }

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 113));

  n1->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  n3->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  int i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::PRE_COMMIT) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(i, 101);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  LOG_INFO("max_commit_ts after switch_to_leader",
           K(n3->txs_.tx_version_mgr_.get_max_commit_ts(false)));


  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;
  int64_t val1 = 0;
  ObTxReadSnapshot snapshot3;
  {
    ASSERT_EQ(OB_SUCCESS, n3->get_read_snapshot(tx3,
                             tx_param.isolation_,
                             n3->ts_after_ms(100),
                             snapshot3));
  }
  std::thread t_read(do_async_read, n3, std::ref(snapshot3), 100, std::ref(val1));
  usleep(100 * 1000);
  n3->add_drop_msg_type(TX_2PC_COMMIT_RESP);
  n3->del_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);
  n3->del_drop_msg_type(TX_2PC_COMMIT_RESP);

  t_read.join();
  ASSERT_EQ(113, val1);
  ASSERT_EQ(OB_SUCCESS, n3->release_tx(tx3));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_forcedly_in_participant_commit_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_0_0_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500ll * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n1->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
    }
    {
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    }
    n1->wait_all_redolog_applied();
  }

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 113));

  n1->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  n3->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n1_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, n1_ctx));
  int i = 0;
  while(!(n1_ctx->exec_info_.state_ == ObTxState::PRE_COMMIT) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(i, 101);
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(n1_ctx));

  n1->fake_tx_log_adapter_->set_log_drop();
  n1->del_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::COMMIT) && i++ < 100) {
    usleep(50000);
  }
  ASSERT_NE(i, 101);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  n1->fake_tx_log_adapter_->clear_log_drop();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  LOG_INFO("max_commit_ts after switch_to_leader",
           K(n3->txs_.tx_version_mgr_.get_max_commit_ts(false)));

  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;
  int64_t val1 = 0;
  ObTxReadSnapshot snapshot3;
  {
    ASSERT_EQ(OB_SUCCESS, n3->get_read_snapshot(tx3,
                             tx_param.isolation_,
                             n3->ts_after_ms(100),
                             snapshot3));
  }
  std::thread t_read(do_async_read, n3, std::ref(snapshot3), 100, std::ref(val1));
  usleep(100 * 1000);
  n3->add_drop_msg_type(TX_2PC_COMMIT_RESP);
  n3->del_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  n3->del_drop_msg_type(TX_2PC_COMMIT_RESP);

  t_read.join();
  ASSERT_EQ(113, val1);
  ASSERT_EQ(OB_SUCCESS, n3->release_tx(tx3));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, interrupt_get_read_snapshot)
{
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  ObTxReadSnapshot snapshot;
  n1->get_ts_mgr_().inject_get_gts_error(OB_EAGAIN);
  ASYNC_DO(acq_snapshot, n1->get_read_snapshot(tx, ObTxIsolationLevel::RC, n1->ts_after_ms(20 * 1000), snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->interrupt(tx, OB_TRANS_KILLED));
  ASYNC_WAIT(acq_snapshot, 2000 * 1000, wait_ret);
  ASSERT_EQ(OB_ERR_INTERRUPTED, wait_ret);
  ROLLBACK_TX(n1, tx);
}

////
/// APPEND NEW TEST HERE, USE PRE DEFINED MACRO IN FILE `test_tx.dsl`
/// SEE EXAMPLE: TEST_F(ObTestTx, rollback_savepoint_timeout)
///

} // oceanbase

int main(int argc, char **argv)
{
  int64_t tx_id = 21533427;
  uint64_t h = murmurhash(&tx_id, sizeof(tx_id), 0);
  system("rm -rf test_tx.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx.log", true, false,
                       "test_tx.log", // rs
                       "test_tx.log", // election
                       "test_tx.log"); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  TRANS_LOG(INFO, "mmhash:", K(h));
  return RUN_ALL_TESTS();
}
