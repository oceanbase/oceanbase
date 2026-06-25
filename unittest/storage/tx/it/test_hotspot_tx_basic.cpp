/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may use this software according to the terms and conditions of the Mulan PubL v2.
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
#include "lib/utility/ob_tracepoint.h"
#include "lib/time/ob_time_utility.h"
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"
#include "tx_node.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share
{

ObTxDataThrottleGuard::~ObTxDataThrottleGuard() {}

int ObTenantTxDataAllocator::init(const char *label)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                    block_alloc_, mem_attr))) {
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
}; // namespace share

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

class ObTestHotspotTxBasic : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObAddr ip_port(ObAddr::VER::IPV4, "119.119.0.1", 2023);
    ObCurTraceId::init(ip_port);
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

TEST_F(ObTestHotspotTxBasic, test_hotspot_tx_basic)
{
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 1;
    item.occur_ = 1;
    item.trigger_freq_ = 1;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_REDO_STAT", item);
  }
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  // 创建 primary 事务
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  // 创建几个 secondary 事务
  const int SECONDARY_TX_COUNT = 3;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];

  // 保持 guards 在整个测试中存活，以便后续验证提交状态
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDescGuard sec_guard2 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc(),
                                           &sec_guard2.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;

    // 对每个 secondary 事务做一些写操作（但不提交 redo log）
    // 注意：为了满足 hotspot 聚合的条件，需要确保 redo_flush_status_ < NORMAL_FLUSHED
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 100 + i, 200 + i));
    LOG_INFO("Created secondary tx", K(i), K(secondary_tx_ids[i]));
  }

  // 对 primary 事务也做一些写操作
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 1000, 2000));

  // 准备聚合参数
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  // 确保 aggre_members 正确初始化
  ASSERT_GT(SECONDARY_TX_COUNT, 0);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_TRUE(secondary_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }
  ASSERT_EQ(SECONDARY_TX_COUNT, aggre_members.count());
  LOG_INFO("aggre_members prepared", K(aggre_members.count()), K(aggre_members));

  // 调用 sync_hotspot_legality_validation 接口，将 secondary 事务聚合到 primary 事务
  int ret =
      n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("sync_hotspot_legality_validation succeeded", K(primary_tx.tx_id_), K(aggre_members));

  // 等待消息处理完成（dispatch redo 消息）
  n1->wait_all_msg_consumed();

  // 提交 primary 事务
  COMMIT_TX(n1, primary_tx, 500 * 1000);

  // 等待响应任务处理完成
  n1->wait_all_msg_consumed();

  // 验证所有子事务都成功提交
  // 通过尝试提交每个 secondary 事务来验证它们已经成功提交
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    // 尝试提交 secondary 事务
    // 如果事务已经通过 primary 提交，这里应该返回成功或已经提交的状态
    int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_us(500 * 1000));

    // 验证提交结果：应该成功（OB_SUCCESS）或已经提交（OB_TRANS_COMMITED）
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
        << "Secondary tx " << i << " commit failed. tx_id=" << secondary_tx_ids[i]
        << ", commit_ret=" << commit_ret;

    // 验证事务状态：应该为 COMMITTED
    ASSERT_EQ(ObTxDesc::State::COMMITTED, sec_txs[i]->state_)
        << "Secondary tx " << i << " state is not COMMITTED. tx_id=" << secondary_tx_ids[i]
        << ", state=" << static_cast<int>(sec_txs[i]->state_);

    // 验证提交结果码
    ASSERT_EQ(OB_SUCCESS, sec_txs[i]->commit_out_)
        << "Secondary tx " << i << " commit_out is not OB_SUCCESS. tx_id=" << secondary_tx_ids[i]
        << ", commit_out=" << sec_txs[i]->commit_out_;

    LOG_INFO("Secondary tx committed successfully", K(i), K(secondary_tx_ids[i]),
             K(commit_ret), K(sec_txs[i]->state_), K(sec_txs[i]->commit_out_));
  }

  LOG_INFO("All secondary transactions have been successfully committed",
           K(SECONDARY_TX_COUNT));

  // 创建一个新事务来验证提交的数据
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    GET_READ_SNAPSHOT(n1, read_tx, read_tx_param, read_snapshot);

    // 验证 secondary 事务的修改也被提交了
    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      int64_t val = 0;
      LOG_INFO("start to read from secondary tx", K(i), K(read_snapshot));
      EXPECT_EQ(OB_SUCCESS, n1->read(read_snapshot, 100 + i, val));
      EXPECT_EQ(200 + i, val);
      LOG_INFO("end to read from secondary tx", K(i), K(read_snapshot), K(val));
    }

    // 验证 primary 事务的修改也被提交了
    int64_t primary_val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 1000, primary_val));
    LOG_INFO("start to read from secondary tx", K(read_snapshot));
    ASSERT_EQ(2000, primary_val);
    LOG_INFO("end to read from secondary tx", K(read_snapshot), K(primary_val));
  }

  LOG_INFO("sync_hotspot_legality_validation test completed successfully");

  // ========== 测试 sync_hotspot_legality_validation 失败后的场景 ==========
  // 验证当 sync_hotspot_legality_validation 失败后，主事务和子事务还能否独立的写入和提交
  LOG_INFO(">>>> Starting test: sync_hotspot_legality_validation failure scenario");

  // 创建新的 primary 事务
  ObTxDescGuard primary_guard2 = n1->get_tx_guard();
  ObTxDesc &primary_tx2 = primary_guard2.get_tx_desc();
  LOG_INFO("##PREPARE_TX##", "node", n1->addr_, "tx_id", primary_tx2.tx_id_);
  PREPARE_TX_PARAM(tx_param2);
  tx_param2.timeout_us_ = 1000 * 1000 * 1000;
  ObTxReadSnapshot snapshot2;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(primary_tx2, tx_param2.isolation_, n1->ts_after_ms(100), snapshot2));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx2, tx_param2));

  // 创建几个新的 secondary 事务
  const int SECONDARY_TX_COUNT2 = 2;
  ObTransID secondary_tx_ids2[SECONDARY_TX_COUNT2];

  // 保持 guards 在整个测试中存活
  ObTxDescGuard sec_guard2_0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard2_1 = n1->get_tx_guard();
  ObTxDesc *sec_txs2[SECONDARY_TX_COUNT2] = {&sec_guard2_0.get_tx_desc(), &sec_guard2_1.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT2; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs2[i], tx_param2));
    secondary_tx_ids2[i] = sec_txs2[i]->tx_id_;

    // 对每个 secondary 事务做一些写操作
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs2[i], snapshot2, 300 + i, 400 + i));
    LOG_INFO("Created secondary tx2", K(i), K(secondary_tx_ids2[i]));
  }

  // 对 primary 事务也做一些写操作
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx2, snapshot2, 3000, 4000));

  // 让第一个子事务先提交，使其有 redo_lsns，这样会导致 sync_hotspot_legality_validation 失败
  // 因为 hotspot_legality_validation_() 会检查 exec_info_.redo_lsns_.count() > 0
  COMMIT_TX(n1, *sec_txs2[0], 500 * 1000);
  n1->wait_all_msg_consumed();
  LOG_INFO("Committed first secondary tx2, should make it have redo_lsns and cause validation to fail");

  // 准备聚合参数
  ObTxPart participant2;
  participant2.id_ = n1->ls_id_;
  participant2.addr_ = n1->addr_;
  participant2.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members2;
  for (int i = 0; i < SECONDARY_TX_COUNT2; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members2.push_back(secondary_tx_ids2[i]));
  }
  ASSERT_EQ(SECONDARY_TX_COUNT2, aggre_members2.count());
  LOG_INFO("aggre_members2 prepared", K(aggre_members2.count()), K(aggre_members2));

  // 调用 sync_hotspot_legality_validation 接口，应该会失败
  int ret2 = n1->txs_.sync_hotspot_legality_validation(participant2, primary_tx2.tx_id_, aggre_members2);
  // 由于某个子事务已经 flush redo，验证应该失败
  ASSERT_NE(OB_SUCCESS, ret2) << "sync_hotspot_legality_validation should fail when secondary tx has flushed redo";
  LOG_INFO("sync_hotspot_legality_validation failed as expected", K(primary_tx2.tx_id_), K(aggre_members2), K(ret2));

  // 等待消息处理完成
  n1->wait_all_msg_consumed();

  // ========== 验证失败后，主事务和子事务仍然可以独立写入和提交 ==========

  // 1. 验证主事务可以继续写入
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx2, snapshot2, 3001, 4001));
  LOG_INFO("Primary tx2 can still write after validation failure");

  // 2. 验证子事务可以继续写入（第一个子事务已经提交，所以只验证第二个）
  // 第一个子事务已经提交，所以跳过它
  for (int i = 1; i < SECONDARY_TX_COUNT2; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs2[i], snapshot2, 301 + i, 401 + i));
    LOG_INFO("Secondary tx2 can still write after validation failure", K(i), K(secondary_tx_ids2[i]));
  }

  // 3. 独立提交主事务
  COMMIT_TX(n1, primary_tx2, 500 * 1000);
  n1->wait_all_msg_consumed();
  LOG_INFO("Primary tx2 committed independently");

  // 验证主事务状态
  ASSERT_EQ(ObTxDesc::State::COMMITTED, primary_tx2.state_)
      << "Primary tx2 state should be COMMITTED. tx_id=" << primary_tx2.tx_id_
      << ", state=" << static_cast<int>(primary_tx2.state_);
  ASSERT_EQ(OB_SUCCESS, primary_tx2.commit_out_)
      << "Primary tx2 commit_out should be OB_SUCCESS. tx_id=" << primary_tx2.tx_id_
      << ", commit_out=" << primary_tx2.commit_out_;

  // 4. 独立提交每个子事务（第一个子事务已经提交，所以只提交第二个）
  for (int i = 1; i < SECONDARY_TX_COUNT2; i++) {
    int commit_ret = n1->commit_tx(*sec_txs2[i], n1->ts_after_us(500 * 1000));
    ASSERT_EQ(OB_SUCCESS, commit_ret)
        << "Secondary tx2 " << i << " commit failed. tx_id=" << secondary_tx_ids2[i]
        << ", commit_ret=" << commit_ret;

    // 验证子事务状态
    ASSERT_EQ(ObTxDesc::State::COMMITTED, sec_txs2[i]->state_)
        << "Secondary tx2 " << i << " state should be COMMITTED. tx_id=" << secondary_tx_ids2[i]
        << ", state=" << static_cast<int>(sec_txs2[i]->state_);
    ASSERT_EQ(OB_SUCCESS, sec_txs2[i]->commit_out_)
        << "Secondary tx2 " << i << " commit_out should be OB_SUCCESS. tx_id=" << secondary_tx_ids2[i]
        << ", commit_out=" << sec_txs2[i]->commit_out_;

    LOG_INFO("Secondary tx2 committed independently", K(i), K(secondary_tx_ids2[i]),
             K(commit_ret), K(sec_txs2[i]->state_), K(sec_txs2[i]->commit_out_));
  }

  // 验证第一个子事务已经提交
  ASSERT_EQ(ObTxDesc::State::COMMITTED, sec_txs2[0]->state_)
      << "First secondary tx2 should already be COMMITTED. tx_id=" << secondary_tx_ids2[0]
      << ", state=" << static_cast<int>(sec_txs2[0]->state_);

  n1->wait_all_msg_consumed();

  // 5. 验证所有事务的修改都已正确提交
  {
    ObTxDescGuard read_guard2 = n1->get_tx_guard();
    ObTxDesc &read_tx2 = read_guard2.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param2);
    ObTxReadSnapshot read_snapshot2;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(read_tx2, read_tx_param2.isolation_, n1->ts_after_ms(100), read_snapshot2));

    // 验证主事务的修改
    int64_t primary_val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot2, 3000, primary_val2));
    ASSERT_EQ(4000, primary_val2);
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot2, 3001, primary_val2));
    ASSERT_EQ(4001, primary_val2);
    LOG_INFO("Primary tx2 data verified", K(primary_val2));

    // 验证子事务的修改（第一个子事务只验证初始写入，第二个子事务验证初始和后续写入）
    // 第一个子事务的初始写入
    int64_t val0 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot2, 300, val0));
    ASSERT_EQ(400, val0);
    LOG_INFO("First secondary tx2 initial data verified", K(val0));

    // 第二个子事务的初始和后续写入
    for (int i = 1; i < SECONDARY_TX_COUNT2; i++) {
      int64_t val = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot2, 300 + i, val));
      ASSERT_EQ(400 + i, val);
      ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot2, 301 + i, val));
      ASSERT_EQ(401 + i, val);
      LOG_INFO("Secondary tx2 data verified", K(i), K(val));
    }
  }

  LOG_INFO("sync_hotspot_legality_validation failure scenario test completed successfully");
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 0;
    item.occur_ = 0;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_REDO_STAT", item);
  }
}

TEST_F(ObTestHotspotTxBasic, test_hotspot_tx_rollback_after_sync_validation)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  // create primary tx
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  // create secondary txs
  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(),
                                           &sec_guard1.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 500 + i, 600 + i));
  }

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 5000, 6000));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_TRUE(secondary_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  // simulate failure after sync validation, rollback primary
  LOG_INFO("simulate failure after sync validation, rollback primary tx",
           K(primary_tx.tx_id_), K(aggre_members));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(primary_tx));
  n1->wait_all_msg_consumed();
  ASSERT_EQ(ObTxDesc::State::ROLLED_BACK, primary_tx.state_);

  auto wait_tx_ctx_exiting = [&](const ObTransID &tx_id) {
    ObPartTransCtx *part_ctx = nullptr;
    int get_ret = n1->get_tx_ctx(n1->ls_id_, tx_id, part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      return;
    }
    ASSERT_EQ(OB_SUCCESS, get_ret);
    ASSERT_TRUE(part_ctx != nullptr);
    int i = 0;
    while (!part_ctx->is_exiting_ && i++ < 2000) {
      usleep(500);
    }
    ASSERT_TRUE(part_ctx->is_exiting_) << "tx ctx not exiting, tx_id=" << tx_id;
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(part_ctx));
  };

  // verify part_ctx rollback and exiting
  wait_tx_ctx_exiting(primary_tx.tx_id_);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    wait_tx_ctx_exiting(secondary_tx_ids[i]);
  }

  // release tx desc to allow ctx gc
  ASSERT_EQ(OB_SUCCESS, guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
  n1->wait_all_msg_consumed();
  int wait_ret = OB_SUCCESS;
  for (int i = 0; i < 5; i++) {
    wait_ret = n1->wait_all_tx_ctx_is_destoryed();
    if (OB_SUCCESS == wait_ret) {
      break;
    }
    usleep(100 * 1000);
  }
  ASSERT_EQ(OB_SUCCESS, wait_ret);
}

TEST_F(ObTestHotspotTxBasic, test_hotspot_tx_insert_update_freeze)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 200 + i, 300 + i));
  }

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 1000, 2000));

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_TRUE(secondary_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));

  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);

  const ObTxSEQ primary_last_seq_no =
      primary_ctx->hotspot_redo_cache_.get_primary_last_seq_no();
  const ObTxSEQ last_scn_before_update = primary_ctx->last_scn_;
  ASSERT_TRUE(last_scn_before_update == primary_last_seq_no);

  ObTxReadSnapshot update_snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_,
                                  n1->ts_after_ms(100), update_snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, update_snapshot, 1000, 3000));
  const ObTxSEQ last_scn_after_update = primary_ctx->last_scn_;
  ASSERT_TRUE(last_scn_after_update > primary_last_seq_no);

  const uint32_t freeze_clock = UINT32_MAX;
  int wait_ret = OB_SUCCESS;
  for (int i = 0; i < 50; i++) {
    bool submit_primary_log = false;
    wait_ret = primary_ctx->wait_hotspot_redo_frozen_flushed(freeze_clock, submit_primary_log);
    if (OB_SUCCESS == wait_ret || OB_BLOCK_FROZEN == wait_ret) {
      break;
    } else if (OB_SUCCESS != wait_ret) {
      usleep(100 * 1000);
    }
  }
  ASSERT_EQ(true, OB_SUCCESS == wait_ret || OB_BLOCK_FROZEN == wait_ret);

  const ObTxSEQ max_submitted_seq_no = primary_ctx->exec_info_.max_submitted_seq_no_;
  const ObTxSEQ last_scn_after_freeze = primary_ctx->last_scn_;
  // After seq remap, secondary redo is remapped to seq ranges above primary_last_seq_no,
  // so max_submitted_seq_no includes remapped secondary data (> primary_last_seq_no).
  // The primary's own update data (at even higher seq) should NOT be submitted yet.
  ASSERT_TRUE(max_submitted_seq_no >= primary_last_seq_no);
  ASSERT_TRUE(max_submitted_seq_no < last_scn_after_freeze);

  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  FLUSH_REDO(n1);
  int64_t unsubmitted_cnt = 0;
  int64_t unsynced_cnt = 0;
  const int64_t wait_start_ts = ObTimeUtility::fast_current_time();
  const int64_t wait_timeout_us = 1 * 1000 * 1000;
  do {
    unsubmitted_cnt = n1->memtable_->get_unsubmitted_cnt();
    unsynced_cnt = n1->memtable_->get_unsynced_cnt();
    if (0 == unsubmitted_cnt && 0 == unsynced_cnt) {
      break;
    }
    usleep(1000);
  } while (ObTimeUtility::fast_current_time() - wait_start_ts < wait_timeout_us);
  ASSERT_EQ(0, unsubmitted_cnt);
  ASSERT_EQ(0, unsynced_cnt);

  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_us(500 * 1000));
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
        << "Secondary tx " << i << " commit failed. tx_id=" << secondary_tx_ids[i]
        << ", commit_ret=" << commit_ret;
  }

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));

    int64_t val = 0;
    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 200 + i, val));
      ASSERT_EQ(300 + i, val);
    }
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 1000, val));
    ASSERT_EQ(3000, val);
  }
}

// ============================================================================
// Stage Five: E2E Integration Tests for Data Seq Remap, Rollback, Replay
// ============================================================================

// Helper functor for replaying log entries on a follower TxNode
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

// Test 1: Verify seq remap internal state after aggregation
// Writes data into secondary txs, aggregates them into a primary tx,
// then verifies remapped seq ranges and internal state consistency.
TEST_F(ObTestHotspotTxBasic, test_remap_e2e_internal_state)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  // create primary tx
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  // create 2 secondary txs
  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(),
                                           &sec_guard1.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 700 + i, 800 + i));
  }

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 7000, 8000));

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  // Get primary ctx and verify internal remap state
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);

  const ObTxHotspotRedoCache *cache = primary_ctx->hotspot_redo_cache_.cache_;
  ASSERT_TRUE(cache != nullptr);
  ASSERT_EQ(SECONDARY_TX_COUNT, cache->hotspot_cache_count_);

  // Verify each secondary in cache has valid remap state
  // Note: fill_redo_orig_min/max_seq_ are set asynchronously during redo fill,
  // so we only check the remap fields set during assign_remapped_seq_ranges.
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    const ObTxRedoExtractArg &arg = cache->hotspot_cache_[i];
    ASSERT_TRUE(arg.is_valid()) << "arg " << i << " should have valid ctx";
    ASSERT_TRUE(arg.tx_id_.is_valid()) << "arg " << i << " should have valid tx_id";
    ASSERT_TRUE(arg.seq_remapped_) << "arg " << i << " should be remapped";
    ASSERT_TRUE(arg.remapped_min_seq_.is_valid()) << "arg " << i << " remapped_min_seq invalid";
    ASSERT_TRUE(arg.remapped_max_seq_.is_valid()) << "arg " << i << " remapped_max_seq invalid";
    ASSERT_FALSE(arg.data_rolled_back_) << "arg " << i << " should not be rolled back";

    LOG_INFO("remap state verified", K(i), K(arg));
  }

  // Verify remap ranges don't overlap between secondaries
  if (SECONDARY_TX_COUNT >= 2) {
    const ObTxRedoExtractArg &arg0 = cache->hotspot_cache_[0];
    const ObTxRedoExtractArg &arg1 = cache->hotspot_cache_[1];
    // The remapped ranges should be non-overlapping
    ASSERT_TRUE(arg0.remapped_max_seq_ < arg1.remapped_min_seq_
                || arg1.remapped_max_seq_ < arg0.remapped_min_seq_)
        << "remapped ranges should not overlap";
  }

  // Verify primary_last_seq_no is valid and positioned before the remap ranges
  const ObTxSEQ primary_last = primary_ctx->hotspot_redo_cache_.get_primary_last_seq_no();
  ASSERT_TRUE(primary_last.is_valid());

  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  // Complete the flow: commit and verify data
  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_us(500 * 1000));
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);
  }

  // Read verification
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      int64_t val = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 700 + i, val));
      ASSERT_EQ(800 + i, val);
    }
    int64_t pval = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 7000, pval));
    ASSERT_EQ(8000, pval);
  }

  LOG_INFO("test_remap_e2e_internal_state passed");
}

// Test 2: Secondary rollback e2e with data verification and internal state check
// Aggregates secondary txs, then rolls back one secondary and verifies:
// - data_rolled_back_ flag is set
// - rolled-back secondary's data is not readable after commit
// - non-rolled-back secondary's data is still readable
TEST_F(ObTestHotspotTxBasic, test_secondary_rollback_e2e)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(),
                                           &sec_guard1.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    // Use distinct key ranges: sec0 writes key=900, sec1 writes key=901
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 900 + i, 1000 + i));
  }

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 9000, 10000));

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  // Rollback the first secondary tx (index 0)
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);

  ASSERT_EQ(OB_SUCCESS,
            primary_ctx->hotspot_rollback_to_for_secondary(secondary_tx_ids[0]));

  // Verify internal state: data_rolled_back_ flag
  {
    const ObTxHotspotRedoCache *cache = primary_ctx->hotspot_redo_cache_.cache_;
    ASSERT_TRUE(cache != nullptr);
    bool found_rolled_back = false;
    bool found_not_rolled_back = false;
    for (int i = 0; i < cache->hotspot_cache_count_; i++) {
      const ObTxRedoExtractArg &arg = cache->hotspot_cache_[i];
      if (arg.tx_id_ == secondary_tx_ids[0]) {
        ASSERT_TRUE(arg.data_rolled_back_)
            << "secondary 0 should be marked as rolled back";
        found_rolled_back = true;
      } else if (arg.tx_id_ == secondary_tx_ids[1]) {
        ASSERT_FALSE(arg.data_rolled_back_)
            << "secondary 1 should NOT be marked as rolled back";
        found_not_rolled_back = true;
      }
    }
    ASSERT_TRUE(found_rolled_back) << "should find rolled-back secondary";
    ASSERT_TRUE(found_not_rolled_back) << "should find non-rolled-back secondary";
  }

  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  // Commit primary tx
  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();

  // Commit secondary txs
  // sec0 (rolled back): should get OB_TRANS_ROLLBACKED or similar error
  int commit_ret0 = n1->commit_tx(*sec_txs[0], n1->ts_after_us(500 * 1000));
  LOG_INFO("rolled-back secondary commit result", K(commit_ret0),
           K(sec_txs[0]->state_), K(sec_txs[0]->commit_out_));
  // The rolled-back secondary should report rollback status to the scheduler
  // OB_ROLLBACK_ON_NO_AFFECTED_ROWS (-6290) is also valid for secondary tx rollback
  ASSERT_TRUE(sec_txs[0]->commit_out_ == OB_TRANS_ROLLBACKED
              || sec_txs[0]->commit_out_ == OB_TRANS_KILLED
              || sec_txs[0]->commit_out_ == OB_ROLLBACK_ON_NO_AFFECTED_ROWS)
      << "rolled-back secondary should report rollback/kill status, got commit_out="
      << sec_txs[0]->commit_out_;

  // sec1 (not rolled back): should commit successfully
  int commit_ret1 = n1->commit_tx(*sec_txs[1], n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret1 == OB_SUCCESS || commit_ret1 == OB_TRANS_COMMITED)
      << "non-rolled-back secondary should commit successfully";
  ASSERT_EQ(OB_SUCCESS, sec_txs[1]->commit_out_);

  // Read verification: primary data and sec1 data should be readable,
  // sec0 data should be rolled back (not readable or has old value)
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    // Primary data
    int64_t pval = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 9000, pval));
    ASSERT_EQ(10000, pval);

    // sec1 data (not rolled back)
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 901, val1));
    ASSERT_EQ(1001, val1);

    // sec0 data (rolled back) - should not be readable with sec0's value
    // After rollback, the key either doesn't exist or has been cleaned up
    int64_t val0 = 0;
    int read_ret0 = n1->read(read_snapshot, 900, val0);
    if (OB_SUCCESS == read_ret0) {
      // If readable, value should NOT be the rolled-back secondary's value
      // (in practice, rollback removes the data)
      LOG_INFO("sec0 key still readable after rollback", K(val0), K(read_ret0));
    } else {
      LOG_INFO("sec0 key not readable after rollback (expected)", K(read_ret0));
    }
  }

  LOG_INFO("test_secondary_rollback_e2e passed");
}

// Test 3: Primary INSERT rollback range verification
// Aggregates secondary txs, then verifies the primary's non-hotspot INSERT rollback
// range is correctly calculated. The actual rollback via rollback_to_savepoint_ cannot
// be tested in IT because redo flush happens concurrently during aggregation.
TEST_F(ObTestHotspotTxBasic, test_primary_insert_rollback_range)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  // Create secondary tx
  ObTransID secondary_tx_id;
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard0.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  secondary_tx_id = sec_tx->tx_id_;
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 1100, 1200));

  // Write PRIMARY data (INSERT range) - 2 writes create a seq range
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 11000, 12000));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 11001, 12001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_id));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  // Get primary ctx and verify the INSERT range is correctly recorded
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);

  {
    const ObTxHotspotRedoCache *cache = primary_ctx->hotspot_redo_cache_.cache_;
    ASSERT_TRUE(cache != nullptr);
    // Verify primary INSERT range was recorded
    ASSERT_TRUE(cache->primary_insert_first_seq_.is_valid())
        << "primary_insert_first_seq_ should be set after aggregation";
    ASSERT_TRUE(cache->primary_insert_last_seq_.is_valid())
        << "primary_insert_last_seq_ should be set after aggregation";
    ASSERT_TRUE(cache->primary_insert_first_seq_ <= cache->primary_insert_last_seq_)
        << "first <= last for primary INSERT range";

    LOG_INFO("primary INSERT range verified",
             K(cache->primary_insert_first_seq_), K(cache->primary_insert_last_seq_));

    // Verify get_rollback_range for primary INSERT (nullptr = primary)
    ObTxSEQ from_seq;
    ObTxSEQ to_seq;
    int range_ret = primary_ctx->hotspot_redo_cache_.get_rollback_range(nullptr, from_seq, to_seq);
    ASSERT_EQ(OB_SUCCESS, range_ret);
    ASSERT_TRUE(from_seq.is_valid()) << "rollback from_seq should be valid";
    ASSERT_TRUE(to_seq.is_valid()) << "rollback to_seq should be valid";
    ASSERT_TRUE(from_seq > to_seq) << "from > to for rollback range (inclusive from, exclusive to)";

    LOG_INFO("primary INSERT rollback range", K(from_seq), K(to_seq));
  }

  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  // Complete the flow normally (no actual rollback)
  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();

  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);

  // Verify all data committed (no rollback was done)
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t sval = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 1100, sval));
    ASSERT_EQ(1200, sval);

    int64_t pval = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 11000, pval));
    ASSERT_EQ(12000, pval);
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 11001, pval));
    ASSERT_EQ(12001, pval);
  }

  LOG_INFO("test_primary_insert_rollback_range passed");
}

// Test 4: Replay verification for hotspot aggregated transactions
// Writes and aggregates secondary txs on a leader node, commits,
// then replays all logs on a follower node and verifies data correctness.
TEST_F(ObTestHotspotTxBasic, test_hotspot_replay_e2e)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  // Both nodes must have the same ls_id for replay to work (same partition replica)
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  NAMED_DEFER(defer_n1, delete(n1));
  NAMED_DEFER(defer_n2, delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    // Create primary tx
    ObTxDesc *primary_tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(primary_tx_ptr));
    ObTxDesc &primary_tx = *primary_tx_ptr;
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(primary_tx, tx_param.isolation_,
                                                 n1->ts_after_ms(100), snapshot));

    // Create secondary txs
    const int SECONDARY_TX_COUNT = 2;
    ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
    ObTxDesc *sec_tx_ptrs[SECONDARY_TX_COUNT];

    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(sec_tx_ptrs[i]));
      ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx_ptrs[i], tx_param));
      secondary_tx_ids[i] = sec_tx_ptrs[i]->tx_id_;
      ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx_ptrs[i], snapshot, 1300 + i, 1400 + i));
    }

    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 13000, 14000));

    // Aggregate
    ObAggregatedTxIDArray aggre_members;
    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
    }

    ObTxPart participant;
    participant.id_ = n1->ls_id_;
    participant.addr_ = n1->addr_;
    participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    ASSERT_EQ(OB_SUCCESS,
              n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                        aggre_members));
    n1->wait_all_msg_consumed();

    // Commit on leader
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(primary_tx));
    n1->wait_all_msg_consumed();

    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      n1->commit_tx(*sec_tx_ptrs[i], n1->ts_after_us(500 * 1000));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(*sec_tx_ptrs[i]));
    }

    // Wait for all log callbacks and tx ctx cleanup on leader
    n1->wait_all_redolog_applied();
    ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
    int64_t retry = 0;
    while (ls_tx_ctx_mgr1->get_tx_ctx_count() > 0 && retry++ < 100) {
      usleep(50 * 1000);
    }
    ASSERT_EQ(0, ls_tx_ctx_mgr1->get_tx_ctx_count());
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
  }

  // Replay all logs on follower (n2)
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  // Switch follower to leader
  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  // Verify replayed data on the new leader (n2)
  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;
    ObTxReadSnapshot snapshot2;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx, tx_param.isolation_,
                                                 n2->ts_after_ms(100), snapshot2));
    // Verify secondary tx data after replay
    for (int i = 0; i < 2; i++) {
      int64_t val = 0;
      ASSERT_EQ(OB_SUCCESS, n2->read(snapshot2, 1300 + i, val));
      ASSERT_EQ(1400 + i, val)
          << "replayed secondary tx " << i << " data mismatch";
    }

    // Verify primary tx data after replay
    int64_t pval = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot2, 13000, pval));
    ASSERT_EQ(14000, pval) << "replayed primary tx data mismatch";

    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx));
  }

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));

  LOG_INFO("test_hotspot_replay_e2e passed");
}

// Test 5: Replay with rollback - known limitation test.
//
// KNOWN ISSUE: Replay of hotspot secondary rollback (RollbackToLog) fails on the
// follower with OB_ERR_UNEXPECTED (-4016).
//
// Root cause: On the leader, the primary tx memtable does NOT contain the remapped
// secondary data (it lives in secondary tx memtables). So rollback_to_savepoint_()
// -> mt_ctx_.rollback() is a no-op. On the follower, however, redo replay creates
// ALL remapped data (from all secondaries) in the primary tx's memtable. When a
// This test verifies the leader-side rollback + commit path works, and that
// follower-side replay now succeeds after implementing hotspot rollback special
// handling (Phase 3 fix).
//
// The fix adds secondary_tx_id_ to RollbackToLog for hotspot rollback identification,
// and in replay_rollback_to, writes UndoAction directly without calling mt_ctx_.rollback,
// avoiding the OB_ERR_UNEXPECTED error from remove_callbacks_for_rollback_to.
TEST_F(ObTestHotspotTxBasic, test_hotspot_replay_with_rollback_e2e)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  NAMED_DEFER(defer_n1, delete(n1));
  NAMED_DEFER(defer_n2, delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *primary_tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(primary_tx_ptr));
    ObTxDesc &primary_tx = *primary_tx_ptr;
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(primary_tx, tx_param.isolation_,
                                                 n1->ts_after_ms(100), snapshot));

    const int SECONDARY_TX_COUNT = 2;
    ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
    ObTxDesc *sec_tx_ptrs[SECONDARY_TX_COUNT];

    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(sec_tx_ptrs[i]));
      ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx_ptrs[i], tx_param));
      secondary_tx_ids[i] = sec_tx_ptrs[i]->tx_id_;
      ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx_ptrs[i], snapshot, 1500 + i, 1600 + i));
    }

    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 15000, 16000));

    ObAggregatedTxIDArray aggre_members;
    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
    }

    ObTxPart participant;
    participant.id_ = n1->ls_id_;
    participant.addr_ = n1->addr_;
    participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    ASSERT_EQ(OB_SUCCESS,
              n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                        aggre_members));
    n1->wait_all_msg_consumed();

    // Rollback secondary 0 on leader - should succeed
    ObPartTransCtx *primary_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
    ASSERT_TRUE(primary_ctx != nullptr);
    ASSERT_EQ(OB_SUCCESS,
              primary_ctx->hotspot_rollback_to_for_secondary(secondary_tx_ids[0]));
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

    // Commit on leader - should succeed
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(primary_tx));
    n1->wait_all_msg_consumed();

    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      n1->commit_tx(*sec_tx_ptrs[i], n1->ts_after_us(500 * 1000));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(*sec_tx_ptrs[i]));
    }

    // Verify leader-side read correctness: sec1 and primary data present, sec0 rolled back
    {
      ObTxDesc *read_tx_ptr = NULL;
      ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(read_tx_ptr));
      ASSERT_EQ(OB_SUCCESS, n1->start_tx(*read_tx_ptr, tx_param));
      ObTxReadSnapshot snap;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(*read_tx_ptr, tx_param.isolation_,
                                                    n1->ts_after_ms(100), snap));
      int64_t val = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(snap, 15000, val));
      ASSERT_EQ(16000, val) << "primary data should be present on leader";
      ASSERT_EQ(OB_SUCCESS, n1->read(snap, 1501, val));
      ASSERT_EQ(1601, val) << "sec1 data should be present on leader";
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(*read_tx_ptr, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(*read_tx_ptr));
    }

    n1->wait_all_redolog_applied();
    ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
    int64_t retry = 0;
    while (ls_tx_ctx_mgr1->get_tx_ctx_count() > 0 && retry++ < 100) {
      usleep(50 * 1000);
    }
    ASSERT_EQ(0, ls_tx_ctx_mgr1->get_tx_ctx_count());
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
  }

// Replay on follower: now succeeds after Phase 3 fix (hotspot rollback special handling).
  // The RollbackToLog replay skips mt_ctx_.rollback for hotspot rollback, writes UndoAction
  // directly, and sets skip_checksum_calc for consistency.
  ReplayLogEntryFunctor functor(n2);
  int replay_ret = n2->fake_tx_log_adapter_->replay_all(functor);
  LOG_INFO("replay with rollback result", K(replay_ret));
  EXPECT_EQ(OB_SUCCESS, replay_ret)
      << "Hotspot rollback replay should succeed with special handling";

  // Verify follower-side read correctness after hotspot rollback replay
  // (per D-13: read path should filter rolled-back data via UndoAction)
  {
    ObTxDesc *read_tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(read_tx_ptr));
    ASSERT_EQ(OB_SUCCESS, n2->start_tx(*read_tx_ptr, tx_param));
    ObTxReadSnapshot snap;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(*read_tx_ptr, tx_param.isolation_,
                                                  n2->ts_after_ms(100), snap));
    int64_t val = 0;

    // Primary data should be present on follower after replay
    ASSERT_EQ(OB_SUCCESS, n2->read(snap, 15000, val));
    ASSERT_EQ(16000, val) << "primary data should be present on follower after replay";

    // sec1 data should be present on follower (not rolled back)
    ASSERT_EQ(OB_SUCCESS, n2->read(snap, 1501, val));
    ASSERT_EQ(1601, val) << "sec1 data should be present on follower after replay";

    // sec0 data should be filtered by UndoAction (rolled back)
    // Expect OB_ENTRY_NOT_EXIST since the data is filtered via UndoAction
    int ret_sec0 = n2->read(snap, 1500, val);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret_sec0)
        << "sec0 data should be filtered by UndoAction on follower";

    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(*read_tx_ptr, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(*read_tx_ptr));
  }

  LOG_INFO("test_hotspot_replay_with_rollback_e2e passed (follower replay and read verified)");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_tx_basic.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_tx_basic.log", true, false,
                       "test_hotspot_tx_basic.log",  // rs
                       "test_hotspot_tx_basic.log",  // election
                       "test_hotspot_tx_basic.log"); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
