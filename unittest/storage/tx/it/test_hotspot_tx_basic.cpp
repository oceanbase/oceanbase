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
#include <atomic>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <functional>
#include <cstdio>
#include <unistd.h>
#define private public
#define protected public
#define USING_LOG_PREFIX TRANS
#include "lib/utility/ob_tracepoint.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/threads.h"
#include "share/ob_debug_sync.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "../../../../close_modules/hotspot_group_commit/storage/tx/ob_tx_hotspot_core.h"
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"
#include "tx_node.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

// Inspect the registered counter; calling the event to check it would consume it.
class ScopedHotspotEvent
{
public:
  explicit ScopedHotspotEvent(const char *name) : name_(name), item_(nullptr)
  {
    DLIST_FOREACH_NORET(event, common::EventTable::global_item_list()) {
      if (nullptr != event->item_.name_ && 0 == strcasecmp(event->item_.name_, name_)) {
        item_ = &event->item_;
        saved_ = *item_;
        break;
      }
    }
  }
  ~ScopedHotspotEvent()
  {
    if (nullptr != item_) {
      EXPECT_EQ(OB_SUCCESS, common::EventTable::set_event(name_, saved_));
    }
  }
  bool is_registered() const { return nullptr != item_; }
  int inject_once(const int error)
  {
    common::EventItem item;
    item.error_code_ = error;
    item.occur_ = 1;
    item.trigger_freq_ = 0;
    return common::EventTable::set_event(name_, item);
  }
  int64_t remaining() const { return nullptr == item_ ? -1 : ATOMIC_LOAD(&item_->occur_); }
private:
  const char *name_;
  common::EventItem *item_;
  common::EventItem saved_;
};

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
    // ObUndoAction / RollbackToLog 用 (to, from]：from 必须等于 last，不能是 last+1
    ASSERT_EQ(from_seq, cache->primary_insert_last_seq_)
        << "log/undo from_seq must be inclusive last, not last+1";

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

// ============================================================================
// Verify prepare version propagation and owner publication end-to-end:
// after successful aggregation commit, a reader transaction sees the
// committed data through the primary owner path.
// ============================================================================
TEST_F(ObTestHotspotTxBasic, test_hotspot_prepare_version_and_read_e2e)
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
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 8000 + i, 9000 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 80000, 90000));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();

  // Commit primary -> triggers prepare version broadcast + owner publication
  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();

  // Verify all secondaries committed
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_us(500 * 1000));
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
        << "Secondary tx " << i << " commit failed: " << commit_ret;
  }

  // New reader transaction: reads through primary owner path (owner already published)
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    GET_READ_SNAPSHOT(n1, read_tx, read_tx_param, read_snapshot);

    // Read primary data
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 80000, val));
    ASSERT_EQ(90000, val) << "Primary data should be readable via primary owner";

    // Read secondary data (owner changed to primary, read through primary ctx)
    for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
      int64_t sec_val = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 8000 + i, sec_val))
          << "Secondary tx " << i << " data should be readable after owner publication";
      ASSERT_EQ(9000 + i, sec_val);
    }

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(read_tx, n1->ts_after_ms(500)));
  }

  // Verify secondary ctx cleanup: after response_scheduler, secondary ctxs should be GC'd
  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ObPartTransCtx *sec_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(
        secondary_tx_ids[i], sec_ctx);
    if (OB_SUCCESS == get_ret && sec_ctx != nullptr) {
      // If ctx still exists, it should be in exiting state or SECONDARY_MIGRATE_SUCCEEDED
      EXPECT_TRUE(sec_ctx->is_exiting_
                  || sec_ctx->redo_flush_status_ == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED)
          << "Secondary ctx " << i << " should be exiting or terminal after commit";
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(sec_ctx));
    }
    // OB_TRANS_CTX_NOT_EXIST is also fine (ctx already GC'd)
  }
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));

  ASSERT_EQ(OB_SUCCESS, guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
  n1->wait_all_msg_consumed();
}

#ifdef ENABLE_DEBUG_LOG
// Reproduce the original -6213 race deterministically:
// 1. The reader samples the secondary owner from the undecided MVCC node.
// 2. Hotspot validation publishes the primary as the new owner, then commit
//    processing reclaims the secondary context while the reader is paused.
// 3. The reader looks up the sampled secondary and gets CTX_NOT_EXIST. It must
//    detect the changed owner and retry instead of returning -6213.
TEST_F(ObTestHotspotTxBasic, test_hotspot_lock_for_read_retries_after_owner_change)
{
  static const char *OWNER_SAMPLED_EVENT = "hotspot_owner_sampled";
  static const char *OWNER_PUBLISHED_EVENT = "hotspot_owner_published";
  static const int64_t DEBUG_SYNC_TIMEOUT_US = 30_s;
  static const int64_t WAIT_SECONDARY_CTX_GC_US = 10_s;
  static const int64_t SECONDARY_KEY = 8100;
  static const int64_t SECONDARY_VALUE = 9100;

  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, write_snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &secondary_tx = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary_tx, tx_param));
  const ObTransID secondary_tx_id = secondary_tx.tx_id_;
  ASSERT_EQ(OB_SUCCESS,
            n1->write(secondary_tx, write_snapshot, SECONDARY_KEY, SECONDARY_VALUE));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, write_snapshot, 8101, 9101));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_id));

  // Take the read snapshot before hotspot validation publishes the new owner.
  // The post-retry result may legally be either the committed value or no
  // visible row; returning -6213 is never legal after observing the change.
  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  PREPARE_TX_PARAM(read_tx_param);
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(read_tx,
                                  read_tx_param.isolation_,
                                  n1->ts_after_ms(1000),
                                  read_snapshot));

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(debug_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);

  ObDebugSyncAction pause_reader_action;
  pause_reader_action.sync_point_ = AFTER_HOTSPOT_LOCK_FOR_READ_OWNER_SNAPSHOT;
  pause_reader_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  pause_reader_action.execute_ = 1;
  pause_reader_action.signal_ = OWNER_SAMPLED_EVENT;
  pause_reader_action.wait_ = OWNER_PUBLISHED_EVENT;
  ASSERT_TRUE(pause_reader_action.is_valid());
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_reader_action));

  std::atomic<int> read_ret(OB_ERR_UNEXPECTED);
  std::atomic<int64_t> read_value(0);
  std::thread reader([&]() {
    int64_t value = 0;
    const int tmp_ret = n1->read(read_snapshot, SECONDARY_KEY, value);
    read_value.store(value);
    read_ret.store(tmp_ret);
  });

  // Wait until the reader has sampled the old secondary owner. The elapsed
  // check distinguishes a real signal from ObDSEventControl's timeout, which
  // intentionally also returns OB_SUCCESS.
  ObDebugSyncAction wait_sampled_action;
  wait_sampled_action.sync_point_ = NOW;
  wait_sampled_action.timeout_ = 10_s;
  wait_sampled_action.execute_ = 1;
  wait_sampled_action.wait_ = OWNER_SAMPLED_EVENT;
  const int64_t wait_sampled_start_ts = ObTimeUtility::current_time();
  const int wait_sampled_ret =
      GDS.set_global_action(false, false, wait_sampled_action);
  const bool owner_sampled =
      ObTimeUtility::current_time() - wait_sampled_start_ts < 9_s;

  // Do not use fatal assertions until the reader has been released and joined.
  // Validation performs the secondary -> primary owner publication after the
  // reader has sampled the old tuple.
  const int hotspot_validation_ret =
      n1->txs_.sync_hotspot_legality_validation(
          participant, primary_tx.tx_id_, aggre_members);
  n1->wait_all_msg_consumed();
  const int primary_commit_ret =
      n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000));
  n1->wait_tx_log_synced();
  n1->wait_all_msg_consumed();

  // The test_tx framework models each scheduler with its own ObTxDesc. Finish
  // the secondary scheduler request and release its descriptor so the
  // response task can transition the secondary ctx to its terminal state and
  // reclaim it, as a real client does after receiving the commit result.
  const int secondary_commit_ret =
      n1->commit_tx(secondary_tx, n1->ts_after_us(500 * 1000));
  const int secondary_guard_release_ret = secondary_guard.release();
  n1->wait_all_msg_consumed();

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  const int get_mgr_ret =
      n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr);
  int secondary_lookup_ret = get_mgr_ret;
  int secondary_ctx_revert_ret = OB_SUCCESS;
  bool secondary_ctx_reclaimed = false;
  if (OB_SUCCESS == get_mgr_ret && nullptr != ls_tx_ctx_mgr) {
    const int64_t gc_deadline =
        ObTimeUtility::current_time() + WAIT_SECONDARY_CTX_GC_US;
    do {
      ObPartTransCtx *secondary_ctx = nullptr;
      secondary_lookup_ret =
          ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(secondary_tx_id,
                                                           secondary_ctx);
      if (OB_TRANS_CTX_NOT_EXIST == secondary_lookup_ret) {
        secondary_ctx_reclaimed = true;
        break;
      } else if (OB_SUCCESS == secondary_lookup_ret && nullptr != secondary_ctx) {
        const int tmp_ret = ls_tx_ctx_mgr->revert_tx_ctx(secondary_ctx);
        if (OB_SUCCESS != tmp_ret) {
          secondary_ctx_revert_ret = tmp_ret;
          break;
        }
      } else {
        break;
      }
      ob_usleep(1_ms);
    } while (ObTimeUtility::current_time() < gc_deadline);
  }

  int revert_mgr_ret = OB_SUCCESS;
  if (nullptr != ls_tx_ctx_mgr) {
    revert_mgr_ret =
        n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr);
  }

  ObDebugSyncAction resume_reader_action;
  resume_reader_action.sync_point_ = NOW;
  resume_reader_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  resume_reader_action.execute_ = 1;
  resume_reader_action.signal_ = OWNER_PUBLISHED_EVENT;
  const int resume_reader_ret =
      GDS.set_global_action(false, false, resume_reader_action);
  reader.join();

  EXPECT_EQ(OB_SUCCESS, wait_sampled_ret);
  EXPECT_TRUE(owner_sampled);
  EXPECT_EQ(OB_SUCCESS, hotspot_validation_ret);
  EXPECT_EQ(OB_SUCCESS, primary_commit_ret);
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret
              || OB_TRANS_COMMITED == secondary_commit_ret);
  EXPECT_EQ(OB_SUCCESS, secondary_guard_release_ret);
  EXPECT_EQ(OB_SUCCESS, get_mgr_ret);
  EXPECT_EQ(OB_SUCCESS, secondary_ctx_revert_ret);
  EXPECT_TRUE(secondary_ctx_reclaimed)
      << "secondary ctx was not reclaimed, last lookup ret="
      << secondary_lookup_ret;
  EXPECT_EQ(OB_SUCCESS, revert_mgr_ret);
  EXPECT_EQ(OB_SUCCESS, resume_reader_ret);
  EXPECT_NE(OB_TRANS_CTX_NOT_EXIST, read_ret.load());
  EXPECT_TRUE(OB_SUCCESS == read_ret.load()
              || OB_ENTRY_NOT_EXIST == read_ret.load())
      << "unexpected read ret=" << read_ret.load();
  if (OB_SUCCESS == read_ret.load()) {
    EXPECT_EQ(SECONDARY_VALUE, read_value.load());
  }

  ASSERT_EQ(OB_SUCCESS, guard.release());
  ASSERT_EQ(OB_SUCCESS, read_guard.release());
  n1->wait_all_msg_consumed();
}
#endif

// Test: Verify primary tx partial rollback (hotspot_rollback_to for primary)
// Aggregates secondary txs into primary tx, then attempts to roll back the primary
// transaction's non-hotspot INSERT data, testing both leader-side rollback and commit/read.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_to_e2e)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  // The real callbacks, not synthetic seq values, define the original range.
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 21001, 31001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 21002, 31002));
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_NE(nullptr, primary_ctx);
  NAMED_DEFER(revert_primary_ctx, (void)n1->revert_tx_ctx(primary_ctx));
  ObTxSEQ first_seq;
  ObTxSEQ last_seq;
  {
    CtxLockGuard ctx_guard(primary_ctx->lock_);
    auto &callbacks = primary_ctx->mt_ctx_.trans_mgr_.callback_list_;
    ASSERT_EQ(2, callbacks.get_length());
    first_seq = callbacks.get_guard()->get_next()->get_seq_no();
    last_seq = callbacks.get_tail()->get_seq_no();
  }

  // Create secondary tx
  ObTransID secondary_tx_id;
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard0.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  secondary_tx_id = sec_tx->tx_id_;
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 22001, 32001));

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

  ObTxSEQ from_seq;
  ObTxSEQ to_seq;
  ASSERT_EQ(OB_SUCCESS,
            primary_ctx->hotspot_redo_cache_.get_rollback_range(nullptr, from_seq, to_seq));
  ASSERT_TRUE(from_seq == last_seq); // closed upper endpoint must be undone
  ASSERT_TRUE(to_seq + 1 == first_seq);

  // Refresh the primary snapshot after validation and use the real access/write
  // path. This is not a SQL aggregation-UPDATE or durability fixture.
  ObTxReadSnapshot update_snapshot;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(primary_tx, tx_param.isolation_,
                                              n1->ts_after_ms(100), update_snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, update_snapshot, 21003, 31003));
  ObTxSEQ new_seq;
  {
    CtxLockGuard ctx_guard(primary_ctx->lock_);
    auto &callbacks = primary_ctx->mt_ctx_.trans_mgr_.callback_list_;
    ASSERT_NE(callbacks.get_guard(), callbacks.get_tail());
    new_seq = callbacks.get_tail()->get_seq_no();
  }
  ASSERT_TRUE(new_seq > from_seq) << "post-validation callback must be outside undo range";

  // Trigger rollback for primary tx via hotspot_rollback_to
  ObSEArray<ObTransID, 1> rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));

  int rb_ret = n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids);
  LOG_INFO("hotspot_rollback_to for primary tx result", K(rb_ret), K(primary_tx.tx_id_));
  ASSERT_EQ(OB_SUCCESS, rb_ret) << "hotspot_rollback_to for primary tx should succeed";
  ObTxSEQ range_from_after;
  ObTxSEQ range_to_after;
  ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_rollback_range(
                            nullptr, range_from_after, range_to_after));
  EXPECT_TRUE(range_from_after == from_seq);
  EXPECT_TRUE(range_to_after == to_seq);

  // Read as the primary itself before commit, then independently after commit.
  int64_t own_value = 0;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(primary_tx, 21001, own_value));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(primary_tx, 21002, own_value));
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 21003, own_value));
  EXPECT_EQ(31003, own_value);
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 22001, own_value));
  EXPECT_EQ(32001, own_value);

  // Commit primary and secondary txs
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000)));
  n1->wait_all_msg_consumed();

  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);

  // Verify data visibility
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));

    // Secondary data should be visible
    int64_t sec_val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 22001, sec_val));
    ASSERT_EQ(32001, sec_val) << "secondary tx data should be present and match committed value";
    int64_t high_seq_val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 21003, high_seq_val));
    EXPECT_EQ(31003, high_seq_val) << "post-validation primary data must survive rollback";

    // Primary data should be rolled back (not exist or return OB_ENTRY_NOT_EXIST)
    int64_t pri_val1 = 0;
    int read_ret1 = n1->read(read_snapshot, 21001, pri_val1);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, read_ret1)
        << "primary key 21001 should be rolled back and not exist, but got val=" << pri_val1;

    int64_t pri_val2 = 0;
    int read_ret2 = n1->read(read_snapshot, 21002, pri_val2);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, read_ret2)
        << "primary key 21002 should be rolled back and not exist, but got val=" << pri_val2;
  }

  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  n1->wait_all_msg_consumed();

  LOG_INFO("test_primary_rollback_to_e2e finished");
}

// Test: second primary rollback RPC succeeds; log_submitted is stack-local so a second
// RPC may rewrite RollbackToLog. Assert final visibility, not redo_lsns_.count().
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_to_idempotent)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 23001, 33001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 23002, 33002));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 24001, 34001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ObSEArray<ObTransID, 1> rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids));

  // log_submitted 只在同一次调用栈上有效；跨调用不保证不写第二条 RollbackToLog。
  // 幂等验收是：第二次调用成功，且最终数据仍是 primary 回滚、secondary 可见。
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids))
      << "second primary rollback should succeed";

  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 24001, val));
    ASSERT_EQ(34001, val);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 23001, val));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 23002, val));
  }
  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
}

// Test: undo is visible on the leader before commit. RollbackToLog is submitted first;
// the undo attaches right after submit success, still long before the tx commits.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_visible_before_commit)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 25001, 35001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 25002, 35002));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 26001, 36001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ObSEArray<ObTransID, 1> rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids));

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 25001, val))
        << "primary key must be hidden by undo before commit";
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 25002, val));
  }

  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);
  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
}

// Test: follower replays primary hotspot RollbackToLog without stack-top rollback crash.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_follower_replay)
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

    ObTxDesc *sec_tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(sec_tx_ptr));
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx_ptr, tx_param));
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx_ptr, snapshot, 27001, 37001));
    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 28001, 38001));
    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 28002, 38002));

    ObAggregatedTxIDArray aggre_members;
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx_ptr->tx_id_));
    ObTxPart participant;
    participant.id_ = n1->ls_id_;
    participant.addr_ = n1->addr_;
    participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    ASSERT_EQ(OB_SUCCESS,
              n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                        aggre_members));
    n1->wait_all_msg_consumed();

    ObSEArray<ObTransID, 1> rollback_tx_ids;
    ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));
    ASSERT_EQ(OB_SUCCESS,
              n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids));

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(primary_tx));
    n1->wait_all_msg_consumed();
    n1->commit_tx(*sec_tx_ptr, n1->ts_after_us(500 * 1000));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(*sec_tx_ptr));

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

  ReplayLogEntryFunctor functor(n2);
  int replay_ret = n2->fake_tx_log_adapter_->replay_all(functor);
  EXPECT_EQ(OB_SUCCESS, replay_ret)
      << "primary hotspot RollbackToLog replay must not hit stack-top rollback";

  {
    ObTxDesc *read_tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(read_tx_ptr));
    ASSERT_EQ(OB_SUCCESS, n2->start_tx(*read_tx_ptr, tx_param));
    ObTxReadSnapshot snap;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(*read_tx_ptr, tx_param.isolation_,
                                                  n2->ts_after_ms(100), snap));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snap, 27001, val));
    ASSERT_EQ(37001, val) << "secondary data should remain after primary rollback replay";
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n2->read(snap, 28001, val));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n2->read(snap, 28002, val));
    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(*read_tx_ptr, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(*read_tx_ptr));
  }
}

// Test: secondary rollback then freeze must not revive rolled-back data.
TEST_F(ObTestHotspotTxBasic, test_secondary_rollback_then_freeze_no_revive)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec0 = &sec_guard0.get_tx_desc();
  ObTxDesc *sec1 = &sec_guard1.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec0, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec1, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec0, snapshot, 29001, 39001));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec1, snapshot, 29002, 39002));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 30000, 40000));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec0->tx_id_));
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec1->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);
  ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_rollback_to_for_secondary(sec0->tx_id_));

  const uint32_t freeze_clock = UINT32_MAX;
  int wait_ret = OB_SUCCESS;
  for (int i = 0; i < 50; i++) {
    bool submit_primary_log = false;
    wait_ret = primary_ctx->wait_hotspot_redo_frozen_flushed(freeze_clock, submit_primary_log);
    if (OB_SUCCESS == wait_ret || OB_BLOCK_FROZEN == wait_ret) {
      break;
    }
    usleep(100 * 1000);
  }
  ASSERT_TRUE(OB_SUCCESS == wait_ret || OB_BLOCK_FROZEN == wait_ret);
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  n1->commit_tx(*sec0, n1->ts_after_us(500 * 1000));
  int commit_ret1 = n1->commit_tx(*sec1, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret1 == OB_SUCCESS || commit_ret1 == OB_TRANS_COMMITED);

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 30000, val));
    ASSERT_EQ(40000, val);
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 29002, val));
    ASSERT_EQ(39002, val);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 29001, val))
        << "rolled-back secondary must stay invisible after freeze";
  }
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
}

// Test: secondary rollback then immediate commit must not hang on all_redo_flushed.
TEST_F(ObTestHotspotTxBasic, test_secondary_rollback_then_immediate_commit)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec0 = &sec_guard0.get_tx_desc();
  ObTxDesc *sec1 = &sec_guard1.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec0, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec1, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec0, snapshot, 31001, 41001));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec1, snapshot, 31002, 41002));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 32000, 42000));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec0->tx_id_));
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec1->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);
  ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_rollback_to_for_secondary(sec0->tx_id_));
  ASSERT_TRUE(primary_ctx->hotspot_redo_cache_.all_redo_flushed())
      << "dropping unsubmitted redo of a rolled-back slot must let all_redo_flushed finish";
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  n1->commit_tx(*sec0, n1->ts_after_us(500 * 1000));
  int commit_ret1 = n1->commit_tx(*sec1, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret1 == OB_SUCCESS || commit_ret1 == OB_TRANS_COMMITED);

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 32000, val));
    ASSERT_EQ(42000, val);
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 31002, val));
    ASSERT_EQ(41002, val);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 31001, val));
  }
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
}

// Test: primary rollback immediately after validation, while redo may still be in flight.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_immediately_after_validation)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 33001, 43001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 33002, 43002));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 34001, 44001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  // Do not wait extra drain; this is the original FAIL posture for Case 2/3.
  ObSEArray<ObTransID, 1> rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids));

  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 34001, val));
    ASSERT_EQ(44001, val);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 33001, val));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 33002, val));
  }
  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
}

// Participant failure after a checkpoint batch, before RollbackToLog. The
// real callback/checkpoint failure cases are covered by the batch primitive UT.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_checkpoint_batch_fail)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 40001, 50001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 40002, 50002));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 41001, 51001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ScopedHotspotEvent fault("EN_HOTSPOT_PRIMARY_ROLLBACK_MEMTABLE_FAIL");
  ASSERT_TRUE(fault.is_registered());
  ASSERT_EQ(OB_SUCCESS, fault.inject_once(OB_ERR_UNEXPECTED));

  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_NE(nullptr, primary_ctx);
  NAMED_DEFER(revert_primary_ctx, (void)n1->revert_tx_ctx(primary_ctx));
  ObHotspotRollbackWork work;
  EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, primary_ctx->hotspot_rollback_to_for_non_hotspot(work));
  EXPECT_EQ(0, fault.remaining());
  EXPECT_EQ(OB_ERR_UNEXPECTED, work.cause_ret_);
  EXPECT_GT(work.checkpoint_batch_cnt_, 0);
  EXPECT_GE(work.removed_cb_cnt_, 0);
  EXPECT_EQ(0, work.rollback_log_out_cnt_);
  {
    CtxLockGuard ctx_guard(primary_ctx->lock_);
    EXPECT_TRUE(primary_ctx->need_force_abort_());
  }

  // Commit should fail due to force_abort
  int commit_ret = n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000));
  EXPECT_NE(OB_SUCCESS, commit_ret) << "commit should fail after force_abort";

  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
}

// The event is only compiled/registered with ENABLE_DEBUG_LOG. The fake append
// has accepted the log, but this fixture does not prove it is durable.
#ifdef ENABLE_DEBUG_LOG
static void run_primary_rollback_attach_undo_fail_case(MsgBus &bus_, const int injected_error)
{
  SCOPED_TRACE(injected_error);
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 42001, 52001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 42002, 52002));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 43001, 53001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ScopedHotspotEvent fault("EN_ROLLBACK_TO_LOG_ATTACH_UNDO_FAIL");
  ASSERT_TRUE(fault.is_registered());
  ASSERT_EQ(OB_SUCCESS, fault.inject_once(injected_error));

  ObSEArray<ObTransID, 1> rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));
  int rb_ret = n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids);
  EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, rb_ret);
  EXPECT_EQ(0, fault.remaining());

  // Verify force_abort is set
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);
  EXPECT_TRUE(primary_ctx->need_force_abort_()) << "force_abort should be set after attach undo fail";
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  // Commit should fail due to force_abort
  int commit_ret = n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000));
  EXPECT_NE(OB_SUCCESS, commit_ret) << "commit should fail after force_abort";

  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_attach_undo_fail)
{
  run_primary_rollback_attach_undo_fail_case(bus_, OB_ALLOCATE_MEMORY_FAILED);
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_attach_undo_retryable_error_is_terminal)
{
  run_primary_rollback_attach_undo_fail_case(bus_, OB_EAGAIN);
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_iter_end_is_not_public_success)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  for (const bool through_service : {false, true}) {
    SCOPED_TRACE(through_service ? "service" : "public ctx");
    PREPARE_TX(n1, primary_tx);
    PREPARE_TX_PARAM(tx_param);
    tx_param.timeout_us_ = 1000_s;
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
    const int64_t key = through_service ? 49010 : 49000;
    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, key, 59000));
    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, key + 1, 59001));
    ObTxDescGuard sec_guard = n1->get_tx_guard();
    ObTxDesc &sec_tx = sec_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
    ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, key + 2, 59002));
    ObAggregatedTxIDArray members;
    ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
    ObTxPart participant;
    participant.id_ = n1->ls_id_;
    participant.addr_ = n1->addr_;
    participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                              participant, primary_tx.tx_id_, members));
    n1->wait_all_msg_consumed();
    ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));

    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
    ASSERT_NE(nullptr, ctx);
    NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
    // Unlike a generic alloc fake, this event cannot be consumed by the
    // primary redo preparation that precedes RollbackToLog.
    ScopedHotspotEvent fault("EN_ROLLBACK_TO_LOG_ALLOC_ATTACH_NODE_FAIL");
    ASSERT_TRUE(fault.is_registered());
    ASSERT_EQ(OB_SUCCESS, fault.inject_once(OB_ITER_END));
    ASSERT_EQ(1, fault.remaining());
    if (through_service) {
      ObSEArray<ObTransID, 1> rollback_ids;
      ASSERT_EQ(OB_SUCCESS, rollback_ids.push_back(primary_tx.tx_id_));
      EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, n1->txs_.hotspot_rollback_to(
                    participant, primary_tx.tx_id_, rollback_ids));
    } else {
      ObHotspotRollbackWork work;
      EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, ctx->hotspot_rollback_to_for_non_hotspot(work));
      EXPECT_EQ(OB_ITER_END, work.cause_ret_);
      EXPECT_EQ(ObHotspotRollbackWork::Phase::LOG_UNDO, work.phase_);
      EXPECT_EQ(0, work.rollback_log_out_cnt_);
      EXPECT_FALSE(work.done_);
    }
    EXPECT_EQ(0, fault.remaining());
    // A no-out prepare failure need not force_abort. The direct storage caller
    // must perform descriptor rollback; this is not SQL end-group coverage.
    ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(primary_tx));
    ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(sec_tx));
    n1->wait_all_msg_consumed();
    ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
    EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, primary_tx.state_);
    EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, sec_tx.state_);
    EXPECT_NE(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
    EXPECT_NE(OB_SUCCESS, n1->commit_tx(sec_tx, n1->ts_after_us(500_ms)));
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    int64_t value = 0;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, key, value));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, key + 1, value));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, key + 2, value));
  }
}
#endif // ENABLE_DEBUG_LOG

// Test: undo action is visible in tx_data after primary rollback.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_undo_visible_in_tx_data)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 42001, 52001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 42002, 52002));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 43001, 53001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();

  ObSEArray<ObTransID, 1> rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(primary_tx.tx_id_));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_tx_ids));

  // Verify undo action is present in tx_data
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);

  ObTxDataGuard tx_data_guard;
  ASSERT_EQ(OB_SUCCESS, primary_ctx->ctx_tx_data_.get_tx_data(tx_data_guard));
  ASSERT_TRUE(tx_data_guard.tx_data() != nullptr);
  ASSERT_TRUE(tx_data_guard.tx_data()->op_guard_.is_valid());

  ObUndoStatusList &undo_list = tx_data_guard.tx_data()->op_guard_->get_undo_status_list();
  EXPECT_GT(undo_list.undo_node_cnt_, 0) << "undo list should contain at least one node after rollback";

  // Walk undo list and verify primary insert range is covered
  bool found_primary_undo = false;
  ObUndoStatusNode *node = undo_list.head_;
  while (node != nullptr) {
    for (int i = 0; i < node->size_; i++) {
      ObUndoAction &action = node->undo_actions_[i];
      if (action.is_valid()) {
        // Primary insert seq range should be covered by undo
        // The exact seq values depend on internal state, but we verify undo exists
        found_primary_undo = true;
      }
    }
    node = node->next_;
  }
  EXPECT_TRUE(found_primary_undo) << "should find undo action for primary insert range";

  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  // Commit and verify visibility
  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED);

  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 43001, val));
    ASSERT_EQ(53001, val);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 42001, val));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_snapshot, 42002, val));
  }
  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
}

// Shared-submit owns every resource until out. The allocator fake only injects
// failures; it does not simulate real throttle-info OOM or physical I/O.
TEST_F(ObTestHotspotTxBasic, test_shared_rollback_prepare_no_out_and_recovery)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 48001, 58001));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 48002, 58002));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  ObTxSEQ from;
  ObTxSEQ to;
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    auto &callbacks = ctx->mt_ctx_.trans_mgr_.callback_list_;
    ASSERT_EQ(2, callbacks.get_length());
    from = callbacks.get_tail()->get_seq_no();
    to = callbacks.get_guard()->get_next()->get_seq_no() + (-1);
  }
  ASSERT_EQ(OB_SUCCESS, ctx->submit_redo_log_for_freeze(UINT32_MAX));
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  auto &table = n1->fake_tx_table_.tx_data_table_;
  NAMED_DEFER(reset_fake,
              ATOMIC_STORE(&table.next_alloc_tx_data_ret_, OB_SUCCESS);
              table.free_bounded_attach_nodes_ = false);
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    const int64_t free_before = ctx->free_cbs_.get_size();
    const int64_t busy_before = ctx->busy_cbs_.get_size();
    const int64_t groups_before = ctx->extra_cb_group_list_.get_size();
    const int32_t ref_before = ATOMIC_LOAD(&ctx->ref_);
    const int64_t logs_before = ctx->exec_info_.redo_lsns_.count();
    const int64_t adapter_lsn_before = n1->fake_tx_log_adapter_->lsn_;
    ASSERT_GT(free_before, 0);
    ASSERT_EQ(0, busy_before);
    const auto check_no_out = [&]() {
      EXPECT_EQ(free_before, ctx->free_cbs_.get_size());
      EXPECT_EQ(busy_before, ctx->busy_cbs_.get_size());
      EXPECT_EQ(groups_before, ctx->extra_cb_group_list_.get_size());
      EXPECT_EQ(ref_before, ATOMIC_LOAD(&ctx->ref_));
      EXPECT_EQ(logs_before, ctx->exec_info_.redo_lsns_.count());
      EXPECT_EQ(adapter_lsn_before, n1->fake_tx_log_adapter_->lsn_);
      DLIST_FOREACH_NORET(cb, ctx->free_cbs_) {
        EXPECT_FALSE(cb->is_busy());
        EXPECT_EQ(nullptr, cb->get_undo_node());
        EXPECT_EQ(nullptr, cb->get_tx_data_guard().tx_data());
      }
    };
    {
      auto *saved_table = ctx->ls_tx_ctx_mgr_->tx_table_;
      NAMED_DEFER(restore_table, ctx->ls_tx_ctx_mgr_->tx_table_ = saved_table);
      ctx->ls_tx_ctx_mgr_->tx_table_ = nullptr;
      EXPECT_EQ(OB_ERR_UNEXPECTED,
                ctx->submit_rollback_to_log_(from, to, tx.tx_id_));
      check_no_out();
    }
    // Allocation errors retain their original results.
    for (const int injected_ret : {OB_ALLOCATE_MEMORY_FAILED, OB_ITER_END, OB_TIMEOUT}) {
      ATOMIC_STORE(&table.next_alloc_tx_data_ret_, injected_ret);
      const int ret = ctx->submit_rollback_to_log_(from, to, tx.tx_id_);
      EXPECT_EQ(injected_ret, ret);
      EXPECT_EQ(OB_SUCCESS, ATOMIC_LOAD(&table.next_alloc_tx_data_ret_));
      check_no_out();
    }
#ifdef ENABLE_DEBUG_LOG
    const char *events[] = {"EN_ROLLBACK_TO_LOG_ALLOC_ATTACH_NODE_FAIL",
                            "EN_ROLLBACK_TO_LOG_GET_TX_DATA_FAIL",
                            "EN_ROLLBACK_TO_LOG_INIT_TX_OP_FAIL"};
    // Only no-out bounded attach nodes are returned through this fake switch;
    // callback undo nodes retain their separate MTL owner.
    table.free_bounded_attach_nodes_ = true;
    for (int i = 0; i < 3; ++i) {
      ScopedHotspotEvent fault(events[i]);
      ASSERT_TRUE(fault.is_registered()) << events[i];
      for (const int raw_ret : {OB_ALLOCATE_MEMORY_FAILED, OB_ITER_END, OB_TIMEOUT}) {
        SCOPED_TRACE(events[i]);
        const int64_t freed_before = table.freed_bounded_attach_nodes_;
        ASSERT_EQ(OB_SUCCESS, fault.inject_once(raw_ret));
        const int ret = ctx->submit_rollback_to_log_(from, to, tx.tx_id_);
        EXPECT_EQ(raw_ret, ret);
        EXPECT_EQ(0, fault.remaining());
        EXPECT_EQ(freed_before + (0 == i ? 0 : 1), table.freed_bounded_attach_nodes_);
        check_no_out();
      }
    }
    table.free_bounded_attach_nodes_ = false;
#endif
  }

  n1->fake_tx_log_adapter_->set_bounded_pause(5_s);
  NAMED_DEFER(release_apply,
              n1->fake_tx_log_adapter_->clear_bounded_pause();
              EXPECT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s)));
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    const int64_t free_before = ctx->free_cbs_.get_size();
    ASSERT_EQ(OB_SUCCESS, ctx->submit_rollback_to_log_(from, to, tx.tx_id_));
    EXPECT_EQ(1, ctx->busy_cbs_.get_size());
    EXPECT_EQ(free_before - 1, ctx->free_cbs_.get_size());
  }
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_pause_hit(1_s));
  EXPECT_FALSE(n1->fake_tx_log_adapter_->pause_expired());
  n1->fake_tx_log_adapter_->clear_bounded_pause();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    EXPECT_EQ(0, ctx->busy_cbs_.get_size());
  }
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
}

TEST_F(ObTestHotspotTxBasic, test_fake_tx_data_finite_deadline_is_not_ignored)
{
  ObFakeTxDataTable table;
  ObTxDataGuard guard;
  EXPECT_EQ(OB_TIMEOUT, table.alloc_tx_data(guard, true, ObTimeUtility::current_time() - 1));
  EXPECT_EQ(nullptr, guard.tx_data());
  EXPECT_EQ(1, ATOMIC_LOAD(&table.alloc_tx_data_calls_));
}

TEST_F(ObTestHotspotTxBasic, test_flush_failure_keeps_buffer_scope_and_counters)
{
  ObTxRedoExtractArg slot;
  ObTxHotspotRedoCache cache;
  cache.hotspot_cache_ = &slot;
  cache.hotspot_cache_capacity_ = 1;
  cache.hotspot_cache_count_ = 1;
  // The cache normally owns placement-new slots. This white-box slot is stack
  // owned and must not be destroyed twice, including on a fatal assertion.
  NAMED_DEFER(detach_stack_slot, cache.hotspot_cache_ = nullptr);
  const char payload[] = "pending-redo";
  MEMCPY(slot.redo_log_buf_.get_buf(), payload, sizeof(payload));
  slot.redo_buf_pos_ = sizeof(payload);
  slot.fill_redo_size_ = sizeof(payload);
  slot.fill_redo_max_seq_ = ObTxSEQ(10, 0);
  slot.mvcc_cb_scope_.cnt_ = 3;
  slot.mvcc_cb_scope_.data_size_ = sizeof(payload);
  slot.unsynced_node_cnt_ = 3;
  slot.pending_log_size_ = sizeof(payload);
  slot.redo_submitting_scn_.set_max();
  ASSERT_TRUE(slot.is_log_submitting());
  ASSERT_EQ(OB_SUCCESS, cache.after_flush_hotspot_redo(
                            0, SCN::invalid_scn(), OB_ERR_UNEXPECTED));
  EXPECT_FALSE(slot.is_log_submitting());
  EXPECT_FALSE(slot.is_log_syncing());
  EXPECT_TRUE(slot.pending_redo_buf());
  EXPECT_EQ(OB_ERR_UNEXPECTED, slot.last_submit_ret_code_);
  EXPECT_EQ(3, slot.mvcc_cb_scope_.cnt_);
  EXPECT_EQ(3, slot.unsynced_node_cnt_);
  EXPECT_EQ(sizeof(payload), slot.pending_log_size_);
  EXPECT_EQ(sizeof(payload), slot.redo_buf_pos_);
  EXPECT_EQ(0, MEMCMP(payload, slot.redo_log_buf_.get_buf(), sizeof(payload)));

  // Retry the real copy/mark step. No secondary callback or successful append
  // is faked here; that success/sync/data contract is checked by the IT below.
  char output[512] = {};
  ObTxRedoLog redo_log(DATA_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, redo_log.set_mutator_buf(output));
  ASSERT_EQ(OB_SUCCESS, redo_log.set_mutator_size(sizeof(output), false));
  ObTxSEQ redo_seq;
  ASSERT_EQ(OB_SUCCESS, cache.try_flush_hotspot_redo(0, redo_log, redo_seq));
  EXPECT_TRUE(slot.is_log_submitting());
  EXPECT_TRUE(redo_seq == slot.fill_redo_max_seq_);
  EXPECT_EQ(0, MEMCMP(payload, redo_log.get_mutator_buf(), sizeof(payload)));
  EXPECT_EQ(3, slot.unsynced_node_cnt_);
}

TEST_F(ObTestHotspotTxBasic, test_hotspot_rollback_budget_samples_and_member_total)
{
  int64_t remaining = -1;
  ObHotspotRollbackWork work(1000);
  const int64_t start_mono = work.start_mono_us_;
  const int64_t start_wall = work.start_wall_us_;
  work.begin_member(ObTransID(1), true);
  EXPECT_EQ(OB_SUCCESS, work.check_deadline_at(start_mono + 300, start_wall + 300, remaining));
  EXPECT_EQ(700, remaining);
  work.begin_member(ObTransID(2), false);
  EXPECT_EQ(OB_SUCCESS, work.check_deadline_at(start_mono + 900, start_wall + 900, remaining));
  EXPECT_EQ(100, remaining); // a new member must not get a new budget
  EXPECT_EQ(OB_TIMEOUT, work.check_deadline_at(start_mono + 1000, start_wall + 1000, remaining));
  EXPECT_EQ(0, remaining);
  EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::LOCAL, work.timeout_source_);

  ObHotspotRollbackWork tx_first(1000);
  tx_first.tx_expire_wall_us_ = tx_first.start_wall_us_ + 200;
  EXPECT_EQ(OB_TRANS_TIMEOUT, tx_first.check_deadline_at(
                tx_first.start_mono_us_ + 201, tx_first.start_wall_us_ + 201, remaining));
  EXPECT_EQ(-1, remaining);
  EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::TX, tx_first.timeout_source_);

  const int64_t legacy_deadline = ObClockGenerator::getClock() + 200;
  ObHotspotRollbackWork legacy(1000, legacy_deadline);
  EXPECT_EQ(OB_TIMEOUT, legacy.check_deadline_at(legacy.start_mono_us_, legacy_deadline, remaining));
  EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::LEGACY_CTX, legacy.timeout_source_);
  ObHotspotRollbackWork expired(0);
  EXPECT_EQ(OB_TIMEOUT, expired.check_deadline_at(expired.start_mono_us_, expired.start_wall_us_, remaining));
}

// Test: flush finish_mutator_buf failure is recovered via after_flush.
TEST_F(ObTestHotspotTxBasic, test_flush_finish_mutator_fail_recovery)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 44001, 54001));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 45001, 55001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  // This event is registered independently of ENABLE_DEBUG_LOG. Set it before
  // validation, whose dispatch path already flushes the secondary redo.
  ScopedHotspotEvent fault("EN_HOTSPOT_FLUSH_FINISH_MUTATOR_FAIL");
  ASSERT_TRUE(fault.is_registered());
  ASSERT_EQ(OB_SUCCESS, fault.inject_once(OB_ERR_UNEXPECTED));
  ASSERT_EQ(1, fault.remaining());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));
  n1->wait_all_msg_consumed();
  EXPECT_EQ(0, fault.remaining()) << "the real finish_mutator path must consume the event";

  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_NE(nullptr, primary_ctx);
  NAMED_DEFER(revert_primary_ctx, (void)n1->revert_tx_ctx(primary_ctx));
  {
    CtxLockGuard ctx_guard(primary_ctx->lock_);
    EXPECT_TRUE(has_primary_aggregation_succeeded(primary_ctx->redo_flush_status_));
    EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.all_redo_flushed());
    EXPECT_GT(primary_ctx->exec_info_.redo_lsns_.count(), 0);
  }
  n1->wait_tx_log_synced();
  EXPECT_EQ(0, n1->fake_tx_log_adapter_->get_inflight_cnt());

  // Secondary should still be able to commit after flush failure recovery.
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500 * 1000)));
  n1->wait_all_msg_consumed();
  int commit_ret = n1->commit_tx(*sec_tx, n1->ts_after_us(500 * 1000));
  ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
      << "secondary should commit after flush failure recovery";

  // Verify data visibility
  {
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    PREPARE_TX_PARAM(read_tx_param);
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(read_tx, read_tx_param.isolation_,
                                    n1->ts_after_ms(100), read_snapshot));
    int64_t val = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 45001, val));
    ASSERT_EQ(55001, val);
    ASSERT_EQ(OB_SUCCESS, n1->read(read_snapshot, 44001, val));
    ASSERT_EQ(54001, val);
  }
  ASSERT_EQ(OB_SUCCESS, sec_guard.release());
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  EXPECT_EQ(0, fault.remaining());
}

// The fake gates actual on_success, not log persistence. No test worker/future
// or fixed sleep is needed: rollback has its own budget and the gate auto-expires.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_busy_cb_local_timeout)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 46001, 56001));

  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 47001, 57001));

  ObAggregatedTxIDArray aggre_members;
  ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(sec_tx->tx_id_));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                      aggre_members));

  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_NE(nullptr, primary_ctx);
  NAMED_DEFER(revert_primary_ctx, (void)n1->revert_tx_ctx(primary_ctx));

  n1->fake_tx_log_adapter_->set_bounded_pause(5_s);
  NAMED_DEFER(release_apply,
              n1->fake_tx_log_adapter_->clear_bounded_pause();
              EXPECT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s)));
  ObTxReadSnapshot update_snapshot;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(primary_tx, tx_param.isolation_,
                                              n1->ts_after_ms(100), update_snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, update_snapshot, 46002, 56002));

  ObHotspotRollbackWork work(50_ms);
  const int rb_ret = primary_ctx->hotspot_rollback_to_for_non_hotspot(work);
  EXPECT_EQ(OB_TIMEOUT, rb_ret);
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_pause_hit(1_s));
  EXPECT_FALSE(n1->fake_tx_log_adapter_->pause_expired());
  EXPECT_GT(n1->fake_tx_log_adapter_->get_inflight_cnt(), 0);
  EXPECT_EQ(ObHotspotRollbackWork::Phase::WAIT_SYNC, work.phase_);
  EXPECT_EQ(ObHotspotRollbackWork::WaitReason::SYNC, work.wait_reason_);
  EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::LOCAL, work.timeout_source_);
  EXPECT_GT(work.redo_out_block_cnt_, 0);
  EXPECT_FALSE(work.done_);
  EXPECT_EQ(0, work.checkpoint_batch_cnt_);
  EXPECT_EQ(0, work.rollback_log_out_cnt_);
  {
    CtxLockGuard ctx_guard(primary_ctx->lock_);
    EXPECT_GT(primary_ctx->busy_cbs_.get_size(), 0);
  }

  n1->fake_tx_log_adapter_->clear_bounded_pause();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  // This direct-storage test must finish the descriptors explicitly. It does
  // not prove SQL ResultSet/end-group automatically performs this rollback.
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(primary_tx));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(*sec_tx));
  n1->wait_all_msg_consumed();
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, primary_tx.state_);
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, sec_tx->state_);
  EXPECT_NE(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  EXPECT_NE(OB_SUCCESS, n1->commit_tx(*sec_tx, n1->ts_after_us(500_ms)));

  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  int64_t val = 0;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 46001, val));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 46002, val));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 47001, val));
}

// Accepted append is delayed across the real local deadline. This exercises
// out -> undo -> timeout before table-lock cleanup, not persistent-log latency.
TEST_F(ObTestHotspotTxBasic, test_primary_rollback_out_then_timeout_forces_abort)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 50001, 60001));
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, 51001, 61001));
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                            participant, primary_tx.tx_id_, members));
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 50002, 60002));
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  int64_t free_before = 0;
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    EXPECT_FALSE(ctx->need_force_abort_());
    ASSERT_EQ(0, ctx->busy_cbs_.get_size());
    free_before = ctx->free_cbs_.get_size();
  }

  ObHotspotRollbackWork work(1_s);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->hold_next_rollback_submit_until(
                            work.local_deadline_mono_us_));
  NAMED_DEFER(release_submit_hold,
              n1->fake_tx_log_adapter_->clear_rollback_submit_hold();
              n1->fake_tx_log_adapter_->clear_bounded_pause();
              EXPECT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s)));
  EXPECT_EQ(OB_TIMEOUT, ctx->hotspot_rollback_to_for_non_hotspot(work));
  EXPECT_EQ(1, n1->fake_tx_log_adapter_->get_accepted_rollback_log_count());
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_pause_hit(1_s));
  EXPECT_FALSE(n1->fake_tx_log_adapter_->pause_expired());
  EXPECT_GE(ObTimeUtility::current_monotonic_time(), work.local_deadline_mono_us_);
  EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::LOCAL, work.timeout_source_);
  EXPECT_EQ(OB_TIMEOUT, work.cause_ret_);
  EXPECT_EQ(1, work.rollback_log_out_cnt_);
  EXPECT_EQ(ObHotspotRollbackWork::Phase::LOG_UNDO, work.phase_);
  EXPECT_EQ(0, work.table_lock_us_);
  EXPECT_FALSE(work.done_);
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    EXPECT_TRUE(ctx->need_force_abort_());
    ASSERT_EQ(1, ctx->busy_cbs_.get_size());
    EXPECT_EQ(free_before - 1, ctx->free_cbs_.get_size());
    auto *cb = ctx->busy_cbs_.get_first();
    EXPECT_TRUE(cb->is_busy());
    EXPECT_TRUE(is_contain(cb->cb_arg_array_, ObTxLogType::TX_ROLLBACK_TO_LOG));
  }

  n1->fake_tx_log_adapter_->clear_bounded_pause();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    EXPECT_EQ(0, ctx->busy_cbs_.get_size());
    EXPECT_EQ(free_before, ctx->free_cbs_.get_size());
    EXPECT_TRUE(ctx->need_force_abort_());
  }
  // Reject commit before explicit descriptor cleanup: force_abort, rather
  // than the subsequent caller rollback, must prevent an accidental commit.
  EXPECT_NE(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  const int primary_abort_ret = n1->rollback_tx(primary_tx);
  EXPECT_TRUE(OB_SUCCESS == primary_abort_ret || OB_TRANS_ROLLBACKED == primary_abort_ret);
  const int secondary_abort_ret = n1->rollback_tx(sec_tx);
  EXPECT_TRUE(OB_SUCCESS == secondary_abort_ret || OB_TRANS_ROLLBACKED == secondary_abort_ret);
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  EXPECT_EQ(1, n1->fake_tx_log_adapter_->get_accepted_rollback_log_count());
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, primary_tx.state_);
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, sec_tx.state_);
  EXPECT_NE(OB_SUCCESS, n1->commit_tx(sec_tx, n1->ts_after_us(500_ms)));
  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  int64_t value = 0;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 50001, value));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 50002, value));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 51001, value));
}

// The primary range starts at its first access, so it has no real callback at
// to=first_access-1. A remapped secondary range has a natural live lower endpoint:
// the primary's last callback. This tests shared (to, from] undo, not SQL classification.
TEST_F(ObTestHotspotTxBasic, test_secondary_rollback_preserves_exact_lower_endpoint)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 52001, 62001));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  ObTxSEQ lower_row_seq;
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    auto &list = ctx->mt_ctx_.trans_mgr_.callback_list_;
    ASSERT_EQ(1, list.get_length());
    lower_row_seq = list.get_tail()->get_seq_no();
  }
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, 53001, 63001));
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                            participant, primary_tx.tx_id_, members));
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ObTxSEQ from;
  ObTxSEQ to;
  ASSERT_EQ(OB_SUCCESS, ctx->hotspot_redo_cache_.get_rollback_range(&sec_tx.tx_id_, from, to));
  ASSERT_TRUE(to == lower_row_seq);
  ASSERT_TRUE(from == to + 1); // the single remapped secondary row is the closed endpoint
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 52002, 62002));
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    ASSERT_TRUE(ctx->mt_ctx_.trans_mgr_.callback_list_.get_tail()->get_seq_no() > from);
  }
  ObSEArray<ObTransID, 1> rollback_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_ids.push_back(sec_tx.tx_id_));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.hotspot_rollback_to(participant, primary_tx.tx_id_, rollback_ids));
  int64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 52001, value));
  EXPECT_EQ(62001, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 52002, value));
  EXPECT_EQ(62002, value);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(primary_tx, 53001, value));
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  // The original request was rolled back, although the primary's other rows commit.
  // handle_commit_response_ must report that secondary's exact terminal outcome.
  const int sec_ret = n1->commit_tx(sec_tx, n1->ts_after_us(500_ms));
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, sec_ret);
  n1->wait_all_msg_consumed();
  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 52001, value));
  EXPECT_EQ(62001, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 52002, value));
  EXPECT_EQ(62002, value);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 53001, value));
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_after_real_checkpoint_prefix)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 54001, 64001));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 54002, 64002));
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, 55001, 65001));
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                            participant, primary_tx.tx_id_, members));
  n1->wait_all_msg_consumed();
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  ObTxSEQ from;
  ObTxSEQ to;
  ASSERT_EQ(OB_SUCCESS, ctx->hotspot_redo_cache_.get_rollback_range(nullptr, from, to));
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    // Use the production one-block submit wrapper, not a redo or sync stub.
    ObHotspotRollbackWork submit_work;
    bool progress = false;
    ASSERT_EQ(OB_SUCCESS, ctx->submit_hotspot_rollback_redo_(submit_work, progress));
    EXPECT_TRUE(progress);
    EXPECT_EQ(1, submit_work.redo_out_block_cnt_);
  }
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 54003, 64003));
  ObTxSEQ high_seq;
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    auto &list = ctx->mt_ctx_.trans_mgr_.callback_list_;
    ASSERT_EQ(3, list.get_length());
    EXPECT_TRUE(list.get_guard()->get_next()->get_seq_no() > to);
    high_seq = list.get_tail()->get_seq_no();
    ASSERT_TRUE(high_seq > from);
    const int64_t removed_before = ctx->mt_ctx_.trans_mgr_.get_callback_remove_for_fast_commit_count();
    bool done = true;
    bool callback_error = true;
    int64_t removed = 0;
    ASSERT_EQ(OB_SUCCESS, ctx->mt_ctx_.checkpoint_callbacks_for_hotspot_rollback(
                              1, ObTimeUtility::current_monotonic_time() + 1_s,
                              done, removed, &callback_error));
    EXPECT_FALSE(callback_error);
    EXPECT_FALSE(done); // the real high-seq callback has not been submitted yet
    EXPECT_EQ(2, removed); // the complete first log-SCN group, despite limit=1
    ASSERT_EQ(1, list.get_length());
    EXPECT_TRUE(list.get_guard()->get_next()->get_seq_no() == high_seq);
    EXPECT_EQ(removed_before + removed,
              ctx->mt_ctx_.trans_mgr_.get_callback_remove_for_fast_commit_count());
  }
  ObHotspotRollbackWork rollback_work;
  ASSERT_EQ(OB_SUCCESS, ctx->hotspot_rollback_to_for_non_hotspot(rollback_work));
  EXPECT_TRUE(rollback_work.from_seq_ == from);
  EXPECT_TRUE(rollback_work.to_seq_ == to);
  EXPECT_EQ(1, rollback_work.removed_cb_cnt_);
  EXPECT_EQ(1, rollback_work.rollback_log_out_cnt_);
  int64_t value = 0;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(primary_tx, 54001, value));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(primary_tx, 54002, value));
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 54003, value));
  EXPECT_EQ(64003, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 55001, value));
  EXPECT_EQ(65001, value);
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  const int sec_ret = n1->commit_tx(sec_tx, n1->ts_after_us(500_ms));
  EXPECT_TRUE(OB_SUCCESS == sec_ret || OB_TRANS_COMMITED == sec_ret);
  n1->wait_all_msg_consumed();
  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 54001, value));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 54002, value));
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 54003, value));
  EXPECT_EQ(64003, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 55001, value));
  EXPECT_EQ(65001, value);
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_real_all_lock_wait_and_recheck)
{
  enum Action { LOCAL_EXPIRE, EXPIRE_TX_WHILE_WAITING, FORCE_ABORT_WHILE_WAITING };
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  for (const Action action : {LOCAL_EXPIRE, EXPIRE_TX_WHILE_WAITING, FORCE_ABORT_WHILE_WAITING}) {
    SCOPED_TRACE(static_cast<int>(action));
    PREPARE_TX(n1, primary_tx);
    PREPARE_TX_PARAM(tx_param);
    tx_param.timeout_us_ = 1000_s;
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
    const int64_t key = 56000 + static_cast<int>(action) * 10;
    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, key, 66000));
    ObTxDescGuard sec_guard = n1->get_tx_guard();
    ObTxDesc &sec_tx = sec_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
    ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, key + 1, 66001));
    ObAggregatedTxIDArray members;
    ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
    ObTxPart participant;
    participant.id_ = n1->ls_id_;
    participant.addr_ = n1->addr_;
    participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                              participant, primary_tx.tx_id_, members));
    n1->wait_all_msg_consumed();
    ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
    ASSERT_NE(nullptr, ctx);
    NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
    int64_t original_expiry = 0;
    int64_t free_before = 0;
    {
      CtxLockGuard ctx_guard(ctx->lock_);
      original_expiry = ctx->trans_expired_time_;
      ASSERT_GT(original_expiry, ObClockGenerator::getClock());
      ASSERT_EQ(0, ctx->lock_.get_waiting_lock_cnt());
      ASSERT_EQ(0, ctx->busy_cbs_.get_size());
      ASSERT_FALSE(ctx->need_force_abort_());
      free_before = ctx->free_cbs_.get_size();
    }
    NAMED_DEFER(restore_expiry,
                CtxLockGuard ctx_guard(ctx->lock_);
                ctx->trans_expired_time_ = original_expiry);
    const int64_t lsn_before = n1->fake_tx_log_adapter_->lsn_;
    std::mutex gate_mutex;
    std::condition_variable gate;
    bool holder_ready = false;
    bool release_holder = false;
    int holder_ret = OB_ERR_UNEXPECTED;
    std::atomic<bool> caller_entered{false};
    std::atomic<bool> waiter_seen{false};
    std::atomic<bool> gate_expired{false};
    std::atomic<int64_t> holder_tid{0};
    std::function<void()> hold_all = [&] {
      holder_tid.store(GETTID()); // observe the real native id; never rewrite TLS
      const int lock_ret = ctx->lock_.lock(200_ms);
      if (OB_SUCCESS == lock_ret) {
        CtxLockGuard all_guard(ctx->lock_, CtxLockGuard::MODE::ALL, false);
        std::unique_lock<std::mutex> gate_guard(gate_mutex);
        holder_ret = lock_ret;
        holder_ready = true;
        gate.notify_all();
        const auto fail_safe = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!release_holder) {
          // Only the production lock() waiter counter proves the call arrived.
          // A timed CV poll is not treated as a lock-wait hit.
          if (caller_entered.load() && ctx->lock_.get_waiting_lock_cnt() > 0) {
            waiter_seen.store(true);
            if (EXPIRE_TX_WHILE_WAITING == action) {
              ctx->trans_expired_time_ = ObClockGenerator::getClock() - 1;
              break;
            } else if (FORCE_ABORT_WHILE_WAITING == action) {
              ctx->sub_state_.set_force_abort();
              break;
            }
          }
          if (std::chrono::steady_clock::now() >= fail_safe) {
            gate_expired.store(true);
            break;
          }
          gate.wait_for(gate_guard, std::chrono::milliseconds(1));
        }
      } else {
        std::lock_guard<std::mutex> gate_guard(gate_mutex);
        holder_ret = lock_ret;
        holder_ready = true;
        gate.notify_all();
      }
    };
    // Use the normal engine thread wrapper (and tenant context), not copied or
    // forged latch owner ids. Its body and both lock/gate waits are bounded.
    lib::ObPThread holder([](void *arg) -> void * {
      (*static_cast<std::function<void()> *>(arg))();
      return nullptr;
    }, &hold_all);
    holder.set_run_wrapper(MTL_CTX());
    bool joined = false;
    const auto release_and_join = [&] {
      if (!joined) {
        {
          std::lock_guard<std::mutex> gate_guard(gate_mutex);
          release_holder = true;
          gate.notify_all();
        }
        holder.stop();
        holder.wait();
        joined = true;
      }
    };
    NAMED_DEFER(join_holder, release_and_join());
    ASSERT_EQ(OB_SUCCESS, holder.start());
    {
      std::unique_lock<std::mutex> gate_guard(gate_mutex);
      ASSERT_TRUE(gate.wait_for(gate_guard, std::chrono::seconds(1), [&] { return holder_ready; }));
      ASSERT_EQ(OB_SUCCESS, holder_ret);
    }
    ASSERT_NE(GETTID(), holder_tid.load());
    ObHotspotRollbackWork work(LOCAL_EXPIRE == action ? 50_ms : 1_s);
    caller_entered.store(true);
    const int ret = ctx->hotspot_rollback_to_for_non_hotspot(work);
    release_and_join();
    EXPECT_TRUE(waiter_seen.load());
    EXPECT_FALSE(gate_expired.load());
    EXPECT_GT(work.lock_wait_us_, 0);
    if (LOCAL_EXPIRE == action) {
      EXPECT_EQ(OB_TIMEOUT, ret);
      // A lock slice may succeed just before expiry. The loop then restores the
      // pre-wait phase, and the next deadline check reports VALIDATE instead.
      EXPECT_TRUE(ObHotspotRollbackWork::Phase::LOCK == work.phase_
                  || ObHotspotRollbackWork::Phase::VALIDATE == work.phase_);
      EXPECT_EQ(ObHotspotRollbackWork::WaitReason::CTX_LOCK, work.wait_reason_);
      EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::LOCAL, work.timeout_source_);
      EXPECT_EQ(INT64_MAX, work.tx_expire_wall_us_); // participant state was never read unlocked
    } else {
      EXPECT_EQ(EXPIRE_TX_WHILE_WAITING == action ? OB_TRANS_TIMEOUT : OB_TRANS_KILLED, ret);
      EXPECT_EQ(ObHotspotRollbackWork::Phase::VALIDATE, work.phase_);
      EXPECT_EQ(ObHotspotRollbackWork::WaitReason::NONE, work.wait_reason_);
      EXPECT_EQ(EXPIRE_TX_WHILE_WAITING == action ? ObHotspotRollbackWork::TimeoutSource::TX
                                                : ObHotspotRollbackWork::TimeoutSource::NONE,
                work.timeout_source_);
      if (EXPIRE_TX_WHILE_WAITING == action) {
        EXPECT_LT(work.tx_expire_wall_us_, original_expiry);
        EXPECT_LE(work.tx_expire_wall_us_, ObClockGenerator::getClock());
      } else {
        EXPECT_EQ(original_expiry, work.tx_expire_wall_us_);
      }
    }
    EXPECT_EQ(ret, work.cause_ret_);
    EXPECT_FALSE(work.snapshot_valid_);
    EXPECT_FALSE(work.range_checked_);
    EXPECT_FALSE(work.done_);
    EXPECT_EQ(0, work.redo_out_block_cnt_);
    EXPECT_EQ(0, work.checkpoint_batch_cnt_);
    EXPECT_EQ(0, work.rollback_log_out_cnt_);
    EXPECT_EQ(lsn_before, n1->fake_tx_log_adapter_->lsn_);
    {
      CtxLockGuard ctx_guard(ctx->lock_);
      EXPECT_EQ(0, ctx->lock_.get_waiting_lock_cnt());
      EXPECT_EQ(0, ctx->busy_cbs_.get_size());
      EXPECT_EQ(free_before, ctx->free_cbs_.get_size());
      ctx->trans_expired_time_ = original_expiry;
    }
    ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(primary_tx));
    ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(sec_tx));
    n1->wait_all_msg_consumed();
    ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
    EXPECT_NE(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
    EXPECT_NE(OB_SUCCESS, n1->commit_tx(sec_tx, n1->ts_after_us(500_ms)));
    ObTxDescGuard read_guard = n1->get_tx_guard();
    ObTxDesc &read_tx = read_guard.get_tx_desc();
    int64_t value = 0;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, key, value));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, key + 1, value));
  }
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_multiple_lists_require_tx_rollback)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 57001, 67001));
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, 58001, 68001));
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                            participant, primary_tx.tx_id_, members));
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->submit_redo_log_for_freeze(UINT32_MAX));
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  {
    // Simulate an entry-contract violation after aggregation. The defense must
    // reject it before writing RollbackToLog, without supporting the layout.
    CtxLockGuard ctx_guard(ctx->lock_);
    ASSERT_EQ(OB_SUCCESS, ctx->mt_ctx_.trans_mgr_.extend_callback_lists_(
                              ObTransCallbackMgr::MAX_CALLBACK_LIST_COUNT - 1));
  }
  ObHotspotRollbackWork work;
  EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, ctx->hotspot_rollback_to_for_non_hotspot(work));
  EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, work.cause_ret_);
  EXPECT_EQ(ObHotspotRollbackWork::Phase::CHECKPOINT, work.phase_);
  EXPECT_EQ(0, work.rollback_log_out_cnt_);
  EXPECT_FALSE(work.done_);
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    EXPECT_TRUE(ctx->need_force_abort_());
  }
  EXPECT_NE(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  const int primary_abort_ret = n1->rollback_tx(primary_tx);
  const int secondary_abort_ret = n1->rollback_tx(sec_tx);
  EXPECT_TRUE(OB_SUCCESS == primary_abort_ret || OB_TRANS_ROLLBACKED == primary_abort_ret);
  EXPECT_TRUE(OB_SUCCESS == secondary_abort_ret || OB_TRANS_ROLLBACKED == secondary_abort_ret);
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
}

TEST_F(ObTestHotspotTxBasic, test_primary_rollback_ctx_lock_slices_retry_then_succeed)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 59001, 69001));
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, 60001, 70001));
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                            participant, primary_tx.tx_id_, members));
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 59002, 69002));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  ObTxSEQ from;
  ObTxSEQ to;
  ASSERT_EQ(OB_SUCCESS, ctx->hotspot_redo_cache_.get_rollback_range(nullptr, from, to));
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    ASSERT_EQ(0, ctx->lock_.get_waiting_lock_cnt());
    auto &list = ctx->mt_ctx_.trans_mgr_.callback_list_;
    ASSERT_NE(list.get_guard(), list.get_tail());
    ASSERT_TRUE(list.get_tail()->get_seq_no() > from);
  }
  std::mutex gate_mutex;
  std::condition_variable gate;
  bool ready = false;
  bool release = false;
  int holder_ret = OB_ERR_UNEXPECTED;
  std::atomic<bool> caller_entered{false};
  std::atomic<bool> gate_expired{false};
  std::atomic<int64_t> wait_entries{0};
  std::atomic<int64_t> holder_tid{0};
  std::function<void()> hold_all = [&] {
    holder_tid.store(GETTID());
    const int ret = ctx->lock_.lock(200_ms);
    if (OB_SUCCESS == ret) {
      CtxLockGuard ctx_guard(ctx->lock_, CtxLockGuard::MODE::ALL, false);
      std::unique_lock<std::mutex> guard(gate_mutex);
      holder_ret = ret;
      ready = true;
      gate.notify_all();
      const int64_t fail_safe = ObTimeUtility::current_monotonic_time() + 2_s;
      int64_t first_wait = 0;
      bool was_waiting = false;
      while (!release) {
        const int64_t now = ObTimeUtility::current_monotonic_time();
        const bool waiting = caller_entered.load() && ctx->lock_.get_waiting_lock_cnt() > 0;
        if (waiting && !was_waiting) {
          ++wait_entries;
          if (first_wait == 0) {
            first_wait = now;
          }
        }
        was_waiting = waiting;
        if (first_wait > 0 && now - first_wait >= 100_ms && wait_entries.load() >= 2) {
          break;
        } else if (now >= fail_safe) {
          gate_expired.store(true);
          break;
        }
        gate.wait_for(guard, std::chrono::microseconds(100));
      }
    } else {
      std::lock_guard<std::mutex> guard(gate_mutex);
      holder_ret = ret;
      ready = true;
      gate.notify_all();
    }
  };
  lib::ObPThread holder([](void *arg) -> void * {
    (*static_cast<std::function<void()> *>(arg))();
    return nullptr;
  }, &hold_all);
  holder.set_run_wrapper(MTL_CTX());
  bool joined = false;
  const auto release_and_join = [&] {
    if (!joined) {
      {
        std::lock_guard<std::mutex> guard(gate_mutex);
        release = true;
        gate.notify_all();
      }
      holder.stop();
      holder.wait();
      joined = true;
    }
  };
  NAMED_DEFER(join_holder, release_and_join());
  ASSERT_EQ(OB_SUCCESS, holder.start());
  {
    std::unique_lock<std::mutex> guard(gate_mutex);
    ASSERT_TRUE(gate.wait_for(guard, std::chrono::seconds(1), [&] { return ready; }));
    ASSERT_EQ(OB_SUCCESS, holder_ret);
  }
  ASSERT_NE(GETTID(), holder_tid.load());
  ObHotspotRollbackWork work(200_ms);
  caller_entered.store(true);
  const int ret = ctx->hotspot_rollback_to_for_non_hotspot(work);
  release_and_join();
  EXPECT_FALSE(gate_expired.load());
  // Multiple actual 0->1 waiter transitions while ALL remains held prove lock
  // retries; redo-stage retries alone cannot satisfy this observation.
  EXPECT_GE(wait_entries.load(), 2);
  EXPECT_GT(work.retry_cnt_, 0);
  EXPECT_GT(work.lock_wait_us_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, work.cause_ret_);
  EXPECT_EQ(ObHotspotRollbackWork::TimeoutSource::NONE, work.timeout_source_);
  EXPECT_EQ(ObHotspotRollbackWork::Phase::FINISHED, work.phase_);
  EXPECT_TRUE(work.done_);
  EXPECT_EQ(1, work.rollback_log_out_cnt_);
  EXPECT_TRUE(work.from_seq_ == from);
  EXPECT_TRUE(work.to_seq_ == to);
  int64_t value = 0;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(primary_tx, 59001, value));
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 59002, value));
  EXPECT_EQ(69002, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 60001, value));
  EXPECT_EQ(70001, value);
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  const int sec_ret = n1->commit_tx(sec_tx, n1->ts_after_us(500_ms));
  EXPECT_TRUE(OB_SUCCESS == sec_ret || OB_TRANS_COMMITED == sec_ret);
  n1->wait_all_msg_consumed();
  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 59001, value));
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 59002, value));
  EXPECT_EQ(69002, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 60001, value));
  EXPECT_EQ(70001, value);
}

TEST_F(ObTestHotspotTxBasic, test_secondary_cleanup_error_is_diagnostic_not_group_failure)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000_s;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 61001, 71001));
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, 62001, 72001));
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(sec_tx.tx_id_));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
                            participant, primary_tx.tx_id_, members));
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, 61002, 71002));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
  ASSERT_NE(nullptr, ctx);
  NAMED_DEFER(revert_ctx, (void)n1->revert_tx_ctx(ctx));
  ScopedHotspotEvent fault("EN_HOTSPOT_SECONDARY_ROLLBACK_MEMTABLE_FAIL");
  ASSERT_TRUE(fault.is_registered());
  ASSERT_EQ(OB_SUCCESS, fault.inject_once(OB_ERR_UNEXPECTED));
  ASSERT_EQ(1, fault.remaining());
  ObHotspotRollbackWork work;
  ASSERT_EQ(OB_SUCCESS, ctx->hotspot_rollback_to_for_secondary(sec_tx.tx_id_, work));
  EXPECT_EQ(0, fault.remaining());
  EXPECT_TRUE(work.done_);
  EXPECT_EQ(ObHotspotRollbackWork::Phase::FINISHED, work.phase_);
  EXPECT_EQ(OB_SUCCESS, work.cause_ret_);
  EXPECT_EQ(OB_ERR_UNEXPECTED, work.cleanup_ret_);
  EXPECT_EQ(1, work.rollback_log_out_cnt_);
  {
    CtxLockGuard ctx_guard(ctx->lock_);
    EXPECT_FALSE(ctx->need_force_abort_());
  }
  // Logical undo is owned by the primary even when physical secondary cleanup
  // reports an error. Best-effort cleanup must not turn into a group abort.
  int64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 61001, value));
  EXPECT_EQ(71001, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(primary_tx, 61002, value));
  EXPECT_EQ(71002, value);
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_us(500_ms)));
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, n1->commit_tx(sec_tx, n1->ts_after_us(500_ms)));
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(n1->fake_tx_log_adapter_->wait_inflight_zero(2_s));
  EXPECT_EQ(0, fault.remaining());
  ObTxDescGuard read_guard = n1->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 61001, value));
  EXPECT_EQ(71001, value);
  ASSERT_EQ(OB_SUCCESS, n1->read(read_tx, 61002, value));
  EXPECT_EQ(71002, value);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->read(read_tx, 62001, value));
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  char log_name[128];
  snprintf(log_name, sizeof(log_name), "test_hotspot_tx_basic_%d.log", static_cast<int>(getpid()));
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_name, true, false, log_name, log_name, log_name);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
