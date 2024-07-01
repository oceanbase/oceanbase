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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>
#define private public
#define protected public
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_redo_submitter.h"
#define USING_LOG_PREFIX TRANS

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;
namespace memtable {
// Test fill redo logic works for multi-callback-list
//
// target interface to test:
// - fill_from_one_list
// - fill_from_all_list
//
// target situation:
// - BLOCK_FROZEN
// - blocked by other list
// - all callback filled successfully
//
// logging mode:
// - parallel logging
// - serial logging
//
// mock callback list interface
// - fill_log
// - get_log_epoch
//
struct MockDelegate {
  virtual int fill_log(ObTxFillRedoCtx &fill_ctx) = 0;
  virtual int64_t get_log_epoch(int i) const = 0;
  virtual int get_logging_list_count() const  = 0;
};
struct MockImpl : MockDelegate {
  struct MockCallback : public ObITransCallback
  {
  };
  int a_;
  ObTxFillRedoCtx fill_ctx_;
  MockCallback cb1_;
  MockCallback cb2_;
  MockCallback cb3_;
  MockImpl() : cb1_() {
    cb1_.next_ = &cb2_;
    cb1_.prev_ = &cb3_;
    cb2_.next_ = &cb3_;
    cb2_.prev_ = &cb1_;
    cb3_.next_ = &cb1_;
    cb3_.prev_ = &cb2_;
  }
  void init_callback_scope()
  {
    fill_ctx_.callback_scope_->start_.cur_ = &cb1_;
    fill_ctx_.callback_scope_->end_.cur_ = &cb2_;
  }
  public:
  MOCK_CONST_METHOD0(get_logging_list_count, int());
  MOCK_CONST_METHOD1(get_log_epoch, int64_t(int));
  MOCK_METHOD1(fill_log, int(ObTxFillRedoCtx&));
};
thread_local MockImpl *mock_ptr;
int ObTxCallbackList::fill_log(ObITransCallback* cursor, ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &func)
{
  mock_ptr->init_callback_scope();
  return mock_ptr->fill_log(ctx);
}
int64_t ObTxCallbackList::get_log_epoch() const
{
  return mock_ptr->get_log_epoch(id_);
}
int ObTransCallbackMgr::get_logging_list_count() const
{
  return mock_ptr->get_logging_list_count();
}
class ObTestRedoFill : public ::testing::Test
{
public:
  ObTestRedoFill():
    mem_ctx_(),
    cb_allocator_(),
    callback_mgr_(mem_ctx_, cb_allocator_, mem_ctx_.mem_ctx_obj_pool_) {}
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObAddr ip_port(ObAddr::VER::IPV4, "119.119.0.1",2023);
    ObCurTraceId::init(ip_port);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
    mdo_.fill_ctx_.helper_ = &helper_;
    mock_ptr = &mdo_;
    cb_allocator_.init(1001);
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
  void extend_callback_lists_(int cnt)
  {
    callback_mgr_.callback_lists_ = (ObTxCallbackList*)new char[(sizeof(ObTxCallbackList) * cnt)];
    for (int i=0; i<cnt; i++) {
      new (callback_mgr_.callback_lists_ + i) ObTxCallbackList(callback_mgr_, i + 1);
    }
  }
  void set_parallel_logging(bool t)
  {
    if (t) {
      share::SCN scn;
      scn.convert_for_tx(1231231231);
      transaction::ObTxSEQ seq(1000, 0);
      callback_mgr_.set_parallel_logging(scn, seq);
    } else {
      callback_mgr_.set_parallel_logging(share::SCN::max_scn(), transaction::ObTxSEQ::INVL());
    }
  }
  ObRedoLogSubmitHelper helper_;
  ObMemtableCtx mem_ctx_;
  ObMemtableCtxCbAllocator cb_allocator_;
  ObTransCallbackMgr callback_mgr_;
  MockImpl mdo_;
  class ObTxFillRedoFunctor : public ObITxFillRedoFunctor {
    int operator()(ObITransCallback * cb) { return OB_SUCCESS; }
  } fill_func;
};


TEST_F(ObTestRedoFill, serial_single_list_fill_all_BLOCK_FROZEN)
{
  set_parallel_logging(true);
  // single list
  callback_mgr_.callback_lists_ = NULL;
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(1));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 999;
      return OB_BLOCK_FROZEN;
    }));
  EXPECT_EQ(OB_BLOCK_FROZEN, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, INT64_MAX);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 999);
}

TEST_F(ObTestRedoFill, serial_multi_list_fill_all_ALL_FILLED)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100, 97, 98, 101};
      return epochs[i];
    }));
  int i = 0;
  {
    InSequence s;
    // fill orders: init: {100,97,98,101}
    // -> {100,100,98,101}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 2: 97 -> 100
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 97;
      ctx.next_epoch_ = 100;
      return OB_ITER_END;
    }));
    ++i;
    // -> {100,100,101,101}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 3: 98 -> 101
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 98;
      ctx.next_epoch_ = 101;
      return OB_ITER_END;
    }));
    ++i;
    // -> {102,100,101,101}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 1: 100 -> 102
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 100;
      ctx.next_epoch_ = 102;
      return OB_ITER_END;
    }));
    ++i;
    // -> {102,102,101,101}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 2: 101 -> 102
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 100;
      ctx.next_epoch_ = 102;
      return OB_ITER_END;
    }));
    ++i;
    // -> {102,102,END,101}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 3: 101 -> END
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 101;
      return OB_SUCCESS;
    }));
    ++i;
    // -> {102,102,END,END}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 4: 101 -> END
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 101;
      return OB_SUCCESS;
    }));
    ++i;
    // -> {END,102,END,END}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 1: 102 -> END
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 102;
      return OB_SUCCESS;
    }));
    ++i;
    // -> {END,END,END,END}
    EXPECT_CALL(mdo_, fill_log(_)).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // list 2: 102 -> END
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 102;
      return OB_SUCCESS;
    }));
    ++i;
  }
  auto &ctx = mdo_.fill_ctx_;
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(ctx, fill_func));
  EXPECT_EQ(ctx.epoch_from_, 102);
  EXPECT_EQ(ctx.epoch_to_, 102);
  EXPECT_EQ(ctx.fill_count_, i);
  EXPECT_EQ(ctx.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[1], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[2], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[3], INT64_MAX);
}
TEST_F(ObTestRedoFill, serial_multi_list_fill_all_BLOCK_FROZEN)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100, 100, 100, 100};
      return epochs[i];
    }));
  {
    InSequence s;
    // list 1,2,3 blocked
    EXPECT_CALL(mdo_, fill_log(_)).Times(3)
    .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 1;
      ctx.cur_epoch_ = 100;
      return OB_BLOCK_FROZEN;
    }));
    // list 4 filled
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
    .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 10;
      ctx.cur_epoch_ = 100;
      return OB_SUCCESS;
    }));
    // next round
    // fill list 1,2,3 again success
    EXPECT_CALL(mdo_, fill_log(_)).Times(3)
      .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 100;
        ctx.cur_epoch_ = 100;
        return OB_SUCCESS;
      }));
  }
  auto &ctx = mdo_.fill_ctx_;
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(ctx, fill_func));
  EXPECT_EQ(ctx.epoch_from_, 100);
  EXPECT_EQ(ctx.epoch_to_, 100);
  EXPECT_EQ(ctx.fill_count_, 313);
  EXPECT_EQ(ctx.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[1], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[2], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[3], INT64_MAX);
}

TEST_F(ObTestRedoFill, serial_single_list_fill_all_BUF_NOT_ENOUGH)
{
  set_parallel_logging(false);
  // single list
  callback_mgr_.callback_lists_ = NULL;
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(1));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 999;
      return OB_BUF_NOT_ENOUGH;
    }));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, INT64_MAX);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 999);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 999);
}

TEST_F(ObTestRedoFill, serial_single_list_fill_all_list_BIG_ROW)
{
  set_parallel_logging(false);
  // single list
  callback_mgr_.callback_lists_ = NULL;
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(1));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 0;
      ctx.cur_epoch_ = 100;
      return OB_BUF_NOT_ENOUGH;
    }));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, INT64_MAX);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 100);
}

TEST_F(ObTestRedoFill, serial_multi_list_fill_all_list_BIG_ROW)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,99,100,99};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 0;
      ctx.cur_epoch_ = 99;
      return OB_BUF_NOT_ENOUGH;
    }));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 1);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[1], 99);
}

TEST_F(ObTestRedoFill, serial_multi_list_fill_all_OTHER_ERROR_WHEN_FILL_OTHERS)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100, 100, 100, 100};
      return epochs[i];
    }));
  {
    InSequence s;
    // list 1 ALL FILLED
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
      .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 1;
        ctx.cur_epoch_ = 100;
        return OB_SUCCESS;
      }));
    // list 2 FROZEN BLOCKED
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
    .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 10;
      ctx.cur_epoch_ = 100;
      return OB_BLOCK_FROZEN;
    }));
    // list 3 MEMORY_ALLOCATE_FAILED
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
    .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 100;
      return OB_ALLOCATE_MEMORY_FAILED;
    }));
  }
  auto &ctx = mdo_.fill_ctx_;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, callback_mgr_.fill_from_all_list(ctx, fill_func));
  EXPECT_EQ(ctx.epoch_from_, 100);
  EXPECT_EQ(ctx.epoch_to_, 100);
  EXPECT_EQ(ctx.fill_count_, 111);
  EXPECT_EQ(ctx.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[1], 100);
  EXPECT_EQ(ctx.list_log_epoch_arr_[2], 100);
  EXPECT_EQ(ctx.list_log_epoch_arr_[3], 100);
}

TEST_F(ObTestRedoFill, serial_logging_fill_from_all_list_ALL_OTHERS_BLOCK_FROZEN_EMPTY)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,100,100,100};
      return epochs[i];
    }));
  {
    InSequence s;
    // list 1 ALL FILLED
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 1;
        ctx.cur_epoch_ = 100;
        return OB_SUCCESS;
      }));
    // list 2,3,4 BLOCK_FROZEN AND NOTHING FILLED
    EXPECT_CALL(mdo_, fill_log(_)).Times(3)
      .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 0;
        ctx.cur_epoch_ = 100;
        return OB_BLOCK_FROZEN;
      }));
  }
  EXPECT_EQ(OB_BLOCK_FROZEN, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 1);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 3); // point to list 4
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[1], 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[3], 100);
}

TEST_F(ObTestRedoFill, serial_logging_fill_from_all_list_FIRST_ITER_END_OTHERS_BLOCK_FROZEN_EMPTY)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,101,100,200};
      return epochs[i];
    }));
  {
    InSequence s;
    // list 1 ITER END
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 1;
        ctx.cur_epoch_ = 100;
        ctx.next_epoch_ = 101;
        return OB_ITER_END;
      }));
    // list 2 skipped
    // list 3, BLOCK_FROZEN AND NOTHING FILLED
    EXPECT_CALL(mdo_, fill_log(_)).Times(1)
      .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 0;
        ctx.cur_epoch_ = 100;
        return OB_BLOCK_FROZEN;
      }));
    // list 4 skipped
  }
  EXPECT_EQ(OB_BLOCK_FROZEN, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 1);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2); // point to list 3
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 101);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[1], 101);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[3], 200);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_FROZEN)
{
  set_parallel_logging(true);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,99,98,99};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 99;
      return OB_BLOCK_FROZEN;
    }));
  EXPECT_EQ(OB_BLOCK_FROZEN, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 99);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_FROZEN_EMPTY)
{
  set_parallel_logging(true);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,99,98,99};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 0;
      ctx.cur_epoch_ = 98;
      return OB_BLOCK_FROZEN;
    }));
  EXPECT_EQ(OB_BLOCK_FROZEN, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 0);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 98);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_FROZEN_FILL_FROM_OTHERS)
{
  set_parallel_logging(true);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,98,98,99};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(2)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 0;
      ctx.cur_epoch_ = 98;
      return OB_BLOCK_FROZEN;
    }))
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 99;
      ctx.next_epoch_ = 101;
      return OB_ITER_END;
    }));
  EXPECT_EQ(OB_ITER_END, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[1], 98);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 101);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_BY_OTHERS)
{
  set_parallel_logging(true);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,99,98,99};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 98;
      ctx.next_epoch_ = 101;
      return OB_ITER_END;
    }));
  EXPECT_EQ(OB_ITER_END, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 101);
}

TEST_F(ObTestRedoFill, parallel_logging_fill_from_all_list_ALL_FILLED_OTHERS_REMAIN)
{
  set_parallel_logging(true);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,99,98,99};
      return epochs[i];
    }));
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 100;
      ctx.cur_epoch_ = 98;
      return OB_SUCCESS;
    }));
  EXPECT_EQ(OB_EAGAIN, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99); // consume done, the cur_epoch point to epoch_to
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], INT64_MAX);
}

TEST_F(ObTestRedoFill, parallel_logging_fill_from_one_list_OTHERS_IS_EMPTY)
{
  set_parallel_logging(true);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,INT64_MAX,INT64_MAX,INT64_MAX};
      return epochs[i];
    }));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_log(_)).Times(1).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 123;
      ctx.cur_epoch_ = 100;
      return OB_SUCCESS;
    }));
  }
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, INT64_MAX);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100); // consume done, the cur_epoch point to epoch_to
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 123);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 0);
  EXPECT_TRUE(mdo_.fill_ctx_.is_all_filled_);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], INT64_MAX);
}

TEST_F(ObTestRedoFill, serial_logging_fill_from_one_list_OTHERS_IS_EMPTY)
{
  set_parallel_logging(false);
  // 4 list
  extend_callback_lists_(3);
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(4));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100,INT64_MAX,INT64_MAX,INT64_MAX};
      return epochs[i];
    }));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_log(_)).Times(1).WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 123;
      ctx.cur_epoch_ = 100;
      return OB_SUCCESS;
    }));
  }
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  EXPECT_EQ(mdo_.fill_ctx_.epoch_from_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.epoch_to_, INT64_MAX);
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100); // consume done, the cur_epoch point to epoch_to
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 123);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 0);
  EXPECT_TRUE(mdo_.fill_ctx_.is_all_filled_);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], INT64_MAX);
}

} // memtable
} // oceanbase

int main(int argc, char **argv)
{
  const char *log_name = "test_trans_callback_mgr_fill_redo.log";
  system("rm -rf test_trans_callback_mgr_fill_redo.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_name, true, false,
                       log_name,
                       log_name,
                       log_name);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
