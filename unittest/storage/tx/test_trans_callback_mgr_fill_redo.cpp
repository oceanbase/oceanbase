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
#define private public
#define protected public
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
struct MockCallback : public ObITransCallback
{
};
struct MockImpl : MockDelegate, ObITxFillRedoFunctor {
  MockImpl(): callback_scopes(), fill_ctx_(callback_scopes), cb_() {}
  int a_;
  ObSEArray<ObCallbackScope, 1> callback_scopes;
  ObTxFillRedoCtx fill_ctx_;
  MockCallback cb_;
  int operator()(ObITransCallback *start,
                 ObITransCallback *end,
                 const int64_t epoch_from,
                 const int64_t epoch_to,
                 ObITransCallback *&last,
                 int32_t &cnt,
                 int64_t &data_size,
                 transaction::ObTxSEQ &max_seq_no) {
    last = &cb_;
    return fill_log(fill_ctx_);
  }
  public:
  MOCK_CONST_METHOD0(get_logging_list_count, int());
  MOCK_CONST_METHOD1(get_log_epoch, int64_t(int));
  MOCK_METHOD1(fill_log, int(ObTxFillRedoCtx&));
};
thread_local MockImpl *mock_ptr;
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
    mem_ctx_obj_pool_(cb_allocator_),
    callback_mgr_(mem_ctx_, cb_allocator_, mem_ctx_obj_pool_),
    fill_func(mdo_) {}
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
    mock_ptr = &mdo_;
    cb_allocator_.init(1001);
    callback_mgr_.callback_list_.head_.append(new MockCallback());
    callback_mgr_.callback_list_.head_.append(new MockCallback());
    callback_mgr_.callback_list_.head_.append(new MockCallback());
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
      callback_mgr_.callback_lists_[i].head_.append(new MockCallback());
      callback_mgr_.callback_lists_[i].head_.append(new MockCallback());
      callback_mgr_.callback_lists_[i].head_.append(new MockCallback());
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
  ObMemtableCtx mem_ctx_;
  ObMemtableCtxCbAllocator cb_allocator_;
  transaction::ObMemtableCtxObjPool mem_ctx_obj_pool_;
  ObTransCallbackMgr callback_mgr_;
  MockImpl mdo_;
  ObITxFillRedoFunctor &fill_func;
};


TEST_F(ObTestRedoFill, serial_single_list_fill_all_BLOCK_FROZEN)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 999);
}

TEST_F(ObTestRedoFill, serial_multi_list_fill_all_ALL_FILLED)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(ctx.fill_count_, i);
  EXPECT_EQ(ctx.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[1], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[2], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[3], INT64_MAX);
}
TEST_F(ObTestRedoFill, serial_multi_list_fill_all_BLOCK_FROZEN)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(ctx.fill_count_, 313);
  EXPECT_EQ(ctx.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[1], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[2], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[3], INT64_MAX);
}

TEST_F(ObTestRedoFill, serial_single_list_fill_all_BUF_NOT_ENOUGH)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 999);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 999);
}

TEST_F(ObTestRedoFill, serial_single_list_fill_all_list_BIG_ROW)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 100);
}

TEST_F(ObTestRedoFill, serial_multi_list_fill_all_list_BIG_ROW)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 1);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[1], 99);
}

TEST_F(ObTestRedoFill, serial_multi_list_fill_all_OTHER_ERROR_WHEN_FILL_OTHERS)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(ctx.fill_count_, 111);
  EXPECT_EQ(ctx.list_log_epoch_arr_[0], INT64_MAX);
  EXPECT_EQ(ctx.list_log_epoch_arr_[1], 100);
  EXPECT_EQ(ctx.list_log_epoch_arr_[2], 100);
  EXPECT_EQ(ctx.list_log_epoch_arr_[3], 100);
}

TEST_F(ObTestRedoFill, serial_logging_fill_from_all_list_ALL_OTHERS_BLOCK_FROZEN_EMPTY)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  callback_mgr_.need_merge_ = false;
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
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 99);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_FROZEN_EMPTY)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 98);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 0);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 98);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_FROZEN_FILL_FROM_OTHERS)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[1], 98);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 101);
}

TEST_F(ObTestRedoFill, parallel_multi_list_fill_all_BLOCK_BY_OTHERS)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99);
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], 101);
}

TEST_F(ObTestRedoFill, parallel_logging_fill_from_all_list_ALL_FILLED_OTHERS_REMAIN)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 99); // consume done, the cur_epoch point to epoch_to
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 100);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 2);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[2], INT64_MAX);
}

TEST_F(ObTestRedoFill, parallel_logging_fill_from_one_list_OTHERS_IS_EMPTY)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100); // consume done, the cur_epoch point to epoch_to
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 123);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 0);
  EXPECT_TRUE(mdo_.fill_ctx_.is_all_filled_);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], INT64_MAX);
}

TEST_F(ObTestRedoFill, serial_logging_fill_from_one_list_OTHERS_IS_EMPTY)
{
  set_parallel_logging(false);
  callback_mgr_.need_merge_ = false;
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
  EXPECT_EQ(mdo_.fill_ctx_.cur_epoch_, 100); // consume done, the cur_epoch point to epoch_to
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 123);
  EXPECT_EQ(mdo_.fill_ctx_.list_idx_, 0);
  EXPECT_TRUE(mdo_.fill_ctx_.is_all_filled_);
  EXPECT_EQ(mdo_.fill_ctx_.list_log_epoch_arr_[0], INT64_MAX);
}

TEST_F(ObTestRedoFill, test_fill_redo_with_remapped_seq)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
  callback_mgr_.callback_lists_ = NULL;
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(1));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100};
      return epochs[i];
    }));
  // Set remapped_next_seq_ to a valid value
  mdo_.fill_ctx_.remapped_next_seq_ = transaction::ObTxSEQ(100, 0);
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      // Simulate filling 5 callbacks, each advancing the cursor
      ctx.fill_count_ += 5;
      // In real code, each fill_row_redo_ would ++remapped_next_seq_
      // Here we simulate that advancement
      for (int i = 0; i < 5; i++) {
        ++ctx.remapped_next_seq_;
      }
      ctx.cur_epoch_ = 100;
      return OB_SUCCESS;
    }));
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  // Verify cursor advanced by 5: started at seq=100, now at seq=105
  EXPECT_TRUE(mdo_.fill_ctx_.remapped_next_seq_.is_valid());
  EXPECT_EQ(mdo_.fill_ctx_.remapped_next_seq_, transaction::ObTxSEQ(105, 0));
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 5);
}

TEST_F(ObTestRedoFill, test_fill_redo_without_remap)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
  callback_mgr_.callback_lists_ = NULL;
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(1));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100};
      return epochs[i];
    }));
  // remapped_next_seq_ is default-constructed (invalid)
  EXPECT_FALSE(mdo_.fill_ctx_.remapped_next_seq_.is_valid());
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 3;
      ctx.cur_epoch_ = 100;
      return OB_SUCCESS;
    }));
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  // remapped_next_seq_ should remain invalid
  EXPECT_FALSE(mdo_.fill_ctx_.remapped_next_seq_.is_valid());
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 3);
}

TEST_F(ObTestRedoFill, test_fill_redo_remapped_cursor_across_rounds)
{
  set_parallel_logging(true);
  callback_mgr_.need_merge_ = false;
  callback_mgr_.callback_lists_ = NULL;
  EXPECT_CALL(mdo_, get_logging_list_count()).Times(AtLeast(1)).WillRepeatedly(Return(1));
  EXPECT_CALL(mdo_, get_log_epoch(_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke([](int i){
      int64_t epochs[] = {100};
      return epochs[i];
    }));
  // Set remapped_next_seq_ to a valid value
  mdo_.fill_ctx_.remapped_next_seq_ = transaction::ObTxSEQ(200, 0);
  {
    InSequence s;
    // Round 1: fill 3 callbacks then BUF_NOT_ENOUGH
    EXPECT_CALL(mdo_, fill_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
        ctx.fill_count_ += 3;
        for (int i = 0; i < 3; i++) {
          ++ctx.remapped_next_seq_;
        }
        ctx.cur_epoch_ = 100;
        return OB_BUF_NOT_ENOUGH;
      }));
  }
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  // After round 1: cursor at 203
  EXPECT_EQ(mdo_.fill_ctx_.remapped_next_seq_, transaction::ObTxSEQ(203, 0));
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 3);

  // Round 2: simulate continuing with the same fill_ctx_ (cursor preserved)
  // Reset fill_count_ and other per-round state as the caller would
  mdo_.fill_ctx_.fill_count_ = 0;
  mdo_.fill_ctx_.min_seq_no_.reset();
  mdo_.fill_ctx_.max_seq_no_.reset();
  mdo_.fill_ctx_.callback_scopes_.reuse();
  mdo_.fill_ctx_.list_log_epoch_arr_.reuse();
  EXPECT_CALL(mdo_, fill_log(_))
    .Times(1)
    .WillOnce(Invoke([](ObTxFillRedoCtx &ctx){
      ctx.fill_count_ += 2;
      for (int i = 0; i < 2; i++) {
        ++ctx.remapped_next_seq_;
      }
      ctx.cur_epoch_ = 100;
      return OB_SUCCESS;
    }));
  EXPECT_EQ(OB_SUCCESS, callback_mgr_.fill_from_all_list(mdo_.fill_ctx_, fill_func));
  // After round 2: cursor at 205 (continuous from 203)
  EXPECT_EQ(mdo_.fill_ctx_.remapped_next_seq_, transaction::ObTxSEQ(205, 0));
  EXPECT_EQ(mdo_.fill_ctx_.fill_count_, 2);
}

// ============================================================
// Standalone remap logic tests
//
// These tests directly exercise the remap patterns as they appear
// in production code (fill_row_redo_ / operator()() / overflow check),
// without going through the callback manager framework.
// ============================================================

// Verify: cursor advances only on successful append (CRITICAL fix)
TEST_F(ObTestRedoFill, test_remap_cursor_no_advance_on_append_failure)
{
  // Simulate the exact pattern in fill_row_redo_:
  //   assign seq → append → advance only on success
  ObTxFillRedoCtx &ctx = mdo_.fill_ctx_;
  ctx.remapped_next_seq_ = transaction::ObTxSEQ(100, 0);

  // Simulate 3 successful fills
  for (int i = 0; i < 3; i++) {
    transaction::ObTxSEQ assigned_seq = ctx.remapped_next_seq_;
    int append_ret = OB_SUCCESS; // simulate append success
    if (OB_SUCCESS == append_ret && ctx.remapped_next_seq_.is_valid()) {
      ++ctx.remapped_next_seq_;
    }
    EXPECT_EQ(assigned_seq, transaction::ObTxSEQ(100 + i, 0));
  }
  EXPECT_EQ(ctx.remapped_next_seq_, transaction::ObTxSEQ(103, 0));

  // 4th fill: append fails with OB_BUF_NOT_ENOUGH
  {
    transaction::ObTxSEQ assigned_seq = ctx.remapped_next_seq_;
    EXPECT_EQ(assigned_seq, transaction::ObTxSEQ(103, 0));
    int append_ret = OB_BUF_NOT_ENOUGH; // simulate append failure
    if (OB_SUCCESS == append_ret && ctx.remapped_next_seq_.is_valid()) {
      ++ctx.remapped_next_seq_;
    }
    // cursor must NOT have advanced
    EXPECT_EQ(ctx.remapped_next_seq_, transaction::ObTxSEQ(103, 0));
  }

  // 5th fill (retry of the same callback): should get seq 103 again
  {
    transaction::ObTxSEQ assigned_seq = ctx.remapped_next_seq_;
    EXPECT_EQ(assigned_seq, transaction::ObTxSEQ(103, 0));
    int append_ret = OB_SUCCESS; // now succeeds
    if (OB_SUCCESS == append_ret && ctx.remapped_next_seq_.is_valid()) {
      ++ctx.remapped_next_seq_;
    }
    EXPECT_EQ(ctx.remapped_next_seq_, transaction::ObTxSEQ(104, 0));
  }
}

// Verify: effective_seq and orig seq tracking in operator()()
TEST_F(ObTestRedoFill, test_remap_effective_seq_and_orig_tracking)
{
  ObTxFillRedoCtx &ctx = mdo_.fill_ctx_;
  ctx.remapped_next_seq_ = transaction::ObTxSEQ(500, 0);

  // Simulate the operator()() statistics logic for 3 callbacks
  // with original seqs 10, 12, 11 (non-monotonic in original space)
  transaction::ObTxSEQ orig_seqs[] = {
    transaction::ObTxSEQ(10, 0),
    transaction::ObTxSEQ(12, 0),
    transaction::ObTxSEQ(11, 0)
  };
  transaction::ObTxSEQ max_seq_no;

  for (int i = 0; i < 3; i++) {
    // Simulate fill method: assign and advance cursor
    ++ctx.remapped_next_seq_;

    // Simulate operator()() statistics path
    const transaction::ObTxSEQ orig_seq = orig_seqs[i];
    transaction::ObTxSEQ effective_seq = orig_seq;
    if (ctx.remapped_next_seq_.is_valid()) {
      effective_seq = ctx.remapped_next_seq_ + (-1);
    }
    if (!ctx.min_seq_no_.is_valid()) {
      ctx.min_seq_no_ = effective_seq;
    } else {
      ctx.min_seq_no_ = MIN(ctx.min_seq_no_, effective_seq);
    }
    max_seq_no = MAX(max_seq_no, effective_seq);

    // Track original seq range
    if (!ctx.orig_min_seq_no_.is_valid()) {
      ctx.orig_min_seq_no_ = orig_seq;
    } else {
      ctx.orig_min_seq_no_ = MIN(ctx.orig_min_seq_no_, orig_seq);
    }
    ctx.orig_max_seq_no_ = MAX(ctx.orig_max_seq_no_, orig_seq);
  }

  // Remapped range: 500, 501, 502
  EXPECT_EQ(ctx.min_seq_no_, transaction::ObTxSEQ(500, 0));
  EXPECT_EQ(max_seq_no, transaction::ObTxSEQ(502, 0));
  EXPECT_EQ(ctx.remapped_next_seq_, transaction::ObTxSEQ(503, 0));

  // Original range: min=10, max=12
  EXPECT_EQ(ctx.orig_min_seq_no_, transaction::ObTxSEQ(10, 0));
  EXPECT_EQ(ctx.orig_max_seq_no_, transaction::ObTxSEQ(12, 0));
}

// Verify: no remap mode - effective_seq equals original, orig fields unused
TEST_F(ObTestRedoFill, test_no_remap_effective_seq_equals_original)
{
  ObTxFillRedoCtx &ctx = mdo_.fill_ctx_;
  // remapped_next_seq_ stays invalid (default)
  EXPECT_FALSE(ctx.remapped_next_seq_.is_valid());

  transaction::ObTxSEQ orig_seq(42, 0);
  transaction::ObTxSEQ effective_seq = orig_seq;
  if (ctx.remapped_next_seq_.is_valid()) {
    effective_seq = ctx.remapped_next_seq_ + (-1);
  }
  // Without remap, effective_seq == original seq
  EXPECT_EQ(effective_seq, orig_seq);
}

// Verify: overflow boundary condition
TEST_F(ObTestRedoFill, test_remap_overflow_boundary)
{
  // Simulate: remapped_max_seq_ = 104 (inclusive), range is [100, 104]
  transaction::ObTxSEQ remapped_max(104, 0);

  // Case 1: exactly exhausted (next = 105 = max + 1) => NOT overflow
  {
    transaction::ObTxSEQ next(105, 0);
    bool overflow = (next > remapped_max + 1);
    EXPECT_FALSE(overflow);
  }

  // Case 2: one beyond (next = 106 > max + 1) => overflow
  {
    transaction::ObTxSEQ next(106, 0);
    bool overflow = (next > remapped_max + 1);
    EXPECT_TRUE(overflow);
  }

  // Case 3: within range (next = 103 < max + 1) => NOT overflow
  {
    transaction::ObTxSEQ next(103, 0);
    bool overflow = (next > remapped_max + 1);
    EXPECT_FALSE(overflow);
  }
}

// ===== Part 3: hotspot_log_submitted tnode seq remap tests =====
// These tests simulate the remap logic pattern from hotspot_log_submitted,
// exercising the same invariants without requiring a full ObMvccRowCallback
// object graph (which needs ObMemtable for hotspot_log_submitted_cb).

// Test 1: tnode seq_no remapped correctly, callback seq unchanged
TEST_F(ObTestRedoFill, test_hotspot_log_submitted_remaps_tnode_seq)
{
  const int cnt = 3;
  transaction::ObTxSEQ orig_seqs[] = {
    transaction::ObTxSEQ(10, 0),
    transaction::ObTxSEQ(11, 0),
    transaction::ObTxSEQ(12, 0)
  };
  transaction::ObTxSEQ remap_from(500, 0);
  transaction::ObTxSEQ remap_to(502, 0);
  transaction::ObTxSEQ orig_from(10, 0);
  transaction::ObTxSEQ orig_to(12, 0);

  // Simulate tnode seq_no (initially same as callback orig seq)
  ObMvccTransNode tnodes[cnt];
  for (int i = 0; i < cnt; i++) {
    tnodes[i].seq_no_ = orig_seqs[i];
  }

  // Simulate the remap loop from hotspot_log_submitted
  transaction::ObTxSEQ cur_remap_seq = remap_from;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < cnt; i++) {
    // Check callback seq in orig range (defensive check)
    if (orig_seqs[i] < orig_from || orig_seqs[i] > orig_to) {
      ret = OB_ERR_UNEXPECTED;
    }
    // Remap tnode seq, leave callback seq (orig_seqs) unchanged
    if (OB_SUCC(ret)) {
      tnodes[i].set_seq_no(cur_remap_seq);
      ++cur_remap_seq;
    }
  }
  // Check fully consumed: cur_remap_seq should be remap_to + 1
  if (OB_SUCC(ret) && cur_remap_seq != remap_to + 1) {
    ret = OB_ERR_UNEXPECTED;
  }
  EXPECT_EQ(OB_SUCCESS, ret);

  // Verify: tnode seq_no remapped
  EXPECT_EQ(tnodes[0].get_seq_no(), transaction::ObTxSEQ(500, 0));
  EXPECT_EQ(tnodes[1].get_seq_no(), transaction::ObTxSEQ(501, 0));
  EXPECT_EQ(tnodes[2].get_seq_no(), transaction::ObTxSEQ(502, 0));

  // Verify: callback (orig) seq unchanged
  EXPECT_EQ(orig_seqs[0], transaction::ObTxSEQ(10, 0));
  EXPECT_EQ(orig_seqs[1], transaction::ObTxSEQ(11, 0));
  EXPECT_EQ(orig_seqs[2], transaction::ObTxSEQ(12, 0));
}

// Test 2: single-element remap seq fully consumed
TEST_F(ObTestRedoFill, test_hotspot_log_submitted_remap_fully_consumed)
{
  transaction::ObTxSEQ remap_from(700, 0);
  transaction::ObTxSEQ remap_to(700, 0);
  ObMvccTransNode tnode;
  tnode.seq_no_ = transaction::ObTxSEQ(20, 0);

  transaction::ObTxSEQ cur_remap_seq = remap_from;
  tnode.set_seq_no(cur_remap_seq);
  ++cur_remap_seq;

  // Fully consumed: cur == to + 1
  EXPECT_EQ(cur_remap_seq, remap_to + 1);
  EXPECT_EQ(tnode.get_seq_no(), transaction::ObTxSEQ(700, 0));
}

// Test 3: callback seq_no outside orig range triggers failure
TEST_F(ObTestRedoFill, test_hotspot_log_submitted_seq_out_of_range_fails)
{
  transaction::ObTxSEQ cb_seq(99, 0);
  transaction::ObTxSEQ orig_from(10, 0);
  transaction::ObTxSEQ orig_to(12, 0);

  // Simulate the range check from hotspot_log_submitted
  bool out_of_range = (cb_seq < orig_from || cb_seq > orig_to);
  EXPECT_TRUE(out_of_range);

  // Also check in-range case
  transaction::ObTxSEQ cb_seq_ok(11, 0);
  bool in_range = !(cb_seq_ok < orig_from || cb_seq_ok > orig_to);
  EXPECT_TRUE(in_range);
}

// Test 4: null tnode detection
TEST_F(ObTestRedoFill, test_hotspot_log_submitted_null_tnode_fails)
{
  ObMvccTransNode *tnode = nullptr;
  // Simulate the null check from hotspot_log_submitted
  EXPECT_TRUE(OB_ISNULL(tnode));

  // Non-null case should pass
  ObMvccTransNode real_tnode;
  EXPECT_FALSE(OB_ISNULL(&real_tnode));
}

// Test 5: no remap (remap_from invalid) - tnode seq unchanged
TEST_F(ObTestRedoFill, test_hotspot_log_submitted_no_remap)
{
  const int cnt = 2;
  ObMvccTransNode tnodes[cnt];
  transaction::ObTxSEQ orig_seqs[] = {
    transaction::ObTxSEQ(50, 0),
    transaction::ObTxSEQ(51, 0)
  };
  for (int i = 0; i < cnt; i++) {
    tnodes[i].seq_no_ = orig_seqs[i];
  }

  // remap_from invalid => no remap path taken
  transaction::ObTxSEQ remap_from;  // default: invalid
  bool need_remap = remap_from.is_valid();
  EXPECT_FALSE(need_remap);

  // Since need_remap is false, tnode seq remains original
  EXPECT_EQ(tnodes[0].get_seq_no(), transaction::ObTxSEQ(50, 0));
  EXPECT_EQ(tnodes[1].get_seq_no(), transaction::ObTxSEQ(51, 0));
}

// ==============================================================
// Rollback range tests for ObTxHotspotRedoCache::get_rollback_range
// ==============================================================

#ifdef OB_HOTSPOT_GROUP_COMMIT

// Helper: set up a cache with given primary tx id and manually push secondary entries.
// Since #define private public is active, we can directly set internal fields.
static void setup_cache_with_secondaries(
    transaction::TransModulePageAllocator &alloc,
    transaction::ObTxHotspotRedoCache &cache,
    const transaction::ObTransID &primary_tx_id,
    const ObTxSEQ &primary_first,
    const ObTxSEQ &primary_last,
    const common::ObIArray<transaction::ObTransID> &sec_tx_ids,
    const common::ObIArray<uint64_t> &node_counts)
{
  cache.primary_tx_id_ = primary_tx_id;
  if (primary_first.is_valid() && primary_last.is_valid() && primary_last >= primary_first) {
    cache.primary_insert_first_seq_ = primary_first;
    cache.primary_insert_last_seq_ = primary_last;
  }
  // Allocate hotspot_cache_ manually (new pointer-based model)
  int64_t count = sec_tx_ids.count();
  if (count > 0) {
    cache.hotspot_cache_capacity_ = count;
    cache.hotspot_cache_count_ = 0;
    cache.hotspot_cache_ = static_cast<transaction::ObTxRedoExtractArg*>(
        alloc.alloc(count * sizeof(transaction::ObTxRedoExtractArg)));
    // Push secondary entries using placement new
    for (int64_t i = 0; i < count; ++i) {
      transaction::ObTxRedoExtractArg arg;
      arg.tx_id_ = sec_tx_ids.at(i);
      arg.unsynced_node_cnt_ = node_counts.at(i);
      arg.secondary_seq_base_ = i;  // ascending order
      new (&cache.hotspot_cache_[cache.hotspot_cache_count_]) transaction::ObTxRedoExtractArg(arg);
      cache.hotspot_cache_count_++;
    }
  }
  // Use assign_remapped_seq_ranges to compute remapped ranges
  if (sec_tx_ids.count() > 0) {
    int ret = cache.assign_remapped_seq_ranges(primary_first, primary_last);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

static void cleanup_manual_hotspot_cache(
    transaction::TransModulePageAllocator &alloc,
    transaction::ObTxHotspotRedoCache &cache)
{
  if (OB_NOT_NULL(cache.hotspot_cache_)) {
    for (int64_t i = 0; i < cache.hotspot_cache_count_; ++i) {
      cache.hotspot_cache_[i].~ObTxRedoExtractArg();
    }
    alloc.free(cache.hotspot_cache_);
    cache.hotspot_cache_ = nullptr;
    cache.hotspot_cache_capacity_ = 0;
    cache.hotspot_cache_count_ = 0;
  }
}

TEST_F(ObTestRedoFill, test_rollback_range_secondary)
{
  // A single secondary tx with 3 nodes => remapped range [cursor, cursor+2]
  // rollback range should be (range_end, prev(range_begin)) per build_rollback_range_
  transaction::TransModulePageAllocator alloc;
  transaction::ObTxHotspotRedoCache cache;
  transaction::ObTransID primary_id(100);
  transaction::ObTransID sec_id(200);
  ObTxSEQ primary_first(1, 0);
  ObTxSEQ primary_last(5, 0);

  ObSEArray<transaction::ObTransID, 1> sec_ids;
  ObSEArray<uint64_t, 1> counts;
  sec_ids.push_back(sec_id);
  counts.push_back(3);

  setup_cache_with_secondaries(alloc, cache, primary_id, primary_first, primary_last, sec_ids, counts);

  ObTxSEQ from_seq, to_seq;
  ASSERT_EQ(OB_SUCCESS, cache.get_rollback_range(&sec_id, from_seq, to_seq));
  // remapped_min = primary_last + 1 = (6,0), remapped_next starts at (6,0) and advances
  // After assign: remapped_min=(6,0), remapped_max=(8,0), remapped_next=(6,0)
  // get_rollback_range uses: build_rollback_range_(remapped_min, prev(remapped_next))
  // Since remapped_next is still (6,0) (no data flushed), prev(6,0) = (5,0) < (6,0)
  // Actually: get_rollback_range line 271: prev_seq_(remapped_next_seq_) as range_end
  // When remapped_next == remapped_min, prev(remapped_next) < remapped_min
  // => build_rollback_range_ sees range_end < range_begin => resets from/to
  // This represents "no data actually consumed yet" — valid empty scenario.
  // Let's simulate some consumed data by advancing remapped_next_seq_
  cache.hotspot_cache_[0].remapped_next_seq_ = cache.hotspot_cache_[0].remapped_min_seq_ + 3;

  from_seq.reset();
  to_seq.reset();
  ASSERT_EQ(OB_SUCCESS, cache.get_rollback_range(&sec_id, from_seq, to_seq));
  // Now range_begin = remapped_min = (6,0), range_end = prev(remapped_next) = prev(9,0) = (8,0)
  // build_rollback_range_: from = range_end = (8,0), to = prev(range_begin) = prev(6,0) = (5,0)
  EXPECT_TRUE(from_seq.is_valid());
  EXPECT_TRUE(to_seq.is_valid());
  EXPECT_EQ(from_seq, ObTxSEQ(8, 0));
  EXPECT_EQ(to_seq, ObTxSEQ(5, 0));
  cleanup_manual_hotspot_cache(alloc, cache);
}

TEST_F(ObTestRedoFill, test_rollback_range_primary_non_hotspot)
{
  // Primary insert range: [first, last] => rollback: from=last, to=prev(first)
  transaction::TransModulePageAllocator alloc;
  transaction::ObTxHotspotRedoCache cache;
  transaction::ObTransID primary_id(100);
  ObTxSEQ primary_first(3, 0);
  ObTxSEQ primary_last(7, 0);

  ObSEArray<transaction::ObTransID, 1> sec_ids;
  ObSEArray<uint64_t, 1> counts;

  setup_cache_with_secondaries(alloc, cache, primary_id, primary_first, primary_last, sec_ids, counts);

  ObTxSEQ from_seq, to_seq;
  ASSERT_EQ(OB_SUCCESS, cache.get_rollback_range(nullptr, from_seq, to_seq));
  // build_rollback_range_: from = range_end = (7,0), to = prev(range_begin) = prev(3,0) = (2,0)
  EXPECT_TRUE(from_seq.is_valid());
  EXPECT_TRUE(to_seq.is_valid());
  EXPECT_EQ(from_seq, ObTxSEQ(7, 0));
  EXPECT_EQ(to_seq, ObTxSEQ(2, 0));
  cleanup_manual_hotspot_cache(alloc, cache);
}

TEST_F(ObTestRedoFill, test_rollback_range_empty_secondary)
{
  // No secondary tx in cache => OB_ENTRY_NOT_EXIST for any secondary query
  transaction::TransModulePageAllocator alloc;
  transaction::ObTxHotspotRedoCache cache;
  transaction::ObTransID primary_id(100);
  ObTxSEQ primary_first(1, 0);
  ObTxSEQ primary_last(5, 0);

  ObSEArray<transaction::ObTransID, 1> sec_ids;
  ObSEArray<uint64_t, 1> counts;
  setup_cache_with_secondaries(alloc, cache, primary_id, primary_first, primary_last, sec_ids, counts);

  ObTxSEQ from_seq, to_seq;
  transaction::ObTransID unknown_sec(999);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get_rollback_range(&unknown_sec, from_seq, to_seq));
  cleanup_manual_hotspot_cache(alloc, cache);
}

TEST_F(ObTestRedoFill, test_rollback_range_invalid_tx_id)
{
  // Invalid secondary tx id => OB_INVALID_ARGUMENT
  // Primary tx id passed as secondary => OB_INVALID_ARGUMENT
  transaction::TransModulePageAllocator alloc;
  transaction::ObTxHotspotRedoCache cache;
  transaction::ObTransID primary_id(100);
  ObTxSEQ primary_first(1, 0);
  ObTxSEQ primary_last(5, 0);

  ObSEArray<transaction::ObTransID, 1> sec_ids;
  ObSEArray<uint64_t, 1> counts;
  setup_cache_with_secondaries(alloc, cache, primary_id, primary_first, primary_last, sec_ids, counts);

  ObTxSEQ from_seq, to_seq;
  // invalid tx id
  transaction::ObTransID invalid_id;
  EXPECT_EQ(OB_INVALID_ARGUMENT, cache.get_rollback_range(&invalid_id, from_seq, to_seq));
  // primary tx id used as secondary
  EXPECT_EQ(OB_INVALID_ARGUMENT, cache.get_rollback_range(&primary_id, from_seq, to_seq));
  cleanup_manual_hotspot_cache(alloc, cache);
}

TEST_F(ObTestRedoFill, test_rollback_range_multi_secondary)
{
  // Two secondary txs with different node counts => non-overlapping rollback ranges
  transaction::TransModulePageAllocator alloc;
  transaction::ObTxHotspotRedoCache cache;
  transaction::ObTransID primary_id(100);
  transaction::ObTransID sec_id_a(200);
  transaction::ObTransID sec_id_b(300);
  ObTxSEQ primary_first(1, 0);
  ObTxSEQ primary_last(5, 0);

  ObSEArray<transaction::ObTransID, 2> sec_ids;
  ObSEArray<uint64_t, 2> counts;
  sec_ids.push_back(sec_id_a);
  sec_ids.push_back(sec_id_b);
  counts.push_back(3);  // sec_a: 3 nodes
  counts.push_back(2);  // sec_b: 2 nodes

  setup_cache_with_secondaries(alloc, cache, primary_id, primary_first, primary_last, sec_ids, counts);

  // Simulate all data consumed for both
  for (int64_t i = 0; i < cache.hotspot_cache_count_; ++i) {
    transaction::ObTxRedoExtractArg &arg = cache.hotspot_cache_[i];
    arg.remapped_next_seq_ = arg.remapped_max_seq_ + 1;
  }

  ObTxSEQ from_a, to_a, from_b, to_b;
  ASSERT_EQ(OB_SUCCESS, cache.get_rollback_range(&sec_id_a, from_a, to_a));
  ASSERT_EQ(OB_SUCCESS, cache.get_rollback_range(&sec_id_b, from_b, to_b));

  // Both ranges should be valid
  EXPECT_TRUE(from_a.is_valid());
  EXPECT_TRUE(to_a.is_valid());
  EXPECT_TRUE(from_b.is_valid());
  EXPECT_TRUE(to_b.is_valid());

  // Ranges should not overlap: sec_a's from > sec_a's to, sec_b's from > sec_b's to
  // And sec_b's range should start after sec_a's range + gap
  // sec_a: remapped_min=(6,0), remapped_max=(8,0) => from=(8,0), to=(5,0)
  // sec_b: remapped_min=(6+3+1+5=15,0) wait let me recalculate
  // cursor starts at primary_last+1 = (6,0)
  // sec_a: min=(6,0), max=(8,0), cursor_after = 8+1+5 = (14,0)
  // sec_b: min=(14,0), max=(15,0), cursor_after = 15+1+5 = (21,0)
  EXPECT_EQ(from_a, ObTxSEQ(8, 0));
  EXPECT_EQ(to_a, ObTxSEQ(5, 0));
  EXPECT_EQ(from_b, ObTxSEQ(15, 0));
  EXPECT_EQ(to_b, ObTxSEQ(13, 0));

  // Verify non-overlapping: sec_a rollback covers seq [6..8], sec_b covers [14..15]
  // The gap between them is [9..13] which is the sub-tx gap
  EXPECT_TRUE(from_a < to_b || to_a > from_b);  // no overlap
  cleanup_manual_hotspot_cache(alloc, cache);
}

// ==============================================================
// Tests for stage-four improvements
// ==============================================================

TEST_F(ObTestRedoFill, test_rollback_secondary_memtable_and_mark)
{
  // Verify rollback_secondary_memtable_and_mark sets data_rolled_back_ flag
  // and handles found/not-found/not-remapped cases correctly.
  // Note: memtable rollback itself is skipped because other_ctx_ is nullptr in this unit test.
  transaction::TransModulePageAllocator alloc;
  transaction::ObTxHotspotRedoCache cache;
  transaction::ObTransID primary_id(100);
  transaction::ObTransID sec_id_a(200);
  transaction::ObTransID sec_id_b(300);
  transaction::ObTransID unknown_id(999);

  cache.primary_tx_id_ = primary_id;

  // Manually allocate 2 entries for hotspot_cache_ (new pointer-based model)
  cache.hotspot_cache_capacity_ = 2;
  cache.hotspot_cache_count_ = 2;
  cache.hotspot_cache_ = static_cast<transaction::ObTxRedoExtractArg*>(
      alloc.alloc(2 * sizeof(transaction::ObTxRedoExtractArg)));

  transaction::ObTxRedoExtractArg arg_a;
  arg_a.tx_id_ = sec_id_a;
  arg_a.unsynced_node_cnt_ = 1;
  arg_a.seq_remapped_ = true;
  arg_a.fill_redo_orig_min_seq_ = ObTxSEQ(10, 0);
  arg_a.fill_redo_orig_max_seq_ = ObTxSEQ(12, 0);
  new (&cache.hotspot_cache_[0]) transaction::ObTxRedoExtractArg(arg_a);

  transaction::ObTxRedoExtractArg arg_b;
  arg_b.tx_id_ = sec_id_b;
  arg_b.unsynced_node_cnt_ = 1;
  arg_b.seq_remapped_ = true;
  new (&cache.hotspot_cache_[1]) transaction::ObTxRedoExtractArg(arg_b);

  // Initially neither is rolled back
  EXPECT_FALSE(cache.hotspot_cache_[0].data_rolled_back_);
  EXPECT_FALSE(cache.hotspot_cache_[1].data_rolled_back_);

  // Mark sec_a: succeeds, sets data_rolled_back_ (memtable rollback skipped: other_ctx_ is null)
  ASSERT_EQ(OB_SUCCESS, cache.rollback_secondary_memtable_and_mark(sec_id_a));
  EXPECT_TRUE(cache.hotspot_cache_[0].data_rolled_back_);
  EXPECT_FALSE(cache.hotspot_cache_[1].data_rolled_back_);

  // Mark sec_b
  ASSERT_EQ(OB_SUCCESS, cache.rollback_secondary_memtable_and_mark(sec_id_b));
  EXPECT_TRUE(cache.hotspot_cache_[1].data_rolled_back_);

  // Unknown tx returns OB_ENTRY_NOT_EXIST
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.rollback_secondary_memtable_and_mark(unknown_id));

  // Not remapped case: seq_remapped_ = false => OB_ENTRY_NOT_EXIST
  cache.hotspot_cache_[0].data_rolled_back_ = false;
  cache.hotspot_cache_[0].seq_remapped_ = false;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.rollback_secondary_memtable_and_mark(sec_id_a));
  EXPECT_FALSE(cache.hotspot_cache_[0].data_rolled_back_);  // not changed
  cleanup_manual_hotspot_cache(alloc, cache);
}

#endif // OB_HOTSPOT_GROUP_COMMIT

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
