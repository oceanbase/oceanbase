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
using namespace memtable;
namespace transaction {
// Test submitter logic works fine in use cases
//
// mock part_ctx's interface:
// - prepare_for_submit_redo
// - fill_log_block
// - submit_log_block_out
// - is_parallel_logging
//
// mock memtable_ctx's interface
// - fill_redo_log
// - log_submitted
//
struct MockDelegate {
  virtual bool is_parallel_logging() const = 0;
  virtual int submit_redo_log_out(ObTxLogBlock &log_block,
                                  ObTxLogCb *&log_cb,
                                  memtable::ObRedoLogSubmitHelper &helper,
                                  const int64_t replay_hint,
                                  const bool has_hold_ctx_lock,
                                  share::SCN &submitted) = 0;
  virtual int fill_redo_log(memtable::ObTxFillRedoCtx &ctx) = 0;

};
struct MockImpl : MockDelegate {
  int a_;
  public:
  MOCK_CONST_METHOD0(is_parallel_logging, bool());
  MOCK_METHOD6(submit_redo_log_out, int(ObTxLogBlock &,
                                        ObTxLogCb *&,
                                        memtable::ObRedoLogSubmitHelper &,
                                        const int64_t,
                                        const bool,
                                        share::SCN &));
  MOCK_METHOD1(fill_redo_log, int(memtable::ObTxFillRedoCtx &));
};
thread_local MockImpl *mock_ptr;
class ObTestRedoSubmitter : public ::testing::Test
{
public:
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

    // prepare for test
    tx_ctx.exec_info_.state_ = ObTxState::INIT;
    tx_ctx.exec_info_.scheduler_ = common::ObAddr(common::ObAddr::VER::IPV4, "127.0.0.1", 8888);
    tx_ctx.exec_info_.next_log_entry_no_ = 0;
    tx_ctx.cluster_version_ = DATA_CURRENT_VERSION;
    ObLSID ls_id(1001); ObTransID tx_id(777);
    EXPECT_EQ(OB_SUCCESS,tx_ctx.init_log_cbs_(ls_id, tx_id));
    mock_ptr = &mdo_;
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
  MockImpl mdo_;
  ObPartTransCtx tx_ctx;
  ObMemtableCtx mt_ctx;
};

int succ_submit_redo_log_out(ObTxLogBlock & b,
                             ObTxLogCb *& log_cb,
                             memtable::ObRedoLogSubmitHelper &h,
                             const int64_t replay_hint,
                             const bool has_hold_ctx_lock,
                             share::SCN &submitted_scn)
{
  submitted_scn.convert_for_tx(123123123);
  if (log_cb) {
    ((ObPartTransCtx*)(log_cb->ctx_))->return_log_cb_(log_cb);
    log_cb = NULL;
  }
  return OB_SUCCESS;
}

bool ObPartTransCtx::is_parallel_logging() const
{
  return mock_ptr->is_parallel_logging();
}

int ObPartTransCtx::submit_redo_log_out(ObTxLogBlock &log_block,
                                        ObTxLogCb *&log_cb,
                                        memtable::ObRedoLogSubmitHelper &helper,
                                        const int64_t replay_hint,
                                        const bool has_hold_ctx_lock,
                                        share::SCN &submitted_scn)
{
  return mock_ptr->submit_redo_log_out(log_block, log_cb, helper, replay_hint, has_hold_ctx_lock, submitted_scn);
}
}// transaction

namespace memtable {
int ObMemtableCtx::fill_redo_log(ObTxFillRedoCtx &ctx)
{
  TRANS_LOG(INFO, "", K(mock_ptr->a_));
  return mock_ptr->fill_redo_log(ctx);
}
int ObMemtableCtx::log_submitted(const memtable::ObRedoLogSubmitHelper &helper)
{
  TRANS_LOG(INFO, "", K(mock_ptr->a_));
  return OB_SUCCESS;
}
int ObMemtableCtx::get_log_guard(const transaction::ObTxSEQ &write_seq,
                                 memtable::ObCallbackListLogGuard &log_guard,
                                 int& cb_list_idx)
{
  TRANS_LOG(INFO, "", K(mock_ptr->a_));
  cb_list_idx = write_seq.get_branch();
  return OB_SUCCESS;
}
} // memtable
namespace transaction {
TEST_F(ObTestRedoSubmitter, serial_submit_by_writer_thread_BLOCK_FROZEN)
{
  mdo_.a_ = 1;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(false));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_BLOCK_FROZEN, submitter.serial_submit(false));
  }
}

TEST_F(ObTestRedoSubmitter, serial_submit_by_writer_thread_BUF_NOT_ENOUGH)
{
  mdo_.a_ = 2;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(false));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BUF_NOT_ENOUGH;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_EAGAIN, submitter.serial_submit(false));
  }
}

TEST_F(ObTestRedoSubmitter, serial_submit_by_writer_thread_ALL_FILLED)
{
  mdo_.a_ = 2;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(false));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_SUCCESS;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_SUCCESS, submitter.serial_submit(false));
  }
}

TEST_F(ObTestRedoSubmitter, serial_submit_by_writer_thread_UNEXPECTED_ERROR)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(false));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_ALLOCATE_MEMORY_FAILED;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, submitter.serial_submit(false));
  }
}

TEST_F(ObTestRedoSubmitter, serial_submit_by_writer_thread_serial_final)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(false));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_SUCCESS;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_SUCCESS, submitter.serial_submit(true/*serial final*/));
  }
}

TEST_F(ObTestRedoSubmitter, parallel_submit_by_writer_thread_BLOCK_FROZEN)
{
  mdo_.a_ = 4;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxSEQ writer_seq(101, 0);
    EXPECT_EQ(OB_BLOCK_FROZEN, submitter.parallel_submit(writer_seq));
  }
}

TEST_F(ObTestRedoSubmitter, parallel_submit_by_writer_thread_BLOCKED_BY_OTHER_LIST)
{
  mdo_.a_ = 4;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_ITER_END;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxSEQ writer_seq(101, 0);
    EXPECT_EQ(OB_ITER_END, submitter.parallel_submit(writer_seq));
  }
}

TEST_F(ObTestRedoSubmitter, parallel_submit_by_writer_thread_ALL_FILLED)
{
  mdo_.a_ = 4;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_SUCCESS;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxSEQ writer_seq(101, 0);
    EXPECT_EQ(OB_SUCCESS, submitter.parallel_submit(writer_seq));
  }
}

TEST_F(ObTestRedoSubmitter, parallel_submit_by_writer_thread_UNEXPECTED_ERROR)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_ALLOCATE_MEMORY_FAILED;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxSEQ writer_seq(101, 0);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, submitter.parallel_submit(writer_seq));
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_freeze_parallel_logging_BLOCK_FROZEN)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 0;
        ctx.buf_pos_ = 0;
        return OB_BLOCK_FROZEN;
      }));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_BLOCK_FROZEN, submitter.submit_for_freeze());
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_freeze_parallel_logging_FROZEN_BLOCKED_BY_OTHERS)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    // first list flushed the flushable part
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_EAGAIN;
      }));
    // submit it out to log service layer
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    // retry from other lists, the list with small epoch flushed hit a block frozen
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }));
    // submit it out to log service layer
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    // retry from other list, can not submit others due to the list with min epoch is block frozen
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 0;
        ctx.buf_pos_ = 0;
        return OB_BLOCK_FROZEN;
      }));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_BLOCK_FROZEN, submitter.submit_for_freeze());
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_freeze_parallel_logging_BUF_NOT_ENOUGH)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    // the list with small epoch is large
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BUF_NOT_ENOUGH;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    // the list with small epoch is all flushed
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_EAGAIN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    // the second list with small epoch is all flushed
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_EAGAIN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    // all list filled, nothing to fill
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 0;
        ctx.buf_pos_ = 0;
        return OB_SUCCESS;
      }));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_SUCCESS, submitter.submit_for_freeze());
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_freeze_parallel_logging_ALL_FILLED)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_SUCCESS;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    EXPECT_EQ(OB_SUCCESS, submitter.submit_for_freeze());
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_switch_leader_or_on_commit_serial_logging)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(false));
  {
    InSequence s1;
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(1)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(1)
      .WillOnce(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxLogBlock log_block;
    ObTransID tx_id(101);
    log_block.get_header().init(1, DATA_CURRENT_VERSION, 101, tx_id, ObAddr());
    log_block.init_for_fill();
    memtable::ObRedoLogSubmitHelper helper;
    EXPECT_EQ(OB_BLOCK_FROZEN, submitter.fill(log_block, helper));
    EXPECT_EQ(0, helper.callbacks_.count());
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_switch_leader_or_on_commit_parallel_logging_ALL_FILLED)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(4)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }))
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 200;
        ctx.buf_pos_ = 333;
        return OB_ITER_END;
      }))
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 300;
        ctx.buf_pos_ = 444;
        return OB_EAGAIN;
      }))
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 400;
        ctx.buf_pos_ = 555;
        ObCallbackScope scope;
        ctx.helper_->callbacks_.push_back(scope);
        return OB_SUCCESS;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(3)
      .WillRepeatedly(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxLogBlock log_block;
    ObTransID tx_id(101);
    log_block.get_header().init(1, DATA_CURRENT_VERSION, 101, tx_id, ObAddr());
    log_block.init_for_fill();
    memtable::ObRedoLogSubmitHelper helper;
    EXPECT_EQ(OB_SUCCESS, submitter.fill(log_block, helper));
    EXPECT_EQ(helper.callbacks_.count(), 1);
  }
}

TEST_F(ObTestRedoSubmitter, submit_by_switch_leader_or_on_commit_parallel_logging_BLOCKED)
{
  mdo_.a_ = 3;
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(4)
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 100;
        ctx.buf_pos_ = 200;
        return OB_BLOCK_FROZEN;
      }))
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 200;
        ctx.buf_pos_ = 333;
        return OB_ITER_END;
      }))
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 300;
        ctx.buf_pos_ = 444;
        return OB_EAGAIN;
      }))
      .WillOnce(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 0;
        ctx.buf_pos_ = 0;
        return OB_BLOCK_FROZEN;
      }));
    EXPECT_CALL(mdo_, submit_redo_log_out(_,_,_,_,_,_))
      .Times(3)
      .WillRepeatedly(Invoke(succ_submit_redo_log_out));
    ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
    ObTxLogBlock log_block;
    ObTransID tx_id(101);
    log_block.get_header().init(1, DATA_CURRENT_VERSION, 101, tx_id, ObAddr());
    log_block.init_for_fill();
    memtable::ObRedoLogSubmitHelper helper;
    EXPECT_EQ(OB_BLOCK_FROZEN, submitter.fill(log_block, helper));
    EXPECT_EQ(helper.callbacks_.count(), 0);
  }
}

TEST_F(ObTestRedoSubmitter, submit_ROW_SIZE_TOO_LARGE)
{
  EXPECT_CALL(mdo_, is_parallel_logging())
    .Times(AtLeast(1))
    .WillRepeatedly(Return(true));
  {
    EXPECT_CALL(mdo_, fill_redo_log(_))
      .Times(3)
      .WillRepeatedly(Invoke([](ObTxFillRedoCtx &ctx) {
        ctx.fill_count_ = 0;
        ctx.buf_pos_ = 0;
        return OB_ERR_TOO_BIG_ROWSIZE;
      }));
    {
      ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
      EXPECT_EQ(OB_ERR_TOO_BIG_ROWSIZE, submitter.submit_all(true));
      EXPECT_EQ(submitter.get_submitted_cnt(), 0);
    }
    {
      ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
      ObTxSEQ write_seq(100,200);
      EXPECT_EQ(OB_ERR_TOO_BIG_ROWSIZE, submitter.parallel_submit(write_seq));
      EXPECT_EQ(submitter.get_submitted_cnt(), 0);
    }
    {
      ObTxRedoSubmitter submitter(tx_ctx, mt_ctx);
      ObTxLogBlock log_block;
      ObTransID tx_id(101);
      log_block.get_header().init(1, DATA_CURRENT_VERSION, 101, tx_id, ObAddr());
      log_block.init_for_fill();
      memtable::ObRedoLogSubmitHelper helper;
      EXPECT_EQ(OB_ERR_TOO_BIG_ROWSIZE, submitter.fill(log_block, helper));
      EXPECT_EQ(submitter.get_submitted_cnt(), 0);
      EXPECT_EQ(helper.callbacks_.count(), 0);
    }
  }
}

} // transaction
} // oceanbase

int main(int argc, char **argv)
{
  const char *log_name = "test_redo_submitter.log";
  system("rm -rf test_redo_submitter.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_name, true, false,
                       log_name,
                       log_name,
                       log_name);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
