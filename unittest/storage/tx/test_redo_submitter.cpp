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
#include <atomic>
#include <thread>
#define private public
#define protected public
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
  virtual int submit_redo_log_out(ObTxLogBlock &log_block,
                                  ObTxLogCb *&log_cb,
                                  memtable::ObRedoLogSubmitHelper &helper,
                                  const int64_t replay_hint,
                                  const bool parallel_logging,
                                  share::SCN &submitted) = 0;
  virtual int fill_redo_log(memtable::ObTxFillRedoCtx &ctx) = 0;

};
struct MockImpl : MockDelegate {
  int a_;
  public:
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
  void set_parallel_logging()
  {
    tx_ctx.exec_info_.serial_final_scn_.convert_from_ts(ObTimeUtility::current_time());
  }
  void set_serial_logging()
  {
    tx_ctx.exec_info_.serial_final_scn_.reset();
  }
  MockImpl mdo_;
  ObPartTransCtx tx_ctx;
  ObMemtableCtx mt_ctx;
};

int succ_submit_redo_log_out(ObTxLogBlock & b,
                             ObTxLogCb *& log_cb,
                             memtable::ObRedoLogSubmitHelper &h,
                             const int64_t replay_hint,
                             const bool parallel_logging,
                             share::SCN &submitted_scn)
{
  submitted_scn.convert_for_tx(123123123);
  if (log_cb) {
    ((ObPartTransCtx*)(log_cb->get_group_ptr()->get_tx_ctx()))->return_log_cb_(log_cb);
    log_cb = NULL;
  }
  if (h.callback_redo_submitted_) {
    h.callbacks_.reset();
  }
  return OB_SUCCESS;
}

int ObPartTransCtx::submit_redo_log_out(ObTxLogBlock &log_block,
                                        ObTxLogCb *&log_cb,
                                        memtable::ObRedoLogSubmitHelper &helper,
                                        const int64_t replay_hint,
                                        const bool parallel_logging,
                                        share::SCN &submitted_scn)
{
  return mock_ptr->submit_redo_log_out(log_block, log_cb, helper, replay_hint, parallel_logging, submitted_scn);
}
}// transaction

namespace memtable {
int ObMemtableCtx::fill_redo_log(ObTxFillRedoCtx &ctx)
{
  TRANS_LOG(INFO, "", K(mock_ptr->a_));
  int ret = mock_ptr->fill_redo_log(ctx);
  if (ctx.fill_count_ > 0) {
    ObCallbackScope scope;
    ctx.callback_scopes_.push_back(scope);
  }
  return ret;
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
  set_serial_logging();
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
  set_serial_logging();
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
  set_serial_logging();
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
  set_serial_logging();
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
  set_serial_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_serial_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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
  set_parallel_logging();
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

// Regression for: when freeze-triggered redo submit asks for the reserved
// FREEZE_LOG_CB_INDEX cb but that one is already busy, prepare_log_cb_(true)
// must fall back to a normal cb from free_cbs_ instead of returning
// OB_TX_NOLOGCB. The fallback path used to leak ret=OB_TX_NOLOGCB into the
// success branch and dropped the cb after remove_first().
TEST_F(ObTestRedoSubmitter, prepare_log_cb_freeze_cb_busy_fallback_to_normal)
{
  // Mark the freeze-reserved cb as busy.
  ObTxLogCb *freeze_cb = tx_ctx.reserve_log_cb_group_.get_log_cb_by_index(
      ObTxLogCbGroup::FREEZE_LOG_CB_INDEX);
  ASSERT_TRUE(freeze_cb != nullptr);
  ASSERT_FALSE(freeze_cb->is_busy());
  freeze_cb->set_busy();

  // At least one normal cb should be available in free_cbs_ from init_log_cbs_.
  const int64_t free_cnt_before = tx_ctx.free_cbs_.get_size();
  ASSERT_GT(free_cnt_before, 0);

  // Ask for a freeze cb: must fall back to a normal cb, not return OB_TX_NOLOGCB.
  ObTxLogCb *log_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(true /*need_freeze_cb*/, log_cb));
  ASSERT_TRUE(log_cb != nullptr);
  // The returned cb must NOT be the freeze-reserved one (that one is busy).
  ASSERT_NE(freeze_cb, log_cb);
  // The cb must be marked busy and accounted out of free_cbs_.
  ASSERT_TRUE(log_cb->is_busy());
  ASSERT_EQ(free_cnt_before - 1, tx_ctx.free_cbs_.get_size());

  // Cleanup so TearDown does not complain about busy cbs left behind.
  tx_ctx.return_log_cb_(log_cb);
  freeze_cb->reuse();
}

TEST_F(ObTestRedoSubmitter, log_cb_busy_ownership_api_keeps_cleanup_private)
{
  ObTxLogCb cb;
  ASSERT_FALSE(cb.is_busy());
  ASSERT_TRUE(cb.try_acquire_busy());
  ASSERT_TRUE(cb.is_busy());
  ASSERT_FALSE(cb.try_acquire_busy());

  ASSERT_EQ(OB_SUCCESS, cb.get_cb_arg_array().push_back(ObTxCbArg(ObTxLogType::TX_ABORT_LOG, nullptr)));
  cb.reuse_without_busy();
  ASSERT_TRUE(cb.is_busy());
  ASSERT_EQ(0, cb.get_cb_arg_array().count());

  cb.release_busy();
  ASSERT_FALSE(cb.is_busy());
  cb.reuse();
  ASSERT_FALSE(cb.is_busy());
}

TEST_F(ObTestRedoSubmitter, log_cb_busy_ownership_api_allows_one_concurrent_acquire)
{
  ObTxLogCb cb;
  std::atomic<int64_t> ready(0);
  std::atomic<int64_t> go(0);
  std::atomic<int64_t> acquired_count(0);
  auto worker = [&]() {
    ready.fetch_add(1);
    while (0 == go.load()) {
    }
    if (cb.try_acquire_busy()) {
      acquired_count.fetch_add(1);
    }
  };

  std::thread thread1(worker);
  std::thread thread2(worker);
  while (2 != ready.load()) {
  }
  go.store(1);
  thread1.join();
  thread2.join();

  ASSERT_TRUE(cb.is_busy());
  ASSERT_EQ(1, acquired_count.load());
  cb.release_busy();
  ASSERT_FALSE(cb.is_busy());
}

TEST_F(ObTestRedoSubmitter, prepare_log_cb_freeze_cb_cleans_before_release)
{
  ObTxLogCb *freeze_cb = tx_ctx.reserve_log_cb_group_.get_log_cb_by_index(
      ObTxLogCbGroup::FREEZE_LOG_CB_INDEX);
  ASSERT_TRUE(freeze_cb != nullptr);

  ObTxLogCb *log_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(true /*need_freeze_cb*/, log_cb));
  ASSERT_EQ(freeze_cb, log_cb);
  ASSERT_TRUE(freeze_cb->is_busy());
  ASSERT_EQ(OB_SUCCESS, freeze_cb->get_cb_arg_array().push_back(
                            ObTxCbArg(ObTxLogType::TX_ABORT_LOG, nullptr)));

  ASSERT_EQ(OB_SUCCESS, tx_ctx.return_log_cb_(log_cb));
  ASSERT_FALSE(freeze_cb->is_busy());
  ASSERT_EQ(0, freeze_cb->get_cb_arg_array().count());

  log_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(true /*need_freeze_cb*/, log_cb));
  ASSERT_EQ(freeze_cb, log_cb);
  ASSERT_TRUE(freeze_cb->is_busy());
  ASSERT_EQ(0, freeze_cb->get_cb_arg_array().count());
  ASSERT_EQ(OB_SUCCESS, tx_ctx.return_log_cb_(log_cb));
}

TEST_F(ObTestRedoSubmitter, prepare_log_cb_freeze_cb_not_reused_while_cleanup_holds_busy)
{
  ObTxLogCb *freeze_cb = tx_ctx.reserve_log_cb_group_.get_log_cb_by_index(
      ObTxLogCbGroup::FREEZE_LOG_CB_INDEX);
  ASSERT_TRUE(freeze_cb != nullptr);
  ASSERT_TRUE(freeze_cb->try_acquire_busy());
  ASSERT_EQ(OB_SUCCESS, freeze_cb->get_cb_arg_array().push_back(
                            ObTxCbArg(ObTxLogType::TX_ABORT_LOG, nullptr)));

  freeze_cb->reuse_without_busy();
  ASSERT_TRUE(freeze_cb->is_busy());
  ASSERT_EQ(0, freeze_cb->get_cb_arg_array().count());

  const int64_t free_cnt_before = tx_ctx.free_cbs_.get_size();
  ASSERT_GT(free_cnt_before, 0);
  ObTxLogCb *fallback_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(true /*need_freeze_cb*/, fallback_cb));
  ASSERT_TRUE(fallback_cb != nullptr);
  ASSERT_NE(freeze_cb, fallback_cb);
  ASSERT_TRUE(fallback_cb->is_busy());
  ASSERT_EQ(free_cnt_before - 1, tx_ctx.free_cbs_.get_size());
  ASSERT_EQ(OB_SUCCESS, tx_ctx.return_log_cb_(fallback_cb));

  freeze_cb->release_busy();
  ObTxLogCb *log_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(true /*need_freeze_cb*/, log_cb));
  ASSERT_EQ(freeze_cb, log_cb);
  ASSERT_EQ(OB_SUCCESS, log_cb->get_cb_arg_array().push_back(
                            ObTxCbArg(ObTxLogType::TX_ABORT_LOG, nullptr)));
  ASSERT_EQ(1, log_cb->get_cb_arg_array().count());
  ASSERT_EQ(ObTxLogType::TX_ABORT_LOG, log_cb->get_last_log_type());
  ASSERT_EQ(OB_SUCCESS, tx_ctx.return_log_cb_(log_cb));
}

TEST_F(ObTestRedoSubmitter, prepare_log_cb_normal_cb_reacquires_busy_from_free_list)
{
  const int64_t free_cnt_before = tx_ctx.free_cbs_.get_size();
  ASSERT_GT(free_cnt_before, 0);

  ObTxLogCb *log_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(false /*need_freeze_cb*/, log_cb));
  ASSERT_TRUE(log_cb != nullptr);
  ASSERT_TRUE(log_cb->is_busy());
  ASSERT_EQ(free_cnt_before - 1, tx_ctx.free_cbs_.get_size());

  ASSERT_EQ(OB_SUCCESS, log_cb->get_cb_arg_array().push_back(
                            ObTxCbArg(ObTxLogType::TX_ABORT_LOG, nullptr)));
  ASSERT_EQ(OB_SUCCESS, tx_ctx.return_log_cb_(log_cb));
  ASSERT_FALSE(log_cb->is_busy());
  ASSERT_EQ(0, log_cb->get_cb_arg_array().count());
  ASSERT_EQ(free_cnt_before, tx_ctx.free_cbs_.get_size());

  ObTxLogCb *next_cb = nullptr;
  ASSERT_EQ(OB_SUCCESS, tx_ctx.prepare_log_cb_(false /*need_freeze_cb*/, next_cb));
  ASSERT_EQ(log_cb, next_cb);
  ASSERT_TRUE(next_cb->is_busy());
  ASSERT_EQ(0, next_cb->get_cb_arg_array().count());
  ASSERT_EQ(free_cnt_before - 1, tx_ctx.free_cbs_.get_size());
  ASSERT_EQ(OB_SUCCESS, tx_ctx.return_log_cb_(next_cb));
}

TEST_F(ObTestRedoSubmitter, prepare_log_cb_normal_cb_busy_in_free_list_keeps_list_membership)
{
  const int64_t free_cnt_before = tx_ctx.free_cbs_.get_size();
  ASSERT_GT(free_cnt_before, 0);
  ObTxLogCb *free_cb = tx_ctx.free_cbs_.get_first();
  ASSERT_TRUE(free_cb != nullptr);
  ASSERT_FALSE(free_cb->is_busy());
  free_cb->set_busy();

  ObTxLogCb *log_cb = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, tx_ctx.prepare_log_cb_(false /*need_freeze_cb*/, log_cb));
  ASSERT_TRUE(log_cb == nullptr);
  ASSERT_EQ(free_cnt_before, tx_ctx.free_cbs_.get_size());
  ASSERT_EQ(free_cb, tx_ctx.free_cbs_.get_first());

  tx_ctx.free_cbs_.remove(free_cb);
  free_cb->reuse();
  tx_ctx.free_cbs_.add_first(free_cb);
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
