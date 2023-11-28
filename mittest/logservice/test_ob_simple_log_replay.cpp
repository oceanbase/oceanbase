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

#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#define private public
#define protected public
#include "logservice/ob_ls_adapter.h"
#include "share/scn.h"
#include "env/ob_simple_log_cluster_env.h"
#include "logservice/palf/palf_iterator.h"
#undef private
#undef protected

const std::string TEST_NAME = "replay_func";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace palf;
namespace logservice
{
bool ObLogReplayService::is_tenant_out_of_memory_() const
{
  return false;
}
};
using namespace logservice;
namespace unittest
{
class TestObSimpleLogReplayFunc : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogReplayFunc() :  ObSimpleLogClusterTestEnv()
  {}
};

class MockLSAdapter : public ObLSAdapter
{
public:
  MockLSAdapter() {
    ObLSAdapter();
    task_count_ = 0;
    retry_count_ = 0;
    pre_barrier_scn_.set_min();
    rp_st_ = NULL;
  }

  int replay(ObLogReplayTask *replay_task)
  {
    int ret = OB_SUCCESS;
    if (NULL == rp_st_) {
      CLOG_LOG(ERROR, "rp_st_ is null");
    } else if (rand() % 2 && retry_count_ < RETRY_LIMIT) {
      //随机模拟错误码和执行时间
      ret = OB_EAGAIN;
      retry_count_++;
      CLOG_LOG(INFO, "replay log retry", K(task_count_));
    } else {
      usleep(10);
      share::SCN pre_barrier_scn = pre_barrier_scn_.atomic_load();
      offset_t post_barrier_lsn = ATOMIC_LOAD(&rp_st_->post_barrier_lsn_.val_);
      EXPECT_EQ(true, (pre_barrier_scn < replay_task->scn_));
      if (LOG_INVALID_LSN_VAL != post_barrier_lsn)
      {
        EXPECT_EQ(true, (replay_task->lsn_.val_ <= post_barrier_lsn));
        CLOG_LOG(INFO, "replay log", K(post_barrier_lsn), KPC(replay_task));
      }
      EXPECT_EQ(true, (pre_barrier_scn < replay_task->scn_));
      if (replay_task->is_pre_barrier_) {
        pre_barrier_scn_.atomic_set(replay_task->scn_);
      }
      if (replay_task->is_post_barrier_) {
        //尽量让后向barrier回放慢,如果有没卡住的后向barrier之后的日志就会增大回放的概率
        usleep(1000);
      }
      ATOMIC_INC(&task_count_);
      CLOG_LOG(INFO, "replay log", K(task_count_), KPC(replay_task));
    }
    return ret;
  }

  void wait_replay_done(const int64_t task_count)
  {
    while (task_count > task_count_) {
      usleep(100);
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(INFO, "wait replay done", K(task_count_));
      }
    }
  }
  static const int64_t RETRY_LIMIT = 10000; //模拟随机重试限制
  int64_t task_count_;
  int64_t retry_count_;
  share::SCN pre_barrier_scn_;
  ObReplayStatus *rp_st_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
int64_t log_entry_size = 2 * 1024 * 1024 + 16 * 1024;

TEST_F(TestObSimpleLogReplayFunc, basic_replay)
{

  SET_CASE_LOG_FILE(TEST_NAME, "basic_replay");
  const int64_t task_count = 1024;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  share::SCN basic_scn = share::SCN::min_scn();
  CLOG_LOG(INFO, "test replay begin", K(id));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogReplayService rp_sv;
  ObReplayStatus *rp_st = NULL;
  PalfEnv *palf_env;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  rp_sv.init(palf_env, &ls_adapter, get_cluster()[0]->get_allocator());
  rp_sv.start();
  get_cluster()[0]->get_tenant_base()->update_thread_cnt(10);
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, basic_scn));
  {
    ObReplayStatusGuard guard;
    EXPECT_EQ(OB_SUCCESS, rp_sv.get_replay_status_(ls_id, guard));
    rp_st = guard.get_replay_status();
    ls_adapter.rp_st_ = rp_st;
  }
  LSN unused_lsn;
  int64_t unused_ts = 0;
  //正向流程
  LSN last_log_lsn;
  share::SCN unused_scn;
  share::SCN last_scn;
  for (int i = 0; i < task_count - 1; i++)
  {
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, unused_lsn, unused_scn));
  }
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, last_log_lsn, last_scn));
  ls_adapter.wait_replay_done(task_count);
  bool is_done = false;
  LSN end_lsn = leader.palf_handle_impl_->get_end_lsn();
  LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(end_lsn.val_, max_lsn.val_);
  while (!is_done) {
    rp_sv.is_replay_done(ls_id, end_lsn, is_done);
  }
  EXPECT_EQ(0, rp_sv.get_pending_task_size());
  EXPECT_EQ(OB_SUCCESS, rp_sv.switch_to_leader(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.switch_to_follower(ls_id, basic_lsn));
  //验证reuse
  EXPECT_EQ(OB_SUCCESS, rp_sv.disable(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, share::SCN::min_scn()));
  is_done = false;
  ls_adapter.pre_barrier_scn_.set_min();
  while (!is_done) {
    usleep(100);
    rp_sv.is_replay_done(ls_id, end_lsn, is_done);
  }
  //测试受控回放
  const int64_t id_shadow = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id_shadow(id_shadow);
  PalfHandleImplGuard leader_shadow;
  SCN first_new_scn = SCN::min_scn();
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_shadow, leader_idx, leader_shadow));
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id_shadow));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(leader_shadow));
  EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(leader, leader_shadow));
  {
    ObReplayStatusGuard guard;
    EXPECT_EQ(OB_SUCCESS, rp_sv.get_replay_status_(ls_id_shadow, guard));
    rp_st = guard.get_replay_status();
    ls_adapter.task_count_ = 0;
    ls_adapter.pre_barrier_scn_.set_min();
    ls_adapter.rp_st_ = rp_st;
  }
  //只卡住最后一条日志的回放
  rp_sv.replayable_point_ = SCN::minus(last_scn, 1);
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id_shadow, basic_lsn, basic_scn));
  ls_adapter.wait_replay_done(task_count - 1);
  //验证最大连续回放位点
  SCN max_replayed_scn = SCN::min_scn();
  while (SCN::minus(last_scn, 1) != max_replayed_scn) {
    EXPECT_EQ(OB_SUCCESS, rp_sv.get_max_replayed_scn(ls_id_shadow, max_replayed_scn));
  }
  EXPECT_EQ(OB_SUCCESS, rp_sv.update_replayable_point(SCN::max_scn()));
  is_done = false;
  while (!is_done) {
    rp_sv.is_replay_done(ls_id, end_lsn, is_done);
  }
  //remove ls
  EXPECT_EQ(OB_SUCCESS, rp_sv.remove_ls(ls_id));
  rp_sv.stop();
  rp_sv.wait();
  rp_sv.destroy();
  CLOG_LOG(INFO, "test replay finish", K(id));
}

TEST_F(TestObSimpleLogReplayFunc, test_flashback_to_padding)
{
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_to_padding");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  share::SCN basic_scn = share::SCN::min_scn();
  CLOG_LOG(INFO, "test replay begin", K(id));
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogReplayService rp_sv;
  ObReplayStatus *rp_st = NULL;
  PalfEnv *palf_env;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  rp_sv.init(palf_env, &ls_adapter, get_cluster()[0]->get_allocator());
  rp_sv.start();
  get_cluster()[0]->get_tenant_base()->update_thread_cnt(10);
  LSN iterator_end_lsn(0);
  LSN *iterator_end_lsn_ptr = &iterator_end_lsn;
  auto get_file_end_lsn =[iterator_end_lsn_ptr]() {
    CLOG_LOG(INFO, "get_file_end_lsn", K(*iterator_end_lsn_ptr));
    return *iterator_end_lsn_ptr;
  };
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, basic_scn));
  {
    ObReplayStatusGuard guard;
    EXPECT_EQ(OB_SUCCESS, rp_sv.get_replay_status_(ls_id, guard));
    rp_st = guard.get_replay_status();
    ls_adapter.rp_st_ = rp_st;
  }
  PalfBufferIterator &iterator = rp_st->submit_log_task_.iterator_;
  iterator.iterator_storage_.get_file_end_lsn_ = get_file_end_lsn;
  // 停止拉日志
  rp_st->block_submit();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, leader_idx, log_entry_size));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
  LogStorage *log_storage = &leader.get_palf_handle_impl()->log_engine_.log_storage_;
  LSN padding_header = log_storage->log_tail_;
  SCN padding_header_scn = leader.get_palf_handle_impl()->get_max_scn();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, log_entry_size));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
  LSN log_tail = log_storage->log_tail_;
  SCN padding_tail_scn = leader.get_palf_handle_impl()->get_end_scn();
  EXPECT_LE(padding_header, LSN(PALF_BLOCK_SIZE));
  EXPECT_GE(padding_header+log_entry_size, LSN(PALF_BLOCK_SIZE));
  int64_t mode_version;
  switch_append_to_flashback(leader, mode_version);

  // Test1: flashback到padding头部, replay先回放到padding再执行flashback
  {
    int ret = OB_SUCCESS;
    CLOG_LOG(WARN, "flashback to padding header case1");
    int64_t abs_timeout_us = 4*1000*1000;
    SCN flashback_scn = padding_header_scn;
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->flashback(mode_version, flashback_scn, abs_timeout_us));
    // iterator看到的终点是padding日志尾
    iterator_end_lsn = LSN(PALF_BLOCK_SIZE);
    // replay看到的committed位点是padding尾
    rp_st->unblock_submit();
    EXPECT_EQ(OB_SUCCESS, rp_st->update_end_offset(LSN(PALF_BLOCK_SIZE)));
    // 开启拉日志
    bool is_done = false;
    while (!is_done) {
      // 由于padding日志被受控回放，replay此时的回放位点最多为padding_header
      rp_sv.is_replay_done(ls_id, padding_header, is_done);
      usleep(10*1000);
      CLOG_LOG(WARN, "not replay done", KPC(rp_st), K(padding_header));
    }
    is_done = false;
    CLOG_LOG(INFO, "runlin trace 3", K(iterator), KPC(rp_st));
    // 预期replay的next_to_submit_lsn是padding_header
    EXPECT_EQ(padding_header, rp_st->submit_log_task_.next_to_submit_lsn_);
    switch_flashback_to_append(leader, mode_version);
    iterator_end_lsn = LSN(100000000);
    // 停止拉日志
    rp_st->block_submit();
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, log_entry_size));
    padding_tail_scn = leader.get_palf_handle_impl()->get_max_scn();
    LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  }
  // Test2: flashback到padding头部, replay先执行flashback，再执行replay padding
  {
    int ret = OB_SUCCESS;
    CLOG_LOG(WARN, "flashback to padding header case2");
    int64_t abs_timeout_us = 4*1000*1000;
    SCN flashback_scn = padding_header_scn;
    switch_append_to_flashback(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->flashback(mode_version, flashback_scn, abs_timeout_us));
    // iterator看到的终点是padding日志头
    iterator_end_lsn = padding_header;
    // replay看到的committed位点是padding尾
    rp_st->unblock_submit();
    bool is_done = false;
    while (!is_done) {
      rp_sv.is_replay_done(ls_id, padding_header, is_done);
      usleep(10*1000);
      CLOG_LOG(WARN, "not replay done", KPC(rp_st), K(padding_header));
    }
    // 预期replay的next_to_submit_lsn是padding头
    EXPECT_EQ(iterator_end_lsn, rp_st->submit_log_task_.next_to_submit_lsn_);
    // 修改iterator看到的终点为padding尾
    iterator_end_lsn = LSN(PALF_BLOCK_SIZE);
    // 触发拉日志
    rp_st->trigger_fetch_log();
    sleep(1);
    // 预期replay的next_to_submit_lsn是padding头
    EXPECT_EQ(padding_header, rp_st->submit_log_task_.next_to_submit_lsn_);
    switch_flashback_to_append(leader, mode_version);
    // 停止拉日志
    rp_st->block_submit();
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, log_entry_size));
    padding_tail_scn = leader.get_palf_handle_impl()->get_max_scn();
    LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  }

  // Test3: flashback到padding尾部, replay先执行flashback，再执行replay padding
  {
    int ret = OB_SUCCESS;
    CLOG_LOG(WARN, "flashback to padding tailer case1");
    int64_t abs_timeout_us = 4*1000*1000;
    // flashback_scn为padding尾
    SCN flashback_scn = SCN::minus(padding_tail_scn, 1);
    switch_append_to_flashback(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->flashback(mode_version, flashback_scn, abs_timeout_us));
    // iterator看到的终点是padding日志头
    iterator_end_lsn = padding_header;
    // replay看到的committed位点是padding头
    rp_st->unblock_submit();
    bool is_done = false;
    // iterator由于文件长度，不会吐出padding日志，replay的next_to_submit_lsn到padding头部
    while (!is_done) {
      rp_sv.is_replay_done(ls_id, padding_header, is_done);
      usleep(10*1000);
      CLOG_LOG(WARN, "not replay done", KPC(rp_st), K(padding_header));
    }
    is_done = false;
    // 预期replay的next_to_submit_lsn是padding头
    EXPECT_EQ(padding_header, rp_st->submit_log_task_.next_to_submit_lsn_);
    // iterator能看到padding日志
    iterator_end_lsn = LSN(PALF_BLOCK_SIZE);
    rp_st->trigger_fetch_log();
    while (!is_done) {
      rp_sv.is_replay_done(ls_id, LSN(PALF_BLOCK_SIZE), is_done);
      usleep(10*1000);
      CLOG_LOG(WARN, "not replay done", KPC(rp_st), K(padding_header));
    }
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), rp_st->submit_log_task_.next_to_submit_lsn_);
    switch_flashback_to_append(leader, mode_version);
    rp_st->block_submit();
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, log_entry_size));
    padding_tail_scn = leader.get_palf_handle_impl()->get_max_scn();
    LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));

  }
}

TEST_F(TestObSimpleLogReplayFunc, test_wait_replay_done)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_wait_replay_done");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  share::SCN basic_scn = share::SCN::min_scn();
  CLOG_LOG(INFO, "test replay begin", K(id));
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogReplayService rp_sv;
  ObReplayStatus *rp_st = NULL;
  PalfEnv *palf_env;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  rp_sv.init(palf_env, &ls_adapter, get_cluster()[0]->get_allocator());
  rp_sv.start();
  get_cluster()[0]->get_tenant_base()->update_thread_cnt(10);
  LSN iterator_end_lsn(0);
  LSN *iterator_end_lsn_ptr = &iterator_end_lsn;
  auto get_file_end_lsn =[iterator_end_lsn_ptr]() {
    CLOG_LOG(INFO, "get_file_end_lsn", K(*iterator_end_lsn_ptr));
    return *iterator_end_lsn_ptr;
  };
  iterator_end_lsn = LSN(PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, basic_scn));
  {
    ObReplayStatusGuard guard;
    EXPECT_EQ(OB_SUCCESS, rp_sv.get_replay_status_(ls_id, guard));
    rp_st = guard.get_replay_status();
    ls_adapter.rp_st_ = rp_st;
  }
  PalfBufferIterator &iterator = rp_st->submit_log_task_.iterator_;
  iterator.iterator_storage_.get_file_end_lsn_ = get_file_end_lsn;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, leader_idx, log_entry_size));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
  int64_t remained_log_size = LSN(PALF_BLOCK_SIZE) - leader.get_palf_handle_impl()->get_max_lsn() - sizeof(LogGroupEntryHeader) - sizeof(LogEntryHeader);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, remained_log_size));
  EXPECT_EQ(LSN(PALF_BLOCK_SIZE), leader.get_palf_handle_impl()->get_max_lsn());
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
  bool is_done = false;
  // 无padding日志，committed位点是文件头
  while (!is_done) {
    rp_sv.is_replay_done(ls_id, LSN(PALF_BLOCK_SIZE), is_done);
  }
  iterator_end_lsn = LSN(2*PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, leader_idx, log_entry_size));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, log_entry_size));
  // 有padding日志，committed位点是文件头
  is_done =false;
  while (!is_done) {
    rp_sv.is_replay_done(ls_id, LSN(PALF_BLOCK_SIZE), is_done);
  }
}

TEST_F(TestObSimpleLogReplayFunc, replay_big_log)
{
  SET_CASE_LOG_FILE(TEST_NAME, "replay_big_log");
  const int64_t task_count = 10;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  share::SCN basic_scn = share::SCN::min_scn();
  CLOG_LOG(INFO, "test replay begin", K(id));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogReplayService rp_sv;
  ObReplayStatus *rp_st = NULL;
  PalfEnv *palf_env;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  rp_sv.init(palf_env, &ls_adapter, get_cluster()[0]->get_allocator());
  rp_sv.start();
  get_cluster()[0]->get_tenant_base()->update_thread_cnt(10);
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, basic_scn));
  {
    ObReplayStatusGuard guard;
    EXPECT_EQ(OB_SUCCESS, rp_sv.get_replay_status_(ls_id, guard));
    rp_st = guard.get_replay_status();
    ls_adapter.rp_st_ = rp_st;
  }
  LSN unused_lsn;
  int64_t unused_ts = 0;
  for (int i = 0; i < task_count; i++)
  {
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, MAX_LOG_BODY_SIZE));
  }
  ls_adapter.wait_replay_done(task_count);
  bool is_done = false;
  LSN end_lsn = leader.palf_handle_impl_->get_end_lsn();
  LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(end_lsn.val_, max_lsn.val_);
  while (!is_done) {
    rp_sv.is_replay_done(ls_id, end_lsn, is_done);
  }
  EXPECT_EQ(0, rp_sv.get_pending_task_size());
}
} // unitest
} // oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
