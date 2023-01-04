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
#include "logservice/ob_ls_adapter.h"
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "replay_func";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
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
    pre_barrier_ts_ = 0;
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
      int64_t pre_barrier_ts = ATOMIC_LOAD(&pre_barrier_ts_);
      offset_t post_barrier_lsn = ATOMIC_LOAD(&rp_st_->post_barrier_lsn_.val_);
      EXPECT_EQ(true, (pre_barrier_ts < replay_task->log_ts_));
      if (LOG_INVALID_LSN_VAL != post_barrier_lsn)
      {
        EXPECT_EQ(true, (replay_task->lsn_.val_ <= post_barrier_lsn));
        CLOG_LOG(INFO, "replay log", K(post_barrier_lsn), KPC(replay_task));
      }
      EXPECT_EQ(true, (pre_barrier_ts < replay_task->log_ts_));
      if (replay_task->is_pre_barrier_) {
        ATOMIC_STORE(&pre_barrier_ts_, replay_task->log_ts_);
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
  int64_t pre_barrier_ts_;
  ObReplayStatus *rp_st_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

TEST_F(TestObSimpleLogReplayFunc, replay)
{
  const int64_t task_count = 1024;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  int64_t basic_log_ts = 0;
  PalfHandleGuard leader;
  CLOG_LOG(INFO, "test replay begin", K(id));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogReplayService rp_sv;
  ObReplayStatus *rp_st = NULL;
  rp_sv.init(get_cluster()[0]->get_palf_env(), &ls_adapter, get_cluster()[0]->get_palf_env()->palf_env_impl_.log_alloc_mgr_);
  rp_sv.start();
  get_cluster()[0]->tenant_base_->update_thread_cnt(10);
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id, ObReplicaType::REPLICA_TYPE_FULL));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, basic_log_ts));
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
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, unused_lsn, unused_ts));
  }
  ls_adapter.wait_replay_done(task_count);
  bool is_done = false;
  LSN end_lsn;
  LSN max_lsn;
  EXPECT_EQ(OB_SUCCESS, leader.get_end_lsn(end_lsn));
  EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle()->get_max_lsn(max_lsn));
  EXPECT_EQ(end_lsn.val_, max_lsn.val_);
  while (!is_done) {
    rp_sv.is_replay_done(ls_id, end_lsn, is_done);
  }
  EXPECT_EQ(0, rp_sv.get_pending_task_size());
  EXPECT_EQ(OB_SUCCESS, rp_sv.switch_to_leader(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.switch_to_follower(ls_id, basic_lsn));
  EXPECT_EQ(OB_SUCCESS, rp_sv.disable(ls_id));
  EXPECT_EQ(OB_SUCCESS, rp_sv.enable(ls_id, basic_lsn, basic_log_ts));
  is_done = false;
  ls_adapter.pre_barrier_ts_ = 0;
  while (!is_done) {
    usleep(100);
    rp_sv.is_replay_done(ls_id, end_lsn, is_done);
  }
  EXPECT_EQ(OB_SUCCESS, rp_sv.remove_ls(ls_id));
  rp_sv.stop();
  rp_sv.wait();
  rp_sv.destroy();
  CLOG_LOG(INFO, "test replay finish", K(id));
}
} // unitest
} // oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
