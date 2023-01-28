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
#include "share/scn.h"
#include "env/ob_simple_log_cluster_env.h"
#undef private

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

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleLogReplayFunc, basic_replay)
{
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
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id, ObReplicaType::REPLICA_TYPE_FULL));
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
  LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();;
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
  EXPECT_EQ(OB_SUCCESS, rp_sv.add_ls(ls_id_shadow, ObReplicaType::REPLICA_TYPE_FULL));
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
} // unitest
} // oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
