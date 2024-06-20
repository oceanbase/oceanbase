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
#include "lib/allocator/ob_qsync.h"
#include "logservice/ob_ls_adapter.h"
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "apply_func";
using namespace oceanbase::common;
using namespace oceanbase;

namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class MockLSAdapter : public ObLSAdapter
{
public:
  MockLSAdapter() {
    ObLSAdapter();
    success_count_ = 0;
    failure_count_ = 0;
  }

  int wait_append_sync(const share::ObLSID &ls_id)
  {
    WaitQuiescent(ls_qs_);
    CLOG_LOG(INFO, "WaitQuiescent", K(ls_id));
    return OB_SUCCESS;
  }
  void critical_guard()
  {
    CriticalGuard(ls_qs_);
  }
  void inc_success_count()
  {
    ATOMIC_INC(&success_count_);
  }
  void inc_failure_count()
  {
    ATOMIC_INC(&failure_count_);
  }
  int replay(ObLogReplayTask *replay_task){
    UNUSED(replay_task);
    return OB_SUCCESS;
  }
  int64_t success_count_;
  int64_t failure_count_;
  common::ObQSync ls_qs_;
};

class TestObSimpleLogApplyFunc : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogApplyFunc() :  ObSimpleLogClusterTestEnv()
  {}
};

class MockAppendCb : public AppendCb
{
public:
  int on_success()
  {
    ls_adapter_->inc_success_count();
    CLOG_LOG(INFO, "on_success", K(log_id_));
    return OB_SUCCESS;
  }

  int on_failure()
  {
    ls_adapter_->inc_failure_count();
    CLOG_LOG(INFO, "on_failure", K(log_id_));
    return OB_SUCCESS;
  }

  void init(const int64_t log_id,
            MockLSAdapter *ls_adapter)
  {
    log_id_ = log_id;
    ls_adapter_ = ls_adapter;
  }
  int64_t log_id_;
  MockLSAdapter *ls_adapter_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleLogApplyFunc, apply)
{
  const int64_t task_count = 50;
  const int64_t truncate_count = 25;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  MockAppendCb *cb_array[task_count];
  LSN unused_apply_end_lsn;
  bool is_apply_done = false;
  CLOG_LOG(INFO, "test apply begin", K(id));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogApplyService ap_sv;
  for (int i = 0; i < task_count; i++)
  {
    cb_array[i] = new MockAppendCb();
    cb_array[i]->init(i + 1, &ls_adapter);
  }
  PalfEnv *palf_env;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, ap_sv.init(palf_env, &ls_adapter));
  EXPECT_EQ(OB_SUCCESS, ap_sv.start());
  EXPECT_EQ(OB_SUCCESS, ap_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_leader(ls_id, 1));
  const int64_t idx_1 = (leader_idx + 1) % get_node_cnt();
  const int64_t idx_2 = (leader_idx + 2) % get_node_cnt();

  //旧主少数派写日志
  block_net(leader_idx, idx_1);
  block_net(leader_idx, idx_2);
  do {
    ObApplyStatus *apply_status = NULL;
    ObApplyStatusGuard guard;
    ap_sv.get_apply_status(ls_id, guard);
    apply_status = guard.get_apply_status();
    LSN lsn;
    share::SCN scn;
    int ret = OB_SUCCESS;
    for (int i = 0; i < truncate_count; i++)
    {
      {
        ls_adapter.critical_guard();
        CLOG_LOG(INFO, "submit log start", K(i));
        share::SCN ref_scn;
        ref_scn.convert_for_logservice(i);
        ret = submit_log(leader, ref_scn, lsn, scn);
        if (OB_SUCC(ret)) {
          EXPECT_EQ(true, lsn.is_valid());
          cb_array[i]->__set_lsn(lsn);
          cb_array[i]->__set_scn(scn);
          EXPECT_EQ(OB_SUCCESS, apply_status->push_append_cb(cb_array[i]));
        } else {
          break;
        }
      }
      CLOG_LOG(INFO, "submit log finish", K(i), K(lsn), K(scn));
    }
  } while (0);
  share::SCN min_scn;
  EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, min_scn));
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_follower(ls_id));

  //切主, truncate旧主日志,预期所有cb都调用on_failure
  sleep(15);
  while (!is_apply_done)
  {
    ap_sv.is_apply_done(ls_id, is_apply_done, unused_apply_end_lsn);
    usleep(100);
  }

  //切回旧主写日志,预期所有cb都调用on_success
  unblock_net(leader_idx, idx_1);
  unblock_net(leader_idx, idx_2);
  int64_t new_leader_idx = 0;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  EXPECT_NE(new_leader_idx, leader_idx);
  //等待membership同步
  sleep(2);
  leader.reset();
  CLOG_LOG(INFO, "new leader", K(new_leader_idx), K(leader_idx));
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, leader));
  leader.reset();
  sleep(2);
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_leader(ls_id, 3));
  EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, min_scn));
  EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, new_leader_idx));
  CLOG_LOG(INFO, "check switch leader", K(new_leader_idx), K(leader_idx));
  EXPECT_EQ(new_leader_idx, leader_idx);
  share::SCN palf_end_scn;
  do {
    ObApplyStatus *apply_status = NULL;
    ObApplyStatusGuard guard;
    ap_sv.get_apply_status(ls_id, guard);
    apply_status = guard.get_apply_status();
    LSN lsn;
    share::SCN scn;
    for (int i = truncate_count; i < task_count; i++)
    {
      EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, scn));
      {
        int64_t ref_ts = i - truncate_count;
        share::SCN ref_scn;
        ref_scn.convert_for_logservice(ref_ts);
        ls_adapter.critical_guard();
        CLOG_LOG(INFO, "submit log start", K(i));
        EXPECT_EQ(OB_SUCCESS, submit_log(leader, ref_scn, lsn, scn));
        EXPECT_EQ(true, lsn.is_valid());
        cb_array[i]->__set_lsn(lsn);
        cb_array[i]->__set_scn(scn);
        palf_end_scn = scn;
        EXPECT_EQ(OB_SUCCESS, apply_status->push_append_cb(cb_array[i]));
      }
      CLOG_LOG(INFO, "submit log finish", K(i), K(lsn), K(scn));
      EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, scn));
    }
  } while (0);
  CLOG_LOG(INFO, "truncate write finish", K(id));

  // remove
  is_apply_done = false;
  while (!is_apply_done)
  {
    ap_sv.is_apply_done(ls_id, is_apply_done, unused_apply_end_lsn);
    usleep(100);
  }
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_follower(ls_id));
  EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, scn));
  EXPECT_EQ(scn.get_val_for_logservice(), palf_end_scn.get_val_for_logservice());
  EXPECT_EQ(truncate_count, ls_adapter.failure_count_);
  EXPECT_EQ(task_count- truncate_count, ls_adapter.success_count_);
  for (int i = 0; i < task_count; i++)
  {
    delete cb_array[i];
  }
  EXPECT_EQ(OB_SUCCESS, ap_sv.remove_ls(ls_id));
  ap_sv.stop();
  ap_sv.wait();
  ap_sv.destroy();
  CLOG_LOG(INFO, "test apply finish", K(id));
}

TEST_F(TestObSimpleLogApplyFunc, get_max_decided_scn)
{
  const int64_t task_count = 10;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  MockAppendCb *cb_array[task_count];
  CLOG_LOG(INFO, "test get_max_decided_scn begin", K(id));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogApplyService ap_sv;
  //submit log first
  for (int i = 0; i < task_count; i++)
  {
    cb_array[i] = new MockAppendCb();
    cb_array[i]->init(i + 1, &ls_adapter);
  }
  PalfEnv *palf_env;
  ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, ap_sv.init(palf_env, &ls_adapter));
  EXPECT_EQ(OB_SUCCESS, ap_sv.start());
  EXPECT_EQ(OB_SUCCESS, ap_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_leader(ls_id, 1));
  const int64_t idx_1 = (leader_idx + 1) % get_node_cnt();
  const int64_t idx_2 = (leader_idx + 2) % get_node_cnt();
  share::SCN ref_scn;
  int i = 0;
  LSN lsn;
  share::SCN scn;

  do {
    ObApplyStatus *apply_status = NULL;
    ObApplyStatusGuard guard;
    ap_sv.get_apply_status(ls_id, guard);
    apply_status = guard.get_apply_status();
    ASSERT_TRUE(NULL != apply_status);
    int ret = OB_SUCCESS;
    for (i = 0; i < task_count; i++)
    {
      {
        ls_adapter.critical_guard();
        CLOG_LOG(INFO, "submit log start", K(i));
        ref_scn.convert_for_logservice(i);
        ret = submit_log(leader, ref_scn, lsn, scn);
        if (OB_SUCC(ret)) {
          EXPECT_EQ(true, lsn.is_valid());
          cb_array[i]->__set_lsn(lsn);
          cb_array[i]->__set_scn(scn);
          EXPECT_EQ(OB_SUCCESS, apply_status->push_append_cb(cb_array[i]));
        } else {
          break;
        }
      }
      CLOG_LOG(INFO, "submit log finish", K(i), K(lsn), K(scn));
    }
  } while (0);

  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  SCN max_scn = leader.palf_handle_impl_->sw_.get_max_scn();

  //wait apply done
  wait_lsn_until_slide(max_lsn, leader);
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_follower(ls_id));
  LSN apply_end_lsn;
  bool is_apply_done = false;
  while (!is_apply_done)
  {
    ap_sv.is_apply_done(ls_id, is_apply_done, apply_end_lsn);
    usleep(100);
  }
  EXPECT_EQ(max_lsn, apply_end_lsn);

  //switch leader and submit  log
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, idx_1, new_leader));

  PalfHandleImplGuard new_get_leader;
  int64_t new_leader_idx = 0;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_get_leader, new_leader_idx));
  EXPECT_NE(new_leader_idx, leader_idx);

  int ret = OB_SUCCESS;
  ++i;
  ref_scn.convert_for_logservice(i);
  ret = submit_log(new_leader, ref_scn, lsn, scn);
  EXPECT_EQ(OB_SUCCESS, ret);
  ++i;
  ref_scn.convert_for_logservice(i);
  ret = submit_log(new_leader, ref_scn, lsn, scn);
  EXPECT_EQ(OB_SUCCESS, ret);

  LSN end_lsn;
  do {
    end_lsn = leader.palf_handle_impl_->get_end_lsn();
    if (end_lsn <= max_lsn) {
      CLOG_LOG(INFO, "wait end_lsn", K(end_lsn), K(max_lsn));
      ret = submit_log(new_leader, 1, id, 100);
      wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->get_max_lsn());
      sleep (1);
    }
  } while (end_lsn <= max_lsn);
  share::SCN max_decided_scn;
  EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, max_decided_scn));
  CLOG_LOG(INFO, "check here", K(max_scn), K(max_decided_scn), K(end_lsn), K(max_lsn), K(apply_end_lsn));
  EXPECT_EQ(max_decided_scn, max_scn);

  for (int i = 0; i < task_count; i++)
  {
    delete cb_array[i];
  }

  ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  EXPECT_EQ(OB_SUCCESS, ap_sv.remove_ls(ls_id));
  ap_sv.stop();
  ap_sv.wait();
  ap_sv.destroy();
  CLOG_LOG(INFO, "test get_max_decided_scn finish", K(id));
}

TEST_F(TestObSimpleLogApplyFunc, get_max_decided_scn_no_more_log_after_switch_to_follower)
{
  const int64_t task_count = 10;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ObLSID ls_id(id);
  int64_t leader_idx = 0;
  LSN basic_lsn(0);
  PalfHandleImplGuard leader;
  MockAppendCb *cb_array[task_count];
  CLOG_LOG(INFO, "test get_max_decided_scn_no_more_log_after_switch_to_follower begin", K(id));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  MockLSAdapter ls_adapter;
  ls_adapter.init((ObLSService *)(0x1));
  ObLogApplyService ap_sv;
  //submit log first
  for (int i = 0; i < task_count; i++)
  {
    cb_array[i] = new MockAppendCb();
    cb_array[i]->init(i + 1, &ls_adapter);
  }
  PalfEnv *palf_env;
  ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, ap_sv.init(palf_env, &ls_adapter));
  EXPECT_EQ(OB_SUCCESS, ap_sv.start());
  EXPECT_EQ(OB_SUCCESS, ap_sv.add_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_leader(ls_id, 1));
  const int64_t idx_1 = (leader_idx + 1) % get_node_cnt();
  const int64_t idx_2 = (leader_idx + 2) % get_node_cnt();
  share::SCN ref_scn;
  int i = 0;
  LSN lsn;
  share::SCN scn;

  do {
    ObApplyStatus *apply_status = NULL;
    ObApplyStatusGuard guard;
    ap_sv.get_apply_status(ls_id, guard);
    apply_status = guard.get_apply_status();
    ASSERT_TRUE(NULL != apply_status);
    int ret = OB_SUCCESS;
    for (i = 0; i < task_count; i++)
    {
      {
        ls_adapter.critical_guard();
        CLOG_LOG(INFO, "submit log start", K(i));
        ref_scn.convert_for_logservice(i);
        ret = submit_log(leader, ref_scn, lsn, scn);
        if (OB_SUCC(ret)) {
          EXPECT_EQ(true, lsn.is_valid());
          cb_array[i]->__set_lsn(lsn);
          cb_array[i]->__set_scn(scn);
          EXPECT_EQ(OB_SUCCESS, apply_status->push_append_cb(cb_array[i]));
        } else {
          break;
        }
      }
      CLOG_LOG(INFO, "submit log finish", K(i), K(lsn), K(scn));
    }
  } while (0);

  LSN max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
  SCN max_scn = leader.palf_handle_impl_->sw_.get_max_scn();

  //wait apply done
  wait_lsn_until_slide(max_lsn, leader);
  EXPECT_EQ(OB_SUCCESS, ap_sv.switch_to_follower(ls_id));
  LSN apply_end_lsn;
  bool is_apply_done = false;
  while (!is_apply_done)
  {
    ap_sv.is_apply_done(ls_id, is_apply_done, apply_end_lsn);
    usleep(100);
  }
  EXPECT_EQ(max_lsn, apply_end_lsn);

  share::SCN max_decided_scn;
  EXPECT_EQ(OB_SUCCESS, ap_sv.get_max_applied_scn(ls_id, max_decided_scn));
  CLOG_LOG(INFO, "check here", K(max_scn), K(max_decided_scn), K(max_lsn), K(apply_end_lsn));
  EXPECT_EQ(max_decided_scn, max_scn);

  ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  EXPECT_EQ(OB_SUCCESS, ap_sv.remove_ls(ls_id));
  ap_sv.stop();
  ap_sv.wait();
  ap_sv.destroy();
  CLOG_LOG(INFO, "test get_max_decided_scn_with_no_more_log finish", K(id));
}

} // unitest
} // oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
