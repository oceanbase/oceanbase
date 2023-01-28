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
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "test_truncate";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterTruncate : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterTruncate() : ObSimpleLogClusterTestEnv()
  {}
public:
  class TestRebuildCbImpl : public PalfRebuildCb
  {
  public:
    TestRebuildCbImpl(): test_base_(NULL), server_idx_(-1), is_inited_(false) {}
  public:
    int init(TestObSimpleLogClusterTruncate *ptr, const int64_t server_idx)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(ptr) || server_idx < 0 || server_idx >= ptr->get_member_cnt()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(ERROR, "invalid_argument", KP(ptr), K(server_idx));
      } else {
        test_base_ = ptr;
        server_idx_ = server_idx;
        is_inited_ = true;
      }
      return ret;
    }
    int on_rebuild(const int64_t id, const LSN &lsn)
    {
      int ret = OB_SUCCESS;
      if (!is_inited_) {
        ret = OB_NOT_INIT;
      } else {
        PalfHandleImplGuard leader;
        PalfHandleImplGuard *rebuild_palf;
        int64_t leader_idx;
        PalfBaseInfo rebuild_base_info;
        std::vector<PalfHandleImplGuard*> palf_list;
        EXPECT_EQ(OB_SUCCESS, test_base_->get_cluster_palf_handle_guard(id, palf_list));
        rebuild_palf = palf_list[server_idx_];
        EXPECT_EQ(OB_SUCCESS, test_base_->get_leader(test_base_->palf_id_, leader, leader_idx));
        EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->disable_sync());
        EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(lsn, rebuild_base_info));
        EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->advance_base_info(rebuild_base_info, true));
        EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->enable_sync());
        rebuild_palf = NULL;
        test_base_->revert_cluster_palf_handle_guard(palf_list);
      }
      return ret;
    }
  public:
    TestObSimpleLogClusterTruncate *test_base_;
    int64_t server_idx_;
    bool is_inited_;
  };
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 5;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleLogClusterTruncate, truncate_log)
{
  SET_CASE_LOG_FILE(TEST_NAME, "truncate_log");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test truncate_log", K(id));
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  const int64_t follower_1 = (leader_idx + 1) % get_member_cnt();
  const int64_t follower_2 = (leader_idx + 2) % get_member_cnt();
  const int64_t follower_3 = (leader_idx + 3) % get_member_cnt();
  const int64_t follower_4 = (leader_idx + 4) % get_member_cnt();
  PALF_LOG(INFO, "begin block net", K(leader_idx), K(follower_1), K(follower_2), K(follower_3), K(follower_4),
      "member count", get_member_cnt());
  block_net(leader_idx, follower_2);
  block_net(leader_idx, follower_3);
  block_net(follower_2, follower_1);
  block_net(follower_3, follower_1);
  block_net(follower_4, follower_1);
  // leader submit log, sync to follower_1, follower_4
  int64_t wanted_group_log_size = 1 * 1024 * 1024;
  int log_count = 2;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, log_count, leader_idx, wanted_group_log_size));
  sleep(1);
  // drop 50% packet from leader -> follower_1
  set_rpc_loss(leader_idx, follower_1, 50);
  // drop 50% packet from leader -> follower_4
  set_rpc_loss(leader_idx, follower_4, 90);
  // follower_4 -> leader 单向断网, 阻断fetch log
  block_net(follower_4, leader_idx, true);
  // follower_1 -> leader 单向断网, 阻断fetch log
  block_net(follower_1, leader_idx, true);

  PALF_LOG(INFO, "begin submit_log", K(leader_idx));
  // 保证只有leader副本拥有日志
  wanted_group_log_size = 1 * 1024 * 1024;
  log_count = 16;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, log_count, leader_idx, wanted_group_log_size));
  PALF_LOG(INFO, "after submit_log", K(leader_idx));
  EXPECT_EQ(OB_ITER_END, read_log(leader));

  PALF_LOG(INFO, "before sleep 15s", K(leader_idx));
  // election 自动切主
  sleep(15);
  int64_t new_leader_idx = 0;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  unblock_net(follower_2, follower_1);
  unblock_net(follower_3, follower_1);
  unblock_net(follower_4, follower_1);
  unblock_net(leader_idx, follower_2);
  unblock_net(leader_idx, follower_3);
  unblock_net(leader_idx, follower_4);
  // new leader is not old leader
  EXPECT_NE(new_leader_idx, leader_idx);
  // new leader is not follower_1
  EXPECT_NE(new_leader_idx, follower_1);
  PALF_LOG(INFO, "begin submit_log", K(new_leader_idx), K(follower_1));
  log_count = 100;
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, log_count, new_leader_idx));
  EXPECT_EQ(OB_ITER_END, read_log(new_leader));
  sleep(10);
  int64_t wait_sync_timeout_us = 20 * 1000 * 1000;
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  PALF_LOG(INFO, "begin check_replica_sync", K(new_leader_idx), K(follower_1));

  EXPECT_EQ(OB_SUCCESS, check_replica_sync(id, palf_list[new_leader_idx], palf_list[follower_1], wait_sync_timeout_us));
  sleep(3);
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test truncate_log", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
