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
#include "share/scn.h"
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "test_rebuild";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterRebuild : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterRebuild() : ObSimpleLogClusterTestEnv()
  {}
public:
  class TestRebuildCbImpl : public PalfRebuildCb, public share::ObThreadPool
  {
  public:
    TestRebuildCbImpl():
        test_base_(NULL),
        server_idx_(-1),
        rebuild_palf_id_(-1),
        rebuild_lsn_(),
        allow_rebuild_(false),
        is_inited_(false) {}
    virtual ~TestRebuildCbImpl() { destroy(); }
  public:
    int init(TestObSimpleLogClusterRebuild *ptr, const int64_t server_idx)
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

    int start()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(share::ObThreadPool::start())) {
        PALF_LOG(ERROR, "RebuildCB thread failed to start");
      } else {
        PALF_LOG(INFO, "RebuildCB start success", K(ret));
      }
      return ret;
    }

    void destroy()
    {
      is_inited_ = false;
      stop();
      wait();
      server_idx_ = -1;
      rebuild_palf_id_ = -1;
      rebuild_lsn_.reset();
      allow_rebuild_ = false;
      test_base_ = NULL;
    }

    int on_rebuild(const int64_t id, const LSN &lsn)
    {
      int ret = OB_SUCCESS;
      if (!is_inited_) {
        ret = OB_NOT_INIT;
      } else if (id < 0 || !lsn.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(ERROR, "invalid rebuild argument", K(server_idx_), K(id), K(lsn));
      } else {
        rebuild_palf_id_ = id;
        rebuild_lsn_ = lsn;
        SERVER_LOG(INFO, "on_rebuild success", K(server_idx_), K(id), K(lsn));
      }
      return ret;
    }

    void run1()
    {
      share::ObTenantBase tenant_base(OB_SYS_TENANT_ID);
      tenant_base.init();
      ObTenantEnv::set_tenant(&tenant_base);
      lib::set_thread_name("RebuildCB");
      while (!has_set_stop()) {
        if (true == allow_rebuild_ && rebuild_palf_id_ != -1 && rebuild_lsn_.is_valid()) {
          PalfHandleImplGuard leader;
          PalfHandleImplGuard *rebuild_palf;
          int64_t leader_idx;
          PalfBaseInfo rebuild_base_info;
          std::vector<PalfHandleImplGuard*> palf_list;
          EXPECT_EQ(OB_SUCCESS, test_base_->get_cluster_palf_handle_guard(rebuild_palf_id_, palf_list));
          rebuild_palf = palf_list[server_idx_];
          EXPECT_EQ(OB_SUCCESS, test_base_->get_leader(test_base_->palf_id_, leader, leader_idx));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->disable_sync());
          EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(rebuild_lsn_, rebuild_base_info));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->advance_base_info(rebuild_base_info, true));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->enable_sync());
          rebuild_palf = NULL;
          test_base_->revert_cluster_palf_handle_guard(palf_list);
          rebuild_palf_id_ = -1;
          rebuild_lsn_.reset();
        }
        SERVER_LOG(INFO, "RebuildCB", K(server_idx_));
        usleep(500 * 1000);
      }
    }
  public:
    TestObSimpleLogClusterRebuild *test_base_;
    int64_t server_idx_;
    int64_t rebuild_palf_id_;
    LSN rebuild_lsn_;
    bool allow_rebuild_;
    bool is_inited_;
  };
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleLogClusterRebuild, test_old_leader_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "old_leader_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  constexpr int64_t KB = 1024;
  constexpr int64_t MB = 1024 * 1024;
  PALF_LOG(INFO, "start test old_leader_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx1, follower_idx2;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  follower_idx1 = (leader_idx + 1) % 3;
  follower_idx2 = (leader_idx + 2) % 3;
  TestRebuildCbImpl rebuild_cb;
  PalfRebuildCbNode rebuild_node(&rebuild_cb);
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.init(this, leader_idx));
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.start());
  // reigster rebuild cb
  rebuild_server = &leader;
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->register_rebuild_cb(&rebuild_node));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1000, id, 6 * KB));
  sleep(1);
  PALF_LOG(INFO, "begin block net", K(id), K(leader_idx), K(follower_idx1), K(follower_idx2));
  block_net(leader_idx, follower_idx1);
  block_net(leader_idx, follower_idx2);
  submit_log(leader, 100, id, 6 * KB);
  PALF_LOG(INFO, "begin submit logs", K(id), K(leader_idx), K(follower_idx1), K(follower_idx2));
  (void) submit_log(leader, 1000, leader_idx, MB);
  PALF_LOG(INFO, "after sleep 16s, begin get_leader", K(id), K(leader_idx), K(follower_idx1), K(follower_idx2));
  int64_t new_leader_idx = 0;
  unittest::PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  PALF_LOG(INFO, "after get_leader", K(id), K(leader_idx), K(new_leader_idx));
  // submit logs
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 64 * 8, id, MB));

  // update new_leader's disk option, only reserves 4 * 80% log blocks,
  // that means 2 blocks will be recycled
  PALF_LOG(INFO, "begin advance_base_lsn", K(id), K(leader_idx), K(new_leader_idx));
  LSN recycle_lsn(2 * PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->set_base_lsn(recycle_lsn));
  update_disk_options(new_leader_idx, 8);
  // recycle 2 block
  sleep(5);
  block_id_t leader_min_block_id;
  share::SCN min_scn;
  block_id_t min_block_id, max_block_id;
  while (true) {
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_min_block_info_for_gc(leader_min_block_id, min_scn));
    // palf will reserve one more block
    if (1 ==leader_min_block_id) {
      break;
    } else {
      sleep(2);
    }
  }

  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->log_engine_.get_block_id_range(min_block_id, max_block_id));
  PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "runlin trace get_block_id_range", K(min_block_id), K(max_block_id));

  // submit a cond task before unblocking net to stop truncating task
  IOTaskCond cond(id, rebuild_server->palf_env_impl_->last_palf_epoch_);
  LogIOWorker *io_worker = rebuild_server->palf_handle_impl_->log_engine_.log_io_worker_;
  io_worker->submit_io_task(&cond);

  // after unblocking net, old leader will do rebuild
  unblock_net(leader_idx, follower_idx1);
  unblock_net(leader_idx, follower_idx2);
  sleep(5);

  // is truncating, can not rebuild
  if (rebuild_server->palf_handle_impl_->sw_.is_truncating_) {
    PalfBaseInfo rebuild_base_info;
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_base_info(rebuild_cb.rebuild_lsn_, rebuild_base_info));
    EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->disable_sync());
    EXPECT_EQ(OB_EAGAIN, rebuild_server->palf_handle_impl_->advance_base_info(rebuild_base_info, true));
  }
  cond.cond_.signal();
  sleep(5);
  rebuild_cb.allow_rebuild_ = true;

  PalfBaseInfo base_info_in_leader;
  PalfBaseInfo base_info_after_rebuild;
  // check block_id
  block_id_t rebuild_server_min_block_id;
  // get block_id may be executed when all blocks are deleted, so wait for OB_SUCCESS
  while (OB_SUCCESS != rebuild_server->palf_handle_impl_->get_min_block_info_for_gc(rebuild_server_min_block_id, min_scn))
  {
    sleep(1);
  }
  EXPECT_EQ(2, rebuild_server_min_block_id);
  // check base_lsn
  EXPECT_EQ(recycle_lsn, rebuild_server->palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_base_info(recycle_lsn, base_info_in_leader));
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->get_base_info(recycle_lsn, base_info_after_rebuild));
  // check prev_log_info
  EXPECT_EQ(base_info_in_leader.prev_log_info_, base_info_after_rebuild.prev_log_info_);

  LSN leader_end_lsn, rebuild_server_end_lsn;
  leader_end_lsn = new_leader.palf_handle_impl_->get_end_lsn();
  rebuild_server_end_lsn = rebuild_server->palf_handle_impl_->get_end_lsn();
  PALF_LOG(INFO, "rebuild result", K(leader_end_lsn), K(rebuild_server_end_lsn));
  // EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  revert_cluster_palf_handle_guard(palf_list);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->set_base_lsn(LSN(64*6*MB)));
  sleep(1);
  EXPECT_EQ(OB_SUCCESS, update_disk_options(new_leader_idx, 30));
  PALF_LOG(INFO, "end test old_leader_rebuild", K(id));
}

TEST_F(TestObSimpleLogClusterRebuild, test_follower_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "follower_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  constexpr int64_t MB = 1024 * 1024;
  PALF_LOG(INFO, "start test follower_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  follower_idx = (leader_idx + 1) % 3;
  TestRebuildCbImpl rebuild_cb;
  rebuild_cb.allow_rebuild_ = true;
  PalfRebuildCbNode rebuild_node(&rebuild_cb);
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.init(this, follower_idx));
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.start());
  // reigster rebuild cb
  rebuild_server = palf_list[follower_idx];
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->register_rebuild_cb(&rebuild_node));
  // the follower is empty
  block_net(leader_idx, follower_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 64, leader_idx, MB));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 64 * 7, leader_idx, MB));
  // recycle one block
  LSN recycle_lsn(2 * PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(recycle_lsn));
  update_disk_options(leader_idx, 8);
  sleep(1);
  block_id_t leader_min_block_id, follower_min_block_id;
  share::SCN min_scn;
  while (true) {
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_min_block_info_for_gc(leader_min_block_id, min_scn));
    if (1 == leader_min_block_id) {
      break;
    } else {
      sleep(2);
    }
  }
  EXPECT_EQ(recycle_lsn, leader.palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);

  // follower_idx will trigger rebuild when fetching log
  unblock_net(leader_idx, follower_idx);
  sleep(10);
  PalfBaseInfo base_info_in_leader;
  PalfBaseInfo base_info_after_rebuild;
  // check block_id
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->get_min_block_info_for_gc(follower_min_block_id, min_scn));
  EXPECT_EQ(2, follower_min_block_id);
  // check base_lsn
  EXPECT_EQ(recycle_lsn, rebuild_server->palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(recycle_lsn, base_info_in_leader));
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->get_base_info(recycle_lsn, base_info_after_rebuild));
  // check prev_log_info
  EXPECT_EQ(base_info_in_leader.prev_log_info_, base_info_after_rebuild.prev_log_info_);

  LSN leader_end_lsn, rebuild_server_end_lsn;
  leader_end_lsn = leader.palf_handle_impl_->get_end_lsn();
  rebuild_server_end_lsn = rebuild_server->palf_handle_impl_->get_end_lsn();
  PALF_LOG(INFO, "rebuild result", K(leader_end_lsn), K(rebuild_server_end_lsn));
  // EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test follower_rebuild", K(id));
}

TEST_F(TestObSimpleLogClusterRebuild, test_leader_cannot_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_leader_cannot_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test_leader_cannot_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx1, follower_idx2;
  palflite::PalfHandleLiteLeaderChanger leader_changer;
  palf::PalfRoleChangeCbNode rc_cb_node(&leader_changer);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, leader_changer.init(&(leader.palf_handle_impl_->election_)));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->role_change_cb_wrpper_.add_cb_impl(&rc_cb_node));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  LSN base_lsn = LSN(0);
  PalfBaseInfo base_info;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(base_lsn, base_info));

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->disable_sync());
  EXPECT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->advance_base_info(base_info, true));

  AccessMode curr_access_mode;
  int64_t mode_version, proposal_id;
  proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::RAW_WRITE, share::SCN::min_scn()));

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->disable_vote(false));
  while(FOLLOWER != leader.palf_handle_impl_->state_mgr_.role_)
  {
    leader_changer.change_leader();
  }
  unittest::PalfHandleImplGuard new_leader;
  int64_t new_leader_idx = 0;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  leader.reset();
  new_leader.reset();
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  PALF_LOG(INFO, "end test_leader_cannot_rebuild", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
