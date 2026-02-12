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

#ifdef OB_BUILD_SHARED_LOG_SERVICE

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/container/ob_se_array.h"
#include "share/rc/ob_tenant_base.h"
#include "share/config/ob_server_config.h"
#include "logservice/replayservice/ob_log_replay_reporter.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"
#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace unittest
{

class DummyLogServiceRpcProxy : public obrpc::ObLogServiceRpcProxy
{};

class ReplayWatcherTest : public ::testing::Test
{
public:
  ReplayWatcherTest()
    : tenant_id_(1001),
      tenant_base_(tenant_id_),
      self_addr_(ObAddr::IPV4, "127.0.0.1", 2881),
      leader_addr_(ObAddr::IPV4, "127.0.0.2", 2882)
  {}

  void SetUp() override
  {
    int ret = ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id_);
    OB_ASSERT(OB_SUCCESS == ret);
    GCONF.self_addr_ = self_addr_;
    // Set tenant context - tenant_base_ is a member so it stays alive during test
    ObTenantEnv::set_tenant(&tenant_base_);
  }

  void TearDown() override
  {
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id_);
  }

  static share::SCN make_scn(const uint64_t val)
  {
    share::SCN scn;
    EXPECT_EQ(OB_SUCCESS, scn.convert_for_logservice(val));
    return scn;
  }

protected:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;  // Member variable - stays alive during test
  DummyLogServiceRpcProxy rpc_proxy_;
  ObAddr self_addr_;
  ObAddr leader_addr_;
};

TEST_F(ReplayWatcherTest, NotInitAndInitGuards)
{
  ObLogReplayProcessWatcher watcher;
  const share::ObLSID ls_id(1);
  share::SCN scn = make_scn(100);
  palf::LSN lsn(1);
  const int64_t proposal_id = 1;

  EXPECT_EQ(OB_NOT_INIT, watcher.add_ls(ls_id));
  EXPECT_EQ(OB_NOT_INIT, watcher.remove_ls(ls_id));
  EXPECT_EQ(OB_NOT_INIT, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, scn, lsn, leader_addr_, proposal_id, true, true));
  EXPECT_EQ(OB_NOT_INIT, watcher.report_replay_reaching_machine_status());
  EXPECT_EQ(OB_NOT_INIT, watcher.notify_follower_move_out_from_rto_group(ls_id, scn));

  EXPECT_EQ(OB_INVALID_ARGUMENT, watcher.init(nullptr));
  EXPECT_EQ(OB_SUCCESS, watcher.init(&rpc_proxy_));
  EXPECT_EQ(OB_INIT_TWICE, watcher.init(&rpc_proxy_));
}

TEST_F(ReplayWatcherTest, AddRemoveAndUpdateValidation)
{
  ObLogReplayProcessWatcher watcher;
  ASSERT_EQ(OB_SUCCESS, watcher.init(&rpc_proxy_));
  const share::ObLSID ls_id(1);
  palf::LSN lsn(1);
  const int64_t proposal_id = 1;

  EXPECT_EQ(OB_SUCCESS, watcher.add_ls(ls_id));
  EXPECT_EQ(OB_ENTRY_EXIST, watcher.add_ls(ls_id));

  share::SCN invalid_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, invalid_scn, lsn, leader_addr_, proposal_id, true, true));

  const share::ObLSID missing_ls(2);
  share::SCN scn = make_scn(100);
  EXPECT_NE(OB_SUCCESS, watcher.update_ls_replica_replay_reaching_machine(
      missing_ls, scn, lsn, leader_addr_, proposal_id, true, true));

  EXPECT_EQ(OB_SUCCESS, watcher.remove_ls(ls_id));
  EXPECT_EQ(OB_SUCCESS, watcher.remove_ls(ls_id));
}

TEST_F(ReplayWatcherTest, ReportAndStatusSwitching)
{
  ObLogReplayProcessWatcher watcher;
  ASSERT_EQ(OB_SUCCESS, watcher.init(&rpc_proxy_));
  const share::ObLSID ls_id(1);
  palf::LSN lsn(1);
  const int64_t proposal_id = 1;
  share::SCN scn1 = make_scn(100);
  share::SCN scn2 = make_scn(200);

  ASSERT_EQ(OB_SUCCESS, watcher.add_ls(ls_id));

  ASSERT_EQ(OB_SUCCESS, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, scn1, lsn, leader_addr_, proposal_id, true, true));
  ASSERT_EQ(OB_SUCCESS, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, scn2, lsn, leader_addr_, proposal_id, true, true));

  {
    ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor functor;
    {
      ObLogReplayProcessWatcher::RLockGuard guard(watcher.lock_);
      ASSERT_EQ(OB_SUCCESS, watcher.ls_replica_replay_reaching_machine_map_.for_each(functor));
    }
    ASSERT_EQ(1, functor.snapshots_.count());
    EXPECT_EQ(logservice::LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
        functor.snapshots_.at(0).snapshot_.status_);
    EXPECT_EQ(scn1, functor.snapshots_.at(0).snapshot_.last_reach_to_sync_scn_);
    EXPECT_TRUE(functor.snapshots_.at(0).snapshot_.is_active_follower_);
  }

  ASSERT_EQ(OB_SUCCESS, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, scn2, lsn, leader_addr_, proposal_id, true, false));
  {
    ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor functor;
    ObLogReplayProcessWatcher::RLockGuard guard(watcher.lock_);
    ASSERT_EQ(OB_SUCCESS, watcher.ls_replica_replay_reaching_machine_map_.for_each(functor));
    EXPECT_EQ(0, functor.snapshots_.count());
  }

  ASSERT_EQ(OB_SUCCESS, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, scn2, lsn, self_addr_, proposal_id, true, true));
  {
    ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor functor;
    ObLogReplayProcessWatcher::RLockGuard guard(watcher.lock_);
    ASSERT_EQ(OB_SUCCESS, watcher.ls_replica_replay_reaching_machine_map_.for_each(functor));
    EXPECT_EQ(0, functor.snapshots_.count());
  }

  ASSERT_EQ(OB_SUCCESS, watcher.update_ls_replica_replay_reaching_machine(
      ls_id, scn2, lsn, leader_addr_, proposal_id, true, true));
  ASSERT_EQ(OB_SUCCESS, watcher.notify_follower_move_out_from_rto_group(ls_id, scn2));
  {
    ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor functor;
    ObLogReplayProcessWatcher::RLockGuard guard(watcher.lock_);
    ASSERT_EQ(OB_SUCCESS, watcher.ls_replica_replay_reaching_machine_map_.for_each(functor));
    ASSERT_EQ(1, functor.snapshots_.count());
    EXPECT_EQ(logservice::LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING,
        functor.snapshots_.at(0).snapshot_.status_);
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif // OB_BUILD_SHARED_LOG_SERVICE
