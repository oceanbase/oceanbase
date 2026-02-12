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
#include "lib/alloc/ob_malloc_allocator.h"
#define private public
#define protected public
#define UNITTEST
#include "share/rc/ob_tenant_base.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_struct.h"
#include "logservice/replayservice/ob_log_follower_replay_process_cache.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"
#include "logservice/logrpc/ob_log_rpc_req.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::palf;

class DummyLogServiceRpcProxy : public obrpc::ObLogServiceRpcProxy
{
public:
  DummyLogServiceRpcProxy()
    : obrpc::ObLogServiceRpcProxy(nullptr)
  {}
};

class RecordingLogServiceRpcProxy : public obrpc::ObLogServiceRpcProxy
{
public:
  RecordingLogServiceRpcProxy()
    : obrpc::ObLogServiceRpcProxy(nullptr),
      notify_calls_(0),
      last_req_()
  {}

  int notify_follower_move_out_from_rto_group(
      const logservice::LogNotifyFollowerMoveOutFromRTOGroupResp &req,
      AsyncCB<obrpc::OB_LOG_NOTIFY_FOLLOWER_MOVE_OUT_FROM_RTO_GROUP_RESP>* cb = nullptr,
      const obrpc::ObRpcOpts &opts = obrpc::ObRpcOpts())
  {
    UNUSED(cb);
    UNUSED(opts);
    last_req_ = req;
    ++notify_calls_;
    return OB_SUCCESS;
  }

  void reset()
  {
    notify_calls_ = 0;
    last_req_.reset();
  }

  int64_t notify_calls_;
  logservice::LogNotifyFollowerMoveOutFromRTOGroupResp last_req_;
};

class ReplayProcessCacheTest : public ::testing::Test
{
public:
  ReplayProcessCacheTest()
    : tenant_id_(1001),
      created_allocator_(false),
      tenant_base_(tenant_id_),
      self_addr_(ObAddr::IPV4, "127.0.0.1", 2881),
      follower_addr_(ObAddr::IPV4, "127.0.0.2", 2882),
      leader_addr_(ObAddr::IPV4, "127.0.0.3", 2883)
  {}

  void SetUp() override
  {
    bool allocator_existed = ObMallocAllocator::get_instance()->is_tenant_allocator_exist(tenant_id_);
    int ret = ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id_);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    created_allocator_ = !allocator_existed && (OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    auto guard = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
        tenant_id_, common::ObCtxIds::DEFAULT_CTX_ID);
    ASSERT_NE(nullptr, guard.ref_allocator());
    GCONF.self_addr_ = self_addr_;
    GCTX.self_addr_seq_.set_addr(self_addr_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
    ObTenantEnv::set_tenant(&tenant_base_);
  }

  void TearDown() override
  {
    ObTenantEnv::set_tenant(nullptr);
    tenant_base_.destroy();
    recorder_.reset();
    if (created_allocator_) {
      ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id_);
    }
  }

  static share::SCN make_scn(const uint64_t val)
  {
    share::SCN scn;
    EXPECT_EQ(OB_SUCCESS, scn.convert_for_logservice(val));
    return scn;
  }

  static LSReplicaReplayReachingMachine make_machine(
      const share::ObLSID &ls_id,
      const LSReplicaReplayReachingStatus status,
      const share::SCN &sync_scn,
      const bool active,
      const share::SCN &max_scn,
      const palf::LSN &min_lsn,
      const ObAddr &leader,
      const int64_t proposal_id)
  {
    LSReplicaReplayReachingMachine m(ls_id);
    m.status_ = status;
    m.last_reach_to_sync_scn_ = sync_scn;
    m.is_active_follower_ = active;
    m.max_replayed_scn_ = max_scn;
    m.min_unreplayed_lsn_ = min_lsn;
    m.election_leader_ = leader;
    m.proposal_id_ = proposal_id;
    return m;
  }

protected:
  const uint64_t tenant_id_;
  bool created_allocator_;
  ObTenantBase tenant_base_;
  DummyLogServiceRpcProxy dummy_rpc_;
  RecordingLogServiceRpcProxy recorder_;
  ObAddr self_addr_;
  ObAddr follower_addr_;
  ObAddr leader_addr_;
};

TEST_F(ReplayProcessCacheTest, ReplicaCacheBasics)
{
  const share::ObLSID ls_id(1);
  ReplicaReplayProcessCache cache(ls_id);
  bool need_notify = false;
  share::SCN notify_scn;
  const int64_t now_us = 1'000'000;

  auto machine_sync = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(50),
      true,
      make_scn(50),
      palf::LSN(5),
      leader_addr_,
      1);

  EXPECT_EQ(OB_SUCCESS, cache.update_replica_replay_process(
      machine_sync, true, now_us, need_notify, notify_scn));
  EXPECT_FALSE(need_notify);
  EXPECT_FALSE(notify_scn.is_valid());

  bool in_group = false;
  share::SCN max_scn;
  palf::LSN min_lsn;
  EXPECT_EQ(OB_SUCCESS, cache.check_and_update_in_rto_group(
      now_us, 500'000, in_group, max_scn, min_lsn));
  EXPECT_TRUE(in_group);
  EXPECT_EQ(machine_sync.max_replayed_scn_, max_scn);
  EXPECT_EQ(machine_sync.min_unreplayed_lsn_, min_lsn);

  auto stale_msg = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(20),
      true,
      make_scn(20),
      palf::LSN(2),
      leader_addr_,
      1);
  EXPECT_EQ(OB_DISCARD_PACKET, cache.update_replica_replay_process(
      stale_msg, true, now_us + 10, need_notify, notify_scn));

  EXPECT_FALSE(cache.is_stale(now_us + 100, 200'000));
  EXPECT_TRUE(cache.is_stale(now_us + 600'000, 200'000));

  in_group = true;
  max_scn.reset();
  min_lsn = palf::LSN();
  EXPECT_EQ(OB_SUCCESS, cache.check_and_update_in_rto_group(
      now_us + 600'000, 200'000, in_group, max_scn, min_lsn));
  EXPECT_FALSE(in_group);
}


TEST_F(ReplayProcessCacheTest, LsCacheUpdateAndFetch)
{
  const share::ObLSID ls_id(1);
  LSReplayProcessCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init(ls_id, &dummy_rpc_));

  const int64_t now_us = 2'000'000;
  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(100),
      true,
      make_scn(100),
      palf::LSN(100),
      leader_addr_,
      1);

  ASSERT_EQ(OB_SUCCESS, cache.update_replica_replay_process(
      follower_addr_, machine, true, now_us));

  share::SCN max_scn;
  palf::LSN min_lsn;
  int64_t publish_us = 0;
  ASSERT_EQ(OB_SUCCESS, cache.get_faster_replica_max_replayed_point(
      max_scn, min_lsn, publish_us));
  EXPECT_EQ(machine.max_replayed_scn_, max_scn);
  EXPECT_EQ(machine.min_unreplayed_lsn_, min_lsn);
  EXPECT_EQ(now_us, publish_us);

  ASSERT_EQ(OB_SUCCESS, cache.check_and_update_rto_group_validity(now_us + 100));
  ASSERT_EQ(OB_SUCCESS, cache.get_faster_replica_max_replayed_point(
      max_scn, min_lsn, publish_us));
  EXPECT_EQ(machine.max_replayed_scn_, max_scn);
  EXPECT_EQ(machine.min_unreplayed_lsn_, min_lsn);
}

// Test SYNCING -> REACHING transition (replica is lost / migrated)
TEST_F(ReplayProcessCacheTest, ReplicaCacheSyncingToReachingTransition)
{
  const share::ObLSID ls_id(1);
  ReplicaReplayProcessCache cache(ls_id);

  const int64_t now_us = 1'000'000;

  // First promote replica to SYNCING
  auto syncing_machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(100),
      true,
      make_scn(100),
      palf::LSN(100),
      leader_addr_,
      1);

  bool need_notify = false;
  share::SCN notify_scn;
  ASSERT_EQ(OB_SUCCESS, cache.update_replica_replay_process(
      syncing_machine, true, now_us, need_notify, notify_scn));
  EXPECT_FALSE(need_notify);
  EXPECT_EQ(LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING, cache.machine_.status_);

  // Now a REACHING report arrives with higher max_replayed_scn (replica was migrated/lost)
  auto reaching_machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING,
      make_scn(0),  // lower sync scn
      true,
      make_scn(200), // must be >= current max_replayed_scn
      palf::LSN(200),
      leader_addr_,
      1);

  const int64_t later_us = now_us + 500'000;
  ASSERT_EQ(OB_SUCCESS, cache.update_replica_replay_process(
      reaching_machine, true, later_us, need_notify, notify_scn));
  // Status should flip back to REACHING
  EXPECT_EQ(LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING, cache.machine_.status_);
  EXPECT_FALSE(need_notify);
  EXPECT_EQ(later_us, cache.last_update_time_us_);
}

TEST_F(ReplayProcessCacheTest, LsCacheEvictsStaleReplica)
{
  const share::ObLSID ls_id(2);
  LSReplayProcessCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init(ls_id, &dummy_rpc_));

  const int64_t now_us = 5'000'000;
  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(200),
      true,
      make_scn(200),
      palf::LSN(200),
      leader_addr_,
      1);

  ASSERT_EQ(OB_SUCCESS, cache.update_replica_replay_process(
      follower_addr_, machine, true, now_us));

  const int64_t stale_now = now_us + 3'000'000; // greater than stale interval
  ASSERT_EQ(OB_SUCCESS, cache.check_and_update_rto_group_validity(stale_now));

  share::SCN max_scn;
  palf::LSN min_lsn;
  int64_t publish_us = 0;
  ASSERT_EQ(OB_SUCCESS, cache.get_faster_replica_max_replayed_point(
      max_scn, min_lsn, publish_us));
  EXPECT_TRUE(max_scn.is_max());
  EXPECT_EQ(palf::LSN(palf::LOG_MAX_LSN_VAL), min_lsn);
  EXPECT_EQ(0, publish_us);
}

TEST_F(ReplayProcessCacheTest, LsCacheTriggersNotifyRpc)
{
  const share::ObLSID ls_id(3);
  RecordingLogServiceRpcProxy recorder;
  obrpc::ObLogServiceRpcProxy proxy(&recorder);
  LSReplayProcessCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init(ls_id, &proxy));

  const int64_t now_us = 8'000'000;
  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(300),
      true,
      make_scn(300),
      palf::LSN(300),
      leader_addr_,
      1);

  ASSERT_EQ(OB_SUCCESS, cache.update_replica_replay_process(
      follower_addr_, machine, false, now_us));
  EXPECT_EQ(1, recorder.notify_calls_);
  EXPECT_EQ(ls_id, recorder.last_req_.ls_id_);
  EXPECT_EQ(machine.last_reach_to_sync_scn_, recorder.last_req_.replica_last_reach_to_sync_scn_);

  share::SCN max_scn;
  palf::LSN min_lsn;
  int64_t publish_us = 0;
  ASSERT_EQ(OB_SUCCESS, cache.get_faster_replica_max_replayed_point(
      max_scn, min_lsn, publish_us));
  EXPECT_TRUE(max_scn.is_max());
  EXPECT_EQ(palf::LSN(palf::LOG_MAX_LSN_VAL), min_lsn);
}

// Large test simulating complex leader/follower RTO group membership changes
// Time sequence:
// Phase 1: A is leader, B and C are followers
//   1. B, C join RTO group
//   2. B out
//   3. B in, C out
//   4. B out
//   5. B, C in
// Phase 2: B becomes leader, A and C are followers
//   6. A, C in RTO group
//   7. C out
//   8. C in, A out
//   9. C out
//   10. A, C in
TEST_F(ReplayProcessCacheTest, ComplexLeaderFollowerRTOGroupMembershipChanges)
{
  const share::ObLSID ls_id(100);
  const ObAddr addr_A(ObAddr::IPV4, "10.0.0.1", 2881);
  const ObAddr addr_B(ObAddr::IPV4, "10.0.0.2", 2881);
  const ObAddr addr_C(ObAddr::IPV4, "10.0.0.3", 2881);

  // Helper to check RTO group validity and members
  auto verify_rto_group = [](LSReplayProcessCache &cache, bool expect_valid,
                             const share::SCN &expect_max_scn = share::SCN(),
                             const palf::LSN &expect_min_lsn = palf::LSN()) {
    share::SCN max_scn;
    palf::LSN min_lsn;
    int64_t publish_us = 0;
    ASSERT_EQ(OB_SUCCESS, cache.get_faster_replica_max_replayed_point(max_scn, min_lsn, publish_us));
    if (expect_valid) {
      EXPECT_FALSE(max_scn.is_max()) << "Expected valid RTO group but got invalid";
      if (expect_max_scn.is_valid()) {
        EXPECT_EQ(expect_max_scn, max_scn);
      }
      if (expect_min_lsn.is_valid()) {
        EXPECT_EQ(expect_min_lsn, min_lsn);
      }
    } else {
      EXPECT_TRUE(max_scn.is_max()) << "Expected invalid RTO group but got valid";
      EXPECT_EQ(palf::LSN(palf::LOG_MAX_LSN_VAL), min_lsn);
    }
  };

  // Stale time interval (from implementation: REPLICA_CACHE_STALE_TIME_INTERVAL_US = 2s)
  const int64_t STALE_INTERVAL_US = 3'000'000;  // 3s to ensure staleness

  // ============================================
  // Phase 1: A is leader, B and C are followers
  // ============================================
  {
    LSReplayProcessCache leader_A_cache;
    ASSERT_EQ(OB_SUCCESS, leader_A_cache.init(ls_id, &dummy_rpc_));

    int64_t time_us = 10'000'000;
    uint64_t scn_val = 1000;

    // Step 1: B and C join RTO group
    {
      auto machine_B = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_A, 1);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_cache.update_replica_replay_process(
          addr_B, machine_B, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_A_cache.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      verify_rto_group(leader_A_cache, true, make_scn(scn_val), palf::LSN(scn_val));
    }

    // Step 2: B out (timeout/stale)
    {
      time_us += STALE_INTERVAL_US;
      scn_val += 100;

      // Only C reports, B becomes stale
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_cache.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // After staleness check, only C should be in group
      verify_rto_group(leader_A_cache, true, make_scn(scn_val), palf::LSN(scn_val));
    }

    // Step 3: B in, C out
    {
      time_us += STALE_INTERVAL_US;
      scn_val += 100;

      // Only B reports, C becomes stale
      auto machine_B = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_cache.update_replica_replay_process(
          addr_B, machine_B, true, time_us));

      // Only B in group now
      verify_rto_group(leader_A_cache, true, make_scn(scn_val), palf::LSN(scn_val));
    }

    // Step 4: B out
    {
      time_us += STALE_INTERVAL_US;

      // No one reports, B becomes stale
      ASSERT_EQ(OB_SUCCESS, leader_A_cache.check_and_update_rto_group_validity(time_us));

      // RTO group should be invalid now
      verify_rto_group(leader_A_cache, false);
    }

    // Step 5: B, C in
    {
      time_us += 1'000'000;
      scn_val += 100;

      auto machine_B = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_A, 1);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val + 50), true, make_scn(scn_val + 50), palf::LSN(scn_val + 50), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_cache.update_replica_replay_process(
          addr_B, machine_B, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_A_cache.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Both B and C in group, max is from C
      verify_rto_group(leader_A_cache, true, make_scn(scn_val + 50), palf::LSN(scn_val + 50));
    }
  }

  // ============================================
  // Phase 2: B becomes leader, A and C are followers
  // ============================================
  {
    LSReplayProcessCache leader_B_cache;
    ASSERT_EQ(OB_SUCCESS, leader_B_cache.init(ls_id, &dummy_rpc_));

    int64_t time_us = 50'000'000;
    uint64_t scn_val = 5000;

    // Step 6: A, C join RTO group
    {
      auto machine_A = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_B, 2);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val + 20), true, make_scn(scn_val + 20), palf::LSN(scn_val + 20), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_cache.update_replica_replay_process(
          addr_A, machine_A, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_B_cache.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Both A and C in group, max is from C
      verify_rto_group(leader_B_cache, true, make_scn(scn_val + 20), palf::LSN(scn_val + 20));
    }

    // Step 7: C out
    {
      time_us += STALE_INTERVAL_US;
      scn_val += 100;

      // Only A reports, C becomes stale
      auto machine_A = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_cache.update_replica_replay_process(
          addr_A, machine_A, true, time_us));

      // Only A in group
      verify_rto_group(leader_B_cache, true, make_scn(scn_val), palf::LSN(scn_val));
    }

    // Step 8: C in, A out
    {
      time_us += STALE_INTERVAL_US;
      scn_val += 100;

      // Only C reports, A becomes stale
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_cache.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Only C in group
      verify_rto_group(leader_B_cache, true, make_scn(scn_val), palf::LSN(scn_val));
    }

    // Step 9: C out
    {
      time_us += STALE_INTERVAL_US;

      // No one reports, C becomes stale
      ASSERT_EQ(OB_SUCCESS, leader_B_cache.check_and_update_rto_group_validity(time_us));

      // RTO group should be invalid
      verify_rto_group(leader_B_cache, false);
    }

    // Step 10: A, C in
    {
      time_us += 1'000'000;
      scn_val += 100;

      auto machine_A = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val), true, make_scn(scn_val), palf::LSN(scn_val), addr_B, 2);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          make_scn(scn_val + 30), true, make_scn(scn_val + 30), palf::LSN(scn_val + 30), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_cache.update_replica_replay_process(
          addr_A, machine_A, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_B_cache.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Both A and C in group, max is from C
      verify_rto_group(leader_B_cache, true, make_scn(scn_val + 30), palf::LSN(scn_val + 30));
    }
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif // OB_BUILD_SHARED_LOG_SERVICE
