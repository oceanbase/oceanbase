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
#include "share/rc/ob_tenant_base.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_struct.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "logservice/ipalf/ipalf_env.h"
#include "logservice/ob_log_submit_log_rate_limiter.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"
#include "logservice/logrpc/ob_log_rpc_req.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::palf;
using namespace oceanbase::ipalf;

namespace oceanbase
{
namespace unittest
{

// =====================================================================
// Mock IPalfHandle - Implements IPalfHandle with controllable SCN/LSN
// =====================================================================
class MockIPalfHandle : public ipalf::IPalfHandle
{
public:
  MockIPalfHandle()
    : is_valid_(true),
      end_scn_(),
      end_lsn_(),
      palf_id_(1)
  {
    end_scn_.set_min();
    end_lsn_.reset();
  }
  virtual ~MockIPalfHandle() {}

  void set_end_scn(const share::SCN &scn) { end_scn_ = scn; }
  void set_end_lsn(const palf::LSN &lsn) { end_lsn_ = lsn; }
  void set_valid(bool valid) { is_valid_ = valid; }

  virtual bool is_valid() const override { return is_valid_; }
  virtual bool operator==(const IPalfHandle &rhs) const override { return false; }

  virtual int append(const PalfAppendOptions &opts,
                     const void *buffer,
                     const int64_t nbytes,
                     const share::SCN &ref_scn,
                     palf::LSN &lsn,
                     share::SCN &scn) override
  { return OB_NOT_SUPPORTED; }

  virtual int raw_write(const PalfAppendOptions &opts,
                        const palf::LSN &lsn,
                        const void *buffer,
                        const int64_t nbytes) override
  { return OB_NOT_SUPPORTED; }

  virtual int raw_read(const palf::LSN &lsn,
                       void *buffer,
                       const int64_t nbytes,
                       int64_t &read_size,
                       palf::LogIOContext &io_ctx) override
  { return OB_NOT_SUPPORTED; }

  virtual int seek(const palf::LSN &lsn, palf::PalfBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const palf::LSN &lsn, palf::PalfGroupBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const palf::LSN &lsn, ipalf::IPalfIterator<ILogEntry> &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const palf::LSN &lsn, ipalf::IPalfIterator<IGroupEntry> &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, palf::PalfGroupBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, palf::PalfBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, ipalf::IPalfIterator<ILogEntry> &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, ipalf::IPalfIterator<IGroupEntry> &iter) override
  { return OB_NOT_SUPPORTED; }

  virtual int locate_by_scn_coarsely(const share::SCN &scn, palf::LSN &result_lsn) override
  { return OB_NOT_SUPPORTED; }
  virtual int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &result_scn) override
  { return OB_NOT_SUPPORTED; }

  virtual int enable_sync() override { return OB_SUCCESS; }
  virtual int disable_sync() override { return OB_SUCCESS; }
  virtual bool is_sync_enabled() const override { return true; }
  virtual int advance_base_lsn(const palf::LSN &lsn) override { return OB_SUCCESS; }
  virtual int flashback(const int64_t mode_version, const share::SCN &flashback_scn,
                        const int64_t timeout_us) override
  { return OB_NOT_SUPPORTED; }

  virtual int get_begin_lsn(palf::LSN &lsn) const override
  { lsn.reset(); return OB_SUCCESS; }
  virtual int get_begin_scn(share::SCN &scn) const override
  { scn.set_min(); return OB_SUCCESS; }
  virtual int get_base_lsn(palf::LSN &lsn) const override
  { lsn.reset(); return OB_SUCCESS; }
  virtual int get_base_info(const palf::LSN &lsn, palf::PalfBaseInfo &palf_base_info) override
  { return OB_NOT_SUPPORTED; }

  virtual int get_end_lsn(palf::LSN &lsn) const override
  {
    lsn = end_lsn_;
    return OB_SUCCESS;
  }
  virtual int get_end_scn(share::SCN &scn) const override
  {
    scn = end_scn_;
    return OB_SUCCESS;
  }
  virtual int get_max_lsn(palf::LSN &lsn) const override
  { lsn = end_lsn_; return OB_SUCCESS; }
  virtual int get_max_scn(share::SCN &scn) const override
  { scn = end_scn_; return OB_SUCCESS; }

  virtual int get_role(common::ObRole &role, int64_t &proposal_id,
                       bool &is_pending_state) const override
  {
    role = common::LEADER;
    proposal_id = 1;
    is_pending_state = false;
    return OB_SUCCESS;
  }
  virtual int get_palf_id(int64_t &palf_id) const override
  { palf_id = palf_id_; return OB_SUCCESS; }
  virtual int get_palf_epoch(int64_t &palf_epoch) const override
  { palf_epoch = 1; return OB_SUCCESS; }
  virtual int get_election_leader(common::ObAddr &addr) const override
  { return OB_NOT_SUPPORTED; }
  virtual int advance_election_epoch_and_downgrade_priority(
      const int64_t proposal_id,
      const int64_t downgrade_priority_time_us,
      const char *reason) override
  { return OB_NOT_SUPPORTED; }
  virtual int change_leader_to(const common::ObAddr &dst_addr) override
  { return OB_NOT_SUPPORTED; }
  virtual int change_access_mode(const int64_t proposal_id,
                                 const int64_t mode_version,
                                 const ipalf::AccessMode &access_mode,
                                 const share::SCN &ref_scn) override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode(int64_t &mode_version, ipalf::AccessMode &access_mode) const override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode(ipalf::AccessMode &access_mode) const override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode_version(int64_t &mode_version) const override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode_ref_scn(int64_t &mode_version,
                                      ipalf::AccessMode &access_mode,
                                      share::SCN &ref_scn) const override
  { return OB_NOT_SUPPORTED; }
  virtual int register_file_size_cb(palf::PalfFSCb *fs_cb) override
  { return OB_SUCCESS; }
  virtual int unregister_file_size_cb() override
  { return OB_SUCCESS; }
  virtual int register_role_change_cb(palf::PalfRoleChangeCb *rc_cb) override
  { return OB_SUCCESS; }
  virtual int unregister_role_change_cb() override
  { return OB_SUCCESS; }
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  virtual int register_refresh_priority_cb() override
  { return OB_SUCCESS; }
  virtual int unregister_refresh_priority_cb() override
  { return OB_SUCCESS; }
  virtual int set_allow_election_without_memlist(const bool allow) override
  { return OB_SUCCESS; }
#endif
  virtual int set_election_priority(palf::election::ElectionPriority *priority) override
  { return OB_SUCCESS; }
  virtual int reset_election_priority() override
  { return OB_SUCCESS; }
  virtual int set_location_cache_cb(palf::PalfLocationCacheCb *lc_cb) override
  { return OB_SUCCESS; }
  virtual int reset_location_cache_cb() override
  { return OB_SUCCESS; }
  virtual int set_locality_cb(palf::PalfLocalityInfoCb *locality_cb) override
  { return OB_SUCCESS; }
  virtual int reset_locality_cb() override
  { return OB_SUCCESS; }
  virtual int set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker) override
  { return OB_SUCCESS; }
  virtual int reset_reconfig_checker_cb() override
  { return OB_SUCCESS; }
  virtual int stat(palf::PalfStat &palf_stat) const override
  { return OB_NOT_SUPPORTED; }
  virtual int diagnose(palf::PalfDiagnoseInfo &diagnose_info) const override
  { return OB_NOT_SUPPORTED; }
#ifdef OB_BUILD_ARBITRATION
  virtual int get_arbitration_member(common::ObMember &arb_member) const override
  { return OB_NOT_SUPPORTED; }
#endif

private:
  bool is_valid_;
  share::SCN end_scn_;
  palf::LSN end_lsn_;
  int64_t palf_id_;
};

// =====================================================================
// Mock IPalfEnv - Returns MockIPalfHandle from open()
// =====================================================================
class MockIPalfEnv : public ipalf::IPalfEnv
{
public:
  MockIPalfEnv()
    : mock_handle_(),
      tenant_id_(1001)
  {}
  virtual ~MockIPalfEnv() {}

  MockIPalfHandle &get_mock_handle() { return mock_handle_; }
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }

  virtual bool operator==(const IPalfEnv &rhs) const override { return false; }

  virtual int create(const int64_t id,
                     const AccessMode &access_mode,
                     const palf::PalfBaseInfo &palf_base_info,
                     IPalfHandle *&handle) override
  { return OB_NOT_SUPPORTED; }

  virtual int start() override { return OB_SUCCESS; }

  virtual int load(const int64_t id, IPalfHandle *&handle) override
  { return open(id, handle); }

  virtual int open(const int64_t id, IPalfHandle *&handle) override
  {
    UNUSED(id);
    handle = &mock_handle_;
    return OB_SUCCESS;
  }

  virtual int close(IPalfHandle *&handle) override
  {
    handle = nullptr;
    return OB_SUCCESS;
  }

  virtual int remove(int64_t id) override
  { return OB_NOT_SUPPORTED; }

  virtual int for_each(const ObFunction<int(const IPalfHandle&)> &func) override
  { return OB_NOT_SUPPORTED; }

  virtual int update_replayable_point(const share::SCN &replayable_scn) override
  { return OB_SUCCESS; }

  virtual int advance_base_lsn(int64_t id, palf::LSN base_lsn) override
  { return OB_SUCCESS; }

  virtual int64_t get_tenant_id() override
  { return tenant_id_; }

private:
  MockIPalfHandle mock_handle_;
  int64_t tenant_id_;
};

// =====================================================================
// Dummy RPC Proxy - No-op implementation for basic tests
// =====================================================================
class DummyLogServiceRpcProxy : public obrpc::ObLogServiceRpcProxy
{
public:
  DummyLogServiceRpcProxy()
    : obrpc::ObLogServiceRpcProxy(nullptr)
  {}
};

// =====================================================================
// Recording RPC Proxy - Captures RPC calls for verification
// =====================================================================
class RecordingLogServiceRpcProxy : public obrpc::ObLogServiceRpcProxy
{
public:
  RecordingLogServiceRpcProxy()
    : obrpc::ObLogServiceRpcProxy(nullptr),
      notify_calls_(0),
      last_notify_req_()
  {}

  int notify_follower_move_out_from_rto_group(
      const logservice::LogNotifyFollowerMoveOutFromRTOGroupResp &req,
      AsyncCB<obrpc::OB_LOG_NOTIFY_FOLLOWER_MOVE_OUT_FROM_RTO_GROUP_RESP>* cb = nullptr,
      const obrpc::ObRpcOpts &opts = obrpc::ObRpcOpts())
  {
    UNUSED(cb);
    UNUSED(opts);
    last_notify_req_ = req;
    ++notify_calls_;
    return OB_SUCCESS;
  }

  void reset()
  {
    notify_calls_ = 0;
    last_notify_req_.reset();
  }

  int64_t notify_calls_;
  logservice::LogNotifyFollowerMoveOutFromRTOGroupResp last_notify_req_;
};

// =====================================================================
// Test Fixture
// =====================================================================
class SubmitLogRateLimiterTest : public ::testing::Test
{
public:
  SubmitLogRateLimiterTest()
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
    GCONF.enable_logservice = true;
    GCTX.self_addr_seq_.set_addr(self_addr_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
    ObTenantEnv::set_tenant(&tenant_base_);
    mock_palf_env_.set_tenant_id(tenant_id_);
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
  MockIPalfEnv mock_palf_env_;
  ObAddr self_addr_;
  ObAddr follower_addr_;
  ObAddr leader_addr_;
};

// =====================================================================
// Basic Function Tests (dummy RPC)
// =====================================================================

TEST_F(SubmitLogRateLimiterTest, InitAndDestroy)
{
  ObLogLSSubmitLogRateLimiter limiter;

  // Not initialized yet
  EXPECT_FALSE(limiter.is_inited_);

  // Invalid arguments
  share::ObLSID invalid_ls_id;
  share::ObLSID valid_ls_id(1001);

  EXPECT_EQ(OB_INVALID_ARGUMENT, limiter.init(invalid_ls_id, &dummy_rpc_, &mock_palf_env_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, limiter.init(valid_ls_id, nullptr, &mock_palf_env_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, limiter.init(valid_ls_id, &dummy_rpc_, nullptr));

  // Successful init
  EXPECT_EQ(OB_SUCCESS, limiter.init(valid_ls_id, &dummy_rpc_, &mock_palf_env_));
  EXPECT_TRUE(limiter.is_inited_);
  EXPECT_EQ(valid_ls_id, limiter.ls_id_);

  // Double init
  EXPECT_EQ(OB_INIT_TWICE, limiter.init(valid_ls_id, &dummy_rpc_, &mock_palf_env_));

  // Destroy
  limiter.destroy();
  EXPECT_FALSE(limiter.is_inited_);
  EXPECT_EQ(nullptr, limiter.ipalf_handle_);
  EXPECT_EQ(nullptr, limiter.ipalf_env_);
  EXPECT_EQ(nullptr, limiter.rpc_proxy_);
}

TEST_F(SubmitLogRateLimiterTest, ForceAllowsLogic)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Initial state - not force allows
  EXPECT_FALSE(limiter.is_force_allows());

  // Set force allows
  limiter.set_force_allows();
  // Note: set_force_allows() is currently a no-op in implementation
  // TODO by qingxia: add test when set_force_allows() is implemented

  // Reset force allows
  limiter.reset_force_allows();
  // Note: reset_force_allows() is currently a no-op in implementation
  // TODO by qingxia: add test when reset_force_allows() is implemented
}

TEST_F(SubmitLogRateLimiterTest, TryAcquireNotInit)
{
  ObLogLSSubmitLogRateLimiter limiter;

  // Should return OB_NOT_INIT when not initialized
  EXPECT_EQ(OB_NOT_INIT, limiter.try_acquire(1, 0, 0));
}

TEST_F(SubmitLogRateLimiterTest, TryAcquireWithIgnoreRateLimit)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Set need_ignore_rate_limit_ to bypass rate limiting
  limiter.need_ignore_rate_limit_ = true;

  // Should always succeed when rate limiting is ignored
  EXPECT_EQ(OB_SUCCESS, limiter.try_acquire(1, 100, 0));
}

TEST_F(SubmitLogRateLimiterTest, AcquireNotSupported)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // acquire() is not supported
  EXPECT_EQ(OB_NOT_SUPPORTED, limiter.acquire(1, 0, 0));
}

TEST_F(SubmitLogRateLimiterTest, PassOneLogToKeepAlive)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  const int64_t now_us = 1000000; // 1 second

  // First call should return true (pass one log)
  EXPECT_TRUE(limiter.pass_one_log_to_keep_alive_(now_us));
  EXPECT_EQ(now_us, limiter.last_pass_one_log_to_keep_alive_us_);

  // Immediate second call should return false (within interval)
  EXPECT_FALSE(limiter.pass_one_log_to_keep_alive_(now_us + 1000)); // 1ms later

  // Call after interval should return true
  const int64_t after_interval = now_us + 60000; // 60ms later (> 50ms interval)
  EXPECT_TRUE(limiter.pass_one_log_to_keep_alive_(after_interval));
  EXPECT_EQ(after_interval, limiter.last_pass_one_log_to_keep_alive_us_);
}

// =====================================================================
// RPC Path Tests (recording proxy)
// =====================================================================

TEST_F(SubmitLogRateLimiterTest, UpdateReplicaReplayProcess)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &recorder_, &mock_palf_env_));

  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(100),
      true,
      make_scn(100),
      palf::LSN(100),
      leader_addr_,
      1);

  const int64_t now_us = common::ObTimeUtil::current_monotonic_raw_time();

  // Update replica replay process
  EXPECT_EQ(OB_SUCCESS, limiter.update_replica_replay_process(
      follower_addr_, machine, true));

  // Verify internal cache was updated
  share::SCN max_scn;
  palf::LSN min_lsn;
  int64_t publish_us = 0;
  EXPECT_EQ(OB_SUCCESS, limiter.ls_replay_process_cache_.get_faster_replica_max_replayed_point(
      max_scn, min_lsn, publish_us));

  EXPECT_EQ(machine.max_replayed_scn_, max_scn);
  EXPECT_EQ(machine.min_unreplayed_lsn_, min_lsn);
}

TEST_F(SubmitLogRateLimiterTest, UpdateReplicaReplayProcessNotInit)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(100),
      true,
      make_scn(100),
      palf::LSN(100),
      leader_addr_,
      1);

  // Should return OB_NOT_INIT
  EXPECT_EQ(OB_NOT_INIT, limiter.update_replica_replay_process(
      follower_addr_, machine, true));
}

TEST_F(SubmitLogRateLimiterTest, ReplayProcessCacheNotifyRpc)
{
  share::ObLSID ls_id(1001);

  // Test the need_notify logic directly using ReplicaReplayProcessCache
  // The RPC sending uses builder pattern which doesn't go through the mock,
  // so we test the internal logic that determines when notify should happen
  ReplicaReplayProcessCache replica_cache(ls_id);

  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(300),
      true,
      make_scn(300),
      palf::LSN(300),
      leader_addr_,
      1);

  const int64_t now_us = 8000000;
  bool need_notify = false;
  share::SCN notify_scn;

  // When leader_view_follower_is_in_sync is false and incoming status is SYNCING,
  // while cached status is REACHING (default), should set need_notify to true
  ASSERT_EQ(OB_SUCCESS, replica_cache.update_replica_replay_process(
      machine, false, now_us, need_notify, notify_scn));

  EXPECT_TRUE(need_notify);
  EXPECT_EQ(machine.last_reach_to_sync_scn_, notify_scn);
}

// =====================================================================
// Combined Behavior Tests
// =====================================================================

TEST_F(SubmitLogRateLimiterTest, RateLimitWithReplayProgress)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Set up mock palf handle with known SCN/LSN values
  MockIPalfHandle &mock_handle = mock_palf_env_.get_mock_handle();
  share::SCN leader_scn = make_scn(1000000000); // 1 second in nanoseconds
  palf::LSN leader_lsn(1000);
  mock_handle.set_end_scn(leader_scn);
  mock_handle.set_end_lsn(leader_lsn);

  // Disable force allows
  limiter.is_force_allows_ = false;
  limiter.need_ignore_rate_limit_ = false;

  // Update replica replay process with close SCN (within sync range)
  auto machine = make_machine(
      ls_id,
      LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
      make_scn(1000000000 - 500000000), // 0.5 second behind
      true,
      make_scn(1000000000 - 500000000), // 0.5 second behind
      palf::LSN(900), // close to leader
      leader_addr_,
      1);

  ASSERT_EQ(OB_SUCCESS, limiter.update_replica_replay_process(
      follower_addr_, machine, true));

  // Try acquire - should succeed since follower is close to leader
  // Note: The actual rate limiting logic is complex and depends on timing
  // This test verifies the basic flow works without errors
  int ret = limiter.try_acquire(1, leader_scn.get_val_for_logservice() / 1000, 0);
  // Result depends on internal timing and random factors
  EXPECT_TRUE(ret == OB_SUCCESS || ret == OB_EAGAIN);
}

TEST_F(SubmitLogRateLimiterTest, TickSampleReset)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Verify tick sample initial state
  EXPECT_FALSE(limiter.tick_last_sample_.valid_);
  EXPECT_EQ(0, limiter.tick_last_sample_.rw_lsn_);
  EXPECT_EQ(0, limiter.tick_last_sample_.ro_lsn_);

  // Reset tick sample
  const int64_t reset_time = 5000000;
  limiter.tick_last_sample_.reset(reset_time);

  EXPECT_FALSE(limiter.tick_last_sample_.valid_);
  EXPECT_EQ(reset_time, limiter.tick_last_sample_.rw_time_us_);
  EXPECT_EQ(reset_time, limiter.tick_last_sample_.ro_time_us_);
}

TEST_F(SubmitLogRateLimiterTest, ScnNeedRateLimit)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Set committed_end_scn_val_us_ (in microseconds)
  const int64_t leader_scn_us = 5000000; // 5 seconds
  limiter.committed_end_scn_val_us_ = leader_scn_us;

  // Test case 1: Follower is very close (within BEGIN threshold of 2s)
  // diff < BEGIN_RATE_LIMIT_SCN_DIFF_US (2s) => always OB_SUCCESS
  share::SCN close_scn;
  close_scn.convert_for_logservice((leader_scn_us - 1000000) * 1000); // 1 second behind (ns)
  EXPECT_EQ(OB_SUCCESS, limiter.scn_need_rate_limit_(close_scn));

  // Test case 2: Follower is very far (beyond END threshold of 4s)
  // diff >= END_RATE_LIMIT_SCN_DIFF_US (4s) => always OB_EAGAIN
  share::SCN far_scn;
  far_scn.convert_for_logservice((leader_scn_us - 5000000) * 1000); // 5 seconds behind (ns)
  EXPECT_EQ(OB_EAGAIN, limiter.scn_need_rate_limit_(far_scn));
}

TEST_F(SubmitLogRateLimiterTest, ScnNeedRateLimitProbability)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Constants from implementation:
  // BEGIN_RATE_LIMIT_SCN_DIFF_US = 2_s = 2000000
  // END_RATE_LIMIT_SCN_DIFF_US = 4_s = 4000000
  // Random zone is [BEGIN, END) where probability varies

  const int64_t leader_scn_us = 10000000; // 10 seconds
  limiter.committed_end_scn_val_us_ = leader_scn_us;

  // Test boundary: exactly at BEGIN threshold (2s behind)
  // diff = 0 in random zone => should always succeed (random_val >= 0 is always true)
  share::SCN begin_boundary_scn;
  begin_boundary_scn.convert_for_logservice((leader_scn_us - 2000000) * 1000); // exactly 2s behind
  EXPECT_EQ(OB_SUCCESS, limiter.scn_need_rate_limit_(begin_boundary_scn));

  // Test probability zone: 3s behind (middle of random zone)
  // Run multiple iterations to verify probabilistic behavior
  share::SCN mid_zone_scn;
  mid_zone_scn.convert_for_logservice((leader_scn_us - 3000000) * 1000); // 3s behind

  int success_count = 0;
  int eagain_count = 0;
  const int iterations = 1000;

  for (int i = 0; i < iterations; ++i) {
    int ret = limiter.scn_need_rate_limit_(mid_zone_scn);
    if (ret == OB_SUCCESS) {
      ++success_count;
    } else if (ret == OB_EAGAIN) {
      ++eagain_count;
    }
  }

  // In middle of zone (diff = 1s out of 2s zone), expect ~50% success rate
  // Allow wide margin for randomness: 20%-80%
  EXPECT_GT(success_count, iterations * 0.2);
  EXPECT_LT(success_count, iterations * 0.8);
  EXPECT_EQ(iterations, success_count + eagain_count);

  // Test boundary: just before END threshold
  share::SCN near_end_scn;
  near_end_scn.convert_for_logservice((leader_scn_us - 3999000) * 1000); // 3.999s behind

  success_count = 0;
  eagain_count = 0;
  for (int i = 0; i < iterations; ++i) {
    int ret = limiter.scn_need_rate_limit_(near_end_scn);
    if (ret == OB_SUCCESS) {
      ++success_count;
    } else if (ret == OB_EAGAIN) {
      ++eagain_count;
    }
  }
  // Near end of zone, expect very low success rate (< 10%)
  EXPECT_LT(success_count, iterations * 0.15);
}

TEST_F(SubmitLogRateLimiterTest, LsnNeedRateLimit)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Set commited_end_lsn_val_
  const uint64_t leader_lsn_val = 100 * 1024 * 1024; // 100MB
  limiter.commited_end_lsn_val_ = leader_lsn_val;

  // Test case 1: Follower is close (within MAX_UNREPLAYED_LSN_COMMIT_END_LSN_DIFF = 64MB)
  palf::LSN close_lsn(leader_lsn_val - 1 * 1024 * 1024); // 1MB behind
  EXPECT_EQ(OB_SUCCESS, limiter.lsn_need_rate_limit_(close_lsn));

  // Test case 2: Follower is far (beyond 64MB threshold)
  palf::LSN far_lsn(leader_lsn_val - 70 * 1024 * 1024); // 70MB behind
  EXPECT_EQ(OB_EAGAIN, limiter.lsn_need_rate_limit_(far_lsn));

  // Test boundary: just within 64MB threshold (64MB - 1 byte behind)
  // MAX_UNREPLAYED_LSN_COMMIT_END_LSN_DIFF = 64 * 1024 * 1024 = 67108864
  // The condition is: commited_end_lsn_val < min_unreplayed_lsn_val + 64MB (strictly less than)
  palf::LSN within_boundary_lsn(leader_lsn_val - 64 * 1024 * 1024 + 1); // 64MB - 1 byte behind
  EXPECT_EQ(OB_SUCCESS, limiter.lsn_need_rate_limit_(within_boundary_lsn));

  // Test exactly at boundary (64MB behind) - should fail due to strict less than
  palf::LSN boundary_lsn(leader_lsn_val - 64 * 1024 * 1024); // exactly 64MB behind
  EXPECT_EQ(OB_EAGAIN, limiter.lsn_need_rate_limit_(boundary_lsn));
}

TEST_F(SubmitLogRateLimiterTest, TickNeedRateLimit)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  // Test case 1: Invalid tick sample => always OB_SUCCESS
  limiter.tick_last_sample_.valid_ = false;
  const int64_t now_us = 5000000;
  EXPECT_EQ(OB_SUCCESS, limiter.check_rw_ro_tick_budget_(now_us));

  // Test case 2: Valid tick sample but leader not ahead of follower enough
  // (leader_end_lsn <= ro_lsn + MAX_ALLOW_RO_DISTANCE_BYTES)
  limiter.tick_last_sample_.valid_ = true;
  limiter.tick_last_sample_.rw_lsn_ = 1000000;
  limiter.tick_last_sample_.ro_lsn_ = 900000;  // close to rw
  limiter.tick_last_sample_.rw_time_us_ = now_us - 200000;
  limiter.tick_last_sample_.ro_time_us_ = now_us - 200000;
  limiter.tick_last_sample_.rw_delta_ = 50000;
  limiter.tick_last_sample_.ro_delta_ = 50000;
  limiter.tick_last_sample_.rw_delta_time_us_ = 100000;
  limiter.tick_last_sample_.ro_delta_time_us_ = 100000;
  limiter.commited_end_lsn_val_ = 1000000;  // same as rw_lsn, not ahead

  EXPECT_EQ(OB_SUCCESS, limiter.check_rw_ro_tick_budget_(now_us));

  // Test case 3: Leader is way ahead and follower replay is slow
  // This should trigger rate limiting with some probability
  limiter.tick_last_sample_.valid_ = true;
  limiter.tick_last_sample_.rw_lsn_ = 1000000;
  limiter.tick_last_sample_.ro_lsn_ = 500000;  // 500KB behind
  limiter.tick_last_sample_.rw_time_us_ = now_us - 200000;
  limiter.tick_last_sample_.ro_time_us_ = now_us - 200000;
  limiter.tick_last_sample_.rw_delta_ = 100000;  // leader wrote 100KB
  limiter.tick_last_sample_.ro_delta_ = 10000;   // follower only replayed 10KB
  limiter.tick_last_sample_.rw_delta_time_us_ = 100000;
  limiter.tick_last_sample_.ro_delta_time_us_ = 100000;
  // Leader end LSN is significantly ahead
  // MAX_ALLOW_RO_DISTANCE_BYTES = 512KB, so ro_lsn + 512KB = 1012KB
  // Set leader to 2MB to exceed threshold
  limiter.commited_end_lsn_val_ = 2000000;

  // Run multiple times to test probability
  int success_count = 0;
  int eagain_count = 0;
  const int iterations = 100;

  for (int i = 0; i < iterations; ++i) {
    int ret = limiter.check_rw_ro_tick_budget_(now_us);
    if (ret == OB_SUCCESS) {
      ++success_count;
    } else if (ret == OB_EAGAIN) {
      ++eagain_count;
    }
  }

  // With slow follower, expect some rate limiting
  EXPECT_GT(eagain_count, 0);
  EXPECT_EQ(iterations, success_count + eagain_count);
}

TEST_F(SubmitLogRateLimiterTest, TickNeedRateLimitProbability)
{
  ObLogLSSubmitLogRateLimiter limiter;
  share::ObLSID ls_id(1001);

  ASSERT_EQ(OB_SUCCESS, limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

  const int64_t now_us = 5000000;

  // Set up a scenario where leader is significantly faster than follower
  // This creates high rejection probability
  limiter.tick_last_sample_.valid_ = true;
  limiter.tick_last_sample_.rw_lsn_ = 1000000;
  limiter.tick_last_sample_.ro_lsn_ = 100000;  // very far behind
  limiter.tick_last_sample_.rw_time_us_ = now_us - 200000;
  limiter.tick_last_sample_.ro_time_us_ = now_us - 200000;
  limiter.tick_last_sample_.rw_delta_ = 500000;   // leader wrote 500KB
  limiter.tick_last_sample_.ro_delta_ = 50000;    // follower only replayed 50KB
  limiter.tick_last_sample_.rw_delta_time_us_ = 100000;
  limiter.tick_last_sample_.ro_delta_time_us_ = 100000;
  // Leader is 3MB ahead, way beyond MAX_ALLOW_RO_DISTANCE_BYTES (512KB)
  limiter.commited_end_lsn_val_ = 3000000;

  int success_count = 0;
  int eagain_count = 0;
  const int iterations = 500;

  for (int i = 0; i < iterations; ++i) {
    int ret = limiter.check_rw_ro_tick_budget_(now_us);
    if (ret == OB_SUCCESS) {
      ++success_count;
    } else if (ret == OB_EAGAIN) {
      ++eagain_count;
    }
  }

  // With very slow follower and fast leader, expect high rejection rate
  EXPECT_GT(eagain_count, iterations * 0.3);

  // Now test with balanced throughput (leader and follower at same speed)
  limiter.tick_last_sample_.rw_delta_ = 100000;   // leader wrote 100KB
  limiter.tick_last_sample_.ro_delta_ = 100000;   // follower also replayed 100KB
  limiter.commited_end_lsn_val_ = 1500000;  // just slightly ahead

  success_count = 0;
  eagain_count = 0;

  for (int i = 0; i < iterations; ++i) {
    int ret = limiter.check_rw_ro_tick_budget_(now_us);
    if (ret == OB_SUCCESS) {
      ++success_count;
    } else if (ret == OB_EAGAIN) {
      ++eagain_count;
    }
  }

  // With balanced throughput, expect higher success rate
  EXPECT_GT(success_count, iterations * 0.5);
}

// =====================================================================
// ObLogSSRTOKeeper Tests
// =====================================================================

TEST_F(SubmitLogRateLimiterTest, RTOKeeperInitAndDestroy)
{
  ObLogSSRTOKeeper keeper;

  // Not initialized
  EXPECT_FALSE(keeper.is_inited_);

  // Invalid argument
  EXPECT_EQ(OB_INVALID_ARGUMENT, keeper.init(nullptr));

  // Successful init
  EXPECT_EQ(OB_SUCCESS, keeper.init(&dummy_rpc_));
  EXPECT_TRUE(keeper.is_inited_);

  // Double init
  EXPECT_EQ(OB_INIT_TWICE, keeper.init(&dummy_rpc_));

  // Destroy
  keeper.destroy();
  EXPECT_FALSE(keeper.is_inited_);
}

TEST_F(SubmitLogRateLimiterTest, RTOKeeperAddRemoveLS)
{
  ObLogSSRTOKeeper keeper;
  ASSERT_EQ(OB_SUCCESS, keeper.init(&dummy_rpc_));

  share::ObLSID ls_id(1001);

  // Add LS
  EXPECT_EQ(OB_SUCCESS, keeper.add_ls(ls_id, &mock_palf_env_));

  // Get rate limiter
  ObLogLSSubmitLogRateLimiter *limiter = nullptr;
  EXPECT_EQ(OB_SUCCESS, keeper.get_ls_submit_log_rate_limiter(ls_id, limiter));
  EXPECT_NE(nullptr, limiter);

  // Add duplicate LS
  EXPECT_EQ(OB_ENTRY_EXIST, keeper.add_ls(ls_id, &mock_palf_env_));

  // Remove LS
  EXPECT_EQ(OB_SUCCESS, keeper.remove_ls(ls_id));

  // Get after remove should fail
  limiter = nullptr;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, keeper.get_ls_submit_log_rate_limiter(ls_id, limiter));

  // Remove non-existent LS (should succeed with warning)
  EXPECT_EQ(OB_SUCCESS, keeper.remove_ls(ls_id));
}

TEST_F(SubmitLogRateLimiterTest, RTOKeeperNotInit)
{
  ObLogSSRTOKeeper keeper;
  share::ObLSID ls_id(1001);

  EXPECT_EQ(OB_NOT_INIT, keeper.add_ls(ls_id, &mock_palf_env_));
  EXPECT_EQ(OB_NOT_INIT, keeper.remove_ls(ls_id));

  ObLogLSSubmitLogRateLimiter *limiter = nullptr;
  EXPECT_EQ(OB_NOT_INIT, keeper.get_ls_submit_log_rate_limiter(ls_id, limiter));
}

TEST_F(SubmitLogRateLimiterTest, RTOKeeperInvalidArguments)
{
  ObLogSSRTOKeeper keeper;
  ASSERT_EQ(OB_SUCCESS, keeper.init(&dummy_rpc_));

  share::ObLSID invalid_ls_id;
  share::ObLSID valid_ls_id(1001);

  EXPECT_EQ(OB_INVALID_ARGUMENT, keeper.add_ls(invalid_ls_id, &mock_palf_env_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, keeper.add_ls(valid_ls_id, nullptr));
}

// =====================================================================
// Overflow Tests
// =====================================================================

TEST_F(SubmitLogRateLimiterTest, AddOverflowTest)
{
  int64_t res = 0;
  EXPECT_FALSE(add_overflow(INT64_MAX - 10, static_cast<int64_t>(5), res));
  EXPECT_EQ(INT64_MAX - 5, res);

  EXPECT_TRUE(add_overflow(INT64_MAX, static_cast<int64_t>(1), res));
}

TEST_F(SubmitLogRateLimiterTest, MulOverflowTest)
{
  int64_t res = 0;
  EXPECT_FALSE(mul_overflow(static_cast<int64_t>(1000), static_cast<int64_t>(1000), res));
  EXPECT_EQ(1000000, res);

  EXPECT_TRUE(mul_overflow(INT64_MAX, static_cast<int64_t>(2), res));
}

// =====================================================================
// Large Integration Test: Complex Leader/Follower RTO Group Changes
// =====================================================================

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
//
// For each step, we also test rate limiting behavior:
// - scn_need_rate_limit_: based on SCN diff (< 2s: pass, >= 4s: limit, [2s,4s): probabilistic)
// - lsn_need_rate_limit_: based on LSN diff (< 64MB: pass, >= 64MB: limit)
TEST_F(SubmitLogRateLimiterTest, ComplexLeaderFollowerRTOGroupMembershipChanges)
{
  const share::ObLSID ls_id(100);
  const ObAddr addr_A(ObAddr::IPV4, "10.0.0.1", 2881);
  const ObAddr addr_B(ObAddr::IPV4, "10.0.0.2", 2881);
  const ObAddr addr_C(ObAddr::IPV4, "10.0.0.3", 2881);

  // Constants for rate limiting (from implementation)
  const int64_t BEGIN_RATE_LIMIT_SCN_DIFF_US = 2'000'000;  // 2s
  const int64_t END_RATE_LIMIT_SCN_DIFF_US = 4'000'000;    // 4s
  const uint64_t MAX_LSN_DIFF = 64 * 1024 * 1024;         // 64MB

  // Helper to check RTO group validity and members
  auto verify_rto_group = [](ObLogLSSubmitLogRateLimiter &limiter, bool expect_valid,
                             const share::SCN &expect_max_scn = share::SCN(),
                             const palf::LSN &expect_min_lsn = palf::LSN()) {
    share::SCN max_scn;
    palf::LSN min_lsn;
    int64_t publish_us = 0;
    ASSERT_EQ(OB_SUCCESS, limiter.ls_replay_process_cache_.get_faster_replica_max_replayed_point(
        max_scn, min_lsn, publish_us));
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

  // Helper to test SCN rate limiting with different scenarios
  // leader_scn_us: leader's committed SCN in microseconds
  // follower_scn: follower's max replayed SCN (from RTO group)
  auto verify_scn_rate_limit = [&](ObLogLSSubmitLogRateLimiter &limiter,
                                    int64_t leader_scn_us,
                                    const share::SCN &follower_scn,
                                    const char* step_name) {
    limiter.committed_end_scn_val_us_ = leader_scn_us;

    // Test 1: Follower very close (< 2s behind) - should pass
    share::SCN close_scn;
    close_scn.convert_for_logservice((leader_scn_us - 1'000'000) * 1000); // 1s behind
    EXPECT_EQ(OB_SUCCESS, limiter.scn_need_rate_limit_(close_scn))
        << step_name << ": close SCN should pass";

    // Test 2: Follower very far (>= 4s behind) - should always limit
    share::SCN far_scn;
    far_scn.convert_for_logservice((leader_scn_us - 5'000'000) * 1000); // 5s behind
    EXPECT_EQ(OB_EAGAIN, limiter.scn_need_rate_limit_(far_scn))
        << step_name << ": far SCN should be rate limited";

    // Test 3: Follower at boundary (exactly 2s behind) - should pass
    share::SCN boundary_scn;
    boundary_scn.convert_for_logservice((leader_scn_us - BEGIN_RATE_LIMIT_SCN_DIFF_US) * 1000);
    EXPECT_EQ(OB_SUCCESS, limiter.scn_need_rate_limit_(boundary_scn))
        << step_name << ": boundary SCN should pass";

    // Test 4: Test probabilistic zone (3s behind) - should have mixed results
    share::SCN mid_scn;
    mid_scn.convert_for_logservice((leader_scn_us - 3'000'000) * 1000); // 3s behind
    int success_count = 0, eagain_count = 0;
    const int iterations = 100;
    for (int i = 0; i < iterations; ++i) {
      int ret = limiter.scn_need_rate_limit_(mid_scn);
      if (ret == OB_SUCCESS) ++success_count;
      else if (ret == OB_EAGAIN) ++eagain_count;
    }
    // In middle of probabilistic zone, expect both successes and failures
    EXPECT_GT(success_count, 0) << step_name << ": mid-zone should have some successes";
    EXPECT_GT(eagain_count, 0) << step_name << ": mid-zone should have some rate limits";
    EXPECT_EQ(iterations, success_count + eagain_count);

    // Test 5: Test using actual follower SCN from RTO group
    if (follower_scn.is_valid() && !follower_scn.is_max()) {
      // Follower SCN should be used for rate limiting decisions
      int64_t follower_scn_us = follower_scn.get_val_for_logservice() / 1000;
      int64_t diff_us = leader_scn_us - follower_scn_us;
      if (diff_us < BEGIN_RATE_LIMIT_SCN_DIFF_US) {
        EXPECT_EQ(OB_SUCCESS, limiter.scn_need_rate_limit_(follower_scn))
            << step_name << ": follower SCN close enough should pass";
      }
    }
  };

  // Helper to test LSN rate limiting with different scenarios
  auto verify_lsn_rate_limit = [&](ObLogLSSubmitLogRateLimiter &limiter,
                                    uint64_t leader_lsn_val,
                                    const palf::LSN &follower_lsn,
                                    const char* step_name) {
    limiter.commited_end_lsn_val_ = leader_lsn_val;

    // Test 1: Follower very close (< 64MB behind) - should pass
    palf::LSN close_lsn(leader_lsn_val - 1 * 1024 * 1024); // 1MB behind
    EXPECT_EQ(OB_SUCCESS, limiter.lsn_need_rate_limit_(close_lsn))
        << step_name << ": close LSN should pass";

    // Test 2: Follower very far (>= 64MB behind) - should limit
    if (leader_lsn_val > 70 * 1024 * 1024) {
      palf::LSN far_lsn(leader_lsn_val - 70 * 1024 * 1024); // 70MB behind
      EXPECT_EQ(OB_EAGAIN, limiter.lsn_need_rate_limit_(far_lsn))
          << step_name << ": far LSN should be rate limited";
    }

    // Test 3: Boundary test - just within 64MB (should pass)
    palf::LSN within_boundary_lsn(leader_lsn_val - MAX_LSN_DIFF + 1);
    EXPECT_EQ(OB_SUCCESS, limiter.lsn_need_rate_limit_(within_boundary_lsn))
        << step_name << ": within boundary LSN should pass";

    // Test 4: Boundary test - exactly 64MB (should limit due to strict <)
    palf::LSN exact_boundary_lsn(leader_lsn_val - MAX_LSN_DIFF);
    EXPECT_EQ(OB_EAGAIN, limiter.lsn_need_rate_limit_(exact_boundary_lsn))
        << step_name << ": exact boundary LSN should be rate limited";

    // Test 5: Test using actual follower LSN from RTO group
    if (follower_lsn.is_valid() && follower_lsn.val_ != palf::LOG_MAX_LSN_VAL) {
      uint64_t diff = leader_lsn_val > follower_lsn.val_ ?
                      leader_lsn_val - follower_lsn.val_ : 0;
      if (diff < MAX_LSN_DIFF) {
        EXPECT_EQ(OB_SUCCESS, limiter.lsn_need_rate_limit_(follower_lsn))
            << step_name << ": follower LSN close enough should pass";
      }
    }
  };

  // Helper to test speed/tick-based rate limiting
  // Tests check_rw_ro_tick_budget_ which compares leader write speed vs follower replay speed
  // Constants from implementation:
  // - MAX_ALLOW_RO_DISTANCE_BYTES = 512KB - if follower is within this distance, no rate limit
  // - Rate limit probability depends on rw_delta_ vs ro_delta_ (write speed vs replay speed)
  auto verify_speed_rate_limit = [&](ObLogLSSubmitLogRateLimiter &limiter,
                                      uint64_t leader_lsn_val,
                                      uint64_t follower_lsn_val,
                                      uint64_t leader_write_delta,
                                      uint64_t follower_replay_delta,
                                      int64_t now_us,
                                      const char* step_name) {
    const uint64_t MAX_ALLOW_RO_DISTANCE_BYTES = 512 * 1024;  // 512KB

    // Set up leader end LSN
    limiter.commited_end_lsn_val_ = leader_lsn_val;

    // Test 1: Invalid tick sample - should always pass
    {
      limiter.tick_last_sample_.valid_ = false;
      EXPECT_EQ(OB_SUCCESS, limiter.check_rw_ro_tick_budget_(now_us))
          << step_name << ": invalid tick sample should pass";
    }

    // Test 2: Follower within MAX_ALLOW_RO_DISTANCE_BYTES - should pass
    {
      limiter.tick_last_sample_.valid_ = true;
      limiter.tick_last_sample_.rw_lsn_ = leader_lsn_val - 100 * 1024;  // 100KB behind current
      limiter.tick_last_sample_.ro_lsn_ = leader_lsn_val - 200 * 1024;  // 200KB behind current
      limiter.tick_last_sample_.rw_time_us_ = now_us - 200'000;
      limiter.tick_last_sample_.ro_time_us_ = now_us - 200'000;
      limiter.tick_last_sample_.rw_delta_ = 50 * 1024;
      limiter.tick_last_sample_.ro_delta_ = 50 * 1024;
      limiter.tick_last_sample_.rw_delta_time_us_ = 100'000;
      limiter.tick_last_sample_.ro_delta_time_us_ = 100'000;
      // Leader is within 512KB of follower (200KB < 512KB)
      EXPECT_EQ(OB_SUCCESS, limiter.check_rw_ro_tick_budget_(now_us))
          << step_name << ": follower within distance should pass";
    }

    // Test 3: Follower far behind but replay speed matches write speed - mixed results
    // For rate limiting to potentially trigger, leader must have generated significant bytes since sample
    {
      limiter.tick_last_sample_.valid_ = true;
      // Leader generated 500KB since sample
      limiter.tick_last_sample_.rw_lsn_ = leader_lsn_val - leader_write_delta;
      limiter.tick_last_sample_.ro_lsn_ = follower_lsn_val - follower_replay_delta;
      // Use same elapsed time as delta time for fair comparison
      limiter.tick_last_sample_.rw_time_us_ = now_us - 125'000;
      limiter.tick_last_sample_.ro_time_us_ = now_us - 125'000;
      limiter.tick_last_sample_.rw_delta_ = leader_write_delta;
      limiter.tick_last_sample_.ro_delta_ = follower_replay_delta;
      limiter.tick_last_sample_.rw_delta_time_us_ = 125'000;  // tick interval
      limiter.tick_last_sample_.ro_delta_time_us_ = 125'000;

      // Only test if follower is far enough to trigger rate limiting check
      uint64_t distance = leader_lsn_val > follower_lsn_val ?
                          leader_lsn_val - follower_lsn_val : 0;
      if (distance > MAX_ALLOW_RO_DISTANCE_BYTES) {
        int success_count = 0, eagain_count = 0;
        const int iterations = 100;
        for (int i = 0; i < iterations; ++i) {
          int ret = limiter.check_rw_ro_tick_budget_(now_us);
          if (ret == OB_SUCCESS) ++success_count;
          else if (ret == OB_EAGAIN) ++eagain_count;
        }
        EXPECT_EQ(iterations, success_count + eagain_count)
            << step_name << ": all iterations should return SUCCESS or EAGAIN";

        // If follower replay speed >= leader generated bytes, expect mostly success
        // Note: comparison is leader_generated_bytes vs follower_effective_bytes (min 64KB)
        // The implementation uses FIRST_ORDER_RATE_LIMIT_FACTOR = 3 in rejection calculation
        uint64_t leader_generated_bytes = leader_write_delta;
        uint64_t follower_effective_bytes = follower_replay_delta > 64 * 1024 ?
                                            follower_replay_delta : 64 * 1024;
        if (follower_effective_bytes >= leader_generated_bytes) {
          EXPECT_GT(success_count, iterations * 0.4)
              << step_name << ": balanced speed should have good success rate";
        }
        // If follower is much slower than leader generated, expect some rate limiting
        // Due to FIRST_ORDER_RATE_LIMIT_FACTOR = 3, follower needs to be ~3x+ slower
        // to see significant rate limiting
        if (follower_effective_bytes < leader_generated_bytes / 3) {
          EXPECT_GT(eagain_count, 0)
              << step_name << ": slow follower (3x+ slower) should see some rate limiting";
        }
      }
    }

    // Test 4: Follower very far behind and much slower - high rate limiting
    // Key insight: rate limiting compares (leader_generated_bytes * ro_delta_time) vs (follower_effective_bytes * elapsed_time)
    // leader_generated_bytes = commited_end_lsn_val_ - rw_lsn (how much leader wrote since sample)
    // follower_effective_bytes = max(ro_delta, 64KB)
    // FIRST_ORDER_RATE_LIMIT_FACTOR = 3 means rejection_weight = 3 * follower_throughput * 100
    // To trigger rate limiting: leader_generated / follower_effective needs to be > ~3x
    // Here: leader_generated = 1MB, follower_effective = 64KB, ratio = 16x (well beyond 3x)
    {
      limiter.tick_last_sample_.valid_ = true;
      // Leader generated 1MB since sample (large delta)
      limiter.tick_last_sample_.rw_lsn_ = leader_lsn_val - 1 * 1024 * 1024;
      limiter.tick_last_sample_.ro_lsn_ = leader_lsn_val - 2 * 1024 * 1024;  // 2MB behind
      // Use same elapsed time as delta time to make comparison fair
      limiter.tick_last_sample_.rw_time_us_ = now_us - 125'000;
      limiter.tick_last_sample_.ro_time_us_ = now_us - 125'000;
      limiter.tick_last_sample_.rw_delta_ = 500 * 1024;  // leader wrote 500KB in delta interval
      limiter.tick_last_sample_.ro_delta_ = 50 * 1024;   // follower only replayed 50KB
      // follower_effective = max(50KB, 64KB) = 64KB
      // ratio = 1MB / 64KB = 16x >> 3x, should trigger high rate limiting
      limiter.tick_last_sample_.rw_delta_time_us_ = 125'000;
      limiter.tick_last_sample_.ro_delta_time_us_ = 125'000;

      int success_count = 0, eagain_count = 0;
      const int iterations = 100;
      for (int i = 0; i < iterations; ++i) {
        int ret = limiter.check_rw_ro_tick_budget_(now_us);
        if (ret == OB_SUCCESS) ++success_count;
        else if (ret == OB_EAGAIN) ++eagain_count;
      }
      EXPECT_GT(eagain_count, iterations * 0.3)
          << step_name << ": very slow follower (16x slower) should see high rate limiting";
    }

    // Test 5: Reset to actual follower state for subsequent tests in the main test
    // This sets up a reasonable state based on the actual step's follower position
    {
      limiter.tick_last_sample_.valid_ = true;
      // Leader generated a reasonable amount since sample
      limiter.tick_last_sample_.rw_lsn_ = leader_lsn_val - 100 * 1024;
      limiter.tick_last_sample_.ro_lsn_ = follower_lsn_val;
      limiter.tick_last_sample_.rw_time_us_ = now_us - 125'000;
      limiter.tick_last_sample_.ro_time_us_ = now_us - 125'000;
      limiter.tick_last_sample_.rw_delta_ = leader_write_delta;
      limiter.tick_last_sample_.ro_delta_ = follower_replay_delta;
      limiter.tick_last_sample_.rw_delta_time_us_ = 125'000;
      limiter.tick_last_sample_.ro_delta_time_us_ = 125'000;
    }
  };

  // Stale time interval (from implementation: REPLICA_CACHE_STALE_TIME_INTERVAL_US = 2s)
  const int64_t STALE_INTERVAL_US = 3'000'000;  // 3s to ensure staleness

  // ============================================
  // Phase 1: A is leader, B and C are followers
  // ============================================
  {
    ObLogLSSubmitLogRateLimiter leader_A_limiter;
    ASSERT_EQ(OB_SUCCESS, leader_A_limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

    int64_t time_us = 10'000'000;
    // Use realistic SCN values (in nanoseconds, but stored as uint64_t)
    // 10 seconds = 10,000,000 microseconds = 10,000,000,000 nanoseconds
    int64_t leader_scn_us = 10'000'000;  // 10 seconds
    uint64_t leader_lsn_val = 100 * 1024 * 1024;  // 100MB

    // Step 1: B and C join RTO group
    // Note: We call ls_replay_process_cache_.update_replica_replay_process directly
    // to control the time parameter for staleness testing
    {
      // Follower SCN is 0.5s behind leader
      int64_t follower_scn_us = leader_scn_us - 500'000;  // 0.5s behind
      uint64_t follower_lsn_val = leader_lsn_val - 10 * 1024 * 1024;  // 10MB behind

      share::SCN follower_scn;
      follower_scn.convert_for_logservice(follower_scn_us * 1000);

      auto machine_B = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_scn, true, follower_scn, palf::LSN(follower_lsn_val), addr_A, 1);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_scn, true, follower_scn, palf::LSN(follower_lsn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_B, machine_B, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      verify_rto_group(leader_A_limiter, true, follower_scn, palf::LSN(follower_lsn_val));

      // Test rate limiting: B and C in sync, should be lenient
      verify_scn_rate_limit(leader_A_limiter, leader_scn_us, follower_scn, "Step1:B,C in");
      verify_lsn_rate_limit(leader_A_limiter, leader_lsn_val, palf::LSN(follower_lsn_val), "Step1:B,C in");
      // Speed test: followers replaying at same speed as leader writing (balanced)
      verify_speed_rate_limit(leader_A_limiter, leader_lsn_val, follower_lsn_val,
          100 * 1024, 100 * 1024, time_us, "Step1:B,C in");
    }

    // Step 2: B out (timeout/stale)
    {
      time_us += STALE_INTERVAL_US;
      leader_scn_us += 3'000'000;  // Leader advances 3s
      leader_lsn_val += 30 * 1024 * 1024;  // Leader advances 30MB

      // Only C reports, follower is 1s behind now
      int64_t follower_scn_us = leader_scn_us - 1'000'000;  // 1s behind
      uint64_t follower_lsn_val = leader_lsn_val - 20 * 1024 * 1024;  // 20MB behind

      share::SCN follower_scn;
      follower_scn.convert_for_logservice(follower_scn_us * 1000);

      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_scn, true, follower_scn, palf::LSN(follower_lsn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // After staleness check, only C should be in group
      verify_rto_group(leader_A_limiter, true, follower_scn, palf::LSN(follower_lsn_val));

      // Test rate limiting: Only C in group
      verify_scn_rate_limit(leader_A_limiter, leader_scn_us, follower_scn, "Step2:B out");
      verify_lsn_rate_limit(leader_A_limiter, leader_lsn_val, palf::LSN(follower_lsn_val), "Step2:B out");
      // Speed test: C replaying slightly slower than leader writing
      verify_speed_rate_limit(leader_A_limiter, leader_lsn_val, follower_lsn_val,
          120 * 1024, 100 * 1024, time_us, "Step2:B out");
    }

    // Step 3: B in, C out
    {
      time_us += STALE_INTERVAL_US;
      leader_scn_us += 3'000'000;  // Leader advances 3s
      leader_lsn_val += 30 * 1024 * 1024;  // Leader advances 30MB

      // Only B reports, B is catching up (1.5s behind)
      int64_t follower_scn_us = leader_scn_us - 1'500'000;  // 1.5s behind
      uint64_t follower_lsn_val = leader_lsn_val - 25 * 1024 * 1024;  // 25MB behind

      share::SCN follower_scn;
      follower_scn.convert_for_logservice(follower_scn_us * 1000);

      auto machine_B = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_scn, true, follower_scn, palf::LSN(follower_lsn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_B, machine_B, true, time_us));

      // Only B in group now
      verify_rto_group(leader_A_limiter, true, follower_scn, palf::LSN(follower_lsn_val));

      // Test rate limiting: Only B in group
      verify_scn_rate_limit(leader_A_limiter, leader_scn_us, follower_scn, "Step3:B in, C out");
      verify_lsn_rate_limit(leader_A_limiter, leader_lsn_val, palf::LSN(follower_lsn_val), "Step3:B in, C out");
      // Speed test: B catching up, replaying faster than leader writing
      verify_speed_rate_limit(leader_A_limiter, leader_lsn_val, follower_lsn_val,
          100 * 1024, 130 * 1024, time_us, "Step3:B in, C out");
    }

    // Step 4: B out
    {
      time_us += STALE_INTERVAL_US;
      leader_scn_us += 3'000'000;  // Leader advances 3s
      leader_lsn_val += 30 * 1024 * 1024;  // Leader advances 30MB

      // No one reports, B becomes stale
      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.check_and_update_rto_group_validity(time_us));

      // RTO group should be invalid now
      share::SCN max_scn;
      palf::LSN min_lsn;
      int64_t publish_us = 0;
      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.get_faster_replica_max_replayed_point(
          max_scn, min_lsn, publish_us));
      verify_rto_group(leader_A_limiter, false);

      // Test rate limiting: RTO group invalid, max_scn is SCN::max, min_lsn is LOG_MAX_LSN_VAL
      // When RTO group is invalid, SCN/LSN rate limit should use max values
      verify_scn_rate_limit(leader_A_limiter, leader_scn_us, max_scn, "Step4:B out (invalid)");
      verify_lsn_rate_limit(leader_A_limiter, leader_lsn_val, min_lsn, "Step4:B out (invalid)");
      // Speed test: No follower info, use max LSN (LOG_MAX_LSN_VAL) - no rate limit
      verify_speed_rate_limit(leader_A_limiter, leader_lsn_val, palf::LOG_MAX_LSN_VAL,
          100 * 1024, 100 * 1024, time_us, "Step4:B out (invalid)");
    }

    // Step 5: B, C in
    {
      time_us += 1'000'000;
      leader_scn_us += 1'000'000;  // Leader advances 1s
      leader_lsn_val += 10 * 1024 * 1024;  // Leader advances 10MB

      // B is 0.8s behind, C is 0.5s behind
      int64_t follower_B_scn_us = leader_scn_us - 800'000;
      int64_t follower_C_scn_us = leader_scn_us - 500'000;  // C is faster
      uint64_t follower_B_lsn_val = leader_lsn_val - 15 * 1024 * 1024;
      uint64_t follower_C_lsn_val = leader_lsn_val - 10 * 1024 * 1024;  // C is faster

      share::SCN follower_B_scn, follower_C_scn;
      follower_B_scn.convert_for_logservice(follower_B_scn_us * 1000);
      follower_C_scn.convert_for_logservice(follower_C_scn_us * 1000);

      auto machine_B = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_B_scn, true, follower_B_scn, palf::LSN(follower_B_lsn_val), addr_A, 1);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_C_scn, true, follower_C_scn, palf::LSN(follower_C_lsn_val), addr_A, 1);

      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_B, machine_B, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_A_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Both B and C in group, max is from C (faster)
      verify_rto_group(leader_A_limiter, true, follower_C_scn, palf::LSN(follower_C_lsn_val));

      // Test rate limiting: Both in group, use max (C's position)
      verify_scn_rate_limit(leader_A_limiter, leader_scn_us, follower_C_scn, "Step5:B,C in");
      verify_lsn_rate_limit(leader_A_limiter, leader_lsn_val, palf::LSN(follower_C_lsn_val), "Step5:B,C in");
      // Speed test: Both followers replaying, use fastest (C) - balanced speed
      verify_speed_rate_limit(leader_A_limiter, leader_lsn_val, follower_C_lsn_val,
          100 * 1024, 110 * 1024, time_us, "Step5:B,C in");
    }
  }

  // ============================================
  // Phase 2: B becomes leader, A and C are followers
  // ============================================
  {
    ObLogLSSubmitLogRateLimiter leader_B_limiter;
    ASSERT_EQ(OB_SUCCESS, leader_B_limiter.init(ls_id, &dummy_rpc_, &mock_palf_env_));

    int64_t time_us = 50'000'000;
    int64_t leader_scn_us = 50'000'000;  // 50 seconds
    uint64_t leader_lsn_val = 500 * 1024 * 1024;  // 500MB

    // Step 6: A, C join RTO group
    {
      // A is 1s behind, C is 0.8s behind
      int64_t follower_A_scn_us = leader_scn_us - 1'000'000;
      int64_t follower_C_scn_us = leader_scn_us - 800'000;  // C is faster
      uint64_t follower_A_lsn_val = leader_lsn_val - 20 * 1024 * 1024;
      uint64_t follower_C_lsn_val = leader_lsn_val - 15 * 1024 * 1024;  // C is faster

      share::SCN follower_A_scn, follower_C_scn;
      follower_A_scn.convert_for_logservice(follower_A_scn_us * 1000);
      follower_C_scn.convert_for_logservice(follower_C_scn_us * 1000);

      auto machine_A = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_A_scn, true, follower_A_scn, palf::LSN(follower_A_lsn_val), addr_B, 2);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_C_scn, true, follower_C_scn, palf::LSN(follower_C_lsn_val), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_A, machine_A, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Both A and C in group, max is from C
      verify_rto_group(leader_B_limiter, true, follower_C_scn, palf::LSN(follower_C_lsn_val));

      // Test rate limiting
      verify_scn_rate_limit(leader_B_limiter, leader_scn_us, follower_C_scn, "Step6:A,C in");
      verify_lsn_rate_limit(leader_B_limiter, leader_lsn_val, palf::LSN(follower_C_lsn_val), "Step6:A,C in");
      // Speed test: Both followers replaying, use fastest (C) - balanced speed
      verify_speed_rate_limit(leader_B_limiter, leader_lsn_val, follower_C_lsn_val,
          100 * 1024, 100 * 1024, time_us, "Step6:A,C in");
    }

    // Step 7: C out
    {
      time_us += STALE_INTERVAL_US;
      leader_scn_us += 3'000'000;
      leader_lsn_val += 30 * 1024 * 1024;

      // Only A reports (1.2s behind now)
      int64_t follower_A_scn_us = leader_scn_us - 1'200'000;
      uint64_t follower_A_lsn_val = leader_lsn_val - 22 * 1024 * 1024;

      share::SCN follower_A_scn;
      follower_A_scn.convert_for_logservice(follower_A_scn_us * 1000);

      auto machine_A = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_A_scn, true, follower_A_scn, palf::LSN(follower_A_lsn_val), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_A, machine_A, true, time_us));

      // Only A in group
      verify_rto_group(leader_B_limiter, true, follower_A_scn, palf::LSN(follower_A_lsn_val));

      // Test rate limiting
      verify_scn_rate_limit(leader_B_limiter, leader_scn_us, follower_A_scn, "Step7:C out");
      verify_lsn_rate_limit(leader_B_limiter, leader_lsn_val, palf::LSN(follower_A_lsn_val), "Step7:C out");
      // Speed test: Only A replaying, much slower than leader
      verify_speed_rate_limit(leader_B_limiter, leader_lsn_val, follower_A_lsn_val,
          500 * 1024, 90 * 1024, time_us, "Step7:C out");
    }

    // Step 8: C in, A out
    {
      time_us += STALE_INTERVAL_US;
      leader_scn_us += 3'000'000;
      leader_lsn_val += 30 * 1024 * 1024;

      // Only C reports (0.9s behind)
      int64_t follower_C_scn_us = leader_scn_us - 900'000;
      uint64_t follower_C_lsn_val = leader_lsn_val - 18 * 1024 * 1024;

      share::SCN follower_C_scn;
      follower_C_scn.convert_for_logservice(follower_C_scn_us * 1000);

      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_C_scn, true, follower_C_scn, palf::LSN(follower_C_lsn_val), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Only C in group
      verify_rto_group(leader_B_limiter, true, follower_C_scn, palf::LSN(follower_C_lsn_val));

      // Test rate limiting
      verify_scn_rate_limit(leader_B_limiter, leader_scn_us, follower_C_scn, "Step8:C in, A out");
      verify_lsn_rate_limit(leader_B_limiter, leader_lsn_val, palf::LSN(follower_C_lsn_val), "Step8:C in, A out");
      // Speed test: Only C replaying, catching up faster
      verify_speed_rate_limit(leader_B_limiter, leader_lsn_val, follower_C_lsn_val,
          100 * 1024, 120 * 1024, time_us, "Step8:C in, A out");
    }

    // Step 9: C out
    {
      time_us += STALE_INTERVAL_US;
      leader_scn_us += 3'000'000;
      leader_lsn_val += 30 * 1024 * 1024;

      // No one reports, C becomes stale
      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.check_and_update_rto_group_validity(time_us));

      // RTO group should be invalid
      share::SCN max_scn;
      palf::LSN min_lsn;
      int64_t publish_us = 0;
      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.get_faster_replica_max_replayed_point(
          max_scn, min_lsn, publish_us));
      verify_rto_group(leader_B_limiter, false);

      // Test rate limiting with invalid RTO group
      verify_scn_rate_limit(leader_B_limiter, leader_scn_us, max_scn, "Step9:C out (invalid)");
      verify_lsn_rate_limit(leader_B_limiter, leader_lsn_val, min_lsn, "Step9:C out (invalid)");
      // Speed test: No follower info, use max LSN - no rate limit
      verify_speed_rate_limit(leader_B_limiter, leader_lsn_val, palf::LOG_MAX_LSN_VAL,
          100 * 1024, 100 * 1024, time_us, "Step9:C out (invalid)");
    }

    // Step 10: A, C in
    {
      time_us += 1'000'000;
      leader_scn_us += 1'000'000;
      leader_lsn_val += 10 * 1024 * 1024;

      // A is 0.7s behind, C is 0.5s behind (C faster)
      int64_t follower_A_scn_us = leader_scn_us - 700'000;
      int64_t follower_C_scn_us = leader_scn_us - 500'000;
      uint64_t follower_A_lsn_val = leader_lsn_val - 12 * 1024 * 1024;
      uint64_t follower_C_lsn_val = leader_lsn_val - 8 * 1024 * 1024;

      share::SCN follower_A_scn, follower_C_scn;
      follower_A_scn.convert_for_logservice(follower_A_scn_us * 1000);
      follower_C_scn.convert_for_logservice(follower_C_scn_us * 1000);

      auto machine_A = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_A_scn, true, follower_A_scn, palf::LSN(follower_A_lsn_val), addr_B, 2);
      auto machine_C = make_machine(ls_id, LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING,
          follower_C_scn, true, follower_C_scn, palf::LSN(follower_C_lsn_val), addr_B, 2);

      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_A, machine_A, true, time_us));
      ASSERT_EQ(OB_SUCCESS, leader_B_limiter.ls_replay_process_cache_.update_replica_replay_process(
          addr_C, machine_C, true, time_us));

      // Both A and C in group, max is from C
      verify_rto_group(leader_B_limiter, true, follower_C_scn, palf::LSN(follower_C_lsn_val));

      // Test rate limiting with both followers in sync
      verify_scn_rate_limit(leader_B_limiter, leader_scn_us, follower_C_scn, "Step10:A,C in");
      verify_lsn_rate_limit(leader_B_limiter, leader_lsn_val, palf::LSN(follower_C_lsn_val), "Step10:A,C in");
      // Speed test: Both followers back, use fastest (C) - balanced and slightly faster
      verify_speed_rate_limit(leader_B_limiter, leader_lsn_val, follower_C_lsn_val,
          100 * 1024, 105 * 1024, time_us, "Step10:A,C in");
    }
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
