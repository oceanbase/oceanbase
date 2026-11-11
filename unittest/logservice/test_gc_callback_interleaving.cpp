/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 */
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/thread/thread_mgr.h"
#define private public
#define protected public
#include "logservice/ob_garbage_collector.h"
#include "logservice/ob_log_service.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "rootserver/ob_ls_service_helper.h"
#include "share/ls/ob_ls_life_manager.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;
using namespace share;

class GCEvent
{
public:
  GCEvent() : ready_(promise_.get_future().share()) {}
  void signal() { if (!signalled_.exchange(true)) { promise_.set_value(); } }
  void wait() { ready_.wait(); }
  bool wait_ready(const int timeout_ms = 5000)
  {
    // Timed futex waits are unsupported by some tenant syscall hooks.
    for (int i = 0; i < timeout_ms; ++i) {
      if (std::future_status::ready == ready_.wait_for(std::chrono::seconds(0))) { return true; }
      ob_usleep(1000);
    }
    return std::future_status::ready == ready_.wait_for(std::chrono::seconds(0));
  }
private:
  std::promise<void> promise_;
  std::shared_future<void> ready_;
  std::atomic<bool> signalled_{false};
};

struct GCInterleavingControl
{
  palf::PalfHandle palf_;
  std::atomic<ObRole> role_{LEADER};
  GCEvent appended_;
  GCEvent first_slog_failed_;
  GCEvent first_slog_entered_;
  GCEvent allow_first_slog_;
  GCEvent retry_entered_;
  GCEvent allow_retry_;
  int append_calls_ = 0;
  std::atomic<int> readable_checks_{0};
  std::atomic<int> slog_calls_{0};
  bool fail_first_slog_ = false;
  bool pause_first_slog_ = false;
  bool pause_retry_ = false;
  bool allow_wait_gc_ = false;
  int readable_ret_ = OB_EAGAIN;
  uint64_t readable_scn_ = 101;
  uint64_t sync_scn_ = 101;
  uint64_t archive_scn_ = 101;
  int status_queries_ = 0;
  int sync_queries_ = 0;
  int archive_queries_ = 0;
  int delete_calls_ = 0;
  int delete_ret_ = OB_SUCCESS;
  bool status_deleted_ = false;
  storage::ObLS *ls_ = nullptr;
};
GCInterleavingControl *interleaving_control = nullptr;

// GC, LogHandler, Apply/Transport marking and ObLS/ObLSMeta state updates are
// real. Only the PALF/GTS/query and disk-write boundaries are controlled.
class TestGCInterleaving : public ::testing::Test
{
protected:
  explicit TestGCInterleaving(const uint64_t tenant_id = OB_SYS_TENANT_ID) : tenant_(tenant_id) {}
  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_);
    interleaving_control = &control_;
    ls_ = std::make_unique<storage::ObLS>();
    log_service_ = std::make_unique<ObLogService>();
    info_loader_ = std::make_unique<rootserver::ObTenantInfoLoader>();
    tenant_.set<ObLogService *>(log_service_.get());
    tenant_.set<rootserver::ObTenantInfoLoader *>(info_loader_.get());
    ASSERT_EQ(OB_SUCCESS, ls_->ls_meta_.init(tenant_.id(), ObLSID(1001),
        storage::ObMigrationStatus::OB_MIGRATION_STATUS_NONE,
        ObLSRestoreStatus(ObLSRestoreStatus::NONE), SCN::min_scn(),
        storage::ObMajorMVMergeInfo(), ObLSStoreFormat()));
    ASSERT_EQ(OB_SUCCESS, ls_->ls_meta_.set_start_work_state());
    ls_->ls_meta_.gc_state_ = LSGCState::LS_BLOCKED;
    ls_->is_inited_ = true;
    gc_ = ls_->get_gc_handler();
    ASSERT_EQ(OB_SUCCESS, gc_->init(ls_.get()));
    ASSERT_EQ(OB_SUCCESS, handlers_.register_handler(GC_LS_LOG_BASE_TYPE, gc_));

    ASSERT_EQ(OB_SUCCESS, TG_CREATE(lib::TGDefIDs::ApplyService, apply_service_.tg_id_));
    apply_service_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, apply_service_.start());
    apply_.is_inited_ = true;
    apply_.is_in_stop_state_ = false;
    apply_.role_ = LEADER;
    apply_.proposal_id_ = 10;
    apply_.is_sync_mode_ = true;
    apply_.palf_committed_end_lsn_ = palf::LSN(200);
    apply_.standby_committed_end_lsn_ = palf::LSN(100);
    apply_.min_committed_end_lsn_ = palf::LSN(100);
    apply_.ap_sv_ = &apply_service_;
    apply_.inc_ref();
    ASSERT_EQ(OB_SUCCESS, apply_.submit_task_.init(&apply_));
    for (int64_t i = 0; i < APPLY_TASK_QUEUE_SIZE; ++i) {
      ASSERT_EQ(OB_SUCCESS, apply_.cb_queues_[i].init(&apply_, i));
    }
    ASSERT_EQ(OB_SUCCESS, transport_service_.transport_status_map_.init("GCTestTp", tenant_.id()));
    transport_service_.is_inited_ = true;
    transport_.inc_ref(); // Keep the fixture alive when temporary guards revert.
    transport_.is_inited_ = true;
    transport_.role_ = LEADER;
    ASSERT_EQ(OB_SUCCESS, transport_service_.transport_status_map_.insert(ObLSID(1001), &transport_));
    auto &handler = ls_->log_handler_;
    handler.is_inited_ = true;
    handler.is_in_stop_state_ = false;
    handler.id_ = 1001;
    handler.role_ = LEADER;
    handler.proposal_id_ = 10;
    handler.sync_mode_ = palf::SyncMode::SYNC;
    handler.palf_handle_ = &control_.palf_;
    handler.apply_status_ = &apply_;
    handler.transport_service_ = &transport_service_;
    handler.enable_logservice_ = false;

    old_write_slog_ = storage::ObLSMeta::write_slog_;
    storage::ObLSMeta::write_slog_ = [this](const storage::ObLSMeta &meta) {
      EXPECT_TRUE(LSGCState::LS_OFFLINE == meta.gc_state_
          || (control_.allow_wait_gc_ && LSGCState::WAIT_GC == meta.gc_state_));
      EXPECT_EQ(101, meta.offline_scn_.get_val_for_logservice());
      const int attempt = ++control_.slog_calls_;
      if (1 == attempt && control_.pause_first_slog_) {
        control_.first_slog_entered_.signal();
        control_.allow_first_slog_.wait();
      }
      if (1 == attempt && control_.fail_first_slog_) {
        control_.first_slog_failed_.signal();
        return OB_IO_ERROR;
      }
      if (attempt > 1 && control_.pause_retry_) {
        control_.retry_entered_.signal();
        control_.allow_retry_.wait();
      }
      return OB_SUCCESS;
    };
  }
  void TearDown() override
  {
    control_.allow_first_slog_.signal();
    control_.allow_retry_.signal();
    if (work_.valid()) {
      (void)apply_.switch_to_follower();
      (void)work_.get();
    }
    if (apply_service_.tg_id_ >= 0) {
      TG_STOP(apply_service_.tg_id_);
      TG_WAIT(apply_service_.tg_id_);
      TG_DESTROY(apply_service_.tg_id_);
      apply_service_.tg_id_ = -1;
    }
    storage::ObLSMeta::write_slog_ = old_write_slog_;
    transport_service_.transport_status_map_.erase(ObLSID(1001));
    transport_service_.is_inited_ = false;
    // The fixture initializes only the components exercised above, not a full LS.
    ls_->is_inited_ = false;
    ls_->log_handler_.is_inited_ = false;
    ls_->log_handler_.palf_handle_ = nullptr;
    ls_->log_handler_.apply_status_ = nullptr;
    ls_.reset();
    tenant_.set<ObLogService *>(nullptr);
    tenant_.set<rootserver::ObTenantInfoLoader *>(nullptr);
    info_loader_.reset();
    log_service_.reset();
    interleaving_control = nullptr;
    share::ObTenantEnv::set_tenant(nullptr);
  }
  void start_gc()
  {
    work_ = std::async(std::launch::async, [this] {
      share::ObTenantEnv::set_tenant(&tenant_);
      gc_->execute_pre_gc_process(status_);
      share::ObTenantEnv::set_tenant(nullptr);
    });
  }
  int revoke()
  {
    share::ObTenantEnv::set_tenant(&tenant_);
    const int ret = handlers_.switch_to_follower_gracefully();
    if (OB_SUCCESS == ret) {
      ls_->log_handler_.switch_role(FOLLOWER, 10);
    }
    return ret;
  }
  void finish_revoke()
  {
    // Graceful RC drains Apply before changing PALF leader. GC's local state
    // persistence runs on a different thread and need not have finished yet.
    EXPECT_EQ(OB_SUCCESS, apply_.switch_to_follower());
    EXPECT_EQ(OB_SUCCESS, transport_.switch_to_follower());
    control_.role_.store(FOLLOWER);
  }
  void confirm_standby()
  {
    SCN end_scn;
    ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_logservice(200));
    ASSERT_EQ(OB_SUCCESS, apply_.update_standby_committed_end_lsn(palf::LSN(200), end_scn));
  }
  void expect_offline()
  {
    LSGCState state;
    SCN scn;
    ASSERT_EQ(OB_SUCCESS, ls_->get_gc_state(state));
    ASSERT_EQ(OB_SUCCESS, ls_->get_offline_scn(scn));
    EXPECT_EQ(LSGCState::LS_OFFLINE, state);
    EXPECT_EQ(101, scn.get_val_for_logservice());
    EXPECT_TRUE(gc_->get_rec_scn().is_max());
    EXPECT_TRUE(apply_.is_ls_gc_state());
    EXPECT_TRUE(transport_.is_ls_gc_state());
    EXPECT_EQ(1, control_.append_calls_);
    EXPECT_EQ(ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE, status_);
  }
  ObTenantBase tenant_;
  GCInterleavingControl control_;
  std::unique_ptr<storage::ObLS> ls_;
  std::unique_ptr<ObLogService> log_service_;
  std::unique_ptr<rootserver::ObTenantInfoLoader> info_loader_;
  ObGCHandler *gc_ = nullptr;
  ObRoleChangeHandler handlers_;
  ObLogApplyService apply_service_;
  ObApplyStatus apply_;
  ObLogTransportService transport_service_;
  LogTransportStatus transport_;
  storage::ObLSMeta::WriteSlog old_write_slog_;
  std::future<void> work_;
  ObGarbageCollector::LSStatus status_ = ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE;
};

TEST_F(TestGCInterleaving, revoked_gc_finishes_committed_state_without_continuing_cleanup)
{
  control_.pause_first_slog_ = true;
  start_gc();
  ASSERT_TRUE(control_.appended_.wait_ready());
  ASSERT_EQ(OB_SUCCESS, revoke());
  confirm_standby();
  ASSERT_TRUE(control_.first_slog_entered_.wait_ready());
  finish_revoke();
  control_.allow_first_slog_.signal();
  work_.get();
  expect_offline();
  EXPECT_EQ(1, control_.slog_calls_.load());
  EXPECT_EQ(0, control_.readable_checks_.load());
}

TEST_F(TestGCInterleaving, real_state_update_retry_preserves_checkpoint_and_allows_revoke)
{
  control_.fail_first_slog_ = true;
  control_.pause_retry_ = true;
  start_gc();
  ASSERT_TRUE(control_.appended_.wait_ready());
  confirm_standby();
  ASSERT_TRUE(control_.first_slog_failed_.wait_ready());
  GCEvent revoked;
  auto rc = std::async(std::launch::async, [this, &revoked] {
    const int ret = revoke();
    if (OB_SUCCESS == ret) { finish_revoke(); }
    revoked.signal();
    return ret;
  });
  const bool revoke_finished = revoked.wait_ready(500);
  EXPECT_TRUE(revoke_finished);
  // Also release a failing implementation, so assertions do not strand threads.
  if (!revoke_finished) { control_.allow_retry_.signal(); }
  EXPECT_EQ(OB_SUCCESS, rc.get());
  ASSERT_TRUE(control_.retry_entered_.wait_ready());
  LSGCState state;
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, ls_->get_gc_state(state));
  ASSERT_EQ(OB_SUCCESS, ls_->get_offline_scn(scn));
  EXPECT_EQ(LSGCState::LS_BLOCKED, state);
  EXPECT_FALSE(scn.is_valid());
  EXPECT_EQ(101, gc_->get_rec_scn().get_val_for_logservice());
  EXPECT_TRUE(apply_.is_ls_gc_state());
  EXPECT_TRUE(transport_.is_ls_gc_state());
  EXPECT_EQ(FOLLOWER, control_.role_.load());
  control_.allow_retry_.signal();
  work_.get();
  expect_offline();
  EXPECT_EQ(2, control_.slog_calls_.load());
  EXPECT_EQ(0, control_.readable_checks_.load());
}

TEST_F(TestGCInterleaving, current_leader_continues_cleanup_after_committed_state)
{
  start_gc();
  ASSERT_TRUE(control_.appended_.wait_ready());
  confirm_standby();
  work_.get();
  expect_offline();
  EXPECT_EQ(1, control_.readable_checks_.load());
}

class TestGCNewLeader : public TestGCInterleaving
{
protected:
  TestGCNewLeader() : TestGCInterleaving(1002) {}
  void SetUp() override
  {
    auto *allocator = ObMallocAllocator::get_instance();
    if (nullptr == allocator->get_tenant_ctx_allocator(1002, ObCtxIds::DEFAULT_CTX_ID)) {
      ASSERT_EQ(OB_SUCCESS, allocator->create_and_add_tenant_allocator(1002));
    }
    old_memory_limit_ = lib::get_tenant_memory_limit(1002);
    lib::set_tenant_memory_limit(1002, 1L << 30);
    TestGCInterleaving::SetUp();
    ASSERT_FALSE(HasFatalFailure());
    control_.allow_wait_gc_ = true;
    control_.readable_ret_ = OB_SUCCESS;
    control_.ls_ = ls_.get();
    sql_proxy_ = std::make_unique<ObMySQLProxy>();
    old_sql_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = sql_proxy_.get();
    archive_ = std::make_unique<archive::ObArchiveService>();
    tenant_.set<archive::ObArchiveService *>(archive_.get());
    ls_service_ = std::make_unique<storage::ObLSService>();
    ASSERT_EQ(OB_SUCCESS, ls_service_->ls_allocator_.init(
        OB_MALLOC_NORMAL_BLOCK_SIZE, "GCTestLS", 1002, 64L << 20));
    ASSERT_EQ(OB_SUCCESS, ls_service_->iter_allocator_.init(
        OB_MALLOC_NORMAL_BLOCK_SIZE, "GCTestIter", 1002, 64L << 20));
    ASSERT_EQ(OB_SUCCESS, ls_service_->ls_map_.init(1002, &ls_service_->ls_allocator_));
    // unique_ptr owns this LS. Keep an explicit reference until it is removed
    // from the real map, so map cleanup never frees the fixture's allocation.
    ASSERT_EQ(OB_SUCCESS, ls_->get_ref_mgr().inc(storage::ObLSGetMod::OBSERVER_MOD));
    fixture_ref_ = true;
    ASSERT_EQ(OB_SUCCESS, ls_service_->ls_map_.add_ls(*ls_));
    map_added_ = true;
    ls_service_->is_inited_ = true;
    ls_service_->tenant_id_ = 1002;
    tenant_.set<storage::ObLSService *>(ls_service_.get());
    collector_.ls_service_ = ls_service_.get();
    collector_.sql_proxy_ = sql_proxy_.get();
    collector_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, revoke());
    finish_revoke();
  }
  void TearDown() override
  {
    collector_.is_inited_ = false;
    collector_.ls_service_ = nullptr;
    collector_.sql_proxy_ = nullptr;
    if (map_added_) { EXPECT_EQ(OB_SUCCESS, ls_service_->ls_map_.del_ls(ObLSID(1001))); }
    if (fixture_ref_) { (void)ls_->get_ref_mgr().dec(storage::ObLSGetMod::OBSERVER_MOD); }
    tenant_.set<storage::ObLSService *>(nullptr);
    if (ls_service_) {
      ls_service_->is_stopped_ = true;
      ls_service_.reset();
    }
    tenant_.set<archive::ObArchiveService *>(nullptr);
    archive_.reset();
    GCTX.sql_proxy_ = old_sql_proxy_;
    sql_proxy_.reset();
    TestGCInterleaving::TearDown();
    lib::set_tenant_memory_limit(1002, old_memory_limit_);
  }
  int replay_offline()
  {
    ObGCLSLog log(ObGCLSLOGType::OFFLINE_LS);
    char buffer[64];
    int64_t pos = 0;
    int ret = log.serialize(buffer, sizeof(buffer), pos);
    if (OB_SUCCESS == ret) {
      SCN scn;
      ret = scn.convert_for_logservice(101);
      if (OB_SUCCESS == ret) { ret = gc_->replay(buffer, pos, palf::LSN(100), scn); }
    }
    return ret;
  }
  void take_over()
  {
    // Drive the GC/Apply/LogHandler portion after replay; election and
    // Transport's network initialization remain outside this fixture.
    ASSERT_EQ(OB_SUCCESS, apply_.switch_to_leader(11, palf::SyncMode::SYNC));
    ls_->log_handler_.switch_role_and_sync_mode(LEADER, 11, palf::SyncMode::SYNC);
    control_.role_.store(LEADER);
    RCDiagnoseInfo diagnose;
    ASSERT_EQ(OB_SUCCESS, handlers_.switch_to_leader(diagnose));
    EXPECT_TRUE(apply_.is_ls_gc_state());
    EXPECT_TRUE(transport_.is_ls_gc_state());
  }
  void scan_gc_once()
  {
    ObGarbageCollector::ObGCCandidateArray candidates;
    const int before_queries = control_.status_queries_;
    collector_.gc_check_ls_status_(candidates);
    ASSERT_EQ(before_queries + 1, control_.status_queries_);
    ASSERT_EQ(1, candidates.count());
    EXPECT_EQ(ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE, candidates.at(0).ls_status_);
    collector_.execute_gc_(candidates);
    EXPECT_EQ(0, control_.append_calls_);
    EXPECT_EQ(2, ls_->get_ref_mgr().get_total_ref_cnt()); // Fixture + LS map only.
  }
  void expect_state(const LSGCState expected)
  {
    LSGCState state;
    SCN scn;
    ASSERT_EQ(OB_SUCCESS, ls_->get_gc_state(state));
    ASSERT_EQ(OB_SUCCESS, ls_->get_offline_scn(scn));
    EXPECT_EQ(expected, state);
    if (expected == LSGCState::LS_BLOCKED) {
      EXPECT_FALSE(scn.is_valid());
    } else {
      EXPECT_EQ(101, scn.get_val_for_logservice());
      EXPECT_TRUE(apply_.is_ls_gc_state());
      EXPECT_TRUE(transport_.is_ls_gc_state());
    }
    EXPECT_TRUE(gc_->get_rec_scn().is_max());
  }
  std::unique_ptr<storage::ObLSService> ls_service_;
  std::unique_ptr<archive::ObArchiveService> archive_;
  std::unique_ptr<ObMySQLProxy> sql_proxy_;
  ObMySQLProxy *old_sql_proxy_ = nullptr;
  ObGarbageCollector collector_;
  int64_t old_memory_limit_ = 0;
  bool map_added_ = false;
  bool fixture_ref_ = false;
};

TEST_F(TestGCNewLeader, replayed_offline_resumes_only_after_all_gc_conditions)
{
  // Keep the row visible even if a broken implementation tries early deletion.
  control_.delete_ret_ = OB_EAGAIN;
  ASSERT_EQ(OB_SUCCESS, replay_offline());
  expect_state(LSGCState::LS_OFFLINE);
  scan_gc_once(); // Still a follower: even satisfied conditions cannot delete.
  EXPECT_EQ(0, control_.readable_checks_.load());
  EXPECT_EQ(0, control_.delete_calls_);
  take_over();

  control_.readable_scn_ = 100;
  scan_gc_once();
  expect_state(LSGCState::LS_OFFLINE);
  EXPECT_EQ(0, control_.sync_queries_);
  EXPECT_EQ(0, control_.delete_calls_);

  control_.readable_scn_ = 101;
  control_.sync_scn_ = 100;
  scan_gc_once();
  expect_state(LSGCState::LS_OFFLINE);
  EXPECT_EQ(1, control_.sync_queries_);
  EXPECT_EQ(0, control_.archive_queries_);
  EXPECT_EQ(0, control_.delete_calls_);

  control_.sync_scn_ = 101;
  control_.archive_scn_ = 100;
  scan_gc_once();
  expect_state(LSGCState::LS_OFFLINE);
  EXPECT_EQ(1, control_.archive_queries_);
  EXPECT_EQ(0, control_.delete_calls_);

  control_.archive_scn_ = 101;
  control_.delete_ret_ = OB_SUCCESS;
  scan_gc_once();
  expect_state(LSGCState::WAIT_GC);
  EXPECT_EQ(1, control_.delete_calls_);
  EXPECT_TRUE(control_.status_deleted_);
  EXPECT_EQ(2, control_.slog_calls_.load()); // OFFLINE, then WAIT_GC.
}

TEST_F(TestGCNewLeader, failed_offline_replay_retries_before_takeover_cleanup)
{
  control_.fail_first_slog_ = true;
  ASSERT_EQ(OB_EAGAIN, replay_offline());
  expect_state(LSGCState::LS_BLOCKED);
  scan_gc_once();
  EXPECT_EQ(0, control_.delete_calls_);
  EXPECT_EQ(FOLLOWER, control_.role_.load());
  ASSERT_EQ(OB_SUCCESS, replay_offline());
  expect_state(LSGCState::LS_OFFLINE);
  take_over();
  scan_gc_once();
  expect_state(LSGCState::WAIT_GC);
  EXPECT_EQ(1, control_.delete_calls_);
  EXPECT_EQ(3, control_.slog_calls_.load());
}

TEST_F(TestGCNewLeader, status_delete_failure_retries_from_wait_gc_without_new_log)
{
  ASSERT_EQ(OB_SUCCESS, replay_offline());
  take_over();
  control_.delete_ret_ = OB_EAGAIN;
  scan_gc_once();
  expect_state(LSGCState::WAIT_GC);
  EXPECT_FALSE(control_.status_deleted_);
  const int persisted = control_.slog_calls_.load();
  const int readable_checks = control_.readable_checks_.load();
  control_.delete_ret_ = OB_SUCCESS;
  scan_gc_once();
  expect_state(LSGCState::WAIT_GC);
  EXPECT_EQ(2, control_.delete_calls_);
  EXPECT_TRUE(control_.status_deleted_);
  EXPECT_EQ(persisted, control_.slog_calls_.load());
  EXPECT_EQ(readable_checks, control_.readable_checks_.load());
}

TEST_F(TestGCNewLeader, duplicate_offline_replay_preserves_wait_gc_and_offline_scn)
{
  ASSERT_EQ(OB_SUCCESS, replay_offline());
  take_over();
  scan_gc_once();
  expect_state(LSGCState::WAIT_GC);
  const int persisted = control_.slog_calls_.load();
  ASSERT_EQ(OB_SUCCESS, replay_offline());
  expect_state(LSGCState::WAIT_GC);
  EXPECT_EQ(persisted, control_.slog_calls_.load());
  EXPECT_EQ(0, control_.append_calls_);
  EXPECT_EQ(1, control_.delete_calls_);
}
} // namespace unittest

namespace transaction
{
int ObTsMgr::get_gts(const uint64_t, const MonotonicTs, ObTsCbTask *,
                    share::SCN &scn, MonotonicTs &)
{ return scn.convert_for_logservice(100); }
}
namespace logservice
{
int ObLogService::open_palf(const share::ObLSID &, palf::PalfHandleGuard &guard)
{
  guard.set(&unittest::interleaving_control->palf_, nullptr);
  return OB_SUCCESS;
}
}
namespace palf
{
int PalfHandle::append(const PalfAppendOptions &, const void *, const int64_t,
                       const share::SCN &, logservice::AppendCb *, LSN &lsn, share::SCN &scn)
{
  auto &control = *unittest::interleaving_control;
  ++control.append_calls_;
  lsn = LSN(100);
  scn.convert_for_logservice(101);
  control.appended_.signal();
  return OB_SUCCESS;
}
int PalfHandle::get_role(common::ObRole &role, int64_t &proposal_id, bool &pending) const
{
  role = unittest::interleaving_control->role_.load();
  proposal_id = 10;
  pending = false;
  return OB_SUCCESS;
}
int PalfHandle::get_sync_mode(int64_t &mode_version, ipalf::SyncMode &sync_mode) const
{
  mode_version = 11;
  sync_mode = ipalf::SyncMode::SYNC;
  return OB_SUCCESS;
}
int PalfHandle::get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
{
  mode_version = 11;
  access_mode = AccessMode::APPEND;
  return OB_SUCCESS;
}
}
namespace rootserver
{
int ObTenantInfoLoader::get_readable_scn(share::SCN &scn)
{
  auto &control = *unittest::interleaving_control;
  ++control.readable_checks_;
  if (OB_SUCCESS != control.readable_ret_) { return control.readable_ret_; }
  return scn.convert_for_logservice(control.readable_scn_);
}
int ObLSServiceHelper::get_ls_replica_sync_scn(const uint64_t, const share::ObLSID &, share::SCN &scn)
{
  auto &control = *unittest::interleaving_control;
  ++control.sync_queries_;
  return scn.convert_for_logservice(control.sync_scn_);
}
}
namespace archive
{
int ObArchiveService::check_tenant_in_archive(bool &in_archive)
{
  in_archive = true;
  return OB_SUCCESS;
}
int ObArchiveService::get_ls_archive_progress(const share::ObLSID &, palf::LSN &lsn,
                                             share::SCN &scn, bool &force_wait, bool &ignore)
{
  auto &control = *unittest::interleaving_control;
  ++control.archive_queries_;
  lsn = palf::LSN(200);
  force_wait = true;
  ignore = false;
  return scn.convert_for_logservice(control.archive_scn_);
}
}
namespace share
{
int ObLSStatusOperator::get_ls_status_info(const uint64_t tenant_id, const ObLSID &id,
                                          ObLSStatusInfo &info, common::ObISQLClient &, const int32_t)
{
  auto &control = *unittest::interleaving_control;
  ++control.status_queries_;
  if (control.status_deleted_) { return OB_ENTRY_NOT_EXIST; }
  info.tenant_id_ = tenant_id;
  info.ls_id_ = id;
  info.status_ = OB_LS_WAIT_OFFLINE;
  return OB_SUCCESS;
}
int ObLSLifeAgentManager::drop_ls(const uint64_t &, const ObLSID &,
                                 const ObTenantSwitchoverStatus &)
{
  auto &control = *unittest::interleaving_control;
  logservice::LSGCState state = logservice::LSGCState::INVALID_LS_GC_STATE;
  EXPECT_EQ(OB_SUCCESS, control.ls_->get_gc_state(state));
  EXPECT_EQ(logservice::LSGCState::WAIT_GC, state);
  ++control.delete_calls_;
  if (OB_SUCCESS == control.delete_ret_) { control.status_deleted_ = true; }
  return control.delete_ret_;
}
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_gc_callback_interleaving.log", true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
