/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 */
#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <memory>
#include "lib/thread/thread_mgr.h"
#define private public
#define protected public
#include "logservice/ob_garbage_collector.h"
#include "logservice/ob_log_service.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "logservice/rcservice/ob_role_change_handler.h"
#include "storage/ls/ob_ls.h"
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

// This executable controls GTS, append, persistence, role/readable queries
// and transaction boundaries. GC orchestration, callback, lock and RC code
// are real.
struct GCSubmitControl
{
  ObGCHandler *gc_ = nullptr;
  palf::PalfHandle palf_;
  int block_calls_ = 0;
  std::promise<AppendCb *> appended_;
  std::promise<void> metadata_retry_;
  int gts_ret_ = OB_SUCCESS;
  int append_ret_ = OB_SUCCESS;
  int append_calls_ = 0;
  int state_updates_ = 0;
  int retry_updates_ = 0;
};
GCSubmitControl *submit_control = nullptr;

template <typename T>
std::future_status wait_ready(std::future<T> &future, const int timeout_ms)
{
  // Poll readiness: the tenant syscall hook in some dependency builds does
  // not support libstdc++'s timed FUTEX_WAIT_BITSET operation.
  for (int i = 0; i < timeout_ms; ++i) {
    if (std::future_status::ready == future.wait_for(std::chrono::seconds(0))) {
      return std::future_status::ready;
    }
    ob_usleep(1000);
  }
  return future.wait_for(std::chrono::seconds(0));
}

void expect_gc_write_locked(ObGCHandler &gc)
{
  const bool acquired = gc.rwlock_.try_rdlock();
  EXPECT_FALSE(acquired);
  // A lock assertion failure must not itself leave a reader holding the lock.
  if (acquired) { gc.rwlock_.rdunlock(); }
}

class TestGCCallbackLock : public ::testing::Test
{
protected:
  void SetUp() override
  {
    submit_control = &control_;
    share::ObTenantEnv::set_tenant(&tenant_);
    MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::PRIMARY_TENANT);
    log_service_ = std::make_unique<ObLogService>();
    info_loader_ = std::make_unique<rootserver::ObTenantInfoLoader>();
    tenant_.set<ObLogService *>(log_service_.get());
    tenant_.set<rootserver::ObTenantInfoLoader *>(info_loader_.get());
    ls_ = std::make_unique<storage::ObLS>();
    ASSERT_EQ(OB_SUCCESS, ls_->ls_meta_.init(OB_SYS_TENANT_ID, ObLSID(1001),
        storage::ObMigrationStatus::OB_MIGRATION_STATUS_NONE,
        ObLSRestoreStatus(ObLSRestoreStatus::NONE), SCN::min_scn(),
        storage::ObMajorMVMergeInfo(), ObLSStoreFormat()));
    ls_->ls_meta_.gc_state_ = LSGCState::LS_BLOCKED;
    gc_ = ls_->get_gc_handler();
    ASSERT_EQ(OB_SUCCESS, gc_->init(ls_.get()));
    control_.gc_ = gc_;
    ASSERT_EQ(OB_SUCCESS, handlers_.register_handler(GC_LS_LOG_BASE_TYPE, gc_));
  }
  void TearDown() override
  {
    ls_.reset();
    tenant_.set<ObLogService *>(nullptr);
    tenant_.set<rootserver::ObTenantInfoLoader *>(nullptr);
    info_loader_.reset();
    log_service_.reset();
    share::ObTenantEnv::set_tenant(nullptr);
    submit_control = nullptr;
  }
  std::future<void> submit(const ObGCLSLOGType type)
  {
    if (ObGCLSLOGType::BLOCK_TABLET_TRANSFER_IN == type) {
      ls_->ls_meta_.gc_state_ = LSGCState::NORMAL;
    }
    return std::async(std::launch::async, [this, type] {
      share::ObTenantEnv::set_tenant(&tenant_);
      auto status = ObGCLSLOGType::OFFLINE_LS == type
          ? ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE
          : ObGarbageCollector::LSStatus::LS_DROPPING;
      gc_->execute_pre_gc_process(status);
      // The handler must release its write locks before returning.
      const bool unlocked = gc_->rwlock_.try_wrlock();
      EXPECT_TRUE(unlocked);
      if (unlocked) { gc_->rwlock_.wrunlock(); }
      share::ObTenantEnv::set_tenant(nullptr);
    });
  }
  void check_callback_wait(const bool success)
  {
    ObGCHandler::ObGCLSLogCb cb(gc_);
    ASSERT_EQ(OB_SUCCESS, cb.scn_.convert_for_logservice(101));
    std::promise<void> started;
    auto entered = started.get_future();
    auto work = std::async(std::launch::async, [this, &cb, &started] {
      share::ObTenantEnv::set_tenant(&tenant_);
      started.set_value();
      gc_->wait_gc_log_callback_(ObGCLSLOGType::OFFLINE_LS, cb, ObTimeUtility::current_time());
      EXPECT_TRUE(cb.is_succeed() || cb.is_failed());
      share::ObTenantEnv::set_tenant(nullptr);
    });
    EXPECT_EQ(std::future_status::ready, wait_ready(entered, 1000));
    EXPECT_EQ(std::future_status::timeout, wait_ready(work, 50));
    const bool unlocked = gc_->rwlock_.try_wrlock();
    EXPECT_TRUE(unlocked);
    if (unlocked) { gc_->rwlock_.wrunlock(); }
    EXPECT_EQ(OB_SUCCESS, success ? cb.on_success() : cb.on_failure());
    work.get();
    EXPECT_EQ(0, control_.state_updates_);
    EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
    if (success) {
      EXPECT_EQ(cb.scn_, gc_->get_rec_scn());
    } else {
      EXPECT_TRUE(gc_->get_rec_scn().is_max());
    }
  }
  void check_rc_during_callback_wait(const int operation, const bool success)
  {
    auto appended = control_.appended_.get_future();
    auto gc_work = submit(ObGCLSLOGType::OFFLINE_LS);
    ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
    AppendCb *cb = appended.get();
    ASSERT_NE(nullptr, cb);
    EXPECT_EQ(std::future_status::timeout, gc_work.wait_for(std::chrono::seconds(0)));
    auto role_change = std::async(std::launch::async, [this, operation] {
      RCDiagnoseInfo diagnose;
      switch (operation) {
      case 0: return handlers_.switch_to_follower_gracefully();
      case 1: handlers_.switch_to_follower_forcedly(); return OB_SUCCESS;
      case 2: return handlers_.switch_to_leader(diagnose);
      default: return handlers_.resume_to_leader();
      }
    });
    const auto status = wait_ready(role_change, 1000);
    EXPECT_EQ(0, control_.state_updates_);
    EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
    // Release the real stack callback even on the baseline, so a failed
    // liveness assertion cannot leave the test hanging at thread teardown.
    EXPECT_EQ(OB_SUCCESS, success ? cb->on_success() : cb->on_failure());
    gc_work.get();
    EXPECT_EQ(OB_SUCCESS, role_change.get());
    EXPECT_EQ(std::future_status::ready, status);
    EXPECT_EQ(success ? 1 : 0, control_.state_updates_);
    EXPECT_EQ(success ? LSGCState::LS_OFFLINE : LSGCState::LS_BLOCKED,
              ls_->ls_meta_.gc_state_);
    ASSERT_TRUE(gc_->rwlock_.try_wrlock());
    gc_->rwlock_.wrunlock();
  }
  ObTenantBase tenant_{OB_SYS_TENANT_ID};
  std::unique_ptr<storage::ObLS> ls_;
  std::unique_ptr<ObLogService> log_service_;
  std::unique_ptr<rootserver::ObTenantInfoLoader> info_loader_;
  ObGCHandler *gc_ = nullptr;
  ObRoleChangeHandler handlers_;
  GCSubmitControl control_;
};

TEST_F(TestGCCallbackLock, graceful_revoke_progresses_before_callback)
{ check_rc_during_callback_wait(0, true); }
TEST_F(TestGCCallbackLock, forced_revoke_progresses_before_callback)
{ check_rc_during_callback_wait(1, true); }
TEST_F(TestGCCallbackLock, takeover_progresses_before_callback)
{ check_rc_during_callback_wait(2, true); }
TEST_F(TestGCCallbackLock, resume_progresses_before_callback)
{ check_rc_during_callback_wait(3, true); }
TEST_F(TestGCCallbackLock, failed_callback_does_not_advance_gc_state)
{ check_rc_during_callback_wait(0, false); }

TEST_F(TestGCCallbackLock, callback_wait_preserves_committed_checkpoint_and_gc_state)
{ check_callback_wait(true); }
TEST_F(TestGCCallbackLock, failed_callback_wait_preserves_gc_state)
{ check_callback_wait(false); }

TEST_F(TestGCCallbackLock, failed_append_releases_orchestration_lock_without_waiting)
{
  control_.append_ret_ = OB_NOT_MASTER;
  auto work = submit(ObGCLSLOGType::OFFLINE_LS);
  EXPECT_EQ(std::future_status::ready, wait_ready(work, 1000));
  work.get();
  EXPECT_EQ(1, control_.append_calls_);
  EXPECT_EQ(0, control_.state_updates_);
  EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
}

TEST_F(TestGCCallbackLock, gts_failure_does_not_append_or_advance_gc)
{
  control_.gts_ret_ = OB_TIMEOUT;
  auto work = submit(ObGCLSLOGType::OFFLINE_LS);
  EXPECT_EQ(std::future_status::ready, wait_ready(work, 1000));
  work.get();
  EXPECT_EQ(0, control_.append_calls_);
  EXPECT_EQ(0, control_.state_updates_);
  EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
}

TEST_F(TestGCCallbackLock, failed_append_can_be_retried)
{
  control_.append_ret_ = OB_NOT_MASTER;
  auto first = submit(ObGCLSLOGType::OFFLINE_LS);
  first.get();
  control_.append_ret_ = OB_SUCCESS;
  check_rc_during_callback_wait(0, true);
  EXPECT_EQ(2, control_.append_calls_);
  EXPECT_TRUE(gc_->get_rec_scn().is_max());
}

TEST_F(TestGCCallbackLock, failed_callback_can_be_retried)
{
  check_rc_during_callback_wait(0, false);
  EXPECT_TRUE(gc_->get_rec_scn().is_max());
  control_.appended_ = std::promise<AppendCb *>();
  check_rc_during_callback_wait(0, true);
  EXPECT_EQ(2, control_.append_calls_);
  EXPECT_TRUE(gc_->get_rec_scn().is_max());
}

TEST_F(TestGCCallbackLock, duplicate_block_callback_does_not_offline_ls)
{
  auto appended = control_.appended_.get_future();
  auto work = submit(ObGCLSLOGType::BLOCK_TABLET_TRANSFER_IN);
  ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
  AppendCb *cb = appended.get();
  ObGCLSLog log(ObGCLSLOGType::BLOCK_TABLET_TRANSFER_IN);
  char buffer[64];
  int64_t pos = 0;
  SCN replay_scn;
  ASSERT_EQ(OB_SUCCESS, log.serialize(buffer, sizeof(buffer), pos));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_logservice(150));
  EXPECT_EQ(OB_SUCCESS, gc_->replay(buffer, pos, palf::LSN(200), replay_scn));
  EXPECT_EQ(1, control_.state_updates_);
  auto rc = std::async(std::launch::async, [this] {
    return handlers_.switch_to_follower_gracefully();
  });
  const auto status = wait_ready(rc, 1000);
  EXPECT_EQ(OB_SUCCESS, cb->on_success());
  work.get();
  EXPECT_EQ(OB_SUCCESS, rc.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
  EXPECT_EQ(1, control_.state_updates_);
  EXPECT_EQ(1, control_.block_calls_);
  EXPECT_TRUE(gc_->get_rec_scn().is_max());
}

TEST_F(TestGCCallbackLock, gc_wait_does_not_block_role_change_for_another_ls)
{
  ObGCHandler other_gc;
  ObRoleChangeHandler other_handlers;
  ASSERT_EQ(OB_SUCCESS, other_handlers.register_handler(GC_LS_LOG_BASE_TYPE, &other_gc));
  auto appended = control_.appended_.get_future();
  auto work = submit(ObGCLSLOGType::OFFLINE_LS);
  ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
  AppendCb *cb = appended.get();
  // Execute two LS events in order on the same thread, as the tenant RC does.
  auto rc = std::async(std::launch::async, [this, &other_handlers] {
    int ret = handlers_.switch_to_follower_gracefully();
    RCDiagnoseInfo diagnose;
    if (OB_SUCCESS == ret) { ret = other_handlers.switch_to_leader(diagnose); }
    return ret;
  });
  const auto status = wait_ready(rc, 1000);
  EXPECT_EQ(std::future_status::timeout, work.wait_for(std::chrono::seconds(0)));
  EXPECT_EQ(OB_SUCCESS, cb->on_failure());
  work.get();
  EXPECT_EQ(OB_SUCCESS, rc.get());
  EXPECT_EQ(std::future_status::ready, status);
}

TEST_F(TestGCCallbackLock, replay_during_callback_wait_does_not_regress_offline_scn)
{
  ObGCLSLog log(ObGCLSLOGType::OFFLINE_LS);
  char buffer[256];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, log.serialize(buffer, sizeof(buffer), pos));
  SCN replay_scn;
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_logservice(150));
  auto appended = control_.appended_.get_future();
  auto work = submit(ObGCLSLOGType::OFFLINE_LS);
  ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
  AppendCb *cb = appended.get();
  auto replay = std::async(std::launch::async, [this, &buffer, pos, replay_scn] {
    share::ObTenantEnv::set_tenant(&tenant_);
    const int ret = gc_->replay(buffer, pos, palf::LSN(200), replay_scn);
    share::ObTenantEnv::set_tenant(nullptr);
    return ret;
  });
  const auto status = wait_ready(replay, 1000);
  EXPECT_EQ(OB_SUCCESS, cb->on_success());
  work.get();
  EXPECT_EQ(OB_SUCCESS, replay.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(1, control_.state_updates_);
  EXPECT_EQ(replay_scn, ls_->ls_meta_.offline_scn_);
  EXPECT_TRUE(gc_->get_rec_scn().is_max());
}

TEST_F(TestGCCallbackLock, metadata_retry_does_not_block_rc)
{
  control_.retry_updates_ = 1;
  auto appended = control_.appended_.get_future();
  auto retry = control_.metadata_retry_.get_future();
  auto work = submit(ObGCLSLOGType::OFFLINE_LS);
  ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
  EXPECT_EQ(OB_SUCCESS, appended.get()->on_success());
  ASSERT_EQ(std::future_status::ready, wait_ready(retry, 5000));
  auto resume = std::async(std::launch::async, [this] {
    return handlers_.resume_to_leader();
  });
  const auto status = wait_ready(resume, 250);
  work.get();
  EXPECT_EQ(OB_SUCCESS, resume.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(1, control_.state_updates_);
}

TEST_F(TestGCCallbackLock, block_transfer_callback_wait_also_allows_rc)
{
  auto appended = control_.appended_.get_future();
  auto work = submit(ObGCLSLOGType::BLOCK_TABLET_TRANSFER_IN);
  ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
  AppendCb *cb = appended.get();
  auto revoke = std::async(std::launch::async, [this] {
    return handlers_.switch_to_follower_gracefully();
  });
  const auto status = wait_ready(revoke, 1000);
  EXPECT_EQ(OB_SUCCESS, cb->on_failure());
  work.get();
  EXPECT_EQ(OB_SUCCESS, revoke.get());
  EXPECT_EQ(std::future_status::ready, status);
}

class TestGCCallbackDowngrade : public TestGCCallbackLock
{
protected:
  void SetUp() override
  {
    TestGCCallbackLock::SetUp();
    ASSERT_EQ(OB_SUCCESS, TG_CREATE(lib::TGDefIDs::ApplyService, service_.tg_id_));
    service_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, service_.start());
    apply_.is_inited_ = true;
    apply_.is_in_stop_state_ = false;
    apply_.role_ = LEADER;
    apply_.proposal_id_ = 10;
    apply_.is_sync_mode_ = true;
    apply_.palf_committed_end_lsn_ = palf::LSN(200);
    apply_.standby_committed_end_lsn_ = palf::LSN(100);
    apply_.min_committed_end_lsn_ = palf::LSN(100);
    apply_.ap_sv_ = &service_;
    apply_.inc_ref(); // Keep the fixture alive after Apply tasks release refs.
    ASSERT_EQ(OB_SUCCESS, apply_.submit_task_.init(&apply_));
    for (int64_t i = 0; i < APPLY_TASK_QUEUE_SIZE; ++i) {
      ASSERT_EQ(OB_SUCCESS, apply_.cb_queues_[i].init(&apply_, i));
    }
  }
  void TearDown() override
  {
    if (service_.tg_id_ >= 0) {
      TG_STOP(service_.tg_id_);
      TG_WAIT(service_.tg_id_);
      TG_DESTROY(service_.tg_id_);
      service_.tg_id_ = -1;
    }
    TestGCCallbackLock::TearDown();
  }
  bool wait_apply_idle()
  {
    for (int i = 0; i < 5000; ++i) {
      if (1 == ATOMIC_LOAD(&apply_.ref_cnt_)) { return true; }
      ob_usleep(1000);
    }
    return 1 == ATOMIC_LOAD(&apply_.ref_cnt_);
  }
  void enqueue(AppendCb *cb, const palf::LSN &lsn)
  {
    cb->__set_lsn(lsn);
    ASSERT_EQ(OB_SUCCESS, apply_.cb_queues_[0].push(AppendCb::__get_member_address(cb)));
    apply_.cb_queues_[0].inc_total_submit_cb_cnt();
    bool timeslice_out = false;
    ASSERT_EQ(OB_SUCCESS, apply_.try_handle_cb_queue(&apply_.cb_queues_[0], timeslice_out));
    ASSERT_NE(nullptr, apply_.cb_queues_[0].top());
  }
  void check_uncommitted_log(const palf::SyncMode mode)
  {
    auto appended = control_.appended_.get_future();
    auto work = submit(ObGCLSLOGType::OFFLINE_LS);
    ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
    AppendCb *cb = appended.get();
    enqueue(cb, palf::LSN(250)); // Past the local committed end (200).
    auto rc = std::async(std::launch::async, [this, mode] {
      int ret = handlers_.switch_to_follower_gracefully();
      if (OB_SUCCESS == ret) { ret = apply_.switch_sync_mode(11, mode); }
      return ret;
    });
    const auto status = wait_ready(rc, 1000);
    if (std::future_status::ready == status) {
      EXPECT_EQ(OB_SUCCESS, rc.get());
      EXPECT_TRUE(wait_apply_idle());
      EXPECT_EQ(std::future_status::timeout, work.wait_for(std::chrono::seconds(0)));
      {
        // Also make an incorrectly confirmed callback safe to inspect: its
        // metadata update may be running while this assertion detects it.
        SpinRLockGuard guard(gc_->rwlock_);
        EXPECT_EQ(0, control_.state_updates_);
        EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
      }
      EXPECT_FALSE(apply_.is_ls_gc_state_);
      EXPECT_EQ(100, apply_.standby_committed_end_lsn_.val_);
      // Follower transition must fail this uncommitted callback, not confirm it.
      EXPECT_EQ(OB_SUCCESS, apply_.switch_to_follower());
    } else {
      // Release the original GC lock before joining the blocked RC thread.
      EXPECT_EQ(OB_SUCCESS, apply_.cb_queues_[0].pop());
      apply_.cb_queues_[0].inc_total_apply_cb_cnt();
      EXPECT_EQ(OB_SUCCESS, cb->on_failure());
    }
    work.get();
    if (rc.valid()) { EXPECT_EQ(OB_SUCCESS, rc.get()); }
    EXPECT_EQ(std::future_status::ready, status);
    EXPECT_EQ(0, control_.state_updates_);
    EXPECT_EQ(LSGCState::LS_BLOCKED, ls_->ls_meta_.gc_state_);
  }
  void check_downgrade(const palf::SyncMode mode)
  {
    auto appended = control_.appended_.get_future();
    auto work = submit(ObGCLSLOGType::OFFLINE_LS);
    ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
    AppendCb *cb = appended.get();
    cb->__set_lsn(palf::LSN(150));
    ASSERT_EQ(OB_SUCCESS, apply_.cb_queues_[0].push(AppendCb::__get_member_address(cb)));
    apply_.cb_queues_[0].inc_total_submit_cb_cnt();
    bool timeslice_out = false;
    ASSERT_EQ(OB_SUCCESS, apply_.try_handle_cb_queue(&apply_.cb_queues_[0], timeslice_out));
    ASSERT_NE(nullptr, apply_.cb_queues_[0].top());
    EXPECT_EQ(0, control_.state_updates_);
    auto rc = std::async(std::launch::async, [this, mode] {
      int ret = handlers_.switch_to_follower_gracefully();
      if (OB_SUCCESS == ret) { ret = apply_.switch_sync_mode(11, mode); }
      return ret;
    });
    // No new append, commit update or standby ACK is provided. RC must run
    // the downgrade and wake the real Apply worker to finish GC's callback.
    const auto status = wait_ready(work, 1500);
    if (std::future_status::ready != status) {
      // Rescue the baseline by providing the missing ACK, then drain threads.
      ATOMIC_STORE(&apply_.min_committed_end_lsn_.val_, 200);
      EXPECT_EQ(OB_SUCCESS, apply_.try_handle_cb_queue(&apply_.cb_queues_[0], timeslice_out));
    }
    work.get();
    EXPECT_EQ(OB_SUCCESS, rc.get());
    EXPECT_EQ(std::future_status::ready, status);
    EXPECT_EQ(1, control_.state_updates_);
    EXPECT_EQ(100, apply_.standby_committed_end_lsn_.val_);
  }
  ObLogApplyService service_;
  ObApplyStatus apply_;
};

TEST_F(TestGCCallbackDowngrade, pre_async_completes_gc_without_standby_ack)
{ check_downgrade(palf::SyncMode::PRE_ASYNC); }
TEST_F(TestGCCallbackDowngrade, async_completes_gc_without_standby_ack)
{ check_downgrade(palf::SyncMode::ASYNC); }

TEST_F(TestGCCallbackDowngrade, pre_async_does_not_confirm_uncommitted_gc_log)
{ check_uncommitted_log(palf::SyncMode::PRE_ASYNC); }
TEST_F(TestGCCallbackDowngrade, async_does_not_confirm_uncommitted_gc_log)
{ check_uncommitted_log(palf::SyncMode::ASYNC); }

TEST_F(TestGCCallbackDowngrade, sync_still_requires_standby_ack_after_rc_progresses)
{
  auto appended = control_.appended_.get_future();
  auto work = submit(ObGCLSLOGType::OFFLINE_LS);
  ASSERT_EQ(std::future_status::ready, wait_ready(appended, 5000));
  AppendCb *cb = appended.get();
  enqueue(cb, palf::LSN(150));
  auto rc = std::async(std::launch::async, [this] {
    return handlers_.switch_to_follower_gracefully();
  });
  const auto status = wait_ready(rc, 1000);
  EXPECT_EQ(std::future_status::timeout, work.wait_for(std::chrono::seconds(0)));
  EXPECT_TRUE(apply_.is_sync_mode_);
  EXPECT_FALSE(apply_.is_ls_gc_state_);
  EXPECT_EQ(0, control_.state_updates_);
  SCN end_scn;
  ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_logservice(200));
  EXPECT_EQ(OB_SUCCESS, apply_.update_standby_committed_end_lsn(palf::LSN(200), end_scn));
  work.get();
  EXPECT_EQ(OB_SUCCESS, rc.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(1, control_.state_updates_);
}
} // namespace unittest

namespace transaction
{
int ObTsMgr::get_gts(const uint64_t, const MonotonicTs, ObTsCbTask *,
                    share::SCN &scn, MonotonicTs &)
{
  if (OB_SUCCESS != unittest::submit_control->gts_ret_) {
    return unittest::submit_control->gts_ret_;
  }
  return scn.convert_for_logservice(100);
}
} // namespace transaction

namespace logservice
{
int ObLogService::open_palf(const share::ObLSID &, palf::PalfHandleGuard &guard)
{
  guard.set(&unittest::submit_control->palf_, nullptr);
  return OB_SUCCESS;
}
int ObLogHandler::append(const void *, const int64_t, const share::SCN &,
                         const bool, const bool, AppendCb *cb,
                         palf::LSN &lsn, share::SCN &scn, const bool)
{
  auto &control = *unittest::submit_control;
  unittest::expect_gc_write_locked(*control.gc_);
  ++control.append_calls_;
  if (OB_SUCCESS != control.append_ret_) { return control.append_ret_; }
  lsn = palf::LSN(100);
  scn.convert_for_logservice(101);
  cb->__set_lsn(lsn);
  cb->__set_scn(scn);
  control.appended_.set_value(cb);
  return OB_SUCCESS;
}
} // namespace logservice

namespace palf
{
int PalfHandle::get_role(common::ObRole &role, int64_t &proposal_id, bool &pending) const
{
  unittest::expect_gc_write_locked(*unittest::submit_control->gc_);
  role = common::LEADER;
  proposal_id = 10;
  pending = false;
  return OB_SUCCESS;
}
}
namespace rootserver
{
int ObTenantInfoLoader::get_readable_scn(share::SCN &)
{
  unittest::expect_gc_write_locked(*unittest::submit_control->gc_);
  return OB_EAGAIN; // Observe cleanup without entering archive or SQL work.
}
}
namespace storage
{
int ObLS::block_all()
{
  auto &control = *unittest::submit_control;
  unittest::expect_gc_write_locked(*control.gc_);
  ++control.block_calls_;
  return OB_SUCCESS;
}
int ObLSTxService::check_all_tx_clean_up() const
{
  unittest::expect_gc_write_locked(*unittest::submit_control->gc_);
  return OB_EAGAIN;
}
int ObLS::set_gc_state(const logservice::LSGCState &state, const share::SCN &scn)
{
  auto &control = *unittest::submit_control;
  unittest::expect_gc_write_locked(*control.gc_);
  if (control.retry_updates_-- > 0) {
    control.metadata_retry_.set_value();
    return OB_EAGAIN;
  }
  ++control.state_updates_;
  ls_meta_.gc_state_ = state;
  if (logservice::LSGCState::LS_OFFLINE == state) { ls_meta_.offline_scn_ = scn; }
  return OB_SUCCESS;
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_gc_callback_lock.log", true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
