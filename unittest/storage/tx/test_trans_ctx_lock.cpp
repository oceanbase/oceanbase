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
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#define private public
#define protected public
#include "storage/tx/ob_trans_ctx_lock.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class TestCtxLock : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    lock_.reset();
  }
  virtual void TearDown() override {}
protected:
  CtxLock lock_;
};

// ---------------------------------------------------------------------------
// Basic lock/unlock functionality
// ---------------------------------------------------------------------------

TEST_F(TestCtxLock, lock_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.lock();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock();
}

TEST_F(TestCtxLock, try_lock_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.try_lock();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock();
}

TEST_F(TestCtxLock, wrlock_access_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.wrlock_access();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock_access();
}

TEST_F(TestCtxLock, wrlock_ctx_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.wrlock_ctx();
  ASSERT_EQ(OB_SUCCESS, ret);
  CtxLockArg arg;
  lock_.unlock_ctx(arg);
}

TEST_F(TestCtxLock, wrlock_flush_redo_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.wrlock_flush_redo();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock_flush_redo();
}

TEST_F(TestCtxLock, rdlock_flush_redo_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.rdlock_flush_redo();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock_flush_redo();
}

TEST_F(TestCtxLock, try_rdlock_ctx_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.try_rdlock_ctx();
  ASSERT_EQ(OB_SUCCESS, ret);
  CtxLockArg arg;
  lock_.unlock_ctx(arg);
}

TEST_F(TestCtxLock, try_wrlock_flush_redo_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.try_wrlock_flush_redo();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock_flush_redo();
}

TEST_F(TestCtxLock, try_rdlock_flush_redo_and_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = lock_.try_rdlock_flush_redo();
  ASSERT_EQ(OB_SUCCESS, ret);
  lock_.unlock_flush_redo();
}

// ---------------------------------------------------------------------------
// unlock() order: CTX -> FLUSH -> ACCESS (reverse of acquisition)
// ---------------------------------------------------------------------------

TEST_F(TestCtxLock, full_unlock_reverse_order)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // lock() acquires all three in order: ACCESS -> FLUSH_REDO -> CTX
  ASSERT_EQ(OB_SUCCESS, lock_.lock());
  // unlock() internally: unlock_ctx(arg) -> unlock_flush_redo -> unlock_access -> after_unlock(arg)
  lock_.unlock();
}

// ---------------------------------------------------------------------------
// unlock_ctx with CtxLockArg: before_unlock/after_unlock lifecycle
// ---------------------------------------------------------------------------

TEST_F(TestCtxLock, unlock_ctx_with_arg_fires_after_unlock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ASSERT_EQ(OB_SUCCESS, lock_.wrlock_access());
  ASSERT_EQ(OB_SUCCESS, lock_.wrlock_ctx());

  CtxLockArg arg;
  // unlock_ctx captures state via before_unlock while ctx_lock_ held
  lock_.unlock_ctx(arg);
  // ACCESS still held, after_unlock not yet called
  lock_.unlock_access();
  // Now all released, caller fires after_unlock
  lock_.after_unlock(arg);
}

// ---------------------------------------------------------------------------
// CtxLockGuard tests
// ---------------------------------------------------------------------------

TEST_F(TestCtxLock, guard_all_mode_sets_and_clears)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  {
    CtxLockGuard guard(lock_);
  }
  // guard destructor calls reset => all unlocked + after_unlock fired
}

TEST_F(TestCtxLock, guard_ctx_only)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  {
    CtxLockGuard guard(lock_, CtxLockGuard::MODE::CTX);
  }
}

TEST_F(TestCtxLock, guard_access_only)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  {
    CtxLockGuard guard(lock_, CtxLockGuard::MODE::ACCESS);
  }
}

TEST_F(TestCtxLock, guard_flush_redo_x_only)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  {
    CtxLockGuard guard(lock_, CtxLockGuard::MODE::REDO_FLUSH_X);
  }
}

TEST_F(TestCtxLock, guard_set_reacquires)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  CtxLockGuard guard;

  guard.set(lock_, CtxLockGuard::MODE::CTX);
  guard.reset();
}

TEST_F(TestCtxLock, guard_all_mode_unlock_order)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  {
    CtxLockGuard guard(lock_);
    // guard.reset() will: unlock_ctx(arg) -> unlock_flush_redo -> unlock_access -> after_unlock(arg)
  }
}

// ---------------------------------------------------------------------------
// Concurrent lock/unlock stress test
// ---------------------------------------------------------------------------

TEST_F(TestCtxLock, concurrent_lock_unlock_no_crash)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const int ROUNDS = 10000;
  std::atomic<bool> stop{false};

  // Thread 1: repeatedly lock/unlock full lock
  auto worker_full = [&]() {
    for (int i = 0; i < ROUNDS && !stop.load(); i++) {
      if (OB_SUCCESS == lock_.try_lock()) {
        lock_.unlock();
      }
    }
  };

  // Thread 2: repeatedly wrlock/unlock access
  auto worker_access = [&]() {
    for (int i = 0; i < ROUNDS && !stop.load(); i++) {
      if (OB_SUCCESS == lock_.access_lock_.try_wrlock(common::ObLatchIds::TRANS_ACCESS_LOCK)) {
        lock_.access_lock_.unlock();
      }
    }
  };

  std::thread t1(worker_full);
  std::thread t2(worker_access);
  t1.join();
  t2.join();
}

TEST_F(TestCtxLock, concurrent_guard_no_crash)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const int ROUNDS = 5000;

  auto worker = [&]() {
    for (int i = 0; i < ROUNDS; i++) {
      if (OB_SUCCESS == lock_.try_lock()) {
        lock_.unlock();
      }
    }
  };

  std::thread t1(worker);
  std::thread t2(worker);
  t1.join();
  t2.join();
}

} // namespace unittest
} // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_trans_ctx_lock.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
