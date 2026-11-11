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

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/task/ob_timer.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "plugin/external_table/ob_jdbc_catalog_plugin.h"
#define private public
#include "share/catalog/jdbc/ob_jdbc_connection_pool.h"
#undef private
#include "plugin/external_table/ob_external_java_plugin.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace
{
static_assert(!std::is_move_constructible<ObJdbcConnectionGuard>::value,
              "JDBC connection guards must stay with their original owner");
static_assert(!std::is_move_assignable<ObJdbcConnectionGuard>::value,
              "JDBC connection guards must not transfer ownership");

using ConnectionGuards = std::array<ObJdbcConnectionGuard, ObJdbcConnectionPool::MAX_CONNECTIONS>;

class JdbcCalls
{
public:
  int connect(const ObString &jdbc_url)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    connect_urls_.emplace_back(jdbc_url.ptr(), jdbc_url.length());
    ++opens_;
    cv_.notify_all();
    cv_.wait(lock, [&] { return !block_connect_; });
    return connect_ret_;
  }
  int disconnect()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    ++closes_;
    cv_.notify_all();
    cv_.wait(lock, [&] { return !block_close_; });
    return close_ret_;
  }
  bool await(int opens, int closes)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(5), [&] {
      return opens_ >= opens && closes_ >= closes;
    });
  }
  void unblock()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    block_connect_ = block_close_ = false;
    cv_.notify_all();
  }
  int connect_ret_ = OB_SUCCESS;
  int close_ret_ = OB_SUCCESS;
  bool block_connect_ = false;
  bool block_close_ = false;
  std::vector<std::string> connect_urls_;
  std::atomic<int> opens_{0};
  std::atomic<int> closes_{0};
private:
  std::mutex mutex_;
  std::condition_variable cv_;
};

JdbcCalls *jdbc_calls = nullptr;

// Use the existing Worker boundary to exercise cancellation.
class TestWorker : public lib::Worker
{
public:
  explicit TestWorker(std::atomic<int> &status) : previous_(lib::Worker::self_), status_(status)
  {
    lib::Worker::set_worker_to_thread_local(this);
  }
  ~TestWorker() override { lib::Worker::set_worker_to_thread_local(previous_); }
  int check_status() override { return status_.load(); }
private:
  lib::Worker *previous_;
  std::atomic<int> &status_;
};

bool await_waiter(ObJdbcConnectionPool &pool)
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  ObJdbcConnectionPoolStats stats;
  do {
    pool.snapshot(stats);
    if (stats.waiters_ > 0) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  } while (std::chrono::steady_clock::now() < deadline);
  return false;
}

void wait_until(int64_t deadline_ts)
{
  std::this_thread::sleep_until(std::chrono::system_clock::time_point(std::chrono::microseconds(deadline_ts)));
}
} // namespace

// Only this test executable substitutes the JDBC boundary. The pool itself is
// the production implementation, with no injected backend or test callbacks.
namespace oceanbase
{
namespace plugin
{
int ObJdbcCatalogPlugin::init(const ObString &)
{
  return OB_NOT_NULL(jdbc_calls) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObJdbcCatalogPlugin::connect(const ObString &jdbc_url, ObJavaGlobalRef &)
{
  return OB_NOT_NULL(jdbc_calls) ? jdbc_calls->connect(jdbc_url) : OB_ERR_UNEXPECTED;
}

int ObJdbcCatalogPlugin::disconnect(ObJavaGlobalRef &)
{
  return OB_NOT_NULL(jdbc_calls) ? jdbc_calls->disconnect() : OB_ERR_UNEXPECTED;
}
} // namespace plugin
} // namespace oceanbase

namespace
{
ObJdbcConnectionOptions options(uint64_t tenant = 1001, uint64_t catalog = 500, int64_t version = 123)
{
  ObJdbcConnectionOptions value;
  value.key_ = ObJdbcConnectionPoolKey(tenant, catalog, version);
  value.jdbc_url_ = ObString("jdbc:sqlserver://server;databaseName=test");
  return value;
}

class PoolTest : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    ASSERT_TRUE(OB_NOT_NULL(allocator));
    if (OB_NOT_NULL(allocator)) {
      ASSERT_EQ(OB_SUCCESS, allocator->create_and_add_tenant_allocator(1001));
      ASSERT_EQ(OB_SUCCESS, allocator->set_tenant_limit(1001, 128L * 1024 * 1024));
    }
  }
  static void TearDownTestCase()
  {
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    if (OB_NOT_NULL(allocator)) {
      EXPECT_EQ(OB_SUCCESS, allocator->recycle_tenant_allocator(1001));
    }
  }
  void SetUp() override
  {
    jdbc_calls = &jdbc_;
    ASSERT_EQ(OB_SUCCESS, pool_.init(options()));
  }
  void TearDown() override
  {
    pool_.drain(true);
    pool_.cleanup();
    jdbc_calls = nullptr;
  }
  int acquire(ObJdbcConnectionGuard &guard, int64_t deadline_ts = 0)
  {
    TestWorker worker(status_);
    return pool_.acquire(options().jdbc_url_,
                         deadline_ts > 0 ? deadline_ts : ObTimeUtility::current_time() + 60000000, guard);
  }
  void fill(ConnectionGuards &leases)
  {
    for (auto &lease : leases) {
      ASSERT_EQ(OB_SUCCESS, acquire(lease));
    }
  }
  ObJdbcConnectionPoolStats stats()
  {
    ObJdbcConnectionPoolStats value;
    pool_.snapshot(value);
    EXPECT_LE(value.total(), 20);
    EXPECT_GE(value.idle_, 0);
    EXPECT_GE(value.in_use_, 0);
    EXPECT_GE(value.creating_, 0);
    EXPECT_GE(value.closing_, 0);
    return value;
  }
  JdbcCalls jdbc_;
  std::atomic<int> status_{OB_SUCCESS};
  ObJdbcConnectionPool pool_;
};

TEST_F(PoolTest, lazy_init_and_exclusive_reuse)
{
  EXPECT_EQ(0, jdbc_.opens_);
  ObJdbcConnectionGuard first, second;
  ASSERT_EQ(OB_SUCCESS, acquire(first));
  ASSERT_EQ(OB_SUCCESS, acquire(second));
  EXPECT_EQ(2, jdbc_.opens_);
  EXPECT_EQ(2, stats().in_use_);
  first.mark_reusable();
  EXPECT_EQ(OB_SUCCESS, first.release());
  EXPECT_EQ(OB_SUCCESS, acquire(first));
  EXPECT_EQ(2, jdbc_.opens_);
  EXPECT_EQ(1, stats().reused_);
  EXPECT_EQ(3, pool_.refs());
}

TEST_F(PoolTest, request_url_is_used_only_for_physical_connect)
{
  ObJdbcConnectionGuard first, second;
  const ObString first_url("jdbc:sqlserver://first;databaseName=销售😀");
  const ObString second_url("jdbc:sqlserver://second;databaseName=test");
  ASSERT_EQ(OB_SUCCESS, pool_.acquire(first_url, ObTimeUtility::current_time() + 60000000, first));
  first.mark_reusable();
  ASSERT_EQ(OB_SUCCESS, first.release());
  ASSERT_EQ(OB_SUCCESS, pool_.acquire(first_url, ObTimeUtility::current_time() + 60000000, first));
  ASSERT_EQ(1, jdbc_.connect_urls_.size());
  ASSERT_EQ(OB_SUCCESS, pool_.acquire(second_url, ObTimeUtility::current_time() + 60000000, second));
  ASSERT_EQ(2, jdbc_.connect_urls_.size());
  EXPECT_EQ(std::string(first_url.ptr(), first_url.length()), jdbc_.connect_urls_[0]);
  EXPECT_EQ(std::string(second_url.ptr(), second_url.length()), jdbc_.connect_urls_[1]);
  EXPECT_EQ(1, stats().reused_);
}

TEST_F(PoolTest, default_discard_and_release_exactly_once)
{
  {
    ObJdbcConnectionGuard guard;
    ASSERT_EQ(OB_SUCCESS, acquire(guard));
  }
  EXPECT_EQ(1, jdbc_.closes_);
  EXPECT_EQ(1, pool_.refs());
  EXPECT_EQ(0, stats().total());

  ObJdbcConnectionGuard guard;
  ASSERT_EQ(OB_SUCCESS, acquire(guard));
  EXPECT_EQ(OB_SUCCESS, guard.release());
  EXPECT_FALSE(guard.is_valid());
  EXPECT_EQ(OB_SUCCESS, guard.release());
  EXPECT_EQ(2, jdbc_.closes_);
  EXPECT_EQ(1, pool_.refs());
  EXPECT_EQ(0, stats().total());
}

TEST_F(PoolTest, scanner_owns_connection_before_java_initialization)
{
  plugin::ObJavaDataSourceFactoryHelper factory;
  plugin::ObJavaLogGuard java_log_guard;
  {
    plugin::ObJavaExternalScanner scanner(factory, java_log_guard);
    ASSERT_EQ(OB_SUCCESS, acquire(scanner.get_connection_guard()));
    EXPECT_EQ(1, stats().in_use_);
    EXPECT_EQ(2, pool_.refs());
    // Java scanner creation/promotion can fail while only the connection is installed.
    scanner.mark_failed();
    EXPECT_EQ(OB_SUCCESS, scanner.close());
    EXPECT_FALSE(scanner.get_connection_guard().is_valid());
    EXPECT_EQ(OB_SUCCESS, scanner.close());
  }
  EXPECT_EQ(1, jdbc_.closes_);
  EXPECT_EQ(1, pool_.refs());
  EXPECT_EQ(0, stats().total());

  {
    plugin::ObJavaExternalScanner scanner(factory, java_log_guard);
    ASSERT_EQ(OB_SUCCESS, acquire(scanner.get_connection_guard()));
  }
  EXPECT_EQ(2, jdbc_.closes_);
  EXPECT_EQ(1, pool_.refs());
  EXPECT_EQ(0, stats().total());
}

TEST_F(PoolTest, scanner_close_reports_disconnect_failure)
{
  TestWorker worker(status_);
  plugin::ObJavaDataSourceFactoryHelper factory;
  plugin::ObJavaLogGuard java_log_guard;
  plugin::ObJavaExternalScanner scanner(factory, java_log_guard);
  ASSERT_EQ(OB_SUCCESS, acquire(scanner.get_connection_guard()));
  jdbc_.close_ret_ = OB_JNI_ERROR;
  scanner.mark_failed();
  EXPECT_EQ(OB_JNI_ERROR, scanner.close());
  EXPECT_FALSE(scanner.get_connection_guard().is_valid());
  EXPECT_EQ(0, stats().total());
  EXPECT_EQ(1, stats().evicted_);
  EXPECT_EQ(1, pool_.refs());
  EXPECT_EQ(OB_SUCCESS, scanner.close());
  EXPECT_EQ(1, jdbc_.closes_);
}

TEST_F(PoolTest, scanner_close_preserves_worker_error_on_disconnect_failure)
{
  TestWorker worker(status_);
  plugin::ObJavaDataSourceFactoryHelper factory;
  plugin::ObJavaLogGuard java_log_guard;
  plugin::ObJavaExternalScanner scanner(factory, java_log_guard);
  ASSERT_EQ(OB_SUCCESS, acquire(scanner.get_connection_guard()));
  status_ = OB_TIMEOUT;
  jdbc_.close_ret_ = OB_JNI_ERROR;
  EXPECT_EQ(OB_TIMEOUT, scanner.close());
  EXPECT_FALSE(scanner.get_connection_guard().is_valid());
  EXPECT_EQ(0, stats().total());
  EXPECT_EQ(1, pool_.refs());
  EXPECT_EQ(1, jdbc_.closes_);
}

TEST_F(PoolTest, key_includes_tenant_catalog_and_actual_schema_version)
{
  hash::ObHashMap<ObJdbcConnectionPoolKey, int> map;
  ASSERT_EQ(OB_SUCCESS, map.create(8, "JdbcTest", "JdbcTest"));
  const ObJdbcConnectionPoolKey keys[] = {options().key_, options(1002).key_,
                                        options(1001, 501).key_, options(1001, 500, 124).key_};
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, map.set_refactored(keys[i], i));
  }
  for (int i = 0; i < 4; ++i) {
    int value = -1;
    EXPECT_EQ(OB_SUCCESS, map.get_refactored(keys[i], value));
    EXPECT_EQ(i, value);
  }
}

TEST_F(PoolTest, full_pool_wakes_on_return_without_extra_timeout_cap)
{
  ConnectionGuards leases;
  fill(leases);
  auto waiter = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    const int ret = acquire(lease);
    lease.mark_reusable();
    return ret;
  });
  EXPECT_TRUE(await_waiter(pool_));
  // Wait beyond HMS's 10-second cap while the SQL deadline is still in the future.
  EXPECT_EQ(std::future_status::timeout, waiter.wait_for(std::chrono::seconds(11)));
  leases.front().mark_reusable();
  EXPECT_EQ(OB_SUCCESS, leases.front().release());
  EXPECT_EQ(OB_SUCCESS, waiter.get());
  EXPECT_EQ(20, jdbc_.opens_);
  EXPECT_EQ(1, stats().reused_);
}

TEST_F(PoolTest, sql_deadline_and_cancel)
{
  ConnectionGuards leases;
  fill(leases);
  auto timeout = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return acquire(lease, ObTimeUtility::current_time() + 1000000);
  });
  EXPECT_TRUE(await_waiter(pool_));
  EXPECT_EQ(OB_TIMEOUT, timeout.get());
  EXPECT_EQ(1, stats().wait_timeouts_);
  auto cancelled = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return acquire(lease);
  });
  EXPECT_TRUE(await_waiter(pool_));
  status_ = OB_CANCELED;
  EXPECT_EQ(OB_CANCELED, cancelled.get());
  EXPECT_EQ(0, stats().waiters_);
}

TEST_F(PoolTest, failed_creation_rolls_back_reserved_capacity)
{
  jdbc_.connect_ret_ = OB_JNI_ERROR;
  ObJdbcConnectionGuard lease;
  EXPECT_EQ(OB_JNI_ERROR, acquire(lease));
  EXPECT_FALSE(lease.is_valid());
  EXPECT_EQ(0, stats().total());
  EXPECT_EQ(1, pool_.refs());
  jdbc_.connect_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, acquire(lease));
  EXPECT_EQ(2, jdbc_.opens_);
}

TEST_F(PoolTest, create_is_outside_lock_and_late_connection_is_discarded)
{
  jdbc_.block_connect_ = true;
  const int64_t deadline_ts = ObTimeUtility::current_time() + 1000000;
  auto creator = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return acquire(lease, deadline_ts);
  });
  EXPECT_TRUE(jdbc_.await(1, 0));
  EXPECT_EQ(1, stats().creating_); // Snapshot cannot block on the JDBC call.
  EXPECT_EQ(2, pool_.refs());
  wait_until(deadline_ts);
  jdbc_.unblock();
  EXPECT_EQ(OB_TIMEOUT, creator.get());
  EXPECT_EQ(1, jdbc_.closes_);
  EXPECT_EQ(0, stats().total());
}

TEST_F(PoolTest, driver_failure_after_sql_deadline_returns_timeout)
{
  jdbc_.block_connect_ = true;
  jdbc_.connect_ret_ = OB_JNI_ERROR;
  const int64_t deadline_ts = ObTimeUtility::current_time() + 1000000;
  auto creator = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return acquire(lease, deadline_ts);
  });
  EXPECT_TRUE(jdbc_.await(1, 0));
  wait_until(deadline_ts);
  jdbc_.unblock();
  EXPECT_EQ(OB_TIMEOUT, creator.get());
  EXPECT_EQ(1, jdbc_.closes_);
  EXPECT_EQ(0, stats().connected_);
  EXPECT_EQ(0, stats().total());
  EXPECT_EQ(1, pool_.refs());
}

TEST_F(PoolTest, closing_connections_still_consume_capacity)
{
  ConnectionGuards leases;
  fill(leases);
  jdbc_.block_close_ = true;
  auto closer = std::async(std::launch::async, [&] { return leases.front().release(); });
  EXPECT_TRUE(jdbc_.await(20, 1));
  EXPECT_EQ(1, stats().closing_);
  auto waiter = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return acquire(lease);
  });
  EXPECT_TRUE(await_waiter(pool_));
  EXPECT_EQ(20, jdbc_.opens_);
  EXPECT_EQ(20, stats().total());
  jdbc_.unblock();
  EXPECT_EQ(OB_SUCCESS, closer.get());
  EXPECT_EQ(OB_SUCCESS, waiter.get());
  EXPECT_EQ(21, jdbc_.opens_);
}

TEST_F(PoolTest, draining_closes_idle_returned_and_inflight_connections)
{
  ObJdbcConnectionGuard idle, active;
  ASSERT_EQ(OB_SUCCESS, acquire(idle));
  ASSERT_EQ(OB_SUCCESS, acquire(active));
  idle.mark_reusable();
  idle.release();
  pool_.drain();
  pool_.cleanup();
  EXPECT_EQ(1, jdbc_.closes_);
  ObJdbcConnectionGuard rejected;
  EXPECT_EQ(OB_SCHEMA_EAGAIN, acquire(rejected));
  active.mark_reusable();
  active.release();
  EXPECT_EQ(2, jdbc_.closes_);
  EXPECT_TRUE(pool_.removable());

  ObJdbcConnectionPool new_pool;
  ASSERT_EQ(OB_SUCCESS, new_pool.init(options(1001, 500, 124)));
  jdbc_.block_connect_ = true;
  auto creator = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return new_pool.acquire(options().jdbc_url_, ObTimeUtility::current_time() + 60000000, lease);
  });
  EXPECT_TRUE(jdbc_.await(3, 2));
  new_pool.drain();
  EXPECT_FALSE(new_pool.removable());
  jdbc_.unblock();
  EXPECT_EQ(OB_SCHEMA_EAGAIN, creator.get());
  EXPECT_EQ(3, jdbc_.closes_);
  EXPECT_TRUE(new_pool.removable());
}

TEST_F(PoolTest, idle_ttl_is_per_connection_and_empty_ttl_starts_after_last_close)
{
  ObJdbcConnectionGuard old, recent;
  ASSERT_EQ(OB_SUCCESS, acquire(old));
  ASSERT_EQ(OB_SUCCESS, acquire(recent));
  ObJdbcConnection *old_connection = old.connection_;
  ObJdbcConnection *recent_connection = recent.connection_;
  old.mark_reusable();
  old.release();
  recent.mark_reusable();
  recent.release();
  // Age individual idle entries without replacing the production clock.
  {
    ObThreadCondGuard lock(pool_.cond_);
    old_connection->returned_at_ -= ObJdbcConnectionPool::IDLE_CONNECTION_US;
  }
  pool_.cleanup();
  EXPECT_EQ(1, stats().idle_);
  EXPECT_EQ(1, jdbc_.closes_);
  EXPECT_EQ(0, pool_.empty_since_);
  {
    ObThreadCondGuard lock(pool_.cond_);
    recent_connection->returned_at_ -= ObJdbcConnectionPool::IDLE_CONNECTION_US;
  }
  const int64_t before_last_close = ObTimeUtility::current_time();
  pool_.cleanup();
  EXPECT_EQ(0, stats().total());
  EXPECT_GE(pool_.empty_since_, before_last_close);
  EXPECT_FALSE(pool_.removable());
  {
    ObThreadCondGuard lock(pool_.cond_);
    pool_.empty_since_ -= ObJdbcConnectionPool::EMPTY_POOL_US;
  }
  EXPECT_TRUE(pool_.removable());
  pool_.inc_ref();
  EXPECT_FALSE(pool_.removable());
  pool_.dec_ref();
}

TEST_F(PoolTest, stop_wakes_waiters_and_prevents_reusable_returns)
{
  ConnectionGuards leases;
  fill(leases);
  auto waiter = std::async(std::launch::async, [&] {
    ObJdbcConnectionGuard lease;
    return acquire(lease);
  });
  EXPECT_TRUE(await_waiter(pool_));
  pool_.drain(true);
  EXPECT_EQ(OB_SERVER_IS_STOPPING, waiter.get());
  EXPECT_FALSE(pool_.removable());
  for (auto &lease : leases) {
    lease.mark_reusable();
    lease.release();
  }
  EXPECT_EQ(20, jdbc_.closes_);
  EXPECT_TRUE(pool_.removable());
}

TEST_F(PoolTest, disconnect_failure_is_reported_and_slot_freed)
{
  jdbc_.close_ret_ = OB_JNI_ERROR;
  ObJdbcConnectionGuard lease;
  ASSERT_EQ(OB_SUCCESS, acquire(lease));
  EXPECT_EQ(OB_JNI_ERROR, lease.release());
  EXPECT_EQ(0, stats().total());
  EXPECT_EQ(1, stats().evicted_);
  EXPECT_EQ(OB_SUCCESS, lease.release());
}

TEST_F(PoolTest, new_version_has_independent_capacity_while_old_leases_drain)
{
  ConnectionGuards old_leases;
  fill(old_leases);
  pool_.drain();
  ObJdbcConnectionPool new_pool;
  ASSERT_EQ(OB_SUCCESS, new_pool.init(options(1001, 500, 124)));
  ConnectionGuards new_leases;
  for (auto &lease : new_leases) {
    EXPECT_EQ(OB_SUCCESS, new_pool.acquire(options().jdbc_url_, ObTimeUtility::current_time() + 60000000, lease));
  }
  EXPECT_EQ(20, stats().in_use_);
  ObJdbcConnectionPoolStats new_stats;
  new_pool.snapshot(new_stats);
  EXPECT_EQ(20, new_stats.in_use_);
  EXPECT_EQ(40, jdbc_.opens_);
  for (auto &lease : new_leases) {
    EXPECT_EQ(OB_SUCCESS, lease.release());
  }
}

TEST(JdbcScannerTest, close_before_initialization_is_idempotent)
{
  plugin::ObJavaDataSourceFactoryHelper factory;
  plugin::ObJavaLogGuard java_log_guard;
  plugin::ObJavaExternalScanner scanner(factory, java_log_guard);
  EXPECT_FALSE(scanner.get_connection_guard().is_valid());
  EXPECT_EQ(OB_SUCCESS, scanner.close());
  EXPECT_EQ(OB_SUCCESS, scanner.close());
}

TEST(JdbcJniRefTest, failed_promotion_does_not_install_a_global_handle)
{
  JNINativeInterface_ functions = {};
  functions.NewGlobalRef = [](JNIEnv *, jobject) -> jobject { return nullptr; };
  functions.ExceptionCheck = [](JNIEnv *) -> jboolean { return JNI_FALSE; };
  JNIEnv env = {&functions};
  plugin::ObJavaGlobalRef ref;
  _jobject local;
  EXPECT_EQ(OB_JNI_ERROR, ref.from_local_ref(&local, &env));
  EXPECT_EQ(nullptr, ref.handle());
  EXPECT_EQ(OB_SUCCESS, ref.clear(&env));
}
} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
