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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>
#include <atomic>
#define  private public
#include "rootserver/ob_root_service.h"
#include "share/ob_max_id_cache.h"
#include "share/ob_max_id_fetcher.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ((x), OB_SUCCESS)

// mock 掉对内部表的查询，验证一下缓存的正确性

namespace oceanbase
{
namespace share
{
std::atomic<uint64_t> current_max_id(OB_INVALID_ID);
uint64_t target_size = OB_INVALID_SIZE;
bool hit = false;
std::atomic<int64_t> inner_table_fetch_cnt(0);
std::atomic<int64_t> concurrent_fetch_cnt(0);    // current concurrent fetches in progress
std::atomic<int64_t> max_concurrent_fetch_cnt(0); // max observed concurrent fetches
int64_t mock_delay_us = 0;  // mock delay for inner table fetch
bool strict_size_check = true;  // whether to check size == target_size

int ObMaxIdFetcher::batch_fetch_new_max_id_from_inner_table(
    const uint64_t tenant_id,
    ObMaxIdType id_type,
    uint64_t &max_id,
    const uint64_t size)
{
  int ret = OB_SUCCESS;
  hit = true;
  inner_table_fetch_cnt++;

  // Track concurrent fetches
  int64_t curr = ++concurrent_fetch_cnt;
  int64_t max_val = max_concurrent_fetch_cnt.load();
  while (curr > max_val && !max_concurrent_fetch_cnt.compare_exchange_weak(max_val, curr)) {}

  if (mock_delay_us > 0) {
    ob_usleep(static_cast<uint32_t>(mock_delay_us));
  }
  if (!strict_size_check || size == target_size) {
    // Atomically update current_max_id and return
    uint64_t old_val = current_max_id.load();
    while (!current_max_id.compare_exchange_weak(old_val, old_val + size)) {}
    max_id = old_val + size;
  } else {
    max_id = OB_INVALID_ID;
  }

  concurrent_fetch_cnt--;
  LOG_INFO("hit inner table", K(hit), K(id_type), K(max_id), K(size), "current_max_id", current_max_id.load(), K(target_size), "max_concurrent_fetch_cnt", max_concurrent_fetch_cnt.load());
  return ret;
}
void set_id_size(const uint64_t id, const uint64_t size)
{
  current_max_id = id;
  target_size = size;
  hit = false;
  inner_table_fetch_cnt = 0;
  concurrent_fetch_cnt = 0;
  max_concurrent_fetch_cnt = 0;
  mock_delay_us = 0;
  strict_size_check = true;
}
void reset_id_size()
{
  current_max_id = OB_INVALID_ID;
  target_size = OB_INVALID_SIZE;
  hit = false;
  inner_table_fetch_cnt = 0;
  concurrent_fetch_cnt = 0;
  max_concurrent_fetch_cnt = 0;
  mock_delay_us = 0;
  strict_size_check = true;
}
TEST(MaxIdCache, basic)
{
  rootserver::ObRootService rs;
  GCTX.root_service_ = &rs;
  rs.rs_status_.rs_status_ = share::status::ObRootServiceStatus::FULL_SERVICE;
  ObMaxIdCacheMgr mgr;
  ObMySQLProxy proxy;
  uint64_t id = OB_INVALID_ID;
  ASSERT_SUCCESS(mgr.init(&proxy));

  // 第一次查询，会缓存ObMaxIdCacheItem::CACHE_SIZE个ID
  // 查询前内部表值为1，查询后变为ObMaxIdCacheItem::CACHE_SIZE + 1
  set_id_size(1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, 2);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 查询ObMaxIdCacheItem::CACHE_SIZE-1次，不会触发内部表读取
  for (int64_t i = 1; i < ObMaxIdCacheItem::CACHE_SIZE; i++) {
    ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
    ASSERT_EQ(id, i + 2);
    ASSERT_FALSE(hit);
  }

  // cache用完，触发内部表读取
  // 查询前内部表值为ObMaxIdCacheItem::CACHE_SIZE + 1，查询后变为ObMaxIdCacheItem::CACHE_SIZE * 2 + 1
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE + 1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE + 2);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 读取ObMaxIdCacheItem::CACHE_SIZE - 1个值，不触发内部表读取
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, ObMaxIdCacheItem::CACHE_SIZE - 1));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE + 3);
  ASSERT_FALSE(hit);

  // cache用完，触发内部表读取
  // 查询前内部表值为ObMaxIdCacheItem::CACHE_SIZE * 2 + 1，查询后变为ObMaxIdCacheItem::CACHE_SIZE * 3 + 1
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE * 2 + 1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE * 2 + 2);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 查询前内部表值为ObMaxIdCacheItem::CACHE_SIZE * 3 + 1，查询后变为ObMaxIdCacheItem::CACHE_SIZE * 4 + 2
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE * 3 + 1, ObMaxIdCacheItem::CACHE_SIZE + 1);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, ObMaxIdCacheItem::CACHE_SIZE + 1));
  // 可以复用前一段的缓存
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE * 2 + 3);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 当前缓存的值为[ObMaxIdCacheItem::CACHE_SIZE * 3 + 4, ObMaxIdCacheItem::CACHE_SIZE * 4 + 2]
  // 返回目前内部表使用的最大为ObMaxIdCacheItem::CACHE_SIZE * 4 + 3，表明有其他地方获取了id，无法连续，缓存的值需要丢弃
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE * 4 + 3, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, ObMaxIdCacheItem::CACHE_SIZE));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE * 4 + 4);
  ASSERT_TRUE(hit);
  reset_id_size();
}

// Test fast path: allocate from cache without hitting inner table
TEST(MaxIdCache, fast_path)
{
  rootserver::ObRootService rs;
  GCTX.root_service_ = &rs;
  rs.rs_status_.rs_status_ = share::status::ObRootServiceStatus::FULL_SERVICE;
  ObMaxIdCacheMgr mgr;
  ObMySQLProxy proxy;
  uint64_t id = OB_INVALID_ID;
  ASSERT_SUCCESS(mgr.init(&proxy));

  // First fetch to fill cache
  set_id_size(1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, 2);
  ASSERT_TRUE(hit);
  ASSERT_EQ(inner_table_fetch_cnt, 1);

  // Fast path: subsequent fetches should not hit inner table
  for (int i = 0; i < 100; i++) {
    hit = false;
    ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
    ASSERT_FALSE(hit);  // Should not hit inner table
  }
  ASSERT_EQ(inner_table_fetch_cnt, 1);  // Still only 1 inner table fetch
  reset_id_size();
}

// Test concurrent access with fetch_latch serialization
TEST(MaxIdCache, concurrent_fetch)
{
  rootserver::ObRootService rs;
  GCTX.root_service_ = &rs;
  rs.rs_status_.rs_status_ = share::status::ObRootServiceStatus::FULL_SERVICE;
  ObMaxIdCacheMgr mgr;
  ObMySQLProxy proxy;
  ASSERT_SUCCESS(mgr.init(&proxy));

  // Set up: allow any size fetch, add small delay to increase chance of contention
  set_id_size(1, ObMaxIdCacheItem::CACHE_SIZE);
  strict_size_check = false;  // Allow any size
  mock_delay_us = 1000;       // 1ms delay to make serialization observable

  std::atomic<int64_t> success_cnt(0);
  std::atomic<int64_t> fail_cnt(0);
  const int thread_cnt = 4;
  const int fetch_per_thread = 10;
  // Each fetch requests CACHE_SIZE/2 IDs, so cache will be exhausted quickly
  // This ensures multiple inner table fetches are triggered
  const uint64_t fetch_size = ObMaxIdCacheItem::CACHE_SIZE / 2;

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_cnt; i++) {
    threads.emplace_back([&mgr, &success_cnt, &fail_cnt]() {
      for (int j = 0; j < fetch_per_thread; j++) {
        uint64_t id = OB_INVALID_ID;
        int ret = mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, fetch_size);
        if (OB_SUCCESS == ret) {
          success_cnt++;
        } else {
          fail_cnt++;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  LOG_INFO("concurrent_fetch result", "success_cnt", success_cnt.load(), "fail_cnt", fail_cnt.load(),
           "inner_table_fetch_cnt", inner_table_fetch_cnt.load(), "max_concurrent_fetch_cnt", max_concurrent_fetch_cnt.load());
  // Most requests should succeed (some may get OB_EAGAIN due to concurrent access)
  ASSERT_GT(success_cnt.load(), 0);
  // Should have multiple inner table fetches (not just 1)
  ASSERT_GT(inner_table_fetch_cnt.load(), 1);
  // Verify serialization: max concurrent fetch should be 1 (serialized by fetch_latch)
  // Note: may be > 1 if bypass happened, but should be small
  LOG_INFO("concurrent_fetch serialization check", "max_concurrent_fetch_cnt", max_concurrent_fetch_cnt.load());
  ASSERT_LE(max_concurrent_fetch_cnt.load(), 2);  // At most 2 (1 holding lock + 1 bypass)
  reset_id_size();
}

// Test bypass lock: when fetch_latch is held too long, other threads should bypass
TEST(MaxIdCache, bypass_lock_on_timeout)
{
  rootserver::ObRootService rs;
  GCTX.root_service_ = &rs;
  rs.rs_status_.rs_status_ = share::status::ObRootServiceStatus::FULL_SERVICE;
  ObMaxIdCacheMgr mgr;
  ObMySQLProxy proxy;
  ASSERT_SUCCESS(mgr.init(&proxy));

  // Set mock delay longer than timeout (MAX_FETCH_RETRY_CNT * FETCH_RETRY_INTERVAL_US = 100ms)
  set_id_size(1, ObMaxIdCacheItem::CACHE_SIZE);
  strict_size_check = false;  // Allow any size
  mock_delay_us = 200 * 1000;  // 200ms delay

  std::atomic<bool> first_thread_started(false);
  std::atomic<bool> second_thread_done(false);
  std::atomic<int64_t> second_thread_fetch_cnt(0);

  // First thread: hold fetch_latch for a long time
  std::thread t1([&mgr, &first_thread_started]() {
    uint64_t id = OB_INVALID_ID;
    first_thread_started = true;
    mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1);
  });

  // Wait for first thread to start
  while (!first_thread_started) {
    ob_usleep(1000);
  }
  ob_usleep(10 * 1000);  // Give first thread time to acquire fetch_latch

  // Second thread: should bypass lock after timeout
  std::thread t2([&mgr, &second_thread_done, &second_thread_fetch_cnt]() {
    uint64_t id = OB_INVALID_ID;
    int64_t start_cnt = inner_table_fetch_cnt.load();
    mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1);
    second_thread_fetch_cnt = inner_table_fetch_cnt.load() - start_cnt;
    second_thread_done = true;
  });

  t1.join();
  t2.join();

  // Both threads should have fetched from inner table (bypass happened)
  LOG_INFO("bypass_lock_on_timeout result", "inner_table_fetch_cnt", inner_table_fetch_cnt.load(),
           "second_thread_fetch_cnt", second_thread_fetch_cnt.load());
  ASSERT_GE(inner_table_fetch_cnt.load(), 2);  // At least 2 inner table fetches
  reset_id_size();
}

// Test double check: multiple threads simultaneously find cache empty,
// but only one fetches from inner table, others skip via double check
TEST(MaxIdCache, double_check_skip_fetch)
{
  rootserver::ObRootService rs;
  GCTX.root_service_ = &rs;
  rs.rs_status_.rs_status_ = share::status::ObRootServiceStatus::FULL_SERVICE;
  ObMaxIdCacheMgr mgr;
  ObMySQLProxy proxy;
  ASSERT_SUCCESS(mgr.init(&proxy));

  // Set up: cache is empty, add delay so other threads have time to wait
  set_id_size(1, ObMaxIdCacheItem::CACHE_SIZE);
  strict_size_check = false;
  mock_delay_us = 50 * 1000;  // 50ms delay, less than timeout (100ms)

  std::atomic<int64_t> success_cnt(0);
  std::atomic<bool> all_started(false);
  const int thread_cnt = 4;

  // Use barrier to ensure all threads start simultaneously
  std::atomic<int> ready_cnt(0);

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_cnt; i++) {
    threads.emplace_back([&mgr, &success_cnt, &ready_cnt, &all_started]() {
      // Wait for all threads to be ready
      ready_cnt++;
      while (ready_cnt.load() < thread_cnt) {
        ob_usleep(100);
      }
      all_started = true;

      // All threads simultaneously try to fetch when cache is empty
      uint64_t id = OB_INVALID_ID;
      int ret = mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1);
      if (OB_SUCCESS == ret) {
        success_cnt++;
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  LOG_INFO("double_check_skip_fetch result", "success_cnt", success_cnt.load(),
           "inner_table_fetch_cnt", inner_table_fetch_cnt.load(), "max_concurrent_fetch_cnt", max_concurrent_fetch_cnt.load());
  // All threads should succeed
  ASSERT_EQ(success_cnt.load(), thread_cnt);
  // Key assertion: only 1 thread should fetch from inner table
  // Other threads should skip via double check after waiting
  // (Because mock_delay < timeout, no bypass should happen)
  ASSERT_EQ(inner_table_fetch_cnt.load(), 1);
  ASSERT_EQ(max_concurrent_fetch_cnt.load(), 1);  // Serialized, no concurrent fetch
  reset_id_size();
}
}
}
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
