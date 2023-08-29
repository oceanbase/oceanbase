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

#define USING_LOG_PREFEX COMMON

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#define private public
#include "share/ob_tenant_mgr.h"
#include "share/cache/ob_working_set_mgr.h"
#include "share/cache/ob_kv_storecache.h"
#include "ob_cache_test_utils.h"
#include "observer/ob_signal_handle.h"

using ::testing::_;
namespace oceanbase
{
using namespace share;
namespace common
{
class TestCacheWorkingSet : public ::testing::Test
{
public:
  static const int64_t K_SIZE = 10;
  static const int64_t V_SIZE = 16 * 1024;
  typedef TestKVCacheKey<K_SIZE> Key;
  typedef TestKVCacheValue<V_SIZE> Value;

  TestCacheWorkingSet();
  virtual ~TestCacheWorkingSet();
  virtual void SetUp();
  virtual void TearDown();
protected:
  uint64_t tenant_id_;
  int64_t mem_limit_;
};

TestCacheWorkingSet::TestCacheWorkingSet()
  : tenant_id_(OB_INVALID_ID), mem_limit_(0)
{
}

TestCacheWorkingSet::~TestCacheWorkingSet()
{
}

void TestCacheWorkingSet::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init());
  ObTenantManager &tenant_mgr = ObTenantManager::get_instance();
  mem_limit_ = 1 * 1024 * 1024 * 1024;
  tenant_id_ = 1001;
  ASSERT_EQ(OB_SUCCESS, tenant_mgr.init());
  ASSERT_EQ(OB_SUCCESS, tenant_mgr.add_tenant(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, tenant_mgr.set_tenant_mem_limit(tenant_id_, mem_limit_, mem_limit_));
}

void TestCacheWorkingSet::TearDown()
{
  ObTenantManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
}

TEST_F(TestCacheWorkingSet, basic)
{
  CHUNK_MGR.set_limit(2L * 1024L * 1024L * 1024L);
  Key key;
  Value value;
  const TestKVCacheValue<V_SIZE> *pvalue = NULL;
  ObKVCacheHandle handle;

  ObCacheWorkingSet<Key, Value> ws;

  // not init
  ASSERT_EQ(OB_NOT_INIT, ws.put(key, value));
  ASSERT_EQ(OB_NOT_INIT, ws.put_and_fetch(key, value, pvalue, handle));
  ASSERT_EQ(OB_NOT_INIT, ws.get(key, pvalue, handle));
  ASSERT_EQ(OB_NOT_INIT, ws.erase(key));

  ObKVCache<Key, Value> test_cache;
  ASSERT_EQ(OB_SUCCESS, test_cache.init("test_cache"));

  ASSERT_EQ(OB_SUCCESS, ws.init(tenant_id_, test_cache));

  const int64_t count = ws.get_limit() / V_SIZE * 3;

  // put
  for (int64_t i = 0; i < count; ++i) {
    key.tenant_id_ = tenant_id_;
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, ws.put(key, value));
  }

  // get
  int64_t succeed_cnt = 0;
  for (int64_t i = 0; i < count; ++i) {
    key.tenant_id_ = tenant_id_;
    key.v_ = i;
    int ret = ws.get(key, pvalue, handle);
    succeed_cnt += (OB_SUCCESS == ret ? 1 : 0);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
  }
  ASSERT_TRUE(succeed_cnt > 0);
  COMMON_LOG(INFO, "stat", K(succeed_cnt));

  // erase
  succeed_cnt = 0;
  for (int64_t i = 0; i < count; ++i) {
    key.tenant_id_ = tenant_id_;
    key.v_ = i;
    int ret = ws.erase(key);
    succeed_cnt += (OB_SUCCESS == ret ? 1 : 0);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
  }
  ASSERT_TRUE(succeed_cnt > 0);
  COMMON_LOG(INFO, "stat", K(succeed_cnt));

  // put_and_fetch
  for (int64_t i = 0; i < count; ++i) {
    key.tenant_id_ = tenant_id_;
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, ws.put_and_fetch(key, value, pvalue, handle));
  }
}

TEST_F(TestCacheWorkingSet, parallel)
{
  CHUNK_MGR.set_limit(5L * 1024L * 1024L * 1024L);

  ObWorkingSetStress<K_SIZE, V_SIZE> ws_stress;
  ASSERT_EQ(OB_SUCCESS, ws_stress.init(tenant_id_, true));
  ws_stress.set_thread_count(10);
  ws_stress.start();
  sleep(20);
  ws_stress.stop();
  ws_stress.wait();

  const int64_t put_count = ws_stress.get_put_count();
  const int64_t fail_count = ws_stress.get_fail_count();
  ASSERT_EQ(0, fail_count);
  COMMON_LOG(INFO, "put speed", K(put_count));
  COMMON_LOG(INFO, "stat", "used", ws_stress.get_used(), "limit", ws_stress.get_limit());
}

TEST_F(TestCacheWorkingSet, cache_size_increase)
{
  // close background wash timer task
  // ObKVGlobalCache::get_instance().wash_timer_.stop();
  // ObKVGlobalCache::get_instance().wash_timer_.wait();
  Key key;
  Value value;

  ObKVCache<Key, Value> test_cache;
  ASSERT_EQ(OB_SUCCESS, test_cache.init("test_cache"));
  const int64_t count = mem_limit_ / 2 / V_SIZE;
  for (int64_t i = 0; i < count; ++i) {
    key.tenant_id_ = tenant_id_;
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, test_cache.put(key, value));
  }
  const int64_t direct_size = test_cache.store_size(tenant_id_);

  ObKVCache<Key, Value> ws_cache;
  ASSERT_EQ(OB_SUCCESS, ws_cache.init("ws_cache"));
  ObCacheWorkingSet<Key, Value> ws;
  ASSERT_EQ(OB_SUCCESS, ws.init(tenant_id_, ws_cache));
  for (int64_t i = 0; i < count; ++i) {
    key.tenant_id_ = tenant_id_;
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, ws.put(key, value));
  }
  const int64_t ws_cache_size = ws_cache.store_size(tenant_id_);
  ASSERT_TRUE(ws_cache_size * 2 < direct_size);
  COMMON_LOG(INFO, "result", K(direct_size), K(ws_cache_size), K(ws.get_used()));
}

TEST_F(TestCacheWorkingSet, hit_ratio)
{
  mem_limit_ = 2L * 1024L * 1024L * 1024L;
  ASSERT_EQ(OB_SUCCESS, ObTenantManager::get_instance().set_tenant_mem_limit(tenant_id_, mem_limit_, mem_limit_));
  const int64_t kv_cnt = mem_limit_ / 2 / V_SIZE;
  double hit_ratio = 0.0;
  double ws_hit_ratio = 0.0;
  // test hit ratio when not using working set
  {
    ObCacheGetStress<K_SIZE, V_SIZE> get_stress;
    ASSERT_EQ(OB_SUCCESS, get_stress.init(tenant_id_, kv_cnt));
    get_stress.set_thread_count(10);
    get_stress.start();
    sleep(10);

    ObWorkingSetStress<K_SIZE, V_SIZE> ws_stress;
    ASSERT_EQ(OB_SUCCESS, ws_stress.init(tenant_id_, get_stress.get_cache(), false, kv_cnt + 1));
    ws_stress.set_thread_count(1);
    ws_stress.start();
    sleep(30);

    ws_stress.stop();
    ws_stress.wait();
    sleep(30);

    get_stress.stop();
    get_stress.wait();
    hit_ratio = get_stress.get_hit_ratio();
    COMMON_LOG(INFO, "without working set stat", K(hit_ratio));
    ObKVGlobalCache::get_instance().destroy();
  }

  // test hit ratio when using working set
  {
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init());
    ObCacheGetStress<K_SIZE, V_SIZE> get_stress;
    ASSERT_EQ(OB_SUCCESS, get_stress.init(tenant_id_, kv_cnt));
    get_stress.set_thread_count(10);
    get_stress.start();

    sleep(10);

    ObWorkingSetStress<K_SIZE, V_SIZE> ws_stress;
    ASSERT_EQ(OB_SUCCESS, ws_stress.init(tenant_id_, get_stress.get_cache(), true, kv_cnt + 1));
    ws_stress.set_thread_count(1);
    ws_stress.start();
    sleep(30);

    ws_stress.stop();
    ws_stress.wait();
    sleep(30);

    get_stress.stop();
    get_stress.wait();
    ws_hit_ratio = get_stress.get_hit_ratio();
    COMMON_LOG(INFO, "with working set stat", K(ws_hit_ratio));
  }
  ASSERT_TRUE(ws_hit_ratio > hit_ratio);
}

}//end namespace common
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::observer::ObSignalHandle signal_handle;
  oceanbase::observer::ObSignalHandle::change_signal_mask();
  signal_handle.start();

  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
