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
#define private public
#define protected public
#include "share/ob_thread_mgr.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/utility/ob_tracepoint.h"
// #include "ob_cache_get_stressor.h"
#include "observer/ob_signal_handle.h"
#include "ob_cache_test_utils.h"
#include "share/ob_tenant_mgr.h"

namespace oceanbase
{
using namespace lib;
using namespace observer;
namespace common
{
static ObSimpleMemLimitGetter getter;

class TestKVCache: public ::testing::Test
{
public:
  TestKVCache();
  virtual ~TestKVCache();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestKVCache);
protected:
  // function members
protected:
  // data members
  int64_t tenant_id_;
  int64_t lower_mem_limit_;
  int64_t upper_mem_limit_;
};

TestKVCache::TestKVCache()
  : tenant_id_(900),
    lower_mem_limit_(8 * 1024 * 1024),
    upper_mem_limit_(16 * 1024 * 1024)
{
}

TestKVCache::~TestKVCache()
{
}

void TestKVCache::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = lib::ACHUNK_SIZE;
  ret = getter.add_tenant(tenant_id_,
                          lower_mem_limit_,
                          upper_mem_limit_);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // set observer memory limit
  CHUNK_MGR.set_limit(5L * 1024L * 1024L * 1024L);
}

void TestKVCache::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
  getter.reset();
}

TEST(ObKVCacheInstMap, normal)
{
  int ret = OB_SUCCESS;
  ObKVCacheInstMap inst_map;
  ObKVCacheConfig config;

  //invalid argument
  ret = inst_map.init(0, &config, getter);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = inst_map.init(1000, NULL, getter);
  ASSERT_NE(OB_SUCCESS, ret);

  //normal argument
  ret = inst_map.init(1000, &config, getter);
  ASSERT_EQ(OB_SUCCESS, ret);

  //repeat init
  ret = inst_map.init(1000, &config, getter);
  ASSERT_NE(OB_SUCCESS, ret);

  inst_map.destroy();
}

TEST(ObKVCacheInstMap, memory)
{
  int ret = OB_SUCCESS;
  int64_t cache_inst_cnt = 1000;
  ObKVCacheInstMap inst_map;
  ObKVCacheConfig configs[MAX_CACHE_NUM];
  ObKVCacheInstKey inst_key;
  ObKVCacheInstHandle inst_handle;
  inst_key.cache_id_ = 0;
  inst_key.tenant_id_ = 1;

  //normal argument
  ret = inst_map.init(cache_inst_cnt, configs, getter);
  COMMON_LOG(INFO, "InstMap init ret, ", K(ret));
  ASSERT_EQ(OB_SUCCESS, ret);

  #ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_4, OB_ALLOCATE_MEMORY_FAILED, 0, 1);

  for (int64_t i = 0; i < cache_inst_cnt * 10; ++i) {
    ret = inst_map.get_cache_inst(inst_key, inst_handle);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  TP_SET_EVENT(EventTable::EN_4, OB_SUCCESS, 0, 0);
  #endif

  COMMON_LOG(INFO, "get from InstMap");
  for (int64_t i = 0; i < cache_inst_cnt; ++i) {
    ret = inst_map.get_cache_inst(inst_key, inst_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // COMMON_LOG(INFO, "destroy InstMap");
  // inst_map.destroy();
}

TEST(ObKVGlobalCache, normal)
{
  int ret = OB_SUCCESS;

  //invalid argument
  ret = ObKVGlobalCache::get_instance().init(&getter, -1);
  ASSERT_NE(OB_SUCCESS, ret);
  uint64_t tenant_id_ = 900;
  int64_t washable_size = -1;
  ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, -10);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, 110);
  ASSERT_NE(OB_SUCCESS, ret);

  //repeat init
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = lib::ACHUNK_SIZE;
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  // TestKVCache.SetUp() may execute earlier
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter);
  ASSERT_NE(OB_SUCCESS, ret);

  ObKVGlobalCache::get_instance().destroy();
}

/*
TEST(TestKVCacheValue, wash_stress)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024L * 1024L;
  const int64_t max_cache_size = 100L * 1024L * 1024L * 1024L;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  const uint64_t tenant_id = 900;
  const int64_t lower_mem_limit = 40L * 1024L * 1024L * 1024L;
  const int64_t upper_mem_limit = 60L * 1024L * 1024L * 1024L;
  CHUNK_MGR.set_limit(upper_mem_limit * 3 / 2);
  ret = ObTenantManager::get_instance().init(100000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().add_tenant(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().set_tenant_mem_limit(tenant_id, lower_mem_limit, upper_mem_limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObKVGlobalCache::get_instance().wash_timer_.cancel_all();
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 16 * 1024;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;
  ObKVCache<TestKey, TestValue> cache;
  ASSERT_EQ(OB_SUCCESS, cache.init("test"));

  const int64_t count = 64;
  const int64_t kv_count = upper_mem_limit / V_SIZE;
  ObCacheGetStressor<K_SIZE, V_SIZE> getters[64];
  ObCacheGetStressor<K_SIZE, V_SIZE>::make_cache_full(cache, tenant_id, kv_count);
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, getters[i].init(cache, tenant_id, i, count, kv_count));
    getters[i].start();
  }

  int64_t wash_count = 64;
  while (--wash_count > 0) {
    ObCacheGetStressor<K_SIZE, V_SIZE>::make_cache_full(cache, tenant_id, kv_count);
    sleep(1);
    ObKVGlobalCache::get_instance().wash();
  }

  for (int64_t i = 0; i < count; ++i) {
    getters[i].start();
    getters[i].stop();
    getters[i].wait();
  }
}
*/

TEST_F(TestKVCache, test_hazard_version)
{
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 64;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  int ret = OB_SUCCESS;
  ObKVCache<TestKey, TestValue> cache;
  TestKey key;
  TestValue value;
  const TestValue *pvalue = NULL;
  ObKVCachePair *kvpair = NULL;
  ObKVCacheHandle handle;
  ObKVCacheIterator iter;

  ObKVCacheInstKey inst_key(1, tenant_id_);
  ObKVCacheInstHandle inst_handle;
  ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle);
  GlobalHazardVersion &hazard_version = ObKVGlobalCache::get_instance().map_.global_hazard_version_;

  ret = cache.init("test");
  ASSERT_EQ(ret, OB_SUCCESS);

  COMMON_LOG(INFO, "********** test get hazard thread store **********");
  KVCacheHazardThreadStore *ts = nullptr;
  int64_t thread_id = get_itid();
  hazard_version.get_thread_store(ts);
  COMMON_LOG(INFO, "thread store:", KP(ts), K(ts->thread_id_), K(ts->inited_));

  COMMON_LOG(INFO, "********** test hazard delete node **********");
  TestNode *node = nullptr;
  for (int64_t i = 0 ; i < 20 ; ++i) {
    ret = hazard_version.acquire();
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_LE(ts->acquired_version_, hazard_version.version_);
    hazard_version.print_current_status();
    node = new TestNode();
    node->id_ = i;
    hazard_version.delete_node(node);
    hazard_version.release();
    ASSERT_EQ(ts->acquired_version_, UINT64_MAX);
    hazard_version.print_current_status();
    COMMON_LOG(INFO, "-----");
  }
}

TEST_F(TestKVCache, test_func)
{
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 64;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  int ret = OB_SUCCESS;
  ObKVCache<TestKey, TestValue> cache;
  TestKey key;
  TestValue value;
  const TestValue *pvalue = NULL;
  ObKVCachePair *kvpair = NULL;
  ObKVCacheHandle handle;
  ObKVCacheIterator iter;

  ObKVCacheInstKey inst_key(0, tenant_id_);
  ObKVCacheInstHandle inst_handle;
  ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle);
  GlobalHazardVersion &hazard_version = ObKVGlobalCache::get_instance().map_.global_hazard_version_;
  ObKVCacheStore &store = ObKVGlobalCache::get_instance().store_;

  key.v_ = 900;
  key.tenant_id_ = tenant_id_;
  value.v_ = 4321;

  //invalid invoke when not init
  ret = cache.set_priority(1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.put(key, value);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.get(key, pvalue, handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.put_and_fetch(key, value, pvalue, handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.get_iterator(iter);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.erase(key);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.alloc(tenant_id_, K_SIZE, V_SIZE, kvpair, handle, inst_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = cache.put_kvpair(inst_handle, kvpair, handle);
  ASSERT_NE(OB_SUCCESS, ret);

  //test init
  ret = cache.init("test");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cache.init("test");
  ASSERT_NE(OB_SUCCESS, ret);

  //invalid argument
  ret = cache.set_priority(0);
  ASSERT_NE(OB_SUCCESS, ret);
  TestKey bad_key;
  bad_key.v_ = 900;
  bad_key.tenant_id_ = OB_DEFAULT_TENANT_COUNT+1;
  ret = cache.put(bad_key, value);
  ASSERT_NE(OB_SUCCESS, ret);

  //test put and get
  ret = cache.put(key, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cache.get(key, pvalue, handle);
  COMMON_LOG(INFO, "handle ref count", K(handle.mb_handle_->get_ref_cnt()), K(store.get_handle_ref_cnt(handle.mb_handle_)));
  ASSERT_EQ(OB_SUCCESS, ret);
  if (OB_SUCC(ret)) {
    ASSERT_TRUE(value.v_ == pvalue->v_);
  }

  //test overwrite
  ObKVCacheHandle kvcache_handles[10];
  for (int64_t i = 0 ; i < 10 ; ++i) {
    COMMON_LOG(INFO, "overwrite now at:", K(i));
    value.v_ = i;
    ret = cache.put(key, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = cache.get(key, pvalue, kvcache_handles[i]);
    COMMON_LOG(INFO, "handle ref count", K(kvcache_handles[i].mb_handle_->get_ref_cnt()), K(store.get_handle_ref_cnt(kvcache_handles[i].mb_handle_)));
  }
  COMMON_LOG(INFO, "handle ref count", K(handle.mb_handle_->get_ref_cnt()), K(store.get_handle_ref_cnt(handle.mb_handle_)));

  //test erase
  ret = cache.erase(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cache.erase(key);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  //test alloc and put
  ret = cache.alloc(tenant_id_, K_SIZE, V_SIZE, kvpair, handle, inst_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  kvpair->key_ = new (kvpair->key_) TestKey;
  kvpair->value_ = new (kvpair->value_) TestValue;
  key.deep_copy(reinterpret_cast<char *>(kvpair->key_), key.size(), kvpair->key_);
  value.deep_copy(reinterpret_cast<char *>(kvpair->value_), value.size(), kvpair->value_);
  ret = cache.put_kvpair(inst_handle, kvpair, handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  //test iterator
  handle.reset();
  ret = cache.get_iterator(iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const TestKey *pkey = NULL;
  ret = iter.get_next_kvpair(pkey, pvalue, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_kvpair(pkey, pvalue, handle);
  ASSERT_EQ(OB_ITER_END, ret);

  //test Node release
  ObKVGlobalCache::get_instance().print_all_cache_info();
  for (int64_t i = 1 ; i < 11 ; ++i) {
    key.v_ = i + 2345;
    value.v_ = i + 2345;
    cache.put(key, value, true);
    cache.get(key, pvalue, handle);
    COMMON_LOG(INFO, "kvpair:", K(key.v_), K(value.v_), K(pvalue->v_));
  }
  //test destroy
  cache.destroy();
  ret = cache.init("test2");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cache.get(key, pvalue, handle);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(TestKVCache, test_large_kv)
{
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 2 * 1024 * 1024;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  int ret = OB_SUCCESS;
  ObKVCache<TestKey, TestValue> cache;
  TestKey key;
  TestValue value;
  const TestValue *pvalue = NULL;
  ObKVCacheHandle handle;
  ObKVCacheIterator iter;
  key.v_ = 900;
  key.tenant_id_ = tenant_id_;
  value.v_ = 4321;

  //test init
  ret = cache.init("test");
  ASSERT_EQ(OB_SUCCESS, ret);

  //test put and get
  ret = cache.put(key, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cache.get(key, pvalue, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (OB_SUCC(ret)) {
    ASSERT_TRUE(value.v_ == pvalue->v_);
  }
}

// TEST_F(TestKVCache, test_reuse_wash_struct)
// {
//   TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
//   TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
//   TG_WAIT(lib::TGDefIDs::KVCacheWash);
//   TG_WAIT(lib::TGDefIDs::KVCacheRep);
//   static const int64_t K_SIZE = 16;
//   static const int64_t V_SIZE = 512 * 1024;
//   typedef TestKVCacheKey<K_SIZE> TestKey;
//   typedef TestKVCacheValue<V_SIZE> TestValue;

//   int ret = OB_SUCCESS;
//   ObKVCache<TestKey, TestValue> cache;
//   TestKey key;
//   TestValue value;
//   ObKVCacheHandle handle;
//   ObKVCacheIterator iter;
//   value.v_ = 4321;

//   //test init
//   ret = cache.init("test");
//   ASSERT_EQ(OB_SUCCESS, ret);


//   COMMON_LOG(INFO, "********** Add tenant and put kv **********");
//   for (int64_t t = 900; t < 1244; ++t) {
//     key.tenant_id_ = t;
//     ret = ObTenantManager::get_instance().add_tenant(t);
//     ASSERT_EQ(OB_SUCCESS, ret);
//     ret = ObTenantManager::get_instance().set_tenant_mem_limit(t, lower_mem_limit_, upper_mem_limit_);
//     ASSERT_EQ(OB_SUCCESS, ret);
//     COMMON_LOG(INFO, "put in tenant ", K(t));
//     for (int64_t i = 0; i < 128; ++i) {
//       key.v_ = i;
//       ret = cache.put(key, value);
//       ASSERT_EQ(OB_SUCCESS, ret);
//     }
//   }

//   ObKVCacheStore &store = ObKVGlobalCache::get_instance().store_;
//   // store.reuse_wash_structs();
//   ObKVCacheStore::TenantWashInfo *twinfo;

//   COMMON_LOG(INFO, "********** Show tenant wash info pool after prepare **********");
//   for (int64_t i = 0 ; i < 10 ; ++i) {
//     store.wash_info_free_heap_.sbrk(twinfo);
//     COMMON_LOG(INFO, "TenantWashInfo, ", K(twinfo->cache_size_), K(twinfo->lower_limit_), K(twinfo->upper_limit_),
//       K(twinfo->max_wash_size_), K(twinfo->min_wash_size_), K(twinfo->wash_size_),
//       K(twinfo->cache_wash_heaps_->heap_size_), K(twinfo->cache_wash_heaps_->mb_cnt_), K(i), KP(twinfo->wash_heap_.heap_));
//     for (int64_t j = 0 ; j < MAX_CACHE_NUM ; ++j) {
//       COMMON_LOG(INFO, "\t\tcache_wash_heaps ", K(j), KP(twinfo->cache_wash_heaps_[j].heap_));
//     }
//   }

//   COMMON_LOG(INFO, "********** Compute wash size **********");
//   store.compute_tenant_wash_size();
//   COMMON_LOG(INFO, "********** Show tenant wash info pool after compute **********");
//   for (int64_t i = 0 ; i < 10 ; ++i) {
//     store.wash_info_free_heap_.sbrk(twinfo);
//     COMMON_LOG(INFO, "TenantWashInfo, ", K(twinfo->cache_size_), K(twinfo->lower_limit_), K(twinfo->upper_limit_),
//       K(twinfo->max_wash_size_), K(twinfo->min_wash_size_), K(twinfo->wash_size_),
//       K(twinfo->cache_wash_heaps_->heap_size_), K(twinfo->cache_wash_heaps_->mb_cnt_), K(i), KP(twinfo->wash_heap_.heap_));
//     for (int64_t j = 0 ; j < MAX_CACHE_NUM ; ++j)
//     {
//       COMMON_LOG(INFO, "\t\tcache_wash_heaps ", K(j), KP(twinfo->cache_wash_heaps_[j].heap_));
//     }
//   }

//   COMMON_LOG(INFO, "********** Show tenant wash info pool after reuse **********");
//   store.reuse_wash_structs();
//   for (int64_t i = 0 ; i < 10 ; ++i) {
//     store.wash_info_free_heap_.sbrk(twinfo);
//     COMMON_LOG(INFO, "TenantWashInfo, ", K(twinfo->cache_size_), K(twinfo->lower_limit_), K(twinfo->upper_limit_),
//       K(twinfo->max_wash_size_), K(twinfo->min_wash_size_), K(twinfo->wash_size_),
//       K(twinfo->cache_wash_heaps_->heap_size_), K(twinfo->cache_wash_heaps_->mb_cnt_), K(i), KP(twinfo->wash_heap_.heap_));
//     for (int64_t j = 0 ; j < MAX_CACHE_NUM ; ++j) {
//       COMMON_LOG(INFO, "\t\tcache_wash_heaps ", K(j), KP(twinfo->cache_wash_heaps_[j].heap_));
//     }
//   }
// }

TEST_F(TestKVCache, test_wash)
{
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 512 * 1024;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  int ret = OB_SUCCESS;
  ObKVCache<TestKey, TestValue> cache;
  TestKey key;
  TestValue value;
  ObKVCacheHandle handle;
  ObKVCacheIterator iter;
  value.v_ = 4321;

  //test init
  ret = cache.init("test");
  ASSERT_EQ(OB_SUCCESS, ret);


  COMMON_LOG(INFO, "********** Start nonempty wash every tenant **********");
  for (int64_t t = 900; t < 930; ++t) {
    key.tenant_id_ = t;
    ret = getter.add_tenant(t,
                            lower_mem_limit_,
                            upper_mem_limit_);
    ASSERT_EQ(OB_SUCCESS, ret);
    COMMON_LOG(INFO, "put in tenant ", K(t));
    for (int64_t i = 0; i < 256; ++i) {
      key.v_ = i;
      ret = cache.put(key, value);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ObKVGlobalCache::get_instance().wash();
  }

  COMMON_LOG(INFO, "********** Start nonempty wash all tenant **********");
  for (int64_t j = 0; j < 20; ++j) {
    for (int64_t t = 900; t < 930; ++t) {
      key.tenant_id_ = t;
      ret = getter.add_tenant(t,
                              lower_mem_limit_,
                              upper_mem_limit_);
      ASSERT_EQ(OB_SUCCESS, ret);
      COMMON_LOG(INFO, "put in tenant ", K(t));
      for (int64_t i = 0; i < 128; ++i) {
        key.v_ = i;
        ret = cache.put(key, value);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    ObKVGlobalCache::get_instance().wash();
  }

  COMMON_LOG(INFO, "********** Start empty wash **********");
  for (int64_t i = 0; i < 1000; ++i) {
    ObKVGlobalCache::get_instance().wash();
  }

  sleep(1);
  ASSERT_TRUE(cache.size(tenant_id_) < upper_mem_limit_);
}

TEST_F(TestKVCache, test_washable_size)
{
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 2 * 1024 * 1024;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  int ret = OB_SUCCESS;
  ObKVCache<TestKey, TestValue> cache;
  TestKey key;
  TestValue value;
  ObKVCacheHandle handle;
  ObKVCacheIterator iter;
  key.v_ = 900;
  key.tenant_id_ = tenant_id_;
  value.v_ = 4321;

  int64_t start_time = 0;
  int64_t cur_time = 0;
  int64_t get_wash_time = 0;

  //test init
  ret = cache.init("test");
  ASSERT_EQ(OB_SUCCESS, ret);

  // test calculate washable size before putting kv
  int64_t washable_size = -1;
  ret = ObKVGlobalCache::get_instance().get_washable_size(tenant_id_, washable_size);
  COMMON_LOG(INFO, "washable size before put,", K(ret), K(tenant_id_), K(washable_size));
  ASSERT_EQ(OB_SUCCESS, ret);

  //test put and wash
  for (int64_t i = 0; i < upper_mem_limit_ / V_SIZE * 10; ++i) {
    key.v_ = i;
    ret = cache.put(key, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ObKVGlobalCache::get_instance().get_washable_size(tenant_id_, washable_size);
    COMMON_LOG(INFO, "washable size,",K(ret), K(tenant_id_), K(washable_size));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // test calculate wash size after putting kv
  ret = ObKVGlobalCache::get_instance().get_washable_size(tenant_id_, washable_size);
  COMMON_LOG(INFO, "washable size after push,", K(ret), K(tenant_id_), K(washable_size));
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < upper_mem_limit_ / V_SIZE * 100; ++i) {
    key.v_ = i;
    ret = cache.put(key, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ObKVGlobalCache::get_instance().get_washable_size(tenant_id_, washable_size);
    COMMON_LOG(INFO, "washable size,",K(ret), K(tenant_id_), K(washable_size));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  COMMON_LOG(INFO, "start get washable size by 100% *********************************");
  start_time = ObTimeUtility::current_time();
  for (int i = 0 ; i < 3000 ; ++i) {
    ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size);
  }
  cur_time = ObTimeUtility::current_time();
  get_wash_time = cur_time - start_time;
  COMMON_LOG(INFO, "get washable time,", K(washable_size), K(get_wash_time));

  COMMON_LOG(INFO, "start get washable size by 1/8 *********************************");
  start_time = ObTimeUtility::current_time();
  for (int i = 0 ; i < 3000 ; ++i) {
    ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, 3);
  }
  cur_time = ObTimeUtility::current_time();
  get_wash_time = cur_time - start_time;
  COMMON_LOG(INFO, "get washable time by percentage 10,", K(washable_size), K(get_wash_time));

  ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, 2);
  ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, 4);
  ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, 5);
  ret = ObKVGlobalCache::get_instance().store_.get_washable_size(tenant_id_, washable_size, 6);

  sleep(1);
  // ASSERT_TRUE(cache.size(tenant_id_) < upper_mem_limit_);
}

TEST_F(TestKVCache, test_hold_size)
{
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 2 * 1024 * 1024;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  ObKVCache<TestKey, TestValue> cache;
  ASSERT_EQ(OB_SUCCESS, cache.init("test"));

  int64_t hold_size = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObKVGlobalCache::get_instance().set_hold_size(tenant_id_, "test", hold_size));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObKVGlobalCache::get_instance().get_hold_size(tenant_id_, "test", hold_size));

  TestKey key;
  TestValue value;
  key.v_ = 900;
  key.tenant_id_ = tenant_id_;
  value.v_ = 4321;
  ASSERT_EQ(OB_SUCCESS, cache.put(key, value));

  hold_size = -1;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().get_hold_size(tenant_id_, "test", hold_size));
  ASSERT_EQ(0, hold_size);
  int64_t new_hold_size = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().set_hold_size(tenant_id_, "test", new_hold_size));
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().get_hold_size(tenant_id_, "test", hold_size));
  ASSERT_EQ(new_hold_size, hold_size);

  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().set_hold_size(tenant_id_, "test", 0));
  for (int64_t i = 0; i < upper_mem_limit_ / V_SIZE * 10; ++i) {
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, cache.put(key, value));
  }
  sleep(1);
  SHARE_LOG(INFO, "store_size", "cache_size", cache.store_size(tenant_id_));

  // check hold_size work
  new_hold_size = 8 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().set_hold_size(tenant_id_, "test", new_hold_size));
  for (int64_t i = 0; i < upper_mem_limit_ / V_SIZE * 10; ++i) {
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, cache.put(key, value));
  }
  sleep(1);
  SHARE_LOG(INFO, "store_size", "cache_size", cache.store_size(tenant_id_), K(hold_size));
  ASSERT_TRUE(cache.store_size(tenant_id_) >= hold_size);
}

// TEST_F(TestKVCache, sync_wash_mbs)
// {
//   CHUNK_MGR.set_limit(512 * 1024 * 1024);

//   // close background wash timer task
//   // ObKVGlobalCache::get_instance().wash_timer_.cancel_all();
//   TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
//   TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
//   TG_WAIT(lib::TGDefIDs::KVCacheWash);
//   TG_WAIT(lib::TGDefIDs::KVCacheRep);

//   // put to cache make cache use all memory
//   static const int64_t K_SIZE = 16;
//   static const int64_t V_SIZE = 2 * 1024;
//   typedef TestKVCacheKey<K_SIZE> TestKey;
//   typedef TestKVCacheValue<V_SIZE> TestValue;
//   ObKVCache<TestKey, TestValue> cache;
//   ASSERT_EQ(OB_SUCCESS, cache.init("test"));

//   int ret = OB_SUCCESS;
//   TestKey key;
//   TestValue value;
//   key.v_ = 900;
//   key.tenant_id_ = tenant_id_;
//   value.v_ = 4321;
//   int64_t i = 0;
//   while (OB_SUCC(ret)) {
//     key.v_ = i;
//     if (OB_FAIL(cache.put(key, value))) {
//       SHARE_LOG(WARN, "put to cache failed", K(ret), K(i));
//     } else {
//       ++i;
//     }
//     if (CHUNK_MGR.get_hold() >= CHUNK_MGR.get_limit()) {
//       break;
//     }
//   }
//   const int64_t cache_total_size = i * V_SIZE;
//   SHARE_LOG(INFO, "stat", K(cache_total_size));

//   // try allocate memory, suppose to succeed
//   ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance());
//   ObArenaAllocator allocator(ObNewModIds::OB_KVSTORE_CACHE, 512 * 1024, tenant_id_);
//   int64_t j = 0;
//   void *ptr = NULL;
//   ret = OB_SUCCESS;
//   while (OB_SUCC(ret)) {
//     ObMemAttr attr;
//     attr.tenant_id_ = tenant_id_;
//     attr.label_ = ObModIds::OB_KVSTORE_CACHE;
//     ptr = allocator.alloc(V_SIZE);
//     if (NULL == ptr) {
//       ret = OB_ALLOCATE_MEMORY_FAILED;
//       SHARE_LOG(WARN, "allocate memory failed", K(ret), K(j));
//     } else {
//       ++j;
//     }
//   }
//   const int64_t malloc_total_size = j * V_SIZE;
//   SHARE_LOG(INFO, "stat", K(malloc_total_size));

//   int64_t cache_size = 0;
//   ObTenantResourceMgrHandle resource_handle;
//   ASSERT_EQ(OB_SUCCESS, ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle));
//   cache_size = resource_handle.get_memory_mgr()->get_cache_hold();
//   ASSERT_TRUE(cache_size < 3 * 1024 * 1024);
// }

TEST_F(TestKVCache, cache_wash_self)
{
  CHUNK_MGR.set_limit(1024 * 1024 * 1024);
  ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance());
  ObTenantResourceMgrHandle resource_handle;
  ASSERT_EQ(OB_SUCCESS, ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle));
  resource_handle.get_memory_mgr()->set_limit(512 * 1024 * 1024);

  // close background wash timer task
  // ObKVGlobalCache::get_instance().wash_timer_.stop();
  // ObKVGlobalCache::get_instance().wash_timer_.wait();
  // ObKVGlobalCache::get_instance().replace_timer_.stop();
  // ObKVGlobalCache::get_instance().replace_timer_.wait();
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);

  // put to cache make cache use all memory
  static const int64_t K_SIZE = 16;
  static const int64_t V_SIZE = 2 * 1024;
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;
  ObKVCache<TestKey, TestValue> cache;
  ASSERT_EQ(OB_SUCCESS, cache.init("test"));

  int ret = OB_SUCCESS;
  TestKey key;
  TestValue value;
  key.v_ = 900;
  key.tenant_id_ = tenant_id_;
  value.v_ = 4321;
  int64_t i = 0;
  const int64_t put_count = CHUNK_MGR.get_limit() / V_SIZE * 2;
  while (OB_SUCC(ret)) {
    key.v_ = i;
    if (OB_FAIL(cache.put(key, value))) {
      SHARE_LOG(WARN, "put to cache failed", K(ret), K(i));
    } else {
      ++i;
    }

    // try get
    if (OB_SUCC(ret)) {
      const TestValue *get_value = NULL;
      ObKVCacheHandle handle;
      ASSERT_EQ(OB_SUCCESS, cache.get(key, get_value, handle));
      if (i >= put_count) {
        break;
      }
      if (i % 10000 == 0) {
        SHARE_LOG(INFO, "xx", K(i), K(put_count));
      }
    } else {
      ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
      ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);
    }
  }
  const int64_t cache_put_size = put_count * V_SIZE;
  SHARE_LOG(INFO, "stat", K(cache_put_size));
}

TEST_F(TestKVCache, mix_mode_without_backgroup)
{
  CHUNK_MGR.set_limit(1024 * 1024 * 1024);
  ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance());
  ObTenantResourceMgrHandle resource_handle;
  ASSERT_EQ(OB_SUCCESS, ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle));
  resource_handle.get_memory_mgr()->set_limit(512 * 1024 * 1024);

  // close background wash timer task
  // ObKVGlobalCache::get_instance().wash_timer_.stop();
  // ObKVGlobalCache::get_instance().wash_timer_.wait();
  // ObKVGlobalCache::get_instance().replace_timer_.stop();
  // ObKVGlobalCache::get_instance().replace_timer_.wait();
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);

  ObAllocatorStress alloc_stress;
  ObCacheStress<16, 2*1024> cache_stress;

  ASSERT_EQ(OB_SUCCESS, alloc_stress.init());
  ASSERT_EQ(OB_SUCCESS, cache_stress.init(tenant_id_, 0));

  alloc_stress.start();
  cache_stress.start();

  // wait cache use all memory
  sleep(10);

  // add alloc/free task to alloc_stress, total 400MB alloc then free
  const int64_t alloc_size = 1024;
  const int64_t alloc_count = 50 * 1024;
  const int64_t task_count = 4;
  for (int64_t i = 0; i < task_count; ++i) {
    ObCacheTestTask task(tenant_id_, true, alloc_size, alloc_count, alloc_stress.get_stat());
    ASSERT_EQ(OB_SUCCESS, alloc_stress.add_task(task));
    sleep(1);
  }

  for (int64_t i = 0; i < task_count; ++i) {
    ObCacheTestTask task(tenant_id_, false, alloc_size, alloc_count, alloc_stress.get_stat());
    ASSERT_EQ(OB_SUCCESS, alloc_stress.add_task(task));
    sleep(1);
  }

  alloc_stress.stop();
  cache_stress.stop();
  alloc_stress.wait();
  cache_stress.wait();

  ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);

  ASSERT_EQ(0, alloc_stress.get_fail_count());
  ASSERT_EQ(0, cache_stress.get_fail_count());
}

TEST_F(TestKVCache, mix_mode_with_backgroup)
{
  CHUNK_MGR.set_limit(1024 * 1024 * 1024);
  ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance());

  ObAllocatorStress alloc_stress_array[3];
  ObCacheStress<16, 2*1024> cache_stress_array[3];

  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, alloc_stress_array[i].init());
    ASSERT_EQ(OB_SUCCESS, cache_stress_array[i].init(tenant_id_, i));
  }

  for (int i = 0; i < 3; ++i) {
    alloc_stress_array[i].start();
    cache_stress_array[i].start();
  }

  // wait cache use all memory
  sleep(10);

  // add alloc/free task to alloc_stress
  const int64_t alloc_size = 1024;
  const int64_t alloc_count = 20 * 1024;
  const int64_t task_count = 4;
  for (int64_t i = 0; i < task_count; ++i) {
    for (int j = 0; j < 3; ++j) {
      ObCacheTestTask task(tenant_id_, true, alloc_size, alloc_count, alloc_stress_array[j].get_stat());
      ASSERT_EQ(OB_SUCCESS, alloc_stress_array[j].add_task(task));
    }
  }

  for (int64_t i = 0; i < task_count; ++i) {
    for (int j = 0; j < 3; ++j) {
      ObCacheTestTask task(tenant_id_, false, alloc_size, alloc_count, alloc_stress_array[j].get_stat());
      ASSERT_EQ(OB_SUCCESS, alloc_stress_array[j].add_task(task));
    }
  }

  for (int i = 0; i < 3; ++i) {
    alloc_stress_array[i].stop();
    cache_stress_array[i].stop();
    alloc_stress_array[i].wait();
    cache_stress_array[i].wait();
  }

  ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);

  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(0, alloc_stress_array[i].get_fail_count());
    ASSERT_EQ(0, cache_stress_array[i].get_fail_count());
  }
}

TEST_F(TestKVCache, large_chunk_wash_mb)
{
  CHUNK_MGR.set_limit(1024 * 1024 * 1024);
  ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance());
  ObTenantResourceMgrHandle resource_handle;
  ASSERT_EQ(OB_SUCCESS, ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle));
  resource_handle.get_memory_mgr()->set_limit(512 * 1024 * 1024);

  // close background wash timer task
  // ObKVGlobalCache::get_instance().wash_timer_.stop();
  // ObKVGlobalCache::get_instance().wash_timer_.wait();
  // ObKVGlobalCache::get_instance().replace_timer_.stop();
  // ObKVGlobalCache::get_instance().replace_timer_.wait();
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);

  ObAllocatorStress alloc_stress;
  ObCacheStress<16, 2*1024> cache_stress;

  ASSERT_EQ(OB_SUCCESS, alloc_stress.init());
  ASSERT_EQ(OB_SUCCESS, cache_stress.init(tenant_id_, 0));

  alloc_stress.start();
  cache_stress.start();

  // wait cache use all memory
  sleep(10);

  // add alloc/free task to alloc_stress, total 400MB alloc then free
  const int64_t alloc_size = 20 * 1024 * 1024;
  const int64_t alloc_count = 2;
  const int64_t task_count = 4;
  for (int64_t i = 0; i < task_count; ++i) {
    ObCacheTestTask task(tenant_id_, true, alloc_size, alloc_count, alloc_stress.get_stat());
    ASSERT_EQ(OB_SUCCESS, alloc_stress.add_task(task));
    sleep(1);
  }

  for (int64_t i = 0; i < task_count; ++i) {
    ObCacheTestTask task(tenant_id_, false, alloc_size, alloc_count, alloc_stress.get_stat());
    ASSERT_EQ(OB_SUCCESS, alloc_stress.add_task(task));
    sleep(1);
  }

  alloc_stress.stop();
  cache_stress.stop();
  alloc_stress.wait();
  cache_stress.wait();

  ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);

  ASSERT_EQ(0, alloc_stress.get_fail_count());
  ASSERT_EQ(0, cache_stress.get_fail_count());

}

// TEST_F(TestKVCache, large_mb_wash_mb)
// {
//   // CHUNK_MGR.set_limit(1024 * 1024 * 1024);
//   CHUNK_MGR.set_limit(512 * 1024 * 1024);
//   ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance());
//   ObTenantResourceMgrHandle resource_handle;
//   ASSERT_EQ(OB_SUCCESS, ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle));
//   resource_handle.get_memory_mgr()->set_limit(512 * 1024 * 1024);

//   // close background wash timer task
//   // ObKVGlobalCache::get_instance().wash_timer_.stop();
//   // ObKVGlobalCache::get_instance().wash_timer_.wait();
//   // ObKVGlobalCache::get_instance().replace_timer_.stop();
//   // ObKVGlobalCache::get_instance().replace_timer_.wait();
//   TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
//   TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
//   TG_WAIT(lib::TGDefIDs::KVCacheWash);
//   TG_WAIT(lib::TGDefIDs::KVCacheRep);

//   ObCacheStress<16, 2*1024> cache_stress;
//   ASSERT_EQ(OB_SUCCESS, cache_stress.init(tenant_id_, 0));
//   cache_stress.start();
//   // wait cache use all memory
//   sleep(10);

//   cache_stress.stop();
//   cache_stress.wait();

//   static const int64_t K_SIZE = 16;
//   static const int64_t V_SIZE = 10 * 1024 * 1024;
//   typedef TestKVCacheKey<K_SIZE> TestKey;
//   typedef TestKVCacheValue<V_SIZE> TestValue;
//   ObKVCache<TestKey, TestValue> cache;
//   ASSERT_EQ(OB_SUCCESS, cache.init("test_big_mb"));
//   TestKey key;
//   TestValue value;
//   // put 80 times, total 800MB
//   for (int64_t i = 0; i < 80; ++i) {
//     key.tenant_id_ = tenant_id_;
//     key.v_ = i;
//     ASSERT_EQ(OB_SUCCESS, cache.put(key, value));
//     SHARE_LOG(INFO, "put big mb succeed");
//     ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
//   }

//   cache_stress.stop();
//   cache_stress.wait();
// }

TEST_F(TestKVCache, compute_wash_size_when_min_wash_negative)
{
  // close background wash timer task
  // ObKVGlobalCache::get_instance().wash_timer_.stop();
  // ObKVGlobalCache::get_instance().wash_timer_.wait();
  // ObKVGlobalCache::get_instance().replace_timer_.stop();
  // ObKVGlobalCache::get_instance().replace_timer_.wait();
  TG_CANCEL(lib::TGDefIDs::KVCacheWash, ObKVGlobalCache::get_instance().wash_task_);
  TG_CANCEL(lib::TGDefIDs::KVCacheRep, ObKVGlobalCache::get_instance().replace_task_);
  TG_WAIT(lib::TGDefIDs::KVCacheWash);
  TG_WAIT(lib::TGDefIDs::KVCacheRep);


  const uint64_t min_memory = 6L * 1024L * 1024L * 1024L;
  const uint64_t max_memory = 12L * 1024L * 1024L * 1024L;
  const uint64_t memory_usage = 6L * 1024L * 1024L * 1024L + 100L + 1024L + 1024L;
  ObTenantResourceMgrHandle resource_handle;
  ASSERT_EQ(OB_SUCCESS, ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle));
  resource_handle.get_memory_mgr()->set_limit(max_memory);
  resource_handle.get_memory_mgr()->sum_hold_ = memory_usage;

  // set tenant memory limit
  // ObTenantManager::get_instance().set_tenant_mem_limit(tenant_id_, min_memory, max_memory);
  ObVirtualTenantManager::get_instance().set_tenant_mem_limit(tenant_id_, min_memory, max_memory);

  // set cache size
  ObKVCacheInstKey inst_key;
  inst_key.tenant_id_ = tenant_id_;
  inst_key.cache_id_ = 1;
  ObKVCacheInstHandle inst_handle;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle));
  inst_handle.inst_->status_.store_size_ = memory_usage;
  inst_handle.inst_->status_.map_size_ = 0;

  CHUNK_MGR.set_limit(10L * 1024L * 1024L * 1024L);
  // CHUNK_MGR.hold_bytes_ = 10L * 1024L * 1024L * 1024L;
  CHUNK_MGR.total_hold_ = 10L * 1024L * 1024L * 1024L;
  CHUNK_MGR.set_urgent(1L * 1024L * 1024L * 1024L);

  // compute
  ObKVGlobalCache::get_instance().store_.compute_tenant_wash_size();

  // check tenant wash size
  ObKVCacheStore::TenantWashInfo *tenant_wash_info = NULL;
  ObKVGlobalCache::get_instance().store_.tenant_wash_map_.get(tenant_id_, tenant_wash_info);
  COMMON_LOG(INFO, "xxx", "wash_size", tenant_wash_info->wash_size_);
  ASSERT_TRUE(tenant_wash_info->wash_size_ >= 0);
}

TEST_F(TestKVCache, get_mb_list)
{
  ObKVCacheInstMap &inst_map = ObKVGlobalCache::get_instance().insts_;
  ObTenantMBListHandle handles_[MAX_TENANT_NUM_PER_SERVER];
  ASSERT_EQ(MAX_TENANT_NUM_PER_SERVER, inst_map.list_pool_.get_total());
  for (int64_t i = 0; i < MAX_TENANT_NUM_PER_SERVER; ++i) {
    ASSERT_EQ(OB_SUCCESS, inst_map.get_mb_list(i + 1, handles_[i]));
  }
  ASSERT_EQ(0, inst_map.list_pool_.get_total());

  ObTenantMBListHandle handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, inst_map.get_mb_list(5000, handle));
  for (int64_t i = 0; i < MAX_TENANT_NUM_PER_SERVER; ++i) {
    handles_[i].reset();
  }
  ASSERT_EQ(MAX_TENANT_NUM_PER_SERVER, inst_map.list_pool_.get_total());

  for (int64_t i = 0; i < MAX_TENANT_NUM_PER_SERVER; ++i) {
    ASSERT_EQ(OB_SUCCESS, inst_map.get_mb_list(i + 1, handles_[i]));
  }
  ObTenantMBListHandle second_handles_[MAX_TENANT_NUM_PER_SERVER];
  for (int64_t i = 0; i < MAX_TENANT_NUM_PER_SERVER; ++i) {
    ASSERT_EQ(OB_SUCCESS, inst_map.get_mb_list(i + 1, second_handles_[i]));
  }
  ASSERT_EQ(0, inst_map.list_pool_.get_total());
  for (int64_t i = 0; i < MAX_TENANT_NUM_PER_SERVER; ++i) {
    handles_[i].reset();
    second_handles_[i].reset();
  }
  ASSERT_EQ(MAX_TENANT_NUM_PER_SERVER, inst_map.list_pool_.get_total());

  // make list_map set failed
  inst_map.list_map_.size_ = 5000;
  ASSERT_EQ(MAX_TENANT_NUM_PER_SERVER, inst_map.list_pool_.get_total());
  ASSERT_EQ(OB_SIZE_OVERFLOW, inst_map.get_mb_list(5000, handle));
  ASSERT_EQ(MAX_TENANT_NUM_PER_SERVER, inst_map.list_pool_.get_total());
}

/*
TEST(ObSyncWashRt, sync_wash_mb_rt)
{
  const int64_t max_cache_size = 512L * 1024L * 1024L * 1024L;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(ObKVGlobalCache::DEFAULT_BUCKET_NUM, max_cache_size));
  ObKVGlobalCache::get_instance().wash_timer_.stop();
  ObKVGlobalCache::get_instance().wash_timer_.wait();
  ObKVCacheInst inst;
  inst.tenant_id_ = OB_SYS_TENANT_ID;
  ObKVCacheStore &store = ObKVGlobalCache::get_instance().store_;
  for (int64_t i = 0; i < store.max_mb_num_; ++i) {
    store.mb_handles_[i].handle_ref_.inc_ref_cnt();
    store.mb_handles_[i].inst_ = &inst;
  }

  const int64_t start = ObTimeUtility::current_time();
  const int64_t sync_wash_count = 1000;
  for (int64_t i = 0; i < sync_wash_count; ++i) {
    ObICacheWasher::ObCacheMemBlock *wash_blocks = NULL;
    ASSERT_EQ(OB_CACHE_FREE_BLOCK_NOT_ENOUGH, ObKVGlobalCache::get_instance().sync_wash_mbs(
        OB_SYS_TENANT_ID, 2 * 1024 * 1024, false, wash_blocks));
  }
  const int64_t end = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "wash cost time", "avg", (end - start) / sync_wash_count);
}
*/

}
}

int main(int argc, char** argv)
{
  oceanbase::observer::ObSignalHandle signal_handle;
  oceanbase::observer::ObSignalHandle::change_signal_mask();
  signal_handle.start();

  system("rm -f test_kv_storecache.log*");
  OB_LOGGER.set_file_name("test_kv_storecache.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
