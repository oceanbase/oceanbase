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

#include <cstdlib>
#include <pthread.h>

#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/allocator/page_arena.h"

// Friend class injection.
// So tests can call private functions.
// 1. Declare them in namespace oceanbase::unittest.
using namespace oceanbase::lib;
namespace oceanbase
{
namespace unittest
{
  class ObLinearHashMap_EStest1_Test;
  class ObLinearHashMap_EStest2_Test;
  class ObLinearHashMap_ACCSStest1_Test;
  class ObLinearHashMap_ACCSStest2_Test;
  class ObLinearHashMap_ACCSStest3_Test;
  class ObLinearHashMap_ACCSStest4_Test;
  class ObLinearHashMap_ACCSStest5_Test;
  class ObLinearHashMap_PerfInsert2_Test;
  class ObLinearHashMap_PerfInsert3_Test;
  class ObLinearHashMap_PerfLoadFactor1_Test;
  void PerfLoadFactor2TestFunc(uint64_t bkt_n, double load_factor, uint64_t th_n);
}
}
// 2. Provide a friend class macro that is injected in hash map class body.
#define OB_LINEAR_HASH_MAP_UNITTEST_FRIEND \
  friend class oceanbase::unittest::ObLinearHashMap_EStest1_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_EStest2_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_ACCSStest1_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_ACCSStest2_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_ACCSStest3_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_ACCSStest4_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_ACCSStest5_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_PerfInsert2_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_PerfInsert3_Test; \
  friend class oceanbase::unittest::ObLinearHashMap_PerfLoadFactor1_Test; \
  friend void oceanbase::unittest::PerfLoadFactor2TestFunc(uint64_t bkt_n, double load_factor, uint64_t th_n);

#include "lib/hash/ob_linear_hash_map.h"


// CPU profile switch. Used to find bottleneck.
//#define CPUPROFILE_ON
#ifdef CPUPROFILE_ON
#include <google/profiler.h>
  #define CPUPROF_PATH getenv("PROFILEOUTPUT")
  #define CPUPROF_START() ProfilerStart(CPUPROF_PATH);
  #define CPUPROF_STOP() ProfilerStop();
#else
  #define CPUPROF_START()
  #define CPUPROF_STOP()
#endif

// Run all test switch.
// By default, all tests are disabled.
// Set "RAT" env var to run all tests.
// Use gtest filter to choose which test to run.
// Test Switch.
static bool run_all_test = false;

using namespace oceanbase;
using namespace common;
namespace oceanbase
{
namespace unittest
{

// Test Key type.
// 8B Key, 8B Value.
class UnitTestKey
{
public:
  uint64_t key;
  uint64_t hash() const
  {
    return key;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const UnitTestKey &other) const
  {
    return key == other.key;
  }
};

class UnitTestValue
{
public:
  uint64_t val;
  bool operator==(const UnitTestValue &other) const
  {
    return val == other.val;
  }
};

// Test Map type.
typedef ObLinearHashMap<UnitTestKey, UnitTestValue> Map;

// Timestamp.
uint64_t ts()
{
    timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_usec + 1000000 * (uint64_t)tv.tv_sec;
}

// Busy loop.
uint64_t __thread __trivial_val_anti_opt__ = 0;
uint64_t nop_loop(uint64_t loop_n)
{
  uint64_t t = 0;
  //uint64_t s = ts();
  for (uint64_t i = 0; i < loop_n; ++i) { t += i; }
  __trivial_val_anti_opt__ = t;
  //return ts() - s;
  return 0;
}

// Expand & Shrink tests.
// Test segment and buckets status during expand & shrink.
// With both micro-seg and standard-seg.
TEST(ObLinearHashMap, EStest1)
{
  bool run_this_test = false;
  run_this_test = (run_all_test) ? true : run_this_test;
  if (!run_this_test) return;

  uint64_t m_sz = 1 << 12; // 4KB
  uint64_t s_sz = 1 << 16; // 64KB
  uint64_t maxL = 6;

  uint64_t eL = 0;
  uint64_t ep = 0;

  // Init map.
  Map map;
  EXPECT_EQ(OB_SUCCESS, map.init(m_sz, s_sz, m_sz));
  // Validate some settings.
  EXPECT_EQ(m_sz / sizeof(Map::Bucket), map.L0_bkt_n_);
  {
    uint64_t curL, curp;
    map.load_Lp_(curL, curp);
    EXPECT_EQ(eL, curL);
    EXPECT_EQ(ep, curp);
  }
  // Before expand, dir exists, and the first seg is initialized.
  EXPECT_TRUE(map.dir_ != NULL);
  EXPECT_TRUE(map.dir_[0] != NULL);
  for (uint64_t idx = 0; idx < map.L0_bkt_n_; ++idx) {
    EXPECT_TRUE(map.is_bkt_active_(&map.dir_[0][idx]));
  }
  for (uint64_t idx = 1; idx < map.dir_seg_n_lmt_; ++idx) {
    EXPECT_TRUE(map.dir_[idx] == NULL);
  }
  // Expand.
  {
    uint64_t curL, curp;
    map.load_Lp_(curL, curp);
    while (curL < maxL) {
      EXPECT_TRUE(Map::ES_SUCCESS == map.expand_());
      map.load_Lp_(curL, curp);
      if (ep + 1 == map.L0_bkt_n_ << eL) { ep = 0; eL += 1; } else { ep += 1; }
      EXPECT_EQ(eL, curL);
      EXPECT_EQ(ep, curp);
    }
  }
  // Shrink.
  {
    uint64_t curL, curp;
    map.load_Lp_(curL, curp);
    while (curL > 0 || (curL == 0 && curp > 0)) {
      EXPECT_TRUE(Map::ES_SUCCESS == map.shrink_());
      map.load_Lp_(curL, curp);
      if (ep == 0) { eL -= 1; ep = (map.L0_bkt_n_ << eL) - 1; } else { ep -= 1; }
      EXPECT_EQ(eL, curL);
      EXPECT_EQ(ep, curp);
    }
  }
  EXPECT_TRUE(Map::ES_REACH_LIMIT == map.shrink_());
  // After shrink, the status is the same as the beginning.
  EXPECT_TRUE(map.dir_ != NULL);
  EXPECT_TRUE(map.dir_[0] != NULL);
  for (uint64_t idx = 0; idx < map.L0_bkt_n_; ++idx) {
    EXPECT_TRUE(map.is_bkt_active_(&map.dir_[0][idx]));
  }
  for (uint64_t idx = 1; idx < map.dir_seg_n_lmt_; ++idx) {
    EXPECT_TRUE(map.dir_[idx] == NULL);
  }
  // Destroy map.
  EXPECT_EQ(OB_SUCCESS, map.destroy());
}

// Bucket access test.
// Simple insert, erase and get against buckets.
TEST(ObLinearHashMap, ACCSStest1)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  uint64_t m_sz = 1 << 12; // 4KB
  uint64_t s_sz = 1 << 16; // 64KB
  Map map;
  EXPECT_EQ(OB_SUCCESS, map.init(m_sz, s_sz, m_sz));
  map.load_factor_l_limit_ = 0;
  map.load_factor_u_limit_ = 100000;
  const int64_t limit = 10000;
  for (int64_t idx = 0; idx < limit ; idx++) {
    UnitTestKey key;
    UnitTestValue value;
    key.key = idx;
    // Insert.
    EXPECT_EQ(OB_SUCCESS, map.do_insert_(key, value));
    // Insert dup.
    EXPECT_EQ(OB_ENTRY_EXIST, map.do_insert_(key, value));
  }
  for (int64_t idx = 0; idx < limit; idx++) {
    UnitTestKey key;
    UnitTestValue value;
    key.key = idx;
    // Get twice.
    EXPECT_EQ(OB_SUCCESS, map.do_get_(key, value));
    EXPECT_EQ(OB_SUCCESS, map.do_get_(key, value));
    // Erase twice.
    EXPECT_EQ(OB_SUCCESS, map.do_erase_(key, NULL));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, map.do_erase_(key, NULL));
    // Get again.
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, map.do_get_(key, value));
  }
  EXPECT_EQ(OB_SUCCESS, map.destroy());
}

// Test simple expand and shrink.
// Insert enough keys in bucket[0], and expand them to a large L.
// Then, get them. Shrink to the beginning, and get them again.
TEST(ObLinearHashMap, ACCSStest2)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  Map map;
  uint64_t m_sz = 1 << 12; // 4KB
  uint64_t s_sz = 1 << 16; // 64KB
  const uint64_t maxL = 6; // will use both m-seg and s-seg
  EXPECT_EQ(OB_SUCCESS, map.init(m_sz, s_sz, m_sz));
  map.load_factor_l_limit_ = 0;
  map.load_factor_u_limit_ = 100000000;
  const int64_t key_n = (int64_t)2 << maxL; // more than 2 keys in bucket when it reaches maxL.
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.insert(key, value));
  }
  uint64_t L, p;
  map.load_Lp_(L, p);
  while (L < maxL) {
    EXPECT_TRUE(Map::ES_SUCCESS == map.expand_());
    map.load_Lp_(L, p);
  }
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.do_get_(key, value));
  }
  map.load_Lp_(L, p);
  while (L > 0 || (L == 0 && p > 0)) {
    EXPECT_TRUE(Map::ES_SUCCESS == map.shrink_());
    map.load_Lp_(L, p);
  }
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.do_get_(key, value));
  }
  EXPECT_EQ(OB_SUCCESS, map.destroy());
}

// Test complicated expand and shrink.
// Multiple thread get & one thread keeps expanding and shrinking.
// Dir expansion is tested.
struct ObLinearHashMap_ACCESStest3_test_data
{
  ObLinearHashMap<UnitTestKey, UnitTestValue> *map_;
  bool run_;
  int64_t key_range_l_limit_;
  int64_t key_range_u_limit_;
  int64_t cnt_;
};
void* ObLinearHashMap_ACCESStest3_test_func(void *data)
{
  ObLinearHashMap_ACCESStest3_test_data *test_data =
      static_cast<ObLinearHashMap_ACCESStest3_test_data*>(data);
  int64_t key_n = test_data->key_range_l_limit_;
  while (ATOMIC_LOAD(&test_data->run_)) {
    UnitTestKey key;
    key.key = key_n;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, test_data->map_->get(key, value));
    key_n = key_n == test_data->key_range_u_limit_ ? test_data->key_range_l_limit_ : key_n + 1;
    ATOMIC_FAA(&test_data->cnt_, 1);
  }
  return NULL;
}
TEST(ObLinearHashMap, ACCSStest3)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  uint64_t m_sz = 1 << 12; // 4KB
  uint64_t s_sz = 1 << 16; // 64KB
  const int64_t key_range_l_limit = 0;
  const int64_t key_range_u_limit = 1 << 17;  // Should insert enough keys.
  const int64_t test_loop = 5;
  const int64_t thread_n = 4;
  pthread_t threads[thread_n];
  Map map;
  EXPECT_EQ(OB_SUCCESS, map.init(m_sz, s_sz, m_sz));
  map.load_factor_l_limit_ = 0;
  map.load_factor_u_limit_ = 100000;
  // Modify the dir size to an extremely short size.
  Map::Bucket *seg = map.dir_[0];
  map.des_dir_(map.dir_);
  map.dir_sz_ = 1 * sizeof(Map::Bucket*);
  map.dir_seg_n_lmt_ = 1;
  map.dir_ = map.cons_dir_(map.dir_sz_, NULL, 0);
  map.dir_[0] = seg;
  // Insert.
  for (int64_t idx = key_range_l_limit; idx < key_range_u_limit + 1 ; ++idx) {
    UnitTestKey key;
    key.key = idx;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.do_insert_(key, value));
  }
  ObLinearHashMap_ACCESStest3_test_data data;
  data.cnt_ = 0;
  data.map_ = &map;
  data.key_range_l_limit_ = key_range_l_limit;
  data.key_range_u_limit_ = key_range_u_limit;
  data.run_ = true;
  for (int64_t idx = 0; idx < thread_n ; ++idx) {
    pthread_create(&threads[idx], NULL, ObLinearHashMap_ACCESStest3_test_func, &data);
  }
  ::usleep(100);
  // Expand and Shrink.
  for (int64_t idx = 0; idx < test_loop; ++idx) {
    uint64_t L, p;
    map.load_Lp_(L, p);
    while (L < 11) {
      EXPECT_TRUE(Map::ES_SUCCESS == map.expand_());
      map.load_Lp_(L, p);
    }
    while (L > 0 || (L == 0 && p > 0)) {
      EXPECT_TRUE(Map::ES_SUCCESS == map.shrink_());
      map.load_Lp_(L, p);
    }
  }
  data.run_ = false;
  for (int64_t idx = 0; idx < thread_n; ++idx) {
    pthread_join(threads[idx], NULL);
  }
  //EXPECT_EQ(OB_SUCCESS, map.destroy());
}

// Test clear.
TEST(ObLinearHashMap, ACCSStest4)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  Map map;
  uint64_t m_sz = 1 << 12; // 4KB
  uint64_t s_sz = 1 << 16; // 64KB
  EXPECT_EQ(OB_SUCCESS, map.init(m_sz, s_sz, m_sz));
  map.load_factor_l_limit_ = 0;
  map.load_factor_u_limit_ = 100000;
  uint64_t L, p;
  const int64_t key_n = (int64_t)1 << 12;
  // Insert.
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.insert(key, value));
  }
  EXPECT_EQ(key_n, static_cast<int64_t>(map.count()));
  // Expand a little bit.
  for (int idx = 0; idx < 3 ; idx++) {
    EXPECT_TRUE(Map::ES_SUCCESS == map.expand_());
  }
  EXPECT_EQ(key_n, static_cast<int64_t>(map.count()));
  // Clear and get.
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.do_get_(key, value));
  }
  EXPECT_EQ(OB_SUCCESS, map.clear());
  EXPECT_EQ(0UL, map.count());
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, map.do_get_(key, value));
  }
  // Re-insert.
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.insert(key, value));
  }
  EXPECT_EQ(key_n, static_cast<int64_t>(map.count()));
  // Expand.
  map.load_Lp_(L, p);
  while (L < 10) {
    EXPECT_TRUE(Map::ES_SUCCESS == map.expand_());
    map.load_Lp_(L, p);
  }
  // Clear and get.
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_SUCCESS, map.do_get_(key, value));
  }
  EXPECT_EQ(OB_SUCCESS, map.clear());
  EXPECT_EQ(0UL, map.count());
  for (int64_t idx = 0; idx < key_n ; ++idx) {
    UnitTestKey key;
    key.key = 0 + idx * map.L0_bkt_n_;
    UnitTestValue value;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, map.do_get_(key, value));
  }
  EXPECT_EQ(OB_SUCCESS, map.destroy());
}

// Tests foreach.
// 1 thread keeps expanding and shrinking with some other threads
// calling foreach.
// Odd values are removed by remove_if().
// This functor sums up the val in all values.
struct ObLinearHashMap_ACCESStest5_test_functor
{
  ObLinearHashMap_ACCESStest5_test_functor() : sum_(0), cnt_(0) { }
  bool operator()(const UnitTestKey &key, UnitTestValue &value)
  {
    UNUSED(key);
    sum_ += value.val;
    cnt_++;
    return true;
  }
  uint64_t sum_;
  uint64_t cnt_;
};
struct ObLinearHashMap_ACCESStest5_test_functor2
{
  bool operator()(const UnitTestKey &key, UnitTestValue &value)
  {
    UNUSED(key);
    // Remove odd numbers.
    return (value.val % 2 == 0) ? false : true;
  }
};
struct ObLinearHashMap_ACCESStest5_test_data
{
  ObLinearHashMap<UnitTestKey, UnitTestValue> *map_;
  bool run_;
  uint64_t sum_;
};
void* ObLinearHashMap_ACCESStest5_test_func(void *data)
{
  ObLinearHashMap_ACCESStest5_test_data *test_data =
      static_cast<ObLinearHashMap_ACCESStest5_test_data*>(data);
  while (ATOMIC_LOAD(&test_data->run_)) {
    ObLinearHashMap_ACCESStest5_test_functor fn;
    EXPECT_EQ(OB_SUCCESS, test_data->map_->for_each(fn));
    EXPECT_EQ(test_data->sum_, fn.sum_);
    EXPECT_EQ(test_data->map_->count(), fn.cnt_);
  }
  return NULL;
}
TEST(ObLinearHashMap, ACCSStest5)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  uint64_t m_sz = 1 << 12; // 4KB
  uint64_t s_sz = 1 << 16; // 64KB
  const int64_t key_range_l_limit = 0;
  const int64_t key_range_u_limit = 1 << 18;  // Should insert enough keys.
  const int64_t test_loop = 200;
  const int64_t thread_n = 4;
  pthread_t threads[thread_n];
  Map map;
  EXPECT_EQ(OB_SUCCESS, map.init(m_sz, s_sz, m_sz));
  map.load_factor_l_limit_ = 0;
  map.load_factor_u_limit_ = 100000;
  // Insert.
  uint64_t seed = ts();
  uint64_t sum = 0;
  for (int64_t idx = key_range_l_limit; idx < key_range_u_limit + 1 ; ++idx) {
    UnitTestKey key;
    key.key = idx;
    UnitTestValue value;
    value.val = seed;
    // Odd values are removed from sum, but added into hash map.
    if (value.val % 2 == 0) {
      sum += value.val;
    }
    seed += 993427831;
    EXPECT_EQ(OB_SUCCESS, map.do_insert_(key, value));
  }
  // Remove odd numbers.
  ObLinearHashMap_ACCESStest5_test_functor2 fn2;
  EXPECT_EQ(OB_SUCCESS, map.remove_if(fn2));
  // Start tests.
  ObLinearHashMap_ACCESStest5_test_data data;
  data.map_ = &map;
  data.sum_ = sum;
  data.run_ = true;
  for (int64_t idx = 0; idx < thread_n ; ++idx) {
    pthread_create(&threads[idx], NULL, ObLinearHashMap_ACCESStest5_test_func, &data);
  }
  // Expand and Shrink.
  for (int64_t idx = 0; idx < test_loop; ++idx) {
    uint64_t L, p;
    map.load_Lp_(L, p);
    while (L < 11) {
      int err = map.expand_();
      EXPECT_TRUE(Map::ES_SUCCESS == err || Map::ES_TRYLOCK_FAILED == err);
      map.load_Lp_(L, p);
    }
    while (L > 0 || (L == 0 && p > 0)) {
      int err = map.shrink_();
      EXPECT_TRUE(Map::ES_TRYLOCK_FAILED == err || Map::ES_SUCCESS == err
          || Map::ES_REACH_FOREACH_LIMIT == err);
      map.load_Lp_(L, p);
    }
  }
  data.run_ = false;
  for (int64_t idx = 0; idx < thread_n; ++idx) {
    pthread_join(threads[idx], NULL);
  }
  //EXPECT_EQ(OB_SUCCESS, map.destroy());
}
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// Performance tests.
// Test Key type.
class PerfTestKey
{
public:
  uint64_t key;
  uint64_t data;
  uint64_t hash() const
  {
    return key;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const PerfTestKey &other) const
  {
    return key == other.key;
  }
};

class PerfTestValue
{
public:
  uint64_t val;
};

typedef ObLinearHashMap<PerfTestKey, PerfTestValue> PerfMap;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// Test multi-thread insertion performance.
// Will print report.
struct PerfInsert1_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  PerfMap *map_;
};
struct PerfInsert1_sample
{
  uint64_t count_;
  uint64_t time_stamp_;
  double load_factor_;
};
void* PerfInsert1Func(void *data)
{
  PerfInsert1_data *pdata = reinterpret_cast<PerfInsert1_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    //key.key = idx;
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->insert(key, val)) {
      fprintf(stderr, "key: %lu\n", key.key);
    }
    idx += pdata->th_cnt_;
  }
  return NULL;
}
TEST(ObLinearHashMap, PerfInsert1)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Sample array.
  const uint64_t sample_cnt = 30 * 1; // One sample a second.
  PerfInsert1_sample *samples = new PerfInsert1_sample[sample_cnt];
  // Start threads.
  const uint64_t th_cnt = 16;
  PerfInsert1_data data[th_cnt];
  pthread_t ths[th_cnt];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(0.01, 1);
  CPUPROF_START();
  for (uint64_t idx = 0; idx < th_cnt ; ++idx) {
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_cnt;
    data[idx].run_ = true;
    pthread_create(&ths[idx], NULL, PerfInsert1Func, (void*)&data[idx]);
  }
  uint64_t start_t = ts();
  for (uint64_t idx = 0; idx < sample_cnt; ++idx) {
    ::usleep(1000000);
    samples[idx].count_ = map.count();
    samples[idx].load_factor_ = map.get_load_factor();
    samples[idx].time_stamp_ = ts() - start_t;
    fprintf(stderr, "second:%lu cnt:%lu load_factor:%f\n",
      (samples[idx].time_stamp_) / 1000000, samples[idx].count_, samples[idx].load_factor_);
  }
  CPUPROF_STOP();
  for (uint64_t idx = 0; idx < th_cnt; ++idx) {
    data[idx].run_ = false;
    pthread_join(ths[idx], NULL);
  }
  // Disable destroy so that clear() would not affect the CPU profile result.
  // map.destroy();
  delete[] samples;
}

// Expand before insert.
struct PerfInsert2_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  PerfMap *map_;
};
struct PerfInsert2_sample
{
  uint64_t count_;
  uint64_t time_stamp_;
  double load_factor_;
};
void* PerfInsert2Func(void *data)
{
  PerfInsert2_data *pdata = reinterpret_cast<PerfInsert2_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    //key.key = idx;
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->insert(key, val)) {
      fprintf(stderr, "key: %lu\n", key.key);
    }
    idx += pdata->th_cnt_;
  }
  return NULL;
}
TEST(ObLinearHashMap, PerfInsert2)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Sample array.
  const uint64_t sample_cnt = 30 * 1; // One sample a second.
  PerfInsert2_sample *samples = new PerfInsert2_sample[sample_cnt];
  // Start threads.
  const uint64_t th_cnt = 16;
  PerfInsert2_data data[th_cnt];
  pthread_t ths[th_cnt];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(1e-20, 1e20); // Disable expand & shrink.
  uint64_t expand_n = 100000000;
  for (uint64_t idx = 0; idx < expand_n ; ++idx) {
    map.expand_();
  }
  CPUPROF_START();
  for (uint64_t idx = 0; idx < th_cnt ; ++idx) {
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_cnt;
    data[idx].run_ = true;
    pthread_create(&ths[idx], NULL, PerfInsert2Func, (void*)&data[idx]);
  }
  uint64_t start_t = ts();
  for (uint64_t idx = 0; idx < sample_cnt; ++idx) {
    ::usleep(1000000);
    samples[idx].count_ = map.count();
    samples[idx].load_factor_ = map.get_load_factor();
    samples[idx].time_stamp_ = ts() - start_t;
    fprintf(stderr, "second:%lu cnt:%lu load_factor:%f\n",
      (samples[idx].time_stamp_) / 1000000, samples[idx].count_, samples[idx].load_factor_);
  }
  CPUPROF_STOP();
  for (uint64_t idx = 0; idx < th_cnt; ++idx) {
    data[idx].run_ = false;
    pthread_join(ths[idx], NULL);
  }
  // Disable destroy so that clear() would not affect the CPU profile result.
  // map.destroy();
  delete[] samples;
}

// Expand before insert.
// No count version.
struct PerfInsert3_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  uint64_t key_cnt_;
  PerfMap *map_;
};
struct PerfInsert3_sample
{
  uint64_t count_;
  uint64_t time_stamp_;
  double load_factor_;
};
void* PerfInsert3Func(void *data)
{
  PerfInsert3_data *pdata = reinterpret_cast<PerfInsert3_data*>(data);
  uint64_t idx = pdata->th_idx_;

  // CPU affinity
  //cpu_set_t cpuset;
  //CPU_ZERO(&cpuset);
  //CPU_SET((int)idx, &cpuset);
  //pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    //key.key = idx;
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->insert(key, val)) {
      fprintf(stderr, "key: %lu\n", key.key);
    }
    pdata->key_cnt_ += (uint64_t)1;
    idx += pdata->th_cnt_;
  }
  return NULL;
}
TEST(ObLinearHashMap, PerfInsert3)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Sample array.
  const uint64_t sample_cnt = 30 * 1; // One sample a second.
  PerfInsert3_sample *samples = new PerfInsert3_sample[sample_cnt];
  // Start threads.
  const uint64_t th_cnt = 16;
  PerfInsert3_data data[th_cnt];
  pthread_t ths[th_cnt];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(0, 1e20); // Disable expand & shrink.
  uint64_t expand_n = 400000000;
  for (uint64_t idx = 0; idx < expand_n ; ++idx) {
    map.expand_();
  }
  CPUPROF_START();
  for (uint64_t idx = 0; idx < th_cnt ; ++idx) {
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_cnt;
    data[idx].key_cnt_ = 0;
    data[idx].run_ = true;
    pthread_create(&ths[idx], NULL, PerfInsert3Func, (void*)&data[idx]);
  }
  uint64_t start_t = ts();
  for (uint64_t idx = 0; idx < sample_cnt; ++idx) {
    ::usleep(1000000);
    uint64_t key_cnt = 0;
    for (uint64_t th = 0; th < th_cnt ; ++th) {
      key_cnt += data[th].key_cnt_;
    }
    samples[idx].count_ = key_cnt;
    samples[idx].load_factor_ = map.get_load_factor();
    samples[idx].time_stamp_ = ts() - start_t;
    fprintf(stderr, "second:%lu cnt:%lu load_factor:%f\n",
      (samples[idx].time_stamp_) / 1000000, samples[idx].count_,
      (double)samples[idx].count_ / (double)expand_n);
  }
  CPUPROF_STOP();
  for (uint64_t idx = 0; idx < th_cnt; ++idx) {
    data[idx].run_ = false;
    pthread_join(ths[idx], NULL);
  }
  // Disable destroy so that clear() would not affect the CPU profile result.
  // map.destroy();
  delete[] samples;
}

// Test slower insert speed which may benefit the load factor.
struct PerfInsert4_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  int sleep_usec_;
  int busy_loop_;
  bool do_get_;
  PerfMap *map_;
};
struct PerfInsert4_sample
{
  uint64_t count_;
  uint64_t time_stamp_;
  double load_factor_;
};
void* PerfInsert4Func(void *data)
{
  PerfInsert4_data *pdata = reinterpret_cast<PerfInsert4_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->insert(key, val)) {
      fprintf(stderr, "key: %lu\n", key.key);
    }
    if (pdata->do_get_) {
      // Get a key that may be inserted by another thread.
      uint64_t idx2 = idx + 1;
      key.key = murmurhash64A(&idx2, sizeof(idx2), 0);
      pdata->map_->get(key, val);
    }
    idx += pdata->th_cnt_;
    if (pdata->sleep_usec_ > 0) { ::usleep(pdata->sleep_usec_); }
    if (pdata->busy_loop_ > 0) { nop_loop((uint64_t)pdata->busy_loop_); }
  }
  return NULL;
}
TEST(ObLinearHashMap, PerfInsert4)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Sample array.
  const uint64_t sample_cnt = 30 * 10; // One sample a second.
  int sleep_usec = 0;
  const char *sleep_usec_env = getenv("PI4USLP");
  int busy_loop = 0;
  const char *busy_loop_env = getenv("PI4BL");
  if (sleep_usec_env != NULL) {
    sleep_usec = atoi(sleep_usec_env);
  }
  if (busy_loop_env != NULL) {
    busy_loop = atoi(busy_loop_env);
  }
  bool do_get = false;
  if (getenv("PI4G") != NULL) {
    do_get = true;
  }
  fprintf(stderr, "sleep usec: %d busy loop: %d do get: %s\n",
      sleep_usec, busy_loop, do_get ? "true" : "false");
  PerfInsert4_sample *samples = new PerfInsert4_sample[sample_cnt];
  // Start threads.
  const uint64_t th_cnt = 16;
  PerfInsert4_data data[th_cnt];
  pthread_t ths[th_cnt];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(0.01, 1);
  for (uint64_t idx = 0; idx < th_cnt ; ++idx) {
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_cnt;
    data[idx].run_ = true;
    data[idx].sleep_usec_ = sleep_usec;
    data[idx].busy_loop_ = busy_loop;
    data[idx].do_get_ = do_get;
    pthread_create(&ths[idx], NULL, PerfInsert4Func, (void*)&data[idx]);
  }
  uint64_t start_t = ts();
  for (uint64_t idx = 0; idx < sample_cnt; ++idx) {
    ::usleep(1000000);
    samples[idx].count_ = map.count();
    samples[idx].load_factor_ = map.get_load_factor();
    samples[idx].time_stamp_ = ts() - start_t;
    fprintf(stderr, "second:%lu cnt:%lu load_factor:%f\n",
      (samples[idx].time_stamp_) / 1000000, samples[idx].count_, samples[idx].load_factor_);
  }
  for (uint64_t idx = 0; idx < th_cnt; ++idx) {
    data[idx].run_ = false;
    pthread_join(ths[idx], NULL);
  }
  // Disable destroy so that clear() would not affect the CPU profile result.
  // map.destroy();
  delete[] samples;
}

// Blend insert and get.
struct PerfInsert5Insert_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  PerfMap *map_;
};
struct PerfInsert5Get_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  uint64_t get_cnt_;
  PerfMap *map_;
};
void* PerfInsert5InsertFunc(void *data)
{
  PerfInsert5Insert_data *pdata = reinterpret_cast<PerfInsert5Insert_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->insert(key, val)) {
      fprintf(stderr, "key: %lu\n", key.key);
    }
    idx += pdata->th_cnt_;
  }
  return NULL;
}
void* PerfInsert5GetFunc(void *data)
{
  PerfInsert5Get_data *pdata = reinterpret_cast<PerfInsert5Get_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    pdata->map_->get(key, val);
    idx += pdata->th_cnt_;
    pdata->get_cnt_ += 1;
  }
  return NULL;
}
TEST(ObLinearHashMap, PerfInsert5)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Sample array.
  const uint64_t sample_cnt = 30 * 10; // One sample a second.
  // Get param by env.
  uint64_t insert_th = 0;
  uint64_t get_th = 0;
  if (getenv("PI5I")) { insert_th = atoi(getenv("PI5I")); }
  if (getenv("PI5G")) { get_th = atoi(getenv("PI5G")); }
  fprintf(stderr, "Insert: %lu threads, Get: %lu threads\n", insert_th, get_th);
  // Start threads.
  PerfInsert5Insert_data idata[1024];
  PerfInsert5Get_data gdata[1024];
  pthread_t iths[1024];
  pthread_t gths[1024];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(0.01, 1);
  for (uint64_t idx = 0; idx < insert_th; ++idx) {
    idata[idx].map_ = &map;
    idata[idx].th_idx_ = idx;
    idata[idx].th_cnt_ = insert_th;
    idata[idx].run_ = true;
    pthread_create(&iths[idx], NULL, PerfInsert5InsertFunc, (void*)&idata[idx]);
  }
  for (uint64_t idx = 0; idx < get_th; ++idx) {
    gdata[idx].map_ = &map;
    gdata[idx].th_idx_ = idx;
    gdata[idx].th_cnt_ = get_th;
    gdata[idx].run_ = true;
    gdata[idx].get_cnt_ = 0;
    pthread_create(&gths[idx], NULL, PerfInsert5GetFunc, (void*)&gdata[idx]);
  }
  uint64_t start_t = ts();
  uint64_t last_cnt = 0;
  uint64_t last_get_cnt = 0;
  for (uint64_t idx = 0; idx < sample_cnt; ++idx) {
    ::usleep(1000000);
    uint64_t cnt = map.count();
    uint64_t insert_speed = cnt - last_cnt;
    last_cnt = cnt;
    uint64_t get_cnt = 0;
    for (uint64_t idx2 = 0; idx2 < get_th ; ++idx2) {
      get_cnt += gdata[idx2].get_cnt_;
    }
    uint64_t get_speed = get_cnt - last_get_cnt;
    last_get_cnt = get_cnt;
    fprintf(stderr, "second:%lu cnt:%lu load_factor:%f insert_speed:%lu get_speed:%lu\n",
        (ts() - start_t) / 1000000, map.count(), map.get_load_factor(),
        insert_speed, get_speed);
  }
  for (uint64_t idx = 0; idx < insert_th; ++idx) {
    idata[idx].run_ = false;
    pthread_join(iths[idx], NULL);
  }
  for (uint64_t idx = 0; idx < get_th; ++idx) {
    gdata[idx].run_ = false;
    pthread_join(gths[idx], NULL);
  }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// Performance test.
// Find the relation between Load factor & Access speed.

// # TEST1 Insert performance vs Load factor.
struct PerfLoadFactor1_data
{
  bool run_;
  bool exit_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  PerfMap *map_;
};
void* PerfLoadFactor1Func(void *data)
{
  PerfLoadFactor1_data *pdata = reinterpret_cast<PerfLoadFactor1_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  while (!ATOMIC_LOAD(&pdata->exit_)) {
    while (ATOMIC_LOAD(&pdata->run_)) {
      key.key = murmurhash64A(&idx, sizeof(idx), 0);
      if (OB_SUCCESS != pdata->map_->insert(key, val)) { fprintf(stderr, "key: %lu\n", key.key); }
      idx += pdata->th_cnt_;
    }
    ::usleep(10);
  }
  return NULL;
}
TEST(ObLinearHashMap, PerfLoadFactor1)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Start threads.
  const uint64_t th_cnt = 16;
  const uint64_t sample_cnt = 300;
  PerfLoadFactor1_data data[th_cnt];
  pthread_t ths[th_cnt];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(0, 1e20); // Disable expand & shrink.
  uint64_t expand_n = 2000000;
  for (uint64_t idx = 0; idx < expand_n ; ++idx) {
    map.expand_();
  }
  for (uint64_t idx = 0; idx < th_cnt ; ++idx) {
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_cnt;
    data[idx].run_ = false;
    data[idx].exit_ = false;
    pthread_create(&ths[idx], NULL, PerfLoadFactor1Func, (void*)&data[idx]);
  }
  // Run.
  for (uint64_t idx = 0; idx < th_cnt; ++idx) {
    data[idx].run_ = true;
  }
  uint64_t start_t = ts();
  fprintf(stderr, "th_cnt:%lu\t\n", th_cnt);
  fprintf(stderr, "msec\tload_factor\tkey_count\n");
  for (uint64_t idx = 0; idx < sample_cnt; ++idx) {
    ::usleep(100000); // 10 samples per second.
    fprintf(stderr, "%lu\t%f\t%lu\n", (ts() - start_t) / 1000, map.get_load_factor(), map.count());
  }
  for (uint64_t idx = 0; idx < th_cnt; ++idx) {
    data[idx].run_ = false;
    data[idx].exit_ = true;
    pthread_join(ths[idx], NULL);
  }
}

// # TEST2 Get performance vs Load factor.
struct PerfLoadFactor2_data
{
  bool get_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  uint64_t key_lmt_;
  uint64_t get_n_;
  PerfMap *map_;
};
void* PerfLoadFactor2Func(void *data)
{
  PerfLoadFactor2_data *pdata = reinterpret_cast<PerfLoadFactor2_data*>(data);
  uint64_t idx = pdata->th_idx_;
  PerfTestKey key;
  PerfTestValue val;
  // Insert.
  while (idx < pdata->key_lmt_) {
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->insert(key, val)) { fprintf(stderr, "key: %lu\n", key.key); }
    idx += pdata->th_cnt_;
  }
  // Get.
  while (!ATOMIC_LOAD(&pdata->get_)) { ::usleep(10); }
  idx = pdata->th_idx_;
  while (ATOMIC_LOAD(&pdata->get_)) {
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    if (OB_SUCCESS != pdata->map_->get(key, val)) { fprintf(stderr, "key: %lu\n", key.key); }
    idx += pdata->th_cnt_;
    pdata->get_n_ += 1;
    if (idx >= pdata->key_lmt_) { idx = pdata->th_cnt_; }
  }
  return NULL;
}
void PerfLoadFactor2TestFunc(uint64_t bkt_n, double load_factor, uint64_t th_n)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Insert data.
  PerfLoadFactor2_data data[1024];
  pthread_t ths[1024];
  PerfMap map;
  map.init();
  map.set_load_factor_lmt(0, 1e20); // Disable expand & shrink.
  uint64_t expand_n = bkt_n;
  for (uint64_t idx = 0; idx < expand_n ; ++idx) { map.expand_(); }
  for (uint64_t idx = 0; idx < th_n; ++idx) {
    data[idx].get_ = false;
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_n;
    data[idx].get_n_ = 0;
    data[idx].key_lmt_ = (uint64_t)((double)bkt_n * load_factor);
    pthread_create(&ths[idx], NULL, PerfLoadFactor2Func, (void*)&data[idx]);
  }
  // Get test.
  uint64_t test_sec = 15;
  for (uint64_t idx = 0; idx < th_n; ++idx) { data[idx].get_ = true; }
  for (uint64_t idx = 0; idx < test_sec; ++idx) { sleep(1); }
  for (uint64_t idx = 0; idx < th_n; ++idx) {
    data[idx].get_ = false;
    pthread_join(ths[idx], NULL);
  }
  uint64_t get_n = 0;
  for (uint64_t idx = 0; idx < th_n; ++idx) { get_n += data[idx].get_n_; }
  fprintf(stderr, "th_n:%lu\tbkt_n:%lu\tload_factor:%f\tget_per_sec:%lu\n",
    th_n, bkt_n, map.get_load_factor(), get_n / test_sec);
}
TEST(ObLinearHashMap, PerfLoadFactor2)
{
  uint64_t bkt_n = 1000000;
  PerfLoadFactor2TestFunc(bkt_n, 0.01, 8);
  PerfLoadFactor2TestFunc(bkt_n, 0.05, 8);
  PerfLoadFactor2TestFunc(bkt_n, 0.1, 8);
  PerfLoadFactor2TestFunc(bkt_n, 0.5, 8);
  PerfLoadFactor2TestFunc(bkt_n, 1, 8);
  PerfLoadFactor2TestFunc(bkt_n, 5, 8);
  PerfLoadFactor2TestFunc(bkt_n, 10, 8);
  PerfLoadFactor2TestFunc(bkt_n, 20, 8);
  PerfLoadFactor2TestFunc(bkt_n, 30, 8);
  PerfLoadFactor2TestFunc(bkt_n, 0.01, 16);
  PerfLoadFactor2TestFunc(bkt_n, 0.05, 16);
  PerfLoadFactor2TestFunc(bkt_n, 0.1, 16);
  PerfLoadFactor2TestFunc(bkt_n, 0.5, 16);
  PerfLoadFactor2TestFunc(bkt_n, 1, 16);
  PerfLoadFactor2TestFunc(bkt_n, 5, 16);
  PerfLoadFactor2TestFunc(bkt_n, 10, 16);
  PerfLoadFactor2TestFunc(bkt_n, 20, 16);
}

// Test iterator.
#include <algorithm>
// Insert some keys and iterate them out.
TEST(ObLinearHashMap, IteratorTest1)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  PerfMap map;
  map.init();
  // Insert.
  uint64_t idx = 0;
  uint64_t idx_lmt = 100000;
  PerfTestKey key;
  PerfTestKey *keys = new PerfTestKey[idx_lmt];
  while (idx < idx_lmt) {
    key.key = murmurhash64A(&idx, sizeof(idx), 0);
    keys[idx] = key;
    idx += 1;
  }
  PerfTestValue val;
  idx = 0;
  while (idx < idx_lmt) {
    if (OB_SUCCESS != map.insert(keys[idx], val)) { fprintf(stderr, "key: %lu\n", key.key); }
    idx += 1;
  }
  // Iterate them out.
  PerfMap::BlurredIterator itor(map);
  idx = 0;
  while (idx < idx_lmt) {
    EXPECT_EQ(OB_SUCCESS, itor.next(key, val));
    PerfTestKey *pos = ::std::find(keys, keys + idx_lmt, key);
    EXPECT_NE(keys + idx_lmt, pos);
    pos->key = UINT64_MAX; // mark
    idx += 1;
  }
  idx = 0;
  while (idx < idx_lmt) {
    EXPECT_TRUE(UINT64_MAX == keys[idx].key);
    idx += 1;
  }
  // Time.
  itor.rewind();
  uint64_t start = ts();
  while (OB_SUCCESS == itor.next(key, val)) { }
  uint64_t end = ts();
  if (false) {
    fprintf(stderr, "iterating through %lu keys costs %lu ms speed %lu keys/sec\n",
      idx_lmt, (end - start) / 1000, idx_lmt * 1000000 / (end - start));
  }
  delete[] keys;
}

// Multi-thread testing.
struct ItorTest2_data
{
  bool run_;
  uint64_t th_idx_;
  uint64_t th_cnt_;
  uint64_t key_lmt_;
  PerfMap *map_;
};
void* ItorTest2Func(void *data)
{
  ItorTest2_data *pdata = reinterpret_cast<ItorTest2_data*>(data);
  PerfTestKey key;
  PerfTestValue val;
  while (ATOMIC_LOAD(&pdata->run_)) {
    // Insert.
    uint64_t idx = pdata->th_idx_;
    while (idx < pdata->key_lmt_) {
      key.key = murmurhash64A(&idx, sizeof(idx), 0);
      if (OB_SUCCESS != pdata->map_->insert(key, val)) { fprintf(stderr, "insert dup key: %lu\n", key.key); }
      idx += pdata->th_cnt_;
    }
    // Insert.
    idx = pdata->th_idx_;
    while (idx < pdata->key_lmt_) {
      key.key = murmurhash64A(&idx, sizeof(idx), 0);
      if (OB_SUCCESS != pdata->map_->erase(key, val)) { fprintf(stderr, "erase invalid key: %lu\n", key.key); }
      idx += pdata->th_cnt_;
    }
  }
  return NULL;
}
TEST(ObLinearHashMap, IteratorTest2)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Insert data.
  ItorTest2_data data[1024];
  pthread_t ths[1024];
  PerfMap map;
  map.init();
  uint64_t th_n = 8;
  for (uint64_t idx = 0; idx < th_n; ++idx) {
    data[idx].map_ = &map;
    data[idx].th_idx_ = idx;
    data[idx].th_cnt_ = th_n;
    data[idx].key_lmt_ = 1000000;
    data[idx].run_ = true;
    pthread_create(&ths[idx], NULL, ItorTest2Func, (void*)&data[idx]);
  }
  uint64_t start = ts();
  uint64_t end = start;
  while (end - start < 30 * 1000000) {
    PerfMap::BlurredIterator itor(map);
    PerfTestKey key;
    PerfTestValue val;
    while (OB_SUCCESS == itor.next(key, val)) { }
    end = ts();
  }
  for (uint64_t idx = 0; idx < th_n; ++idx) {
    data[idx].run_= false;
    pthread_join(ths[idx], NULL);
  }
}


// Memory size test. Temp remove me after use.
struct MemUsageTestTag { };
typedef ObLinearHashMap<UnitTestKey, UnitTestValue> Map2;
// Size of test.
TEST(MEM, MemUsageTestA)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Print size of SmallAlloc.
  ObSmallAllocator small_alloc;
  printf(">>> size of small alloc: %lu\n", sizeof(small_alloc));
  // Print size of LinearHash.
  Map2 map;
  printf(">>> size of map: %lu\n", sizeof(map));
}

// Print mem useage after init.
TEST(MEM, MemUsageTestB)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  // Build 100,000 Maps.
  const int64_t map_cnt = 100000;
  Map2 **maps = new Map2*[map_cnt];

  // Alloc Maps and init them.
  PageArena<> arena(OB_MALLOC_BIG_BLOCK_SIZE);
  arena.set_label(ObModIds::OB_LINEAR_HASH_MAP);
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; idx < map_cnt; ++idx) {
    Map2 *map = (Map2*)arena.alloc(sizeof(Map2));
    ret = map->init();
    EXPECT_EQ(OB_SUCCESS, ret);
    maps[idx] = map;
  }

  // Print mem usage.
  ob_print_mod_memory_usage();

  // Destroy.
  for (int64_t idx = 0; idx < map_cnt; ++idx) {
    ret = maps[idx]->destroy();
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  delete[] maps;
}

// Test erase_if.
struct EraseIf1Tester
{
  bool operator()(const UnitTestKey &key, UnitTestValue &value)
  {
    UNUSED(key);
    return (0 == value.val % 2);
  }
};
TEST(ObLinearHashMap, EraseIf1)
{
  bool run_test = false;
  run_test = (run_all_test) ? true : run_test;
  if (!run_test) return;

  const int64_t limit = 100000;

  Map map;
  EXPECT_EQ(OB_SUCCESS, map.init());
  for (int64_t idx = 0; idx < limit ; idx++) {
    UnitTestKey key;
    UnitTestValue value;
    key.key = idx;
    value.val = idx;
    // Insert.
    EXPECT_EQ(OB_SUCCESS, map.insert(key, value));
  }
  // Test, remove odd number.
  EraseIf1Tester ei;
  for (int64_t idx = 0; idx < limit; idx++) {
    UnitTestKey key;
    key.key = idx;
    if (0 == idx % 2) {
      EXPECT_EQ(OB_SUCCESS, map.erase_if(key, ei));
    } else {
      EXPECT_EQ(OB_EAGAIN, map.erase_if(key, ei));
    }
  }
  EXPECT_EQ(OB_SUCCESS, map.clear());
  EXPECT_EQ(OB_SUCCESS, map.destroy());
}

struct ForEachOp
{
  int64_t count_;
  UnitTestValue target_val_;

  explicit ForEachOp(const UnitTestValue &val) : count_(0), target_val_(val) {}

  bool operator () (const UnitTestKey &key, UnitTestValue &val)
  {
    UNUSED(key);
    count_++;
    EXPECT_EQ(target_val_, val);
    return true;
  }
};

TEST(ObLinearHashMap, insert_or_update)
{
  Map map;

  EXPECT_EQ(OB_SUCCESS, map.init());

  UnitTestKey key;
  UnitTestValue value;

  key.key = 1;

  // update value of one key, there must be one kv pair.
  for (int64_t index = 0; index < 10000; index++) {
    UnitTestValue value_get;
    value.val = index;
    EXPECT_EQ(OB_SUCCESS, map.insert_or_update(key, value));
    EXPECT_EQ(OB_SUCCESS, map.get(key, value_get));
    EXPECT_EQ(value, value_get);
    EXPECT_EQ(1, map.count());

    ForEachOp op(value);
    EXPECT_EQ(OB_SUCCESS, map.for_each(op));
    EXPECT_EQ(1, op.count_);
  }
}

}
}

int main(int argc, char **argv)
{
  const char *run_all_test_char = getenv("RAT");
  if (NULL != run_all_test_char) {
    run_all_test = true;
  }
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
