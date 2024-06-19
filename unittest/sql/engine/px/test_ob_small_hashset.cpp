/**
 * Copyright (c) 2024 OceanBase
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
#include <unordered_set>
#include <vector>
#define private public
// #define unittest
#define unittest_bloom_filter
#include "src/sql/engine/px/p2p_datahub/ob_small_hashset.h"
#include "lib/hash/ob_hashset.h"
#include "sql/engine/px/ob_px_bloom_filter.h"

using namespace std;
namespace oceanbase
{
namespace sql
{

static constexpr uint64_t build_count = 4096;

class SimpleTimer
{
public:
  SimpleTimer() {
    cpu_begin_time_ = rdtsc();
  }
  ~SimpleTimer() {
    uint64_t elapse_time = rdtsc() - cpu_begin_time_;
    cout << "elapse time is: " << elapse_time << endl;
  }
private:
  uint64_t cpu_begin_time_;

};

class SmallHashSetTest : public ::testing::Test
{
public:
  SmallHashSetTest() = default;
  virtual ~SmallHashSetTest() = default;
  virtual void SetUp(){};
  virtual void TearDown(){};

  void insert_hash(int64_t insert_count);
  void test_hash(int64_t test_count);
  void performance_test();

public:
  ObArenaAllocator alloc_;
  ObSmallHashSet<true> accurate_small_set_;

  ObSmallHashSet<false> inaccurate_small_set_;
  std::unordered_set<uint64_t> std_set_;
  hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> ob_set_;
  ObPxBloomFilter bloom_filter_;
private:
  DISALLOW_COPY_AND_ASSIGN(SmallHashSetTest);
};

void SmallHashSetTest::insert_hash(int64_t insert_count) {
  constexpr uint64_t rand_upper_bound = 100000;
  for (int64_t i = 0; i < insert_count; ++i) {
    int64_t num = common::ObRandom::rand(1, rand_upper_bound);
    uint64_t hash = murmurhash(&num, 8, 0);
    accurate_small_set_.insert_hash(hash);
    inaccurate_small_set_.insert_hash(hash);
    std_set_.insert(hash);
  }
}

void SmallHashSetTest::test_hash(int64_t test_count) {
  constexpr uint64_t rand_upper_bound = 100000;
  for (int64_t i = 0; i < test_count; ++i) {
    int64_t num = common::ObRandom::rand(1, rand_upper_bound);
    uint64_t hash = murmurhash(&num, 8, 0);
    bool in_accurate_set = accurate_small_set_.test_hash(hash);
    bool in_inaccurate_set = inaccurate_small_set_.test_hash(hash);
    bool in_std_set = std_set_.count(hash) != 0;
    EXPECT_EQ(in_accurate_set, in_std_set);
    EXPECT_GE(in_inaccurate_set, in_accurate_set);
  }

#ifdef unittest
  int ret = OB_SUCCESS;
  uint64_t accurate_total = accurate_small_set_.seek_total_times_;
  uint64_t inaccurate_total = inaccurate_small_set_.seek_total_times_;
  double accurate_avg = accurate_small_set_.seek_total_times_ / double(test_count);
  double inaccurate_avg = inaccurate_small_set_.seek_total_times_ / double(test_count);
  COMMON_LOG(WARN, "avg seek", K(accurate_total), K(inaccurate_total), K(accurate_avg),
             K(inaccurate_avg));
#endif
}

void SmallHashSetTest::performance_test()
{
  constexpr uint64_t probe_count = 1000000;
  std::vector<uint64_t> build_hash_values(probe_count, 0);
  std::vector<uint64_t> probe_hash_values(probe_count, 0);

  accurate_small_set_.clear();
  inaccurate_small_set_.clear();
  std_set_.clear();

// ------------------- build test -----------------------

  for (int64_t i = 0; i < build_count; ++i) {
    int64_t num = common::ObRandom::rand(0, UINT64_MAX);
    build_hash_values[i] = murmurhash(&num, 8, 0);
  }

  {
    cout << "Build:: for accurate_small_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < build_count; ++i) {
      accurate_small_set_.insert_hash(build_hash_values[i]);
    }
  }

  {
    cout << "Build:: for inaccurate_small_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < build_count; ++i) {
      inaccurate_small_set_.insert_hash(build_hash_values[i]);
    }
  }

  {
    cout << "Build:: for std::unordered_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < build_count; ++i) {
      std_set_.insert(build_hash_values[i]);
    }
  }

  {
    cout << "Build:: for ob_hash_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < build_count; ++i) {
      ob_set_.set_refactored(build_hash_values[i]);
    }
  }

#ifdef unittest_bloom_filter
  {
    cout << "Build:: for bloom filter, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < build_count; ++i) {
      bloom_filter_.put(build_hash_values[i]);
    }
  }
#endif

// ------------------- probe test -----------------------

  for (int64_t i = 0; i < probe_count; ++i) {
    int64_t num = common::ObRandom::rand(0, UINT64_MAX);
    probe_hash_values[i] = murmurhash(&num, 8, 0);
  }

  {
    cout << "Probe:: for accurate_small_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < probe_count; ++i) {
      accurate_small_set_.test_hash(probe_hash_values[i]);
    }
  }

  {
    cout << "Probe:: for inaccurate_small_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < probe_count; ++i) {
      inaccurate_small_set_.test_hash(probe_hash_values[i]);
    }
  }

  {
    cout << "Probe:: for std::unordered_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < probe_count; ++i) {
      std_set_.count(probe_hash_values[i]);
    }
  }

  {
    cout << "Probe:: for ob_hash_set, ";
    SimpleTimer timer;
    for (int64_t i = 0; i < probe_count; ++i) {
      ob_set_.exist_refactored(probe_hash_values[i]);
    }
  }

#ifdef unittest_bloom_filter
  {
    cout << "Probe:: for bloom_filter batch, ";
    SimpleTimer timer;
    int64_t batch_size = 256;
    uint64_t round = probe_count / batch_size;
    for (int64_t i = 0; i < round; ++i) {
      uint64_t offset = i * batch_size;
      bloom_filter_.might_contain_batch(probe_hash_values.data() + offset, batch_size);
    }
    int64_t last_batch_size = probe_count % batch_size;
    if (last_batch_size > 0) {
      uint64_t offset = round * batch_size;
      bloom_filter_.might_contain_batch(probe_hash_values.data() + offset, last_batch_size);
    }
  }
#endif

// ------------------- false positive rate -----------------------
  {
    int64_t total_count = probe_count;
    int64_t error_count = 0;
    for (int64_t i = 0; i < probe_count; ++i) {
      bool result_inacc = inaccurate_small_set_.test_hash(probe_hash_values[i]);
      bool result_std = std_set_.count(probe_hash_values[i]) != 0;
      if (result_inacc != result_std) {
        error_count++;
      }
    }
    double error_rate = double(error_count) / double(total_count);
    cout << "Probe:: the false positive rate is: " << error_rate
         << ", error_count: " << error_count
         << ", total_count: " << total_count << endl;
  }
}

TEST_F(SmallHashSetTest, test_small_hash_set)
{
  int64_t tenant_id = 1;
  accurate_small_set_.init(build_count, tenant_id);
  inaccurate_small_set_.init(build_count, tenant_id);
  cout << "the small hash set init size is: " << accurate_small_set_.capacity_ << endl;
  ob_set_.create(build_count * 4);

  bloom_filter_.init(build_count, alloc_, tenant_id, 0.03, 2147483648);

  insert_hash(4000);
  test_hash(100000);
  performance_test();
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}