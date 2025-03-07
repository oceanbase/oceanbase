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
#include <random>
#include <algorithm>

#define private public
// #define unittest
#define unittest_bloom_filter
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "src/sql/engine/expr/ob_expr_in.h"

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
  void insert_key(int64_t insert_count);
  void test_key(int64_t test_count);
  void performance_test();

public:
  ObArenaAllocator alloc_;
  ObSmallHashSet<true> accurate_small_set_;
  ObColumnHashSet<uint64_t> open_set_;
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


void SmallHashSetTest::insert_key(int64_t insert_count) {
  constexpr uint64_t rand_upper_bound = 100000;
  for (int64_t i = 0; i < insert_count; ++i) {
    int64_t num = common::ObRandom::rand(1, rand_upper_bound);
    uint64_t hash = murmurhash(&num, 8, 0);
    open_set_.insert(hash, num);
    std_set_.insert(num);
  }
}

void SmallHashSetTest::test_key(int64_t test_count) {
  constexpr uint64_t rand_upper_bound = 100000;
  for (int64_t i = 0; i < test_count; ++i) {
    int64_t num = common::ObRandom::rand(1, rand_upper_bound);
    uint64_t hash = murmurhash(&num, 8, 0);
    bool in_accurate_set = open_set_.exists(hash, num);
    bool in_std_set = std_set_.count(num) != 0;
    EXPECT_EQ(in_accurate_set, in_std_set);
  }
}

void SmallHashSetTest::performance_test()
{
  constexpr uint64_t probe_count = 1000000;
  std::vector<uint64_t> build_hash_values(probe_count, 0);
  std::vector<uint64_t> build_key_values(probe_count, 0);
  std::vector<uint64_t> probe_hash_values(probe_count, 0);
  std::vector<uint64_t> probe_key_values(probe_count, 0);

  accurate_small_set_.clear();
  inaccurate_small_set_.clear();
  open_set_.clear();
  std_set_.clear();

// ------------------- build test -----------------------

  for (int64_t i = 0; i < build_count; ++i) {
    int64_t num = common::ObRandom::rand(0, UINT64_MAX);
    build_key_values[i] = num;
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
    probe_key_values[i] = num;
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
  open_set_.init(build_count, tenant_id);
  cout << "the small hash set init size is: " << accurate_small_set_.capacity_ << endl;
  ob_set_.create(build_count * 4);

  bloom_filter_.init(build_count, alloc_, tenant_id, 0.03, 2147483648);

  insert_hash(4000);
  test_hash(100000);
  insert_key(4000);
  test_key(100000);
  performance_test();
}

struct MurmurHashTest {
    typedef uint64_t result_type;
    typedef int argument_type;

    result_type operator()(argument_type num) const noexcept {
        return murmurhash(&num, sizeof(num), 0);
    }
};

class OpenHashSetforKeyTest : public ::testing::Test {
public:
    OpenHashSetforKeyTest() = default;
    virtual ~OpenHashSetforKeyTest() = default;
    virtual void SetUp() {};
    virtual void TearDown() {};

    void insert_hash(int64_t insert_count);
    void test_hash(int64_t test_count);
    void insert_key(int64_t insert_count);
    void test_key(int64_t test_count);
    void exist_performance(int build_cnt, int probe_cnt);

public:
    ObColumnHashSet<uint64_t> open_set_;
    ObSmallHashSet<false> inaccurate_small_set_;
    std::unordered_set<uint64_t> std_set_;
    hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> ob_set_;

    std::vector<uint64_t> build_hash_values;
    std::vector<uint64_t> probe_hash_values;
    std::vector<uint64_t> probe_key_values;
protected:
    template<typename SetType>
    void run_probe_test(int cnt, const char* name,
                          const SetType& set,
                          const std::function<void(const SetType&, uint64_t, uint64_t)>& test_fn);
private:
    DISALLOW_COPY_AND_ASSIGN(OpenHashSetforKeyTest);
};

template<typename SetType>
void OpenHashSetforKeyTest::run_probe_test(int probe_count, const char* name,
                                              const SetType& set,
                                              const std::function<void(const SetType&, uint64_t, uint64_t)>& test_fn) {
    std::cout << "Probe time for " << name << ": ";
    SimpleTimer timer;
    for (int64_t i = 0; i < probe_count; ++i) {
        test_fn(set, probe_hash_values[i], probe_key_values[i]);
    }
}

void OpenHashSetforKeyTest::exist_performance(int build_cnt, int probe_count) {
    build_hash_values.resize(probe_count, 0);
    probe_hash_values.resize(probe_count, 0);
    probe_key_values.resize(probe_count, 0);

    // Build phase
    for (int64_t i = 0; i < build_cnt; ++i) {
        int64_t num = common::ObRandom::rand(0, build_cnt);
        build_hash_values[i] = murmurhash(&num, 8, 0);
        open_set_.insert(build_hash_values[i], num);
        inaccurate_small_set_.insert_hash(build_hash_values[i]);
        ob_set_.set_refactored(num);
    }

    // Generate probes
    for (int64_t i = 0; i < probe_count; ++i) {
        int64_t num = common::ObRandom::rand(0, build_cnt);
        probe_key_values[i] = num;
        probe_hash_values[i] = murmurhash(&probe_key_values[i], 8, 0);
    }

    auto test_open_set_fn = [](const ObColumnHashSet<uint64_t>& set, uint64_t h, uint64_t k){
        set.exists(h, k);
    };

    auto test_ob_set_fn = [](const hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode>& set, uint64_t h, uint64_t k){
        set.exist_refactored(k);
    };

    auto test_inaccurate_small_set_fn = [](const ObSmallHashSet<false>& set, uint64_t h, uint64_t k){
        set.test_hash(h);
    };

    std::vector<std::function<void(void)>> tests_to_run = {
        [&]() { return run_probe_test<ObColumnHashSet<uint64_t>>(probe_count, "open_set", open_set_, test_open_set_fn); },
        [&]() { return run_probe_test<hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode>>(probe_count, "ob_set", ob_set_, test_ob_set_fn); },
        [&]() { return run_probe_test<ObSmallHashSet<false>>(probe_count, "inaccurate_small_set", inaccurate_small_set_, test_inaccurate_small_set_fn); }
    };

    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(tests_to_run.begin(), tests_to_run.end(), g);

    for (auto& test : tests_to_run) {
        test();
    }
}

TEST_F(OpenHashSetforKeyTest, test_small_hash_set_exist_performance) {
    int64_t tenant_id = 1;
    int max_build_cnt = 1000000;

    const int build_counts[] = {10000, 100000, 1000000};
    const int probe_counts[] = {10000, 100000, 1000000, 10000000};
    open_set_.init(max_build_cnt * 2, tenant_id);
    inaccurate_small_set_.init(max_build_cnt * 2, tenant_id);
    ob_set_.create(max_build_cnt * 4);
    for (const auto &build_cnt : build_counts) {
        for (const auto &probe_cnt : probe_counts) {
            open_set_.clear();
            inaccurate_small_set_.clear();
            ob_set_.clear();

            std::cout << "===== build_cnt: " << build_cnt << ", probe_cnt: " << probe_cnt << std::endl;
            exist_performance(build_cnt, probe_cnt);
            std::cout << "================" << std::endl;
        }
    }
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
