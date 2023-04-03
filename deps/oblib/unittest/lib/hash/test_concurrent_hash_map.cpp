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

#include "gtest/gtest.h"
#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/random/ob_random.h"
#include "lib/thread/thread_pool.h"

using namespace oceanbase;
using namespace common;

struct Key
{
  int64_t key;
  Key() : key(0) { }
  Key(int64_t k) : key(k) { }
  uint64_t hash() { return murmurhash(&key, sizeof(key), 0); };
  int compare(const Key & r) { return key < r.key ? -1 : key == r.key ? 0 : 1; }
};

struct Adder
{
  int64_t sum;
  Adder() : sum(0) { }
  void operator()(Key k, int64_t* v) { UNUSED(k); UNUSED(v); sum++; }
};

typedef ObConcurrentHashMap<Key, int64_t> HashMap;

int64_t create_num = 0;
class ObStressThread : public lib::ThreadPool
{
public:
  ObStressThread() { }
  void run1()
  {
    int64_t v = 0;
    int err = OB_SUCCESS;
    int64_t N = 1000000;
    int64_t k = 10;//ObRandom::rand(0, N / 10);
    for (int64_t i = 0; i < N; i++) {
      err = hashmap->get_refactored(Key(k), v);
    }
    UNUSED(err);
  }
  HashMap* hashmap;
};

TEST(TestObConcurrentHashMap, concurrent)
{
  HashMap hashmap;
  for (int64_t i = 0; i < 137; i++) {
    Key k(1099511648790 + i);
    ASSERT_EQ(OB_SUCCESS, hashmap.put_refactored(k, k.key * 12));
  }
  ObStressThread threads;
  threads.hashmap = &hashmap;
  threads.set_thread_count(4);
  threads.start();
  threads.wait();
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level(2);
  //OB_LOGGER.set_file_name("test_concurrent_hash_map_with_hazard_value.log", false, true);
  return RUN_ALL_TESTS();
}
