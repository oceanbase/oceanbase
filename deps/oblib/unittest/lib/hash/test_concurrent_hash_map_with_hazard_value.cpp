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
#include "lib/hash/ob_concurrent_hash_map_with_hazard_value.h"
#include "lib/random/ob_random.h"
#include "lib/coro/testing.h"

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

typedef ObConcurrentHashMapWithHazardValue<Key, int64_t*> HashMap;

class ValueAlloc : public HashMap::IValueAlloc
{
public:
  int64_t* alloc() { return op_alloc(int64_t); }
  void free(int64_t* value) { op_free(value); }
};
class ValueReclaim : public HashMap::IValueReclaimCallback
{
public:
  ValueReclaim() : reclaimed_num(0) { }
  void reclaim_value(int64_t* value) { op_free(value); ATOMIC_AAF(&reclaimed_num, 1); }
  int64_t reclaimed_num;
};

TEST(TestObConcurrentHashMapWithHazardValue, init)
{
  HashMap hashmap;
  ASSERT_EQ(OB_SUCCESS, hashmap.init());
  int64_t* v;
  ASSERT_EQ(OB_SUCCESS, hashmap.create_refactored(Key(1), v));
  *v = 1;
  ASSERT_EQ(OB_SUCCESS, hashmap.revert_value(v));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(Key(1), v));
  ASSERT_EQ(1, *v);
  ASSERT_EQ(OB_SUCCESS, hashmap.revert_value(v));
  ASSERT_EQ(OB_ENTRY_EXIST, hashmap.contains_key(Key(1)));
  Adder adder1;
  ASSERT_EQ(OB_SUCCESS, hashmap.for_each(adder1));
  ASSERT_EQ(1, adder1.sum);
  ASSERT_EQ(OB_SUCCESS, hashmap.remove_refactored(Key(1)));
  Adder adder2;
  ASSERT_EQ(OB_SUCCESS, hashmap.for_each(adder2));
  ASSERT_EQ(0, adder2.sum);
}


TEST(TestObConcurrentHashMapWithHazardValue, hazard_value)
{
  HashMap hashmap;
  ValueAlloc value_alloc;
  ValueReclaim value_reclaim;
  ASSERT_EQ(OB_SUCCESS, hashmap.init(&value_alloc, &value_reclaim));
  int64_t* v = op_alloc(int64_t);
  *v = 1;
  ASSERT_EQ(OB_SUCCESS, hashmap.put_refactored(Key(1), v));
  v = op_alloc(int64_t);
  *v = 2;
  ASSERT_EQ(OB_ENTRY_EXIST, hashmap.put_refactored(Key(1), v));
  ASSERT_EQ(OB_SUCCESS, hashmap.put_refactored(Key(2), v));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, hashmap.get_refactored(Key(3), v));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, hashmap.remove_refactored(Key(3)));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(Key(1), v));
  ASSERT_EQ(1, *v);
  Adder adder1;
  ASSERT_EQ(OB_SUCCESS, hashmap.for_each(adder1));
  ASSERT_EQ(2, adder1.sum);
  ASSERT_EQ(OB_SUCCESS, hashmap.remove_refactored(Key(1)));
  Adder adder;
  ASSERT_EQ(OB_SUCCESS, hashmap.for_each(adder));
  ASSERT_EQ(1, adder.sum);
  ASSERT_EQ(0, value_reclaim.reclaimed_num);
  ASSERT_EQ(OB_SUCCESS, hashmap.revert_value(v));
  ASSERT_EQ(0, value_reclaim.reclaimed_num);
  ASSERT_EQ(OB_SUCCESS, hashmap.remove_refactored(Key(2)));
  ASSERT_EQ(1, value_reclaim.reclaimed_num);
}

int64_t create_num = 0;
class ObStressThread : public cotesting::DefaultRunnable
{
public:
  ObStressThread() : finished_count(0) { }
  void run1() final
  {
    int64_t* v = NULL;
    int err = OB_SUCCESS;
    int64_t N = 100000;
    for (int64_t i = 0; i < N; i++) {
      int64_t k = ObRandom::rand(0, N / 10);
      err = hashmap->get_refactored(Key(k), v);
      if (OB_ENTRY_NOT_EXIST == err) {
        err = hashmap->create_refactored(Key(k), v);
        if (OB_SUCCESS == err) {
          ATOMIC_AAF(&create_num, 1);
          *v = k;
          hashmap->revert_value(v);
        }
      } else {
        err = hashmap->remove_refactored(Key(k));
        hashmap->revert_value(v);
      }
    }
    for (int64_t i = 0; i < N; i++) {
      hashmap->remove_refactored(Key(i));
    }
    ATOMIC_AAF(&finished_count, 1);
    while (ATOMIC_LOAD(&finished_count) != _threadCount) ;
    /*
     * In case of concurrent running of multiple threads,
     * ob_hash.h cannot purge completely due to concurrent interference.
     * The code above sets a barrier to ensure all running thread has reached there.
     * The code following is mutually exclusive protected by mutex,
     * gives every thread a change to purge at least one item so that each thread in ob_hazard_pointer
     * can do reclaim at least once.
     */
    mutex.lock();
    err = hashmap->create_refactored(Key(N), v);
    if (OB_SUCCESS == err) {
      //ATOMIC_AAF(&create_num, 1);
      hashmap->revert_value(v);
    }
    hashmap->remove_refactored(Key(N));
    mutex.unlock();
  }
  HashMap* hashmap;
private:
  obsys::ThreadMutex mutex;
  int finished_count;
};

TEST(TestObConcurrentHashMapWithHazardValue, concurrent)
{
  HashMap hashmap;
  ValueAlloc value_alloc;
  ValueReclaim value_reclaim;
  ASSERT_EQ(OB_SUCCESS, hashmap.init(&value_alloc, &value_reclaim));
  ObStressThread threads;
  threads.hashmap = &hashmap;
  threads.set_thread_count((int)sysconf(_SC_NPROCESSORS_ONLN) * 2);
  threads.start();
  threads.wait();
  ASSERT_EQ(create_num, value_reclaim.reclaimed_num);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level(2);
  //OB_LOGGER.set_file_name("test_concurrent_hash_map_with_hazard_value.log", false, true);
  return RUN_ALL_TESTS();
}
