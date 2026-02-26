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
#include "common/ob_clock_generator.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/random/ob_random.h"
#include "lib/hash/ob_linear_hash_map.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{

const int64_t MAX_KEY_NUM = 1000000;
const int64_t GET_THREAD_NUM = 2;
const int64_t ERASE_THREAD_NUM = 2;
const int64_t INSERT_THREAD_NUM = 8;
const int64_t UPDATE_THREAD_NUM = 2;
const int64_t CLEAR_THREAD_NUM = 1;
const int64_t ALL_THREAD_NUM = GET_THREAD_NUM + ERASE_THREAD_NUM + INSERT_THREAD_NUM + UPDATE_THREAD_NUM + CLEAR_THREAD_NUM;

int g_exiting = 0;

int64_t g_get_cnt;
int64_t g_erase_cnt;
int64_t g_insert_cnt;
int64_t g_update_cnt;

class MyKey
{
public:
  MyKey() { key_ = ObRandom::rand(0, MAX_KEY_NUM); }
  uint64_t hash() const { return key_; }
  uint64_t hash(uint64_t &hv) const { hv = key_; return key_; }
  uint64_t get() const { return key_; }
  bool operator==(const MyKey &k1) const { return k1.key_ == this->key_; }
  static MyKey get_random_key() { return MyKey(); }
private:
  uint64_t key_;
};

class MyValue
{
public:
  MyValue() : value_(0) {}
  explicit MyValue(const uint64_t value) : value_(value) {}
private:
  uint64_t value_;
};

typedef ObLinearHashMap<MyKey, MyValue> MyHashMap;

class TestObLinearHashMap : public ::testing::Test
{
public :
  virtual void SetUP() {}
  virtual void TearDown() {}
  int create_task(const int64_t thread_num, void *(*func)(void *), void *arg)
  {
    int ret = 0;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    for (int64_t i = 0; 0 == ret && i < thread_num; ++i) {
      ret = pthread_create(&tids[tid_idx++], &attr, func, arg);
    }
    return ret;
  }
  MyHashMap my_hashmap_;
  pthread_t tids[ALL_THREAD_NUM];
  int64_t tid_idx;
};

void *do_get(void *args)
{
  prctl(PR_SET_NAME, __func__);
  MyHashMap *map = (MyHashMap *)args;
  while (!g_exiting) {
    MyKey key = MyKey::get_random_key();
    MyValue value;
    if (OB_SUCCESS == map->get(key, value)) {
      g_get_cnt++;
    }
    usleep(100);
  }
  return (void *)0;
}

void *do_erase(void *args)
{
  prctl(PR_SET_NAME, __func__);
  MyHashMap *map = (MyHashMap *)args;
  while (!g_exiting) {
    MyKey key = MyKey::get_random_key();
    if (OB_SUCCESS == map->erase(key)) {
      g_erase_cnt++;
    }
    usleep(20);
  }
  return (void *)0;
}

void *do_insert(void *args)
{
  prctl(PR_SET_NAME, __func__);
  MyHashMap *map = (MyHashMap *)args;
  while (!g_exiting) {
    MyKey key = MyKey::get_random_key();
    MyValue value(key.get());
    if (OB_SUCCESS == map->insert(key, value)) {
      g_insert_cnt++;
    }
  }
  return (void *)0;
}

void *do_update(void *args)
{
  prctl(PR_SET_NAME, __func__);
  MyHashMap *map = (MyHashMap *)args;
  while (!g_exiting) {
    MyKey key = MyKey::get_random_key();
    MyValue value(key.get() + 1);
    if (OB_SUCCESS == map->insert_or_update(key, value)) {
      g_update_cnt++;
    }
    usleep(100);
  }
  return (void *)0;
}

void *do_clear(void *args)
{
  prctl(PR_SET_NAME, __func__);
  MyHashMap *map = (MyHashMap *)args;
  while (!g_exiting) {
    (void)map->clear();
    usleep(100000);
  }
  return (void *)0;
}

TEST_F(TestObLinearHashMap, test_currency)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  EXPECT_TRUE(OB_SUCCESS == my_hashmap_.init());

  for (int64_t i = 0; i < ALL_THREAD_NUM; i++) {
    tids[i] = 0;
  }
  tid_idx = 0;

  EXPECT_TRUE(0 == create_task(GET_THREAD_NUM, do_get, &my_hashmap_));
  EXPECT_TRUE(0 == create_task(ERASE_THREAD_NUM, do_erase, &my_hashmap_));
  EXPECT_TRUE(0 == create_task(INSERT_THREAD_NUM, do_insert, &my_hashmap_));
  EXPECT_TRUE(0 == create_task(UPDATE_THREAD_NUM, do_update, &my_hashmap_));
  EXPECT_TRUE(0 == create_task(CLEAR_THREAD_NUM, do_clear, &my_hashmap_));

  const int64_t RUN_TIME_SEC = 60;
  for (int64_t i = 0; i < RUN_TIME_SEC; i++) {
    fprintf(stdout, "g_get_cnt = %ld, g_erase_cnt = %ld, g_insert_cnt = %ld, g_update_cnt = %ld, hash_cnt=%ld\n",
        g_get_cnt, g_erase_cnt, g_insert_cnt, g_update_cnt, my_hashmap_.count());
    g_get_cnt = 0;
    g_erase_cnt = 0;
    g_insert_cnt = 0;
    g_update_cnt = 0;
    sleep(1);
  }
  g_exiting = 1;
  for (int64_t i = 0; i < ALL_THREAD_NUM && tids[i] > 0; i++) {
    pthread_join(tids[i], NULL);
  }
  my_hashmap_.clear();
  EXPECT_TRUE(0 == my_hashmap_.count());
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_linear_hash_map.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  if (OB_SUCCESS != (ret = ObClockGenerator::init())) {
    TRANS_LOG(WARN, "init ObClockGenerator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
