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
#include "lib/container/ob_concurrent_bitset.h"
#include "lib/random/ob_random.h"
#include "lib/atomic/ob_atomic.h"
using namespace oceanbase::common;
class TestConcurrentBitset : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }
  template<typename bitset_type>
  void test_set(bitset_type &bitset);
  template<typename bitset_type>
  void test_reset(bitset_type &bitset);
  template<typename bitset_type>
  void test_concurrent(bitset_type &bitset);
  template<typename bitset_type>
  void test_concurrent_set_if_not_exist(bitset_type &bitset);
  template<typename bitset_type>
  void test_set_if_not_exist(bitset_type &bitset);
  template<typename bitset_type>
  void test_set_find_first_zero_bit_no_reset(bitset_type &bitset);
  template<typename bitset_type>
  void test_set_find_first_zero_bit_with_reset(bitset_type &bitset);
  template<typename bitset_type>
  void concurrent_set_find_first_zero_bit_without_reset(bitset_type &bitset);
  template<typename bitset_type>
  void concurrent_set_find_first_zero_bit_with_reset(bitset_type &bitset);
  template<typename bitset_type>
  void concurrent_set_find_first_zero_bit(bitset_type &bitset);
protected:
  ObConcurrentBitset<UINT16_MAX> bitset_;
  ObConcurrentBitset<UINT16_MAX, true> bitset_lock_;
};

template<typename bitset_type>
void TestConcurrentBitset::test_set(bitset_type &bitset)
{
  const uint64_t test_count = 10000;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset.set(i));
  }
  bool is_exist = false;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset.test(i, is_exist));
    EXPECT_EQ(true, is_exist);
  }

}

TEST_F(TestConcurrentBitset, test_set)
{
  test_set(bitset_);
  test_set(bitset_lock_);
}

template<typename bitset_type>
void TestConcurrentBitset::test_reset(bitset_type &bitset)
{
  const uint64_t test_count = 10000;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset.set(i));
  }
  bool is_exist = false;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset.test(i, is_exist));
    EXPECT_EQ(true, is_exist);
    EXPECT_EQ(OB_SUCCESS, bitset.reset(i));
  }
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset.test(i, is_exist));
    EXPECT_EQ(true, false == is_exist);

  }
}

TEST_F(TestConcurrentBitset, test_reset)
{
  test_reset(bitset_);
  test_reset(bitset_lock_);
}

template<typename bitset_type>
void TestConcurrentBitset::test_set_if_not_exist(bitset_type &bitset)
{
  const uint64_t test_count = 10000;
  for (uint64_t i = 0; i < test_count; ++i) {
    if (i % 2) {
      EXPECT_EQ(OB_SUCCESS, bitset.set(i));
    }
  }
  for (uint64_t i = 0; i < test_count; ++i) {
    if (i % 2) {
      EXPECT_EQ(OB_ENTRY_EXIST, bitset.set_if_not_exist(i));
    } else {
      EXPECT_EQ(OB_SUCCESS, bitset.set_if_not_exist(i));
    }
  }
  bool is_exist = false;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset.test(i, is_exist));
    EXPECT_EQ(true, is_exist);

  }
}

TEST_F(TestConcurrentBitset, test_set_if_not_exist)
{
  test_set_if_not_exist(bitset_);
  test_set_if_not_exist(bitset_lock_);
}

static uint16_t base_sessid = 0;
void *thread_func_test_bitset(void *args)
{
  const uint test_count = 1000;
  uint16_t id_array[test_count];
  ObConcurrentBitset<UINT16_MAX> *bitset = static_cast<ObConcurrentBitset<UINT16_MAX> *>(args);
  for (uint64_t i = 0; i < test_count; ++i) {
    id_array[i] = static_cast<uint16_t>(ATOMIC_AAF(&base_sessid, 1));
    EXPECT_EQ(OB_SUCCESS, bitset->set(id_array[i]));
  }
  bool is_exist = false;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset->test(id_array[i], is_exist));
    EXPECT_EQ(true, is_exist);

    EXPECT_EQ(OB_SUCCESS, bitset->reset(id_array[i]));
  }
  for (uint64_t i = 0; i < test_count; ++i) {
    OB_ASSERT(false == bitset->reset(id_array[i]));
  }
  return NULL;
}

template<typename bitset_type>
void TestConcurrentBitset::test_concurrent(bitset_type &bitset)
{
  const uint64_t THREAD_NUM = 64;
  pthread_t thread_create_arr[THREAD_NUM];
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_create(&thread_create_arr[i], NULL, thread_func_test_bitset, &bitset));
  }
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_join(thread_create_arr[i], NULL));
  }
}

TEST_F(TestConcurrentBitset, concurrent)
{
  base_sessid = 0;
  test_concurrent(bitset_);
  base_sessid = 0;
  test_concurrent(bitset_lock_);
}

void *thread_func_test_set_if_not_exist(void *args)
{
  const uint test_count = 1000;
  uint16_t id_array[test_count];
  ObConcurrentBitset<UINT16_MAX> *bitset = static_cast<ObConcurrentBitset<UINT16_MAX> *>(args);
  for (uint64_t i = 0; i < test_count; ++i) {
    int ret = OB_SUCCESS;
    id_array[i] = static_cast<uint16_t>(ATOMIC_AAF(&base_sessid, 1));
    ret = bitset->set_if_not_exist(id_array[i]);
    (void) ret;
  }
  bool is_exist = false;
  for (uint64_t i = 0; i < test_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, bitset->test(id_array[i], is_exist));
    EXPECT_EQ(true, is_exist);

    EXPECT_EQ(OB_SUCCESS, bitset->reset(id_array[i]));
  }
  for (uint64_t i = 0; i < test_count; ++i) {
    OB_ASSERT(false == bitset->reset(id_array[i]));
  }
  return NULL;
}

template<typename bitset_type>
void TestConcurrentBitset::test_concurrent_set_if_not_exist(bitset_type &bitset)
{
  const uint64_t THREAD_NUM = 64;
  pthread_t thread_create_arr[THREAD_NUM];
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_create(&thread_create_arr[i], NULL, thread_func_test_set_if_not_exist, &bitset));
  }
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_join(thread_create_arr[i], NULL));
  }
}

TEST_F(TestConcurrentBitset, concurrent_set_if_not_exist)
{
  base_sessid = 0;
  test_concurrent_set_if_not_exist(bitset_);
  base_sessid = 0;
  test_concurrent_set_if_not_exist(bitset_lock_);
}


/**********************find zero pos test case************************************/

template<typename bitset_type>
void TestConcurrentBitset::test_set_find_first_zero_bit_no_reset(bitset_type &bitset)
{
  const uint64_t test_count = UINT16_MAX;
  uint64_t value = 0;

  // start_pos starts from 0, increasing value is right.
  bitset.set_start_pos(0);
  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ(i, value);
  }
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, bitset.find_and_set_first_zero(value));

  //clear bitset
  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.reset(i));
  }

  //start_pos starts from UINT16_MAX, increasing value is right.
  bitset.set_start_pos(UINT16_MAX);
  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ((i + UINT16_MAX) % (UINT16_MAX + 1), value);
  }
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, bitset.find_and_set_first_zero(value));
}

TEST_F(TestConcurrentBitset, test_set_find_first_zero_bit_no_reset)
{
  test_set_find_first_zero_bit_no_reset(bitset_);
  test_set_find_first_zero_bit_no_reset(bitset_lock_);
}

template<typename bitset_type>
void TestConcurrentBitset::test_set_find_first_zero_bit_with_reset(bitset_type &bitset)
{
  const uint64_t test_count = UINT16_MAX;
  uint64_t value = 0;

  // start_pos starts from 0, increasing value is right.
  bitset.set_start_pos(0);
  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ(OB_SUCCESS, bitset.reset(value));
    EXPECT_EQ(i, value);
  }

  // start_pos starts from UINT16_MAX, increasing value is right.
  bitset.set_start_pos(UINT16_MAX);
  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ(OB_SUCCESS, bitset.reset(value));
    EXPECT_EQ((i + UINT16_MAX) % (UINT16_MAX + 1), value);
  }
}

// in single thread mode, value should increase when set and reset is continuous.
TEST_F(TestConcurrentBitset, test_set_find_first_zero_bit_with_reset)
{
  test_set_find_first_zero_bit_with_reset(bitset_);
  test_set_find_first_zero_bit_with_reset(bitset_lock_);
}

void *thread_func_find_first_bit_set_without_reset(void *args)
{
  ObConcurrentBitset<UINT16_MAX> *bitset = static_cast<ObConcurrentBitset<UINT16_MAX> *>(args);
  const uint64_t test_count = 1000;
  uint64_t value = 0;
  for (uint64_t i = 0; i < test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset->find_and_set_first_zero(value));
  }

  return NULL;
}

template<typename bitset_type>
void TestConcurrentBitset::concurrent_set_find_first_zero_bit_without_reset(bitset_type &bitset)
{
  // test of parallel execution:
  // 64 threads, every threads get 1000 values.
  // after 64 threads end, value should start from 64000.
  // verify the correctness of value from 64000, until 65000 and return OB_ENTRY_NOT_EXIST
  const uint64_t THREAD_NUM = 64;
  pthread_t thread_create_arr[THREAD_NUM];
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&thread_create_arr[i], NULL, thread_func_find_first_bit_set_without_reset, &bitset));
  }
  const uint64_t test_count = UINT16_MAX - THREAD_NUM * 1000;
  uint64_t value = 0;
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_join(thread_create_arr[i], NULL));
  }

  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ(i + THREAD_NUM * 1000, value);
  }
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, bitset.find_and_set_first_zero(value));
}

TEST_F(TestConcurrentBitset, concurrent_set_find_first_zero_bit_without_reset)
{
  concurrent_set_find_first_zero_bit_without_reset(bitset_);
  concurrent_set_find_first_zero_bit_without_reset(bitset_lock_);
}

void *thread_func_find_first_bit_set_with_reset(void *args)
{
  ObConcurrentBitset<UINT16_MAX> *bitset = static_cast<ObConcurrentBitset<UINT16_MAX> *>(args);
  const uint64_t test_count = 1000;
  uint64_t value = 0;
  for (uint64_t i = 0; i < test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset->find_and_set_first_zero(value));
    EXPECT_EQ(OB_SUCCESS, bitset->reset(value));
  }

  return NULL;
}

template<typename bitset_type>
void TestConcurrentBitset::concurrent_set_find_first_zero_bit_with_reset(bitset_type &bitset)
{
  // test of parallel execution:
  // 64 threads, every threads get 1000 values.
  // after 64 threads end, value should start from 64000.
  const uint64_t THREAD_NUM = 64;
  pthread_t thread_create_arr[THREAD_NUM];

  //start_pos starts from UINT16_MAX, increasing value is right.
  bitset.set_start_pos(UINT16_MAX);
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&thread_create_arr[i], NULL, thread_func_find_first_bit_set_with_reset, &bitset));
  }
  const uint64_t test_count = UINT16_MAX - THREAD_NUM * 1000;
  uint64_t value = 0;
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_join(thread_create_arr[i], NULL));
  }

  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ(i + THREAD_NUM * 1000 - 1, value);
  }
}

TEST_F(TestConcurrentBitset, concurrent_set_find_first_zero_bit_with_reset)
{
  concurrent_set_find_first_zero_bit_with_reset(bitset_);
  concurrent_set_find_first_zero_bit_with_reset(bitset_lock_);
}

template<typename bitset_type>
void TestConcurrentBitset::concurrent_set_find_first_zero_bit(bitset_type &bitset)
{
  // test of parallel execution:
  // 64 threads, every threads get 1000 values.
  // after 64 threads end, value should start from 64000.
  const uint64_t THREAD_NUM = 64;
  pthread_t thread_create_arr_1[THREAD_NUM];
  pthread_t thread_create_arr_2[THREAD_NUM];

  //start_pos starts from UINT16_MAX, increasing value is right.8
  bitset.set_start_pos(UINT16_MAX);
  for (uint64_t i = 0; i < THREAD_NUM / 2; ++i) {
    EXPECT_EQ(0, pthread_create(&thread_create_arr_1[i], NULL, thread_func_find_first_bit_set_with_reset, &bitset));
    EXPECT_EQ(0, pthread_create(&thread_create_arr_2[i], NULL, thread_func_find_first_bit_set_without_reset, &bitset));
  }
  const uint64_t test_count = UINT16_MAX - THREAD_NUM * 1000;
  uint64_t value = 0;
  for (uint64_t i = 0; i < THREAD_NUM / 2; ++i) {
    OB_ASSERT(0 == pthread_join(thread_create_arr_1[i], NULL));
    OB_ASSERT(0 == pthread_join(thread_create_arr_2[i], NULL));
  }

  for (uint64_t i = 0; i <= test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset.find_and_set_first_zero(value));
    EXPECT_EQ(i + THREAD_NUM * 1000 - 1, value);
  }
}

TEST_F(TestConcurrentBitset, concurrent_set_find_first_zero_bit)
{
  concurrent_set_find_first_zero_bit(bitset_);
  concurrent_set_find_first_zero_bit(bitset_lock_);
}

// find zero bit test
// after finding bit A, the next finding start from the position of A+1.
// 1. set first 64bit.
// 2. set start_pos_ = 0, and find_and_set_first_zero.
// 3. judge the correctness of zero bit found.
TEST_F(TestConcurrentBitset, test_increment)
{
  uint64_t value = 0;
  for (uint64_t i = 0; i < 64; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset_lock_.set(i));
  }

  for (uint64_t i = 0; i < 64; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset_lock_.find_and_set_first_zero(value));
    EXPECT_EQ(i + 64, value);
    EXPECT_EQ(OB_SUCCESS, bitset_lock_.reset(value));
  }

}

TEST_F(TestConcurrentBitset, test_only_one_zero)
{
  uint64_t value = 0;
  const uint64_t test_count = UINT16_MAX;
  bitset_lock_.set_start_pos(256);
  for (uint64_t i = 0; i < test_count; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset_lock_.find_and_set_first_zero(value));
  }
  for (uint64_t i = 0; i < 100; i++) {
    EXPECT_EQ(OB_SUCCESS, bitset_lock_.find_and_set_first_zero(value));
    EXPECT_EQ(OB_SUCCESS, bitset_lock_.reset(value));
    EXPECT_EQ(255, value);
  }

}

// void *thread_func_only(void *args)
// {
//   ObConcurrentBitset<UINT16_MAX> *bitset = static_cast<ObConcurrentBitset<UINT16_MAX> *>(args);
//   uint64_t value = 0;
//   EXPECT_EQ(OB_SUCCESS, bitset->find_and_set_first_zero(value));
//   return NULL;
// }

// TEST_F(TestConcurrentBitset, DISABLED_test_only_one_zero2)
// {
//   uint64_t value = 0;
//   const uint64_t test_count = UINT16_MAX;
//   pthread_t thread_create_arr[test_count];
//   bitset_lock_.set_start_pos(256);
//   for (uint64_t i = 0; i < test_count; i++) {
//     EXPECT_EQ(0, pthread_create(&thread_create_arr[i], NULL, thread_func_only, &bitset_lock_));
//   }

//   for (uint64_t i = 0; i < 100; i++) {
//     EXPECT_EQ(OB_SUCCESS, bitset_lock_.find_and_set_first_zero(value));
//     EXPECT_EQ(OB_SUCCESS, bitset_lock_.reset(value));
//     EXPECT_EQ(255, value);
//   }
//   for (uint64_t i = 0; i < test_count; i++) {
//     EXPECT_EQ(0, pthread_join(thread_create_arr[i], NULL));
//   }

// }

int main(int argc, char *argv[])
{
  srand((unsigned)time(NULL));
  OB_LOGGER.set_log_level("ERROR");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
