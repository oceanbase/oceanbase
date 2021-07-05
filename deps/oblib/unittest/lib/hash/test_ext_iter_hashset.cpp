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

#include "lib/hash/ob_ext_iter_hashset.h"
#include "lib/allocator/ob_malloc.h"
#include <gtest/gtest.h>

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
class ObExtIterHashSetTest : public ::testing::Test {
public:
  ObExtIterHashSetTest();
  virtual ~ObExtIterHashSetTest();
  virtual void SetUp();
  virtual void TearDown();

public:
  ObArenaAllocator allocator_;

private:
  // disallow copy
  ObExtIterHashSetTest(const ObExtIterHashSetTest& other);
  ObExtIterHashSetTest& operator=(const ObExtIterHashSetTest& other);

protected:
  // data members
};

ObExtIterHashSetTest::ObExtIterHashSetTest() : allocator_(ObModIds::TEST)
{}

ObExtIterHashSetTest::~ObExtIterHashSetTest()
{}

void ObExtIterHashSetTest::SetUp()
{}

void ObExtIterHashSetTest::TearDown()
{
  allocator_.reset();
}

TEST_F(ObExtIterHashSetTest, basic_test)
{
  static const int64_t N = 256;
  static const int64_t BUCKET_NUM = 8;
  ObExtIterHashSet<int64_t, N> set(allocator_);
  for (int round = 0; round < 5; ++round) {
    ASSERT_EQ(0, set.count());
    ASSERT_EQ(set.begin(), set.end());
    ASSERT_EQ(++(set.begin()), set.end());

    // insert data
    for (int64_t index = 0; index < N * BUCKET_NUM; index++) {
      int64_t value = index + 1;
      ASSERT_EQ(OB_HASH_NOT_EXIST, set.exist_refactored(value));
      ASSERT_EQ(OB_SUCCESS, set.set_refactored(value));
      ASSERT_EQ(OB_HASH_EXIST, set.exist_refactored(value));
      ASSERT_EQ(index + 1, set.count());
    }

    // verify data
    ObExtIterHashSet<int64_t, N>::const_iterator_t it = set.begin();
    for (int64_t value = 1; it != set.end(); ++it, value++) {
      ASSERT_EQ(value, *it);
    }
    set.clear();
    ASSERT_EQ(0, set.count());
  }
}

TEST_F(ObExtIterHashSetTest, single_N)
{
  ObExtIterHashSet<int64_t, 1> hashset(allocator_);
  ObExtIterHashSet<int64_t, 1>::const_iterator_t iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  ASSERT_EQ(0, hashset.count());
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(1));
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(1));
  ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(1));
  ASSERT_EQ(1, hashset.count());
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(2));
  ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(2));
  ASSERT_EQ(2, hashset.count());

  iter = hashset.begin();
  ASSERT_NE(iter, hashset.end());
  ASSERT_EQ(1, *iter);
  ASSERT_NE(++iter, hashset.end());
  ASSERT_EQ(2, *iter);
  ASSERT_EQ(++iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());
}

TEST_F(ObExtIterHashSetTest, many_N_single_bucket)
{
  const uint64_t N = 10345;
  int64_t value = 0;
  ObExtIterHashSet<int64_t, N> hashset(allocator_);
  ObExtIterHashSet<int64_t, N>::const_iterator_t iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(N));

  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(N));

  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(N + 1, hashset.count());
  for (value = 0, iter = hashset.begin(); iter != hashset.end(); ++iter, value++) {
    ASSERT_EQ(value, *iter);
  }

  hashset.clear();

  iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(N));
  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(N));
  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(N + 1, hashset.count());
  for (value = 0, iter = hashset.begin(); iter != hashset.end(); ++iter, value++) {
    ASSERT_EQ(value, *iter);
  }
}

TEST_F(ObExtIterHashSetTest, many_N_single_buckets2)
{
  const uint64_t N = 10345;
  int64_t value = 0;
  ObExtIterHashSet<int64_t, N> hashset(allocator_);
  ObExtIterHashSet<int64_t, N>::const_iterator_t iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(N + 1, hashset.count());
  for (value = N, iter = hashset.begin(); iter != hashset.end(); ++iter, value--) {
    ASSERT_EQ(value, *iter);
  }

  hashset.clear();

  iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(N + 1, hashset.count());
  for (value = N, iter = hashset.begin(); iter != hashset.end(); ++iter, value--) {
    ASSERT_EQ(value, *iter);
  }
}

TEST_F(ObExtIterHashSetTest, many_N_many_buckets)
{
  static const uint64_t N = 128;
  static const uint64_t BUCKET_NUM = 100;
  static const uint64_t ELEMENT_NUM = N * BUCKET_NUM;
  int64_t value = 0;
  ObExtIterHashSet<int64_t, N> hashset(allocator_);
  ObExtIterHashSet<int64_t, N>::const_iterator_t iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(ELEMENT_NUM, hashset.count());
  for (value = 0, iter = hashset.begin(); iter != hashset.end(); ++iter, value++) {
    ASSERT_EQ(value, *iter);
  }

  hashset.clear();

  iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  for (uint64_t i = 0; i < ELEMENT_NUM; i++) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(ELEMENT_NUM, hashset.count());
  for (value = 0, iter = hashset.begin(); iter != hashset.end(); ++iter, value++) {
    ASSERT_EQ(value, *iter);
  }
}

TEST_F(ObExtIterHashSetTest, many_N_many_buckets2)
{
  static const uint64_t N = 1024;
  static const uint64_t BUCKET_NUM = 10;
  static const uint64_t ELEMENT_NUM = N * BUCKET_NUM;
  int64_t value = 0;
  ObExtIterHashSet<int64_t, N> hashset(allocator_);
  ObExtIterHashSet<int64_t, N>::const_iterator_t iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(ELEMENT_NUM, hashset.count());
  for (value = ELEMENT_NUM, iter = hashset.begin(); iter != hashset.end(); ++iter, value--) {
    ASSERT_EQ(value, *iter);
  }

  hashset.clear();

  iter = hashset.begin();
  ASSERT_EQ(iter, hashset.end());
  ASSERT_EQ(++iter, hashset.end());

  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  for (uint64_t i = ELEMENT_NUM; i > 0; i--) {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  ASSERT_EQ(ELEMENT_NUM, hashset.count());
  for (value = ELEMENT_NUM, iter = hashset.begin(); iter != hashset.end(); ++iter, value--) {
    ASSERT_EQ(value, *iter);
  }
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
