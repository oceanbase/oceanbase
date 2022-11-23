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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <algorithm>
#include "lib/allocator/ob_buddy_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
using namespace oceanbase::common;

class BuddyAllocatorTest : public ::testing::Test
{
  public:
    BuddyAllocatorTest();
    virtual ~BuddyAllocatorTest();
    virtual void SetUp();
    virtual void TearDown();

    void assign_ptr(char *ptr, int64_t ptr_len, int64_t seed);
    bool is_valid_ptr(const char *ptr, int64_t ptr_len, int64_t seed);
  private:
    // disallow copy
    BuddyAllocatorTest(const BuddyAllocatorTest &other);
    BuddyAllocatorTest& operator=(const BuddyAllocatorTest &other);
  private:
    // data members
};
BuddyAllocatorTest::BuddyAllocatorTest()
{
}

BuddyAllocatorTest::~BuddyAllocatorTest()
{
}

void BuddyAllocatorTest::SetUp()
{
}

void BuddyAllocatorTest::TearDown()
{
}

void BuddyAllocatorTest::assign_ptr(char *ptr, int64_t ptr_len, int64_t seed)
{
  OB_ASSERT(ptr_len > 0);
  for (int64_t i = 0, j = seed % 255; i < ptr_len - 1; ++i, ++j) {
    ptr[i] = static_cast<char>((j % 255) + 1);
  }
  ptr[ptr_len - 1] = 0;
}

bool BuddyAllocatorTest::is_valid_ptr(const char *ptr, int64_t ptr_len, int64_t seed)
{
  OB_ASSERT(ptr_len > 0);
  for (int64_t i = 0, j = seed % 255; i < ptr_len - 1; ++i, ++j) {
    if (ptr[i] != static_cast<char>((j % 255) + 1)) {
      COMMON_LOG(ERROR, "not equal", K(seed % 255), K(i), K((int)ptr[i]), K((j % 255) + 1), K(ptr_len));
      _COMMON_LOG(ERROR, "ptr=%.*s", (int)ptr_len, ptr);
      return false;
    }
  }
  if (ptr[ptr_len - 1] != 0) {
    COMMON_LOG(ERROR, "last is not 0", K(seed % 255), K((int)ptr[ptr_len - 1]), K(ptr_len));
    _COMMON_LOG(ERROR, "ptr=%.*s", (int)ptr_len, ptr);
    return false;
  }
  return true;
}

TEST_F(BuddyAllocatorTest, basic_test)
{
  ObMalloc a;
  void *ptr1 = NULL;
  void *ptr2 = NULL;
  void *ptr3 = NULL;
  char *tmp = NULL;
  int32_t tmp_order = 0;
  int64_t tmp_size = 0;
  const int64_t block_size = 128;
  //int32_t block_order = 7;
  void * pointer_array[1000];
  ObBuddyAllocator alloc(a);
  alloc.init(block_size);

  //test 1 calloc 1120B
  ASSERT_TRUE(NULL != (ptr1 = alloc.alloc(1120)));
  _OB_LOG(INFO, "alloc 1120,ptr1 point to it");
  tmp = static_cast<char *>(ptr1);
  tmp--;
  tmp_order = (int)(*tmp);
  _OB_LOG(INFO, "order you write to memory is %d",tmp_order);
  tmp_size = (1 << tmp_order) * block_size;
  _OB_LOG(INFO, "you can use %ldB with pointer ptr1", tmp_size - 1);
  if (ptr1 != NULL)
  {
    alloc.free(ptr1);
  }
  //test 2
  _OB_LOG(INFO, "info:test 2 alloc 10^5B");
  ptr2 = alloc.alloc(100000);
  _OB_LOG(INFO, "alloc 100000 with address is %p",ptr2);
  if (ptr2 != NULL)
  {
    alloc.free(ptr2);
  }
  //test 3
  _OB_LOG(INFO, "info:test 3 alloc 1024B");
  ptr3 = alloc.alloc(1024);
  _OB_LOG(INFO, "alloc 1024 with address is %p, with %d basic page(128B)",ptr3,16);
  if (ptr3 != NULL)
  {
    alloc.free(ptr3);
  }
  //test 4:alloc(negative numbaer)
  for (int i = 0; i < 20; i++)
  {
    ASSERT_TRUE(NULL == (pointer_array[i] = alloc.alloc(-(1<<i))));
  }
  for (int i = 0; i < 20; i++)
  {
    if(pointer_array[i] != NULL)
    {
      alloc.free(pointer_array[i]);
    }
  }
  //test5:alloc(2^i)
  ASSERT_TRUE(NULL == (pointer_array[0] = alloc.alloc(0)));
  for (int i = 1; i < 18; i++) {
    ASSERT_TRUE(NULL != (pointer_array[i] = alloc.alloc((1L << i))));
    if (pointer_array[i] != NULL) {
      _OB_LOG(INFO, "alloc(%ld) succeed", 1L << i);
    } else {
      _OB_LOG(INFO, "alloc(%ld) fail", 1L << i);
    }
  }
  ASSERT_TRUE(NULL == (pointer_array[19] = alloc.alloc((1L << 19))));
  ASSERT_TRUE(NULL == (pointer_array[20] = alloc.alloc((1L << 20))));
  for (int i = 0; i < 20; i++)
  {
    if(pointer_array[i] != NULL)
    {
      alloc.free(pointer_array[i]);
    }
  }
  //test6: alloc(2^i - 1)
  //ASSERT_TRUE(NULL == (pointer_array[0] = alloc.alloc(1L << 0)+1));
  for (int i = 0; i < 18; i++)
  {
    ASSERT_TRUE(NULL != (pointer_array[i] = alloc.alloc((1L << i)+1)));
    if (pointer_array[i] != NULL)
    {
      _OB_LOG(INFO, "alloc(%ld) succeed", 1L << i);
    }
  }
  ASSERT_TRUE(NULL == (pointer_array[19] = alloc.alloc((1L << 19)+1)));
  ASSERT_TRUE(NULL == (pointer_array[20] = alloc.alloc((1L << 20)+1)));
  for (int i = 0; i <= 20; i++)
  {
    if(pointer_array[i] != NULL)
    {
      alloc.free(pointer_array[i]);
    }
  }
  //test7:alloc(2^[20,30])
  for (int i = 20; i < 30; i++)
  {
    ASSERT_TRUE(NULL == (pointer_array[i] = alloc.alloc((1L << i))));
    if (pointer_array[i] != NULL)
    {
      _OB_LOG(INFO, "alloc(%ld) succeed", 1L << i);
    }
  }
  for (int i = 20; i < 30; i++)
  {
    if(pointer_array[i] != NULL)
    {
      alloc.free(pointer_array[i]);
    }
  }
  //test6: alloc(2^i - 1)


  _OB_LOG(INFO, "free ptr");
}

class ObPtrStore
{
public:
  int64_t seed_;
  int64_t ptr_len_;
  char *ptr_;
};

TEST_F(BuddyAllocatorTest, random_test)
{
  srand(static_cast<unsigned int>(time(NULL)));
  ObArenaAllocator base_allocator(ObModIds::TEST);
  ObBuddyAllocator alloc(base_allocator);
  ASSERT_TRUE(OB_SUCCESS == alloc.init(2));
  const static int64_t MAX_PTR_ARRAY_LEN = 1024;
  ObPtrStore ptr_store_arr[MAX_PTR_ARRAY_LEN];
  for (int64_t i = 0; i < MAX_PTR_ARRAY_LEN; ++i) {
    int64_t random_alloc_size = rand() % 511 + 2;
    char *tmp_ptr = static_cast<char *>(alloc.alloc(random_alloc_size));
    ASSERT_TRUE(NULL != tmp_ptr);
    int64_t random_seed = rand();
    assign_ptr(tmp_ptr, random_alloc_size, random_seed);
    ObPtrStore tmp_ptr_store;
    tmp_ptr_store.seed_ = random_seed;
    tmp_ptr_store.ptr_len_ = random_alloc_size;
    tmp_ptr_store.ptr_ = tmp_ptr;
    ptr_store_arr[i] = tmp_ptr_store;
  }

  for (int64_t i = 0; i < MAX_PTR_ARRAY_LEN; ++i) {
    bool ptr_is_valid = is_valid_ptr(ptr_store_arr[i].ptr_, ptr_store_arr[i].ptr_len_, ptr_store_arr[i].seed_);
    if (!ptr_is_valid) {
      COMMON_LOG(ERROR, "ptr is not valid", K(ptr_is_valid), K(i));
    }
    ASSERT_TRUE(true == ptr_is_valid);
  }

  for (int64_t time = 0; time < 10000; ++time) {
    // upset
    for (int64_t i = 0; i < MAX_PTR_ARRAY_LEN; ++i) {
      int64_t select_pos = rand() % (MAX_PTR_ARRAY_LEN - i) + i;
      if (select_pos != i) {
        ObPtrStore tmp_store = ptr_store_arr[i];
        ptr_store_arr[i] = ptr_store_arr[select_pos];
        ptr_store_arr[select_pos] = tmp_store;
      }
    }

    // Dequeue at the head of the queue, free
    ASSERT_TRUE(NULL != ptr_store_arr[0].ptr_);
    alloc.free(ptr_store_arr[0].ptr_);

    // Move the entire array forward by one subscript
    for (int64_t i = 0; i < MAX_PTR_ARRAY_LEN - 1; ++i) {
      ptr_store_arr[i] = ptr_store_arr[i + 1];
    }

    // Re-alloc a pointer to the last position of the array
    int64_t random_alloc_size = rand() % 511 + 2;
    char *tmp_ptr = static_cast<char *>(alloc.alloc(random_alloc_size));
    ASSERT_TRUE(NULL != tmp_ptr);
    int64_t random_seed = rand();
    assign_ptr(tmp_ptr, random_alloc_size, random_seed);
    ObPtrStore tmp_ptr_store;
    tmp_ptr_store.seed_ = random_seed;
    tmp_ptr_store.ptr_len_ = random_alloc_size;
    tmp_ptr_store.ptr_ = tmp_ptr;
    ptr_store_arr[MAX_PTR_ARRAY_LEN - 1] = tmp_ptr_store;

    // Check again
    for (int64_t i = 0; i < MAX_PTR_ARRAY_LEN; ++i) {
      ASSERT_TRUE(true == is_valid_ptr(ptr_store_arr[i].ptr_, ptr_store_arr[i].ptr_len_, ptr_store_arr[i].seed_));
    }
  }

  _OB_LOG(INFO, "free ptr");
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

