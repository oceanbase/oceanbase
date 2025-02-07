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
#include <malloc.h>
#include "lib/oblog/ob_log.h"
#include "lib/alloc/malloc_hook.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

int main(int argc, char *argv[])
{
  init_malloc_hook();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestMallocHook
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

struct Header
{
  static const uint32_t MAGIC_CODE = 0XA1B2C3D1;
  static const uint32_t SIZE;
  Header(int32_t size, bool from_mmap)
    : magic_code_(MAGIC_CODE),
      data_size_(size),
      offset_(0),
      from_mmap_(from_mmap)
  {}
  bool check_magic_code() const { return MAGIC_CODE == magic_code_; }
  void mark_unused() { magic_code_ &= ~0x1; }
  static Header *ptr2header(void *ptr) { return reinterpret_cast<Header*>((char*)ptr - SIZE); }
  uint32_t magic_code_;
  int32_t data_size_;
  uint32_t offset_;
  uint8_t from_mmap_;
  char padding_[3];
  char data_[0];
} __attribute__((aligned (16)));

TEST_F(TestMallocHook, Basic)
{
  const static size_t size = 100;
  void *ptr = nullptr;
  int ret = 0;

  // calloc
  char zero_buf[100];
  memset(zero_buf, 0 , 100);
  ptr = calloc(10, 10);
  ASSERT_NE(ptr, nullptr);
  memset(ptr, 0, size);
  Header *header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, 10 * 10);
  // check 0
  ASSERT_EQ(0, memcmp(ptr, zero_buf, 100));
  free(ptr);

  // malloc
  ptr = malloc(size);
  ASSERT_NE(ptr, nullptr);
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size);
  char buf[size];
  memset(buf, 'a', size);
  memcpy(ptr, buf, size);

  // realloc
  void *new_ptr = realloc(ptr, size * 2);
  ASSERT_NE(new_ptr, nullptr);
  header = Header::ptr2header(new_ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size * 2);
  ASSERT_EQ(0, memcmp(new_ptr, buf, size));
  free(new_ptr);

  // realloc(nullptr, size > 0);
  ptr = realloc(nullptr, size);
  ASSERT_NE(ptr, nullptr);
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size);

  // realloc(size zoom in && out);
  ptr = malloc(size);
  ASSERT_NE(ptr, nullptr);
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size);
  // zoom in
  ptr = realloc(ptr, size * 2);
  ASSERT_NE(ptr, nullptr);
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size * 2);
  // zoom out
  ptr = realloc(ptr, size);
  ASSERT_NE(ptr, nullptr);
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size);

  // memalign
  size_t alignment = 4L << 10;
  ptr = memalign(alignment, size);
  ASSERT_NE(ptr, nullptr);
  ASSERT_EQ(0, (size_t)ptr & (alignment - 1));
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size);
  free(ptr);

  // valloc && realloc
  ptr = valloc(size);
  ASSERT_NE(ptr, nullptr);
  ASSERT_EQ(0, (size_t)ptr & (sysconf(_SC_PAGESIZE) - 1));
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size);
  new_ptr = realloc(ptr, size * 2);
  header = Header::ptr2header(new_ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size * 2);
  free(new_ptr);

  // posix_memalign
  ret = posix_memalign(&ptr, alignment, size);
  ASSERT_EQ(0, ret);
  ASSERT_NE(ptr, nullptr);
  ASSERT_EQ(0, (size_t)ptr & (alignment - 1));
  free(ptr);

  // free_aligned_sized
  ptr = aligned_alloc(alignment, size);
  ASSERT_NE(ptr, nullptr);
  ASSERT_EQ(0, (size_t)ptr & (alignment - 1));
  free(ptr);

  // pvalloc
  ptr = pvalloc(size);
  ASSERT_NE(ptr, nullptr);
  ASSERT_EQ(0, (size_t)ptr & (sysconf(_SC_PAGESIZE) - 1));
  header = Header::ptr2header(ptr);
  ASSERT_EQ(header->data_size_, sysconf(_SC_PAGESIZE));
  free(ptr);

  // realloc && cross
  ptr = malloc(size);
  ASSERT_NE(ptr, nullptr);
  header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  memset(buf, 'a', size);
  memcpy(ptr, buf, size);
  new_ptr = realloc(ptr, size * 2);
  ASSERT_NE(new_ptr, nullptr);
  header = Header::ptr2header(new_ptr);
  ASSERT_TRUE(header->check_magic_code());
  ASSERT_EQ(header->data_size_, size * 2);
  ASSERT_EQ(0, memcmp(new_ptr, buf, size));
  free(new_ptr);
}

TEST_F(TestMallocHook, test_strdup)
{
  // strdup
  const char *original = "test";
  char *str = strdup(original);
  ASSERT_NE(str, nullptr);
  Header *header = Header::ptr2header(str);
  ASSERT_EQ(0, strcmp(str, original));
  ASSERT_TRUE(header->check_magic_code());
  free(str);

  // strndup
  size_t n = 3;
  str = strndup(original, n);
  ASSERT_NE(str, nullptr);
  header = Header::ptr2header(str);
  ASSERT_EQ(0, strncmp(str, original, n));
  ASSERT_TRUE(header->check_magic_code());
  free(str);
}

TEST_F(TestMallocHook, test_malloc_usable_size)
{
  const static size_t size = 100;
  void *ptr = malloc(size);
  ASSERT_NE(ptr, nullptr);
  Header *header = Header::ptr2header(ptr);
  ASSERT_TRUE(header->check_magic_code());
  size_t ptr_size = malloc_usable_size(ptr);
  ASSERT_EQ(size, ptr_size);
  free(ptr);
}

TEST_F(TestMallocHook, test_cpp_operator)
{
  // new && delete
  int *ptr = new int;
  ASSERT_NE(ptr, nullptr);
  Header *header = Header::ptr2header((void *)ptr);
  ASSERT_TRUE(header->check_magic_code());
  delete ptr;

  // new[] && delete[]
  size_t n = 10;
  int *ptr_array = new int[n];
  ASSERT_NE(ptr_array, nullptr);
  header = Header::ptr2header((void *)ptr_array);
  ASSERT_TRUE(header->check_magic_code());
  ptr_array[n - 1] = 10;
  delete[] ptr_array;
}
