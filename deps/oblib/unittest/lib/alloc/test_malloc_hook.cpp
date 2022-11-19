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


#if 0
TEST_F(TestMallocHook, Basic)
{
  const static size_t size = 100;
  for (int i = 0; i < 3; i++) {
    if (0 == i) {
      lib::glibc_hook_opt = GHO_NOHOOK;
    } else if (1 == i) {
      lib::glibc_hook_opt = GHO_HOOK;
    } else {
      lib::glibc_hook_opt = GHO_NONULL;
    }
    bool from_glibc = 0 == i;
    void *ptr = nullptr;

    // calloc
    char zero_buf[100];
    memset(zero_buf, 0 , 100);
    ptr = calloc(10, 10);
    ASSERT_NE(ptr, nullptr);
    memset(ptr, 0, size);
    HookHeader *header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, 10 * 10);
    // check 0
    ASSERT_EQ(0, memcmp(ptr, zero_buf, 100));
    free(ptr);

    // malloc
    ptr = malloc(size);
    ASSERT_NE(ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size);
    char buf[size];
    memset(buf, 'a', size);
    memcpy(ptr, buf, size);

    // realloc
    void *new_ptr = realloc(ptr, size * 2);
    ASSERT_NE(new_ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)new_ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size * 2);
    ASSERT_EQ(0, memcmp(new_ptr, buf, size));
    free(new_ptr);

    // realloc(nullptr, size > 0);
    ptr = realloc(nullptr, size);
    ASSERT_NE(ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size);

    // realloc(size zoom in && out);
    ptr = malloc(size);
    ASSERT_NE(ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size);
    // zoom in
    ptr = realloc(ptr, size * 2);
    ASSERT_NE(ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size * 2);
    // zoom out
    ptr = realloc(ptr, size);
    ASSERT_NE(ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size);

    // memalign
    size_t alignment = 4L << 10;
    ptr = memalign(alignment, size);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(0, (size_t)ptr & (alignment - 1));
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size);
    free(ptr);

    // valloc && realloc
    ptr = valloc(size);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(0, (size_t)ptr & (sysconf(_SC_PAGESIZE) - 1));
    header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size);
    new_ptr = realloc(ptr, size * 2);
    header = reinterpret_cast<HookHeader*>((char*)new_ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    ASSERT_EQ(header->data_size_, size * 2);

    // posix_memalign
    posix_memalign(&ptr, 32, size);
    ASSERT_NE(ptr, nullptr);
    free(ptr);
  }

  // realloc && cross
  for (int i = 0; i < 2; i++) {
    lib::glibc_hook_opt = 0 == i ? GHO_NOHOOK : GHO_HOOK;
    bool from_glibc = 0 == i;
    void *ptr = malloc(size);
    ASSERT_NE(ptr, nullptr);
    HookHeader *header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == from_glibc);
    char buf[size];
    memset(buf, 'a', size);
    memcpy(ptr, buf, size);
    lib::glibc_hook_opt = 0 == i ? GHO_HOOK : GHO_NOHOOK;
    void *new_ptr = realloc(ptr, size * 2);
    ASSERT_NE(new_ptr, nullptr);
    header = reinterpret_cast<HookHeader*>((char*)new_ptr - HOOK_HEADER_SIZE);
    ASSERT_TRUE(header->from_glibc_ == !from_glibc);
    ASSERT_EQ(header->data_size_, size * 2);
    ASSERT_EQ(0, memcmp(new_ptr, buf, size));
    lib::glibc_hook_opt = 0 == i ? GHO_NOHOOK : GHO_HOOK;
    void *ret = realloc(new_ptr, 0);
    ASSERT_EQ(ret, nullptr);
  }

  // strdup
  char *str = strdup("test");
  HookHeader *header = reinterpret_cast<HookHeader*>((char*)str - HOOK_HEADER_SIZE);
  ASSERT_EQ(0, strcmp(str, "test"));
  ASSERT_EQ(header->MAGIC_CODE_, HOOK_MAGIC_CODE);
#ifdef NDEBUG
  memset(header, 0 , sizeof(HookHeader));
  // Avoid order rearrangement
  std::cout << str << std::endl;
#endif
  free(str);
}
#endif
