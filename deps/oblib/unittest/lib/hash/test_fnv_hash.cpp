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

#include<stdint.h>
#include "lib/hash/fnv_hash.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace common::hash;

#define TEST_IT(str, fnv1, fnv1a)                                         \
  do {                                                                    \
    const uint64_t hash_val = fnv1_32_and_fnv1a_32_compile_time_hash(str); \
    ASSERT_EQ(hash_val >> 32, fnv1);                                      \
    ASSERT_EQ((hash_val << 32) >> 32, fnv1a);                             \
  } while (0)

TEST(Test_FNV_HASH, baisc)
{
  static_assert(fnv1_32_and_fnv1a_32_compile_time_hash(__FILE__) != 0, "static check");
  TEST_IT("", 0x811c9dc5, 0x811c9dc5);
  TEST_IT("a", 0x050c5d7e, 0xe40c292c);
  TEST_IT("b", 0x050c5d7d, 0xe70c2de5);
  TEST_IT("foo", 0x408f5e13, 0xa9f37ed7);
  TEST_IT("chongo w", 0x846d619e, 0xdd77ed30);
  TEST_IT("http://antwrp.gsfc.nasa.gov/apod/astropix.html", 0xb4448d60, 0xce524afa);
}

TEST(Test_FNV_HASH, for_logger)
{
#define STRINGIZE_(x) #x
#define STRINGIZE(x) STRINGIZE_(x)
#define FILE "/data/1/oceanbase/deps/oblib/src/lib/ob_allocator_v2.cpp"
#define LINE 27
  static_assert(fnv_hash_for_logger(FILE""STRINGIZE(LINE)) == 0xe4ba9a6b0a4736fb, "static check");
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
