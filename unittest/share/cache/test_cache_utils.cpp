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

#define USING_LOG_PREFEX SHARE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_define.h"
#define private public
#include "share/cache/ob_cache_utils.h"

using ::testing::_;
namespace oceanbase
{
using namespace common;
using namespace lib;
namespace share
{
TEST(TestFixedHashMap, basic)
{
  ObFixedHashMap<int64_t, int64_t> fix_map;
  ASSERT_EQ(OB_SUCCESS, fix_map.init(1024, 1024, "1"));
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, fix_map.set(i, i));
    if (i != 1023) {
      ASSERT_EQ(OB_ENTRY_EXIST, fix_map.set(i, i));
    } else {
      ASSERT_EQ(OB_SIZE_OVERFLOW, fix_map.set(i, i));
    }
  }
  ASSERT_EQ(OB_SIZE_OVERFLOW, fix_map.set(5000, 5000));

  for (int64_t i = 0; i < 1024; ++i) {
    int64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, fix_map.get(i, value));
    ASSERT_EQ(value, i);
  }
  int64_t value = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, fix_map.get(5000, value));
  ObFixedHashMap<int64_t, int64_t>::iterator iter = fix_map.begin();
  int64_t i = 0;
  for ( ; iter != fix_map.end(); ++iter) {
    ASSERT_EQ(i, iter->first);
    ASSERT_EQ(i, iter->second);
    ++i;
  }

  fix_map.reuse();
  for (int64_t i = 0; i < 1024; ++i) {
    int64_t value = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, fix_map.get(i, value));
  }
  ASSERT_EQ(fix_map.begin(), fix_map.end());
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, fix_map.set(i, i));
  }
  for (int64_t i = 0; i < 1024; ++i) {
    int64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, fix_map.get(i, value));
    ASSERT_EQ(value, i);
  }
  ASSERT_EQ(OB_SIZE_OVERFLOW, fix_map.set(5000, 5000));
  iter = fix_map.begin();
  i = 0;
  for ( ; iter != fix_map.end(); ++iter) {
    ASSERT_EQ(i, iter->first);
    ASSERT_EQ(i, iter->second);
    ++i;
  }
  for (int64_t j = 0; j < i; ++j) {
    ASSERT_EQ(OB_SUCCESS, fix_map.erase(j));
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, fix_map.erase(0));

  ASSERT_EQ(0, fix_map.size_);
  for (int64_t i = 0; i < 1024; ++i) {
    int64_t value = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, fix_map.get(i, value));
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, fix_map.erase(i));
  }
  ASSERT_EQ(0, fix_map.size_);
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, fix_map.set(i, i));
    int64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, fix_map.get(i, value));
    ASSERT_EQ(value, i);
  }
  ASSERT_EQ(1024, fix_map.size_);
  ASSERT_EQ(OB_SIZE_OVERFLOW, fix_map.set(5000, 5000));
}

TEST(TestFreeHeap, basic)
{
  ObFreeHeap<int64_t> heap;
  ASSERT_EQ(OB_SUCCESS, heap.init(1024, "1"));
  for (int64_t i = 0; i < 1024; ++i) {
    int64_t *ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, heap.sbrk(ptr));
    MEMSET(ptr, 0, sizeof(int64_t));
  }
  int64_t *ptr = NULL;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, heap.sbrk(ptr));
  heap.reuse();
  for (int64_t i = 0; i < 512; ++i) {
    int64_t *ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, heap.sbrk(ptr));
    MEMSET(ptr, 0, sizeof(int64_t));
  }
  ASSERT_EQ(OB_SUCCESS, heap.sbrk(500, ptr));
  MEMSET(ptr, 0, sizeof(int64_t) * 500);
  ASSERT_EQ(OB_SUCCESS, heap.sbrk(12, ptr));
  MEMSET(ptr, 0, sizeof(int64_t) * 12);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, heap.sbrk(ptr));
}

TEST(TestFixArray, basic)
{
  ObSimpleFixedArray<int64_t> fix_array;
  ASSERT_EQ(OB_SUCCESS, fix_array.init(1024, "1"));
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, fix_array.push_back(i));
  }
  ASSERT_EQ(OB_SIZE_OVERFLOW, fix_array.push_back(1025));
  for (int64_t i = 0; i < fix_array.count(); ++i) {
    ASSERT_EQ(fix_array.at(i), i);
  }
  fix_array.reuse();
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, fix_array.push_back(i));
  }
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
