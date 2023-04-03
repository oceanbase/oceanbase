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
#define private public
#include "storage/ob_handle_cache.h"

#define OK(s) ASSERT_EQ(OB_SUCCESS, (s));
#define NOT_EXIST(s) ASSERT_EQ(OB_ENTRY_NOT_EXIST, (s))

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace unittest
{
class TestHandleCache : public ::testing::Test
{
};

class TestKey
{
public:
  TestKey() {}
  TestKey(const int key) : key_(key) {}
  uint64_t hash() const { return key_; }
  int64_t to_string(char *buf, int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
  bool operator == (const TestKey &key) const
  {
    return key_ == key.key_;
  }
  int key_;
};

class TestHandle
{
public:
  void reset() {}
  int v_;
};

TEST_F(TestHandleCache, basic)
{
  typedef ObHandleCache<TestKey, TestHandle, 4> TestCache;
  TestCache handle_cache;
  TestKey key(0);
  TestHandle handle;
  handle.v_ = 0;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(0, handle.v_);

  key.key_ = 1;
  handle.v_ = 1;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(1, handle.v_);

  key.key_ = 64;
  handle.v_ = 64;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(64, handle.v_);
  key.key_= 0;
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(0, handle.v_);

  key.key_ = 32;
  handle.v_ = 32;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(32, handle.v_);
  key.key_= 0;
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(0, handle.v_);
  key.key_= 64;
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(64, handle.v_);

  key.key_ = 2;
  handle.v_ = 2;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(2, handle.v_);
  key.key_ = 1;
  NOT_EXIST(handle_cache.get_handle(key, handle));

  key.key_ = 3;
  handle.v_ = 3;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(3, handle.v_);
  key.key_ = 32;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  key.key_= 0;
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(0, handle.v_);
  key.key_= 64;
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(64, handle.v_);

  key.key_ = 32;
  handle.v_ = 32;
  NOT_EXIST(handle_cache.get_handle(key, handle));
  OK(handle_cache.put_handle(key, handle));
  OK(handle_cache.get_handle(key, handle));
  ASSERT_EQ(32, handle.v_);
  key.key_ = 2;
  NOT_EXIST(handle_cache.get_handle(key, handle));

  ASSERT_EQ(4, handle_cache.lru_list_.get_size());
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
