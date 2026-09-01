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
#include "logservice/archiveservice/large_buffer_pool.h"

namespace oceanbase
{
using namespace archive;
namespace unittest
{
// 2M, larger than BASIC_BUF_SIZE(OB_MALLOC_BIG_BLOCK_SIZE) so the buffer is cached
static const int64_t TEST_BUF_SIZE = 2 * 1024 * 1024L;
static const int64_t TEST_TOTAL_LIMIT = 128 * 1024 * 1024L;

TEST(TestLargeBufferPool, acquire_and_reclaim)
{
  LargeBufferPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init("TestPool", TEST_TOTAL_LIMIT));

  char *buf = pool.acquire(TEST_BUF_SIZE);
  ASSERT_TRUE(NULL != buf);
  // write to the buffer to make sure it is really allocated
  memset(buf, 0, TEST_BUF_SIZE);
  pool.reclaim(buf);

  // acquire again should succeed after reclaim
  char *buf2 = pool.acquire(TEST_BUF_SIZE);
  ASSERT_TRUE(NULL != buf2);
  pool.reclaim(buf2);

  pool.destroy();
}

TEST(TestLargeBufferPool, acquire_multi_and_weed_out)
{
  LargeBufferPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init("TestPool", TEST_TOTAL_LIMIT));

  // acquire two buffers concurrently, forcing the internal array to grow(reserve_)
  char *buf1 = pool.acquire(TEST_BUF_SIZE);
  ASSERT_TRUE(NULL != buf1);
  char *buf2 = pool.acquire(TEST_BUF_SIZE);
  ASSERT_TRUE(NULL != buf2);
  ASSERT_TRUE(buf1 != buf2);

  pool.reclaim(buf1);
  pool.reclaim(buf2);

  // weed_out should not crash and should tolerate idle nodes
  pool.weed_out();

  pool.destroy();
}

// A node must be reusable after its buffer is reclaimed: acquire the same node
// repeatedly and make sure the underlying buffer is handed out and taken back
// cleanly each time (no ref leak, no double free).
TEST(TestLargeBufferPool, reacquire_after_reclaim)
{
  LargeBufferPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init("TestPool", TEST_TOTAL_LIMIT));

  for (int64_t i = 0; i < 8; i++) {
    char *buf = pool.acquire(TEST_BUF_SIZE);
    ASSERT_TRUE(NULL != buf);
    memset(buf, 0, TEST_BUF_SIZE);
    pool.reclaim(buf);
  }

  pool.destroy();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_large_buffer_pool.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
