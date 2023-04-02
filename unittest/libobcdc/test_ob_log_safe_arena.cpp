/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/oblog/ob_log.h"
#include <gtest/gtest.h>
#include "ob_log_safe_arena.h"
#include <thread>

namespace oceanbase
{
namespace libobcdc
{

bool check_memset(const void *ptr, const char c, const int64_t size) {
  bool bret = true;
  const char *cptr = static_cast<const char*>(ptr);
  for (int i = 0; bret && i < size; i++) {
    if (cptr[i] != c) {
      bret = false;
    }
  }
  return bret;
}

TEST(ObLogSafeArena, test_ob_log_safe_arena)
{
  ObCdcSafeArena safe_arena;
  const int64_t THREAD_NUM = 32;
  std::vector<std::thread> threads;
  auto alloc_test_func = [&](int idx) {
    constexpr int64_t ALLOC_CNT = 1024;
    constexpr int64_t ALLOC_SIZE = 1024;
    ObRandom rand;
    char c = static_cast<char>(idx & 0xFF);
    for (int i = 0; i < ALLOC_CNT; i++) {
      int64_t alloc_size = rand.get(1, ALLOC_SIZE);
      void *ptr = safe_arena.alloc(alloc_size);
      EXPECT_TRUE(NULL != ptr);
      MEMSET(ptr, c, alloc_size);
      EXPECT_TRUE(check_memset(ptr, c, alloc_size));
    }
  };
  for (int i = 0; i < THREAD_NUM; i++) {
    threads.emplace_back(std::thread(alloc_test_func, i));
  }
  for (int i = 0; i < THREAD_NUM; i++) {
    threads[i].join();
  }
  safe_arena.clear();
}

} // namespace libobcdc
} // ns oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
