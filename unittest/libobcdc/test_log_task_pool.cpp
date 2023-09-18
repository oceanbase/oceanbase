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

#include <cstdlib>

#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"

#include "logservice/libobcdc/src/ob_log_task_pool.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{

class MockTransTask : public TransTaskBase<MockTransTask>
{
public:
  void foo() { bar_ += 1; }

  void set_allocator(int64_t page_size, common::ObIAllocator &large_allocator)
  {
    UNUSED(page_size);
    UNUSED(large_allocator);
  }

  void set_prealloc_page(void *page)
  {
    UNUSED(page);
  }

  void revert_prealloc_page(void *page)
  {
    UNUSED(page);
  }

  void set_task_info(const logservice::TenantLSID &tls_id,
      const char *tls_id_str)
  {
    UNUSED(tls_id);
    UNUSED(tls_id_str);
  }

private:
  int64_t bar_;
};

TEST(ObLogTransTaskPool, Init)
{
  const int64_t part_trans_task_prealloc_count = 300000;
  const int64_t page_size = 8 * 1024;
  const int64_t prealloc_page_count = 20000;

  ObConcurrentFIFOAllocator fifo;
  int64_t G = 1024 * 1024 * 1024;
  fifo.init(1 * G, 1 * G, OB_MALLOC_BIG_BLOCK_SIZE);

  ObLogTransTaskPool<MockTransTask> pool;

  int ret = pool.init(&fifo, part_trans_task_prealloc_count, true, prealloc_page_count);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST(ObLogTransTaskPool, Function1)
{
  const int64_t task_cnt = 1024 * 32;

  ObConcurrentFIFOAllocator fifo;
  int64_t G = 1024 * 1024 * 1024;
  fifo.init(1 * G, 1 * G, OB_MALLOC_BIG_BLOCK_SIZE);

  ObLogTransTaskPool<MockTransTask> pool;

  int ret = pool.init(&fifo, 1024 * 8, true, 1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  MockTransTask **tasks = new MockTransTask*[task_cnt];
  const char *tls_info = "tenant_ls_id";
  logservice::TenantLSID tls_id;

  for (int64_t idx = 0; idx < task_cnt; ++idx) {
    tasks[idx] = pool.get(tls_info, tls_id);
    EXPECT_TRUE(NULL != tasks[idx]);
  }

  for (int64_t idx = 0; idx < task_cnt; ++idx) {
    tasks[idx]->revert();
  }

  pool.destroy();

  delete []tasks;
  fifo.destroy();
}

// 2 tasks not returned.
TEST(ObLogTransTaskPool, Function2)
{
  const int64_t task_cnt = 1024 * 32;

  ObConcurrentFIFOAllocator fifo;
  int64_t G = 1024 * 1024 * 1024;
  fifo.init(1 * G, 1 * G, OB_MALLOC_BIG_BLOCK_SIZE);

  ObLogTransTaskPool<MockTransTask> pool;

  int ret = pool.init(&fifo, 1024 * 8, true, 1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  MockTransTask **tasks = new MockTransTask*[task_cnt];
  const char *tls_info = "partition";
  logservice::TenantLSID tls_id;

  for (int64_t idx = 0; idx < task_cnt; ++idx) {
    tasks[idx] = pool.get(tls_info, tls_id);
    EXPECT_TRUE(NULL != tasks[idx]);
  }

  for (int64_t idx = 0; idx < task_cnt - 2; ++idx) {
    tasks[idx + 1]->revert();
  }

  pool.destroy();

  delete []tasks;
  fifo.destroy();
}


}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
