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
#include <vector>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/random/ob_random.h"
#include "mittest/mtlenv/storage/tmp_file/ob_tmp_file_write_cache_test_helper.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "mtlenv/mock_tenant_module_env.h"

#define WRITE_CACHE_INSTANCE MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_write_cache()

namespace oceanbase
{
using namespace common;
using namespace tmp_file;

static const int64_t TENANT_MEMORY = 16L * 1024L * 1024L * 1024L;
static const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
static const int64_t BLOCK_SIZE = ObTmpFileWriteCache::WBP_BLOCK_SIZE;
static const int64_t BLOCK_PAGE_NUMS = ObTmpFileWriteCache::BLOCK_PAGE_NUMS;
static const int64_t SMALL_MEMORY_LIMIT = 10 * BLOCK_SIZE;
static const int64_t BIG_MEMORY_LIMIT = 40 * BLOCK_SIZE;
static int64_t TIMEOUT_MS = 30 * 1000 * 1000;

// TODO：IO错误单测; 放到另一个文件里，单独整合一个mock文件夹
class TestWriteCache : public ::testing::Test
{
public:
  TestWriteCache() {}
  virtual ~TestWriteCache() = default;
  virtual void SetUp() {}
  virtual void TearDown() {}
  void check_resource_release();
  void wait_until_shrinking_finish(const int64_t target_write_cache_capacity);
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

    CHUNK_MGR.set_limit(TENANT_MEMORY);
    ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);
    ObMallocAllocator::get_instance()->set_tenant_max_min(MTL_ID(), TENANT_MEMORY, 0);
    ObTmpFileWriteCache &write_cache = WRITE_CACHE_INSTANCE;
    write_cache.default_memory_limit_ = SMALL_MEMORY_LIMIT;
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
public:
  static int64_t global_fd_generator_;
};

int64_t TestWriteCache::global_fd_generator_ = 0;

// 检查是否有页面未被淘汰，有则说明存在page ref泄漏；
void TestWriteCache::check_resource_release()
{
  ObTmpFileWriteCache &write_cache = WRITE_CACHE_INSTANCE;

  printf("checking resource used_page_num:%d\n", ATOMIC_LOAD(&write_cache.used_page_cnt_));
  _OB_LOG(INFO, "checking resource used_page_num:%d\n", ATOMIC_LOAD(&write_cache.used_page_cnt_));
  ATOMIC_SET(&write_cache.is_flush_all_, true);
  write_cache.idle_cond_.signal();

  int64_t begin_ts = ObTimeUtility::current_time();
  while (!write_cache.io_waiting_queue_.is_empty()
          || ATOMIC_LOAD(&write_cache.used_page_cnt_) > 0) {
    usleep(10 * 1000);
    if (ObTimeUtility::current_time() - begin_ts > TIMEOUT_MS) {
      break;
    }
  }

  if (write_cache.used_page_cnt_ > 0
      || write_cache.page_map_.count() > 0
      || write_cache.pages_.size() != write_cache.free_page_list_.size()) {
    ADD_FAILURE() << "write cache resource release failed, used_page_cnt:" << write_cache.used_page_cnt_
           << ", page_map_count:" << write_cache.page_map_.count()
           << ", pages_size:" << write_cache.pages_.size()
           << ", free_page_list_size:" << write_cache.free_page_list_.size();
    LOG_INFO("write cache resource release fail",
        K(write_cache.used_page_cnt_), K(write_cache.page_map_.count()),
        K(write_cache.pages_.size()), K(write_cache.free_page_list_.size()));
    // DEBUG
    int ret = OB_SUCCESS;
    ObArray<uint32_t> array_idx;
    ObDList<PageNode> tmp_list;
    while (write_cache.free_page_list_.size() > 0) {
      PageNode *node = write_cache.free_page_list_.pop_front();
      if (nullptr == node) {
        LOG_WARN("node is null", K(write_cache.free_page_list_.size()));
      } else {
        tmp_list.add_last(node);
        ObTmpFilePage &page = node->page_;
        array_idx.push_back(page.get_array_index());
      }
    }
    write_cache.free_page_list_.push_range(tmp_list);

    std::sort(array_idx.begin(), array_idx.end());
    for (int64_t i = 1; i < array_idx.size(); ++i) {
      if (array_idx[i] - array_idx[i-1] != 1) {
        ADD_FAILURE() << "write cache resource release failed, "
                      <<  array_idx[i - 1] << " to " << array_idx[i];
        LOG_INFO("page array index", K(array_idx[i - 1]), K(array_idx[i]));
      }
    }
    // DEBUG
  }
  for (int64_t i = 0; i < write_cache.pages_.size(); ++i) {
    if (write_cache.pages_[i].is_valid()) {
      char page_str[1024];
      write_cache.pages_[i].to_string(page_str, 1024);
      FAIL() << "write cache resource release failed, page:" << i
             << ", is_valid, " << page_str;
    }
  }
  ATOMIC_SET(&write_cache.is_flush_all_, false);
}

void TestWriteCache::wait_until_shrinking_finish(const int64_t target_write_cache_capacity)
{
  int64_t begin_ts = ObTimeUtility::current_time();
  ObTmpFileWriteCache &write_cache = WRITE_CACHE_INSTANCE;
  while (write_cache.get_current_capacity_() > target_write_cache_capacity) {
    usleep(100 * 1000);
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      printf("current capacity: %ld, used_page_cnt:%d\n", write_cache.get_current_capacity_(), write_cache.used_page_cnt_);
    }

    if (ObTimeUtility::current_time() - begin_ts > TIMEOUT_MS) {
      printf("exit wait_until_shrinking_finish after %ld s, current:%ld, target:%ld\n",
          TIMEOUT_MS/1000/1000, write_cache.get_current_capacity_(), target_write_cache_capacity);
      FAIL() << "write cache shrinking timeout, expect:" << target_write_cache_capacity
             << ", actual:" << write_cache.get_current_capacity_();
      break;
    }
  }
}

TEST_F(TestWriteCache, test_shrink)
{
  LOG_INFO("test_shrink begin");
  const int64_t WRITE_SIZE = 3 * BIG_MEMORY_LIMIT;
  ObTenantBase *tenant_ctx = MTL_CTX();
  ASSERT_NE(nullptr, tenant_ctx);
  char *buffer = generate_data(WRITE_SIZE);

  ObTmpFileWriteCache &write_cache = WRITE_CACHE_INSTANCE;
  write_cache.default_memory_limit_ = BIG_MEMORY_LIMIT;

  // write data to expand write cache
  TestWriteCacheSingleThread test1;
  test1.init(MTL_CTX(), ATOMIC_FAA(&global_fd_generator_, 1), buffer, WRITE_SIZE);
  test1.start();
  test1.wait();
  ASSERT_EQ(BIG_MEMORY_LIMIT, write_cache.get_current_capacity_());

  // set smaller memory limit to trigger shrinking
  write_cache.default_memory_limit_ = SMALL_MEMORY_LIMIT;
  TestWriteCacheSingleThread test2;
  test2.init(MTL_CTX(), ATOMIC_FAA(&global_fd_generator_, 1), buffer, WRITE_SIZE / 2);
  test2.start();
  test2.wait();

  int64_t target_capacity = SMALL_MEMORY_LIMIT;
  ASSERT_NO_FATAL_FAILURE(wait_until_shrinking_finish(target_capacity));

  delete [] buffer;
  EXPECT_EQ(SMALL_MEMORY_LIMIT, write_cache.get_current_capacity_());
  EXPECT_NO_FATAL_FAILURE(check_resource_release());
  LOG_INFO("test_shrink end");
}

TEST_F(TestWriteCache, test_basic)
{
  LOG_INFO("test_basic begin");
  const int64_t WRITE_SIZE = 50 * 1024 * 1024;
  char *buffer = generate_data(WRITE_SIZE);
  TestWriteCacheSingleThread test;
  test.init(MTL_CTX(), ATOMIC_FAA(&global_fd_generator_, 1), buffer, WRITE_SIZE);
  test.start();
  test.wait();
  delete [] buffer;
  EXPECT_NO_FATAL_FAILURE(check_resource_release());
  LOG_INFO("test_basic end");
}

// TODO: 非2MB对齐的写入和刷盘
TEST_F(TestWriteCache, test_concurrent)
{
  LOG_INFO("test_concurrent begin");
  const int64_t WRITE_SIZE = 30 * 1024 * 1024;
  ObTenantBase *tenant_ctx = MTL_CTX();
  ASSERT_NE(nullptr, tenant_ctx);
  char *buffer = generate_data(WRITE_SIZE);

  ObTmpFileWriteCache &write_cache = WRITE_CACHE_INSTANCE;
  write_cache.default_memory_limit_ = BIG_MEMORY_LIMIT;
  ATOMIC_SET(&write_cache.is_flush_all_, true);
  const int THREAD_COUNT = 10;
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < THREAD_COUNT; ++i) {
    std::thread t([tenant_ctx, buffer]() {
      ObTenantEnv::set_tenant(tenant_ctx);
      TestWriteCacheSingleThread test;
      int64_t fd = ATOMIC_FAA(&global_fd_generator_, 1);
      test.init(tenant_ctx, fd, buffer, WRITE_SIZE);
      test.start();
      test.wait();
    });
    threads.push_back(std::move(t));
  }

  for (auto &t : threads) {
    t.join();
  }
  delete [] buffer;
  EXPECT_NO_FATAL_FAILURE(check_resource_release());
  LOG_INFO("test_concurrent end");
}

// 1.写数据扩容；2.修改时间戳、水位线，触发自动缩容
TEST_F(TestWriteCache, test_auto_shrink)
{
  LOG_INFO("test_auto_shrink begin");
  const int64_t WRITE_SIZE = 2 * BIG_MEMORY_LIMIT;
  char *buffer = generate_data(WRITE_SIZE);
  ObTmpFileWriteCache &write_cache = WRITE_CACHE_INSTANCE;
  write_cache.default_memory_limit_ = BIG_MEMORY_LIMIT;

  TestWriteCacheSingleThread test;
  test.init(MTL_CTX(), ATOMIC_FAA(&global_fd_generator_, 1), buffer, WRITE_SIZE);
  test.start();
  test.wait();

  write_cache.shrink_ctx_.current_max_used_watermark_ =
      WriteCacheShrinkContext::AUTO_SHRINKING_WATERMARK_L3;
  write_cache.shrink_ctx_.last_shrink_complete_ts_ =
      ObTimeUtility::current_time() - WriteCacheShrinkContext::SHRINKING_PERIOD * 2;

  int64_t max_capacity = write_cache.default_memory_limit_;
  int64_t target_capacity =
      WriteCacheShrinkContext::AUTO_SHRINKING_TARGET_SIZE_L3 * max_capacity / 100;
  ASSERT_NO_FATAL_FAILURE(wait_until_shrinking_finish(target_capacity));

  delete [] buffer;
  EXPECT_NO_FATAL_FAILURE(check_resource_release());
  LOG_INFO("test_auto_shrink end");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_write_cache.log*");
  OB_LOGGER.set_file_name("test_tmp_file_write_cache.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
