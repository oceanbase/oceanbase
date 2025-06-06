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
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/random/ob_random.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page_array.h"

namespace oceanbase
{
using namespace common;
using namespace tmp_file;

class TestWriteCachePageArray : public ::testing::Test
{
public:
  static const int64_t PAGE_NUM_FOR_1GB_WRITE_CACHE = 131072;
  TestWriteCachePageArray() {}
  virtual ~TestWriteCachePageArray() = default;
};

TEST_F(TestWriteCachePageArray, test_basic)
{
  ObTmpFilePage *page = nullptr;
  ObTmpFilePageArray pages;
  ASSERT_EQ(OB_NOT_INIT, pages.push_back(page));

  ASSERT_EQ(OB_SUCCESS, pages.init());

  const int64_t MAX_ENTRY_COUNT = PAGE_NUM_FOR_1GB_WRITE_CACHE * 100;
  for (int64_t i = 0; i < MAX_ENTRY_COUNT; ++i) {
    page = new ObTmpFilePage();
    page->page_key_.virtual_page_id_ = i;
    ASSERT_EQ(OB_SUCCESS, pages.push_back(page));
  }
  ASSERT_EQ(MAX_ENTRY_COUNT, pages.size());
  ASSERT_EQ(MAX_ENTRY_COUNT, pages.count());

  const int64_t RANDOM_CHECK_NUM = 1000;
  for (int64_t i = 0; i < RANDOM_CHECK_NUM; ++i) {
    int64_t idx = ObRandom::rand(0, MAX_ENTRY_COUNT);
    ASSERT_EQ(idx, pages[idx].page_key_.virtual_page_id_);
  }

  int64_t time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < MAX_ENTRY_COUNT; ++i) {
    ObTmpFilePage &page = pages[i];
    int64_t block_idx = page.get_page_id().block_index_;
  }
  time = ObTimeUtility::current_time() - time;
  printf("iterate all items takes: %ld us\n", time);

  for (int64_t i = MAX_ENTRY_COUNT - 1; i >= 0; --i) {
    delete pages.get_ptr(i);
    pages.pop_back();
  }
  ASSERT_EQ(0, pages.size());
  ASSERT_EQ(0, pages.count());

  pages.pop_back();
  ASSERT_EQ(0, pages.size());
  ASSERT_EQ(0, pages.buckets_.size());
}

TEST_F(TestWriteCachePageArray, test_random_push_and_pop)
{
  ObTmpFilePage *page = nullptr;
  ObTmpFilePageArray pages;
  ASSERT_EQ(OB_SUCCESS, pages.init());

  int64_t pop_cnt = 0;
  const int64_t MAX_ENTRY_COUNT = PAGE_NUM_FOR_1GB_WRITE_CACHE * 10;
  for (int64_t i = 0; i < MAX_ENTRY_COUNT; ++i) {
    page = new ObTmpFilePage();
    page->page_key_.virtual_page_id_ = i;
    ASSERT_EQ(OB_SUCCESS, pages.push_back(page));

    if (ObRandom::rand(0, 100) < 50) {
      delete page;
      pages.pop_back();
      pop_cnt += 1;
    }
  }
  ASSERT_EQ(MAX_ENTRY_COUNT - pop_cnt, pages.size());
  ASSERT_EQ(MAX_ENTRY_COUNT - pop_cnt, pages.count());

  for (int64_t i = 1; i < pages.size(); ++i) {
    ASSERT_LT(pages[i - 1].page_key_.virtual_page_id_, pages[i].page_key_.virtual_page_id_);
  }
}

TEST_F(TestWriteCachePageArray, test_array_shrinking)
{
  ObTmpFilePage *page = nullptr;
  ObTmpFilePageArray pages;
  ASSERT_EQ(OB_SUCCESS, pages.init());

  const int64_t MAX_ENTRY_COUNT = PAGE_NUM_FOR_1GB_WRITE_CACHE;
  for (int64_t i = 0; i < MAX_ENTRY_COUNT / 2; ++i) {
    page = new ObTmpFilePage();
    page->page_key_.virtual_page_id_ = i;
    ASSERT_EQ(OB_SUCCESS, pages.push_back(page));
  }
  ASSERT_EQ(MAX_ENTRY_COUNT / 2, pages.size());
  ASSERT_EQ(PAGE_NUM_FOR_1GB_WRITE_CACHE / ObTmpFilePageArray::MAX_BUCKET_CAPACITY / 2 + 1,
            pages.buckets_.size());

  for (int64_t i = MAX_ENTRY_COUNT / 2; i < MAX_ENTRY_COUNT; ++i) {
    page = new ObTmpFilePage();
    page->page_key_.virtual_page_id_ = i;
    ASSERT_EQ(OB_SUCCESS, pages.push_back(page));
  }
  ASSERT_EQ(PAGE_NUM_FOR_1GB_WRITE_CACHE / ObTmpFilePageArray::MAX_BUCKET_CAPACITY + 1,
            pages.buckets_.size());

  int64_t bucket_capacity = pages.MAX_BUCKET_CAPACITY;
  for (int64_t i = MAX_ENTRY_COUNT - 1; i >=0 ; --i) {
    delete pages.get_ptr(i);
    pages.pop_back();
    int64_t bucket_num = (pages.size() + bucket_capacity - 1) / bucket_capacity;
    ASSERT_EQ(bucket_num, pages.buckets_.size());
  }
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_write_cache_page_array.log*");
  OB_LOGGER.set_file_name("test_tmp_file_write_cache_page_array.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
