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
#include <thread>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/random/ob_random.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_free_page_list.h"

namespace oceanbase
{
using namespace common;
using namespace tmp_file;

class TestWriteCacheFreePageList : public ::testing::Test
{
public:
  TestWriteCacheFreePageList() {}
  virtual ~TestWriteCacheFreePageList() = default;
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
  }
  void SetUp() override
  {
  }

  void check_list_after_pop(ObTmpFileWriteCacheFreePageList &page_list, const int64_t PAGE_NUM);
};

void TestWriteCacheFreePageList::check_list_after_pop(
    ObTmpFileWriteCacheFreePageList &page_list,
    const int64_t PAGE_NUM)
{
  std::vector<int64_t> array_idx;
  for (int64_t i = 0; i < PAGE_NUM; ++i) {
    PageNode *node = nullptr;
    ASSERT_NE(nullptr, node = page_list.pop_front());
    array_idx.push_back(node->page_.get_array_index());
  }

  sort(array_idx.begin(), array_idx.end());
  ASSERT_EQ(0, array_idx[0]);
  for (int64_t i = 1; i < PAGE_NUM; ++i) {
    ASSERT_EQ(array_idx[i - 1] + 1, array_idx[i]);
    ASSERT_EQ(i, array_idx[i]);
  }
  ASSERT_EQ(0, page_list.size());
}

TEST_F(TestWriteCacheFreePageList, test_basic)
{
  ObTmpFileWriteCacheFreePageList page_list;
  ASSERT_EQ(OB_NOT_INIT, page_list.push_back(nullptr));

  ASSERT_EQ(OB_SUCCESS, page_list.init());

  ASSERT_EQ(OB_ERR_UNEXPECTED, page_list.push_back(nullptr));
  ASSERT_EQ(nullptr, page_list.pop_front());

  std::vector<ObTmpFilePage *> array;
  const int64_t PAGE_NUM = 100 * 10000;
  for (int64_t i = 0; i < PAGE_NUM; ++i) {
    ObTmpFilePage *page = new ObTmpFilePage(nullptr, i);
    array.push_back(page);
    ASSERT_EQ(OB_SUCCESS, page_list.push_back(&array[i]->get_list_node()));
  }
  ASSERT_EQ(PAGE_NUM, page_list.size());
  check_list_after_pop(page_list, PAGE_NUM);
  for (auto ptr : array) {
    delete ptr;
  }
}

TEST_F(TestWriteCacheFreePageList, test_concurrent)
{
  ObTmpFileWriteCacheFreePageList page_list;
  ASSERT_EQ(OB_SUCCESS, page_list.init());

  std::vector<ObTmpFilePage *> array;
  int64_t index = 0;
  const int64_t THREAD_NUM = 100;
  const int64_t PAGE_NUM = 100 * 10000;
  for (int64_t i = 0; i < PAGE_NUM; ++i) {
    ObTmpFilePage *page = new ObTmpFilePage(nullptr, i);
    array.push_back(page);
  }

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    threads.push_back(std::thread([&]() {
      int64_t cur_idx = 0;
      while ((cur_idx = ATOMIC_FAA(&index, 1)) < PAGE_NUM) {
        PageNode *node = &array[cur_idx]->get_list_node();
        ASSERT_EQ(OB_SUCCESS, page_list.push_back(node));
      }
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
  ASSERT_EQ(PAGE_NUM, page_list.size());
  check_list_after_pop(page_list, PAGE_NUM);
  for (auto ptr : array) {
    delete ptr;
  }
}

TEST_F(TestWriteCacheFreePageList, test_random_push_and_pop)
{
  ObTmpFileWriteCacheFreePageList page_list;
  ASSERT_EQ(OB_SUCCESS, page_list.init());

  std::vector<ObTmpFilePage *> array;
  const int64_t PAGE_NUM = 100 * 10000;
  int64_t page_cnt = 0;
  for (int64_t i = 0; i < PAGE_NUM; ++i) {
    ObTmpFilePage *page = new ObTmpFilePage(nullptr, i);
    array.push_back(page);
    ASSERT_EQ(OB_SUCCESS, page_list.push_back(&array[i]->get_list_node()));

    if (ObRandom::rand(0, 100) < 50) {
      ASSERT_NE(nullptr, page_list.pop_front());
    } else {
      page_cnt += 1;
    }
  }
  ASSERT_EQ(page_cnt, page_list.size());
  for (auto ptr : array) {
    delete ptr;
  }
}

TEST_F(TestWriteCacheFreePageList, test_pop_all)
{
  ObTmpFileWriteCacheFreePageList page_list;
  ASSERT_EQ(OB_SUCCESS, page_list.init());

  std::vector<ObTmpFilePage *> array;
  const int64_t PAGE_NUM = 100 * 10000;
  for (int64_t i = 0; i < PAGE_NUM; ++i) {
    ObTmpFilePage *page = new ObTmpFilePage(nullptr, i);
    array.push_back(page);
    ASSERT_EQ(OB_SUCCESS, page_list.push_back(&array[i]->get_list_node()));
  }
  ASSERT_EQ(PAGE_NUM, page_list.size());

  // remove half
  const int64_t REMOVE_NUM = PAGE_NUM / 2;
  ObTmpFileWriteCacheFreePageList::Function func = [&](PageNode *node) {
    bool bret = false;
    if (nullptr != node) {
      bret = node->page_.get_array_index() >= REMOVE_NUM;
    }
    return bret;
  };
  ObDList<PageNode> tmp_list;
  ASSERT_EQ(OB_SUCCESS, page_list.remove_if(func, tmp_list));
  ASSERT_EQ(REMOVE_NUM, page_list.size());
  ASSERT_EQ(REMOVE_NUM, tmp_list.get_size());

  std::vector<int64_t> remove_idx;
  while (!tmp_list.is_empty()) {
    PageNode *node = tmp_list.remove_first();
    remove_idx.push_back(node->page_.get_array_index());
  }
  sort(remove_idx.begin(), remove_idx.end());
  for (int64_t i = 0; i < remove_idx.size(); ++i) {
    ASSERT_EQ(REMOVE_NUM + i, remove_idx[i]);
  }

  check_list_after_pop(page_list, PAGE_NUM - REMOVE_NUM);
  for (auto ptr : array) {
    delete ptr;
  }
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_write_cache_free_page_list.log*");
  OB_LOGGER.set_file_name("test_tmp_file_write_cache_free_page_list.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
