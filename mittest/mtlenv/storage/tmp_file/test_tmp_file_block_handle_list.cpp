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
#include "storage/tmp_file/ob_tmp_file_block.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_block_handle_list.h"

namespace oceanbase
{
using namespace common;
using namespace tmp_file;

class TestTmpFileBlockHandleList : public ::testing::Test
{
public:
  TestTmpFileBlockHandleList() {}
  virtual ~TestTmpFileBlockHandleList() = default;
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
  }
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, tmp_file_block_mgr_.init(1));
  }
  void TearDown() override
  {
    tmp_file_block_mgr_.destroy();
  }
  void generate_blocks(const int64_t BLOCK_NUM, ObArray<ObTmpFileBlock *> &vec);

public:
  ObTmpFileBlockManager tmp_file_block_mgr_;
};

void TestTmpFileBlockHandleList::generate_blocks(const int64_t BLOCK_NUM, ObArray<ObTmpFileBlock *> &vec)
{
  for (int64_t i = 0; i < BLOCK_NUM; ++i) {
    void *buf = tmp_file_block_mgr_.get_block_allocator().alloc(sizeof(ObTmpFileBlock));
    ASSERT_NE(nullptr, buf);

    ObTmpFileBlock *block = new (buf) ObTmpFileBlock();
    LOG_INFO("generate block", KPC(block));
    int64_t block_index = ATOMIC_AAF(&tmp_file_block_mgr_.block_index_generator_, 1);
    ASSERT_EQ(OB_SUCCESS, block->init(block_index, ObTmpFileBlock::EXCLUSIVE, &tmp_file_block_mgr_));
    vec.push_back(block);
  }
}


TEST_F(TestTmpFileBlockHandleList, test_basic)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BLOCK_NUM = 10240;
  ObTmpFileBlockHandleList list;

  ObTmpFileBlock block;
  ObTmpFileBlockHandle handle(&block);
  ret = list.append(handle);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  list.init(ObTmpFileBlockHandleList::PREALLOC_NODE);
  ret = list.append(handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = list.remove(handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_exist = false;
  ret = list.remove(handle, is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);

  ObArray<ObTmpFileBlock *> vec;
  generate_blocks(MAX_BLOCK_NUM, vec);
  ASSERT_EQ(vec.size(), MAX_BLOCK_NUM);
  for (int64_t i = 0; i < vec.size(); ++i) {
    ObTmpFileBlockHandle handle(vec[i]);
    ret = list.append(handle);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ASSERT_EQ(list.size(), MAX_BLOCK_NUM);

  for (int64_t i = 0; i < MAX_BLOCK_NUM; ++i) {
    ObTmpFileBlockHandle handle = list.pop_first();
    ASSERT_NE(nullptr, handle.get());
    ASSERT_EQ(i + 1, handle->get_block_index());
  }

  ASSERT_EQ(list.size(), 0);
}

TEST_F(TestTmpFileBlockHandleList, test_concurrent)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BLOCK_NUM = 10240;
  const int64_t MAX_THREAD_NUM = 20;
  ObSpinLock lock;
  ObTmpFileBlockHandleList list;
  list.init(ObTmpFileBlockHandleList::FLUSH_NODE);
  std::vector<int64_t> block_indexes;

  auto single_thread = [&]() {
    ObArray<ObTmpFileBlock *> vec;
    generate_blocks(MAX_BLOCK_NUM, vec);
    ASSERT_EQ(vec.size(), MAX_BLOCK_NUM);
    for (int64_t i = 0; i < vec.size(); ++i) {
      // 1. push back new block
      ObTmpFileBlockHandle handle(vec[i]);
      ret = list.append(handle);
      ASSERT_EQ(OB_SUCCESS, ret);

      // 2. pop first block
      ObTmpFileBlockHandle first_handle = list.pop_first();
      {
        ObSpinLockGuard guard(lock);
        block_indexes.push_back(first_handle->get_block_index());
      }
    }
  };

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    threads.emplace_back(single_thread);
  }
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    threads[i].join();
  }

  ASSERT_EQ(list.size(), 0);
  ASSERT_EQ(block_indexes.size(), MAX_BLOCK_NUM * MAX_THREAD_NUM);
  sort(block_indexes.begin(), block_indexes.end());
  for (int64_t i = 1; i < block_indexes.size(); ++i) {
    ASSERT_EQ(block_indexes[i], block_indexes[i - 1] + 1);
  }
}

TEST_F(TestTmpFileBlockHandleList, test_for_each)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BLOCK_NUM = 10240;
  const int64_t MAX_THREAD_NUM = 20;
  ObSpinLock lock;
  ObTmpFileBlockHandleList list;
  list.init(ObTmpFileBlockHandleList::FLUSH_NODE);
  std::vector<int64_t> block_indexes;

  auto remove_op = [&](tmp_file::ObTmpFileBlkNode *node) {
    if (nullptr != node && node->block_.get_block_index() % 2 == 0) {
      ObTmpFileBlockHandle handle(&node->block_);
      ret = list.remove_without_lock_(handle);
    }
    return true;
  };
  auto generate = [&]() {
    ObArray<ObTmpFileBlock *> vec;
    generate_blocks(MAX_BLOCK_NUM, vec);
    ASSERT_EQ(vec.size(), MAX_BLOCK_NUM);
    for (int64_t i = 0; i < vec.size(); ++i) {
      ObTmpFileBlockHandle handle(vec[i]);
      ret = list.append(handle);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  };
  auto collect = [&](tmp_file::ObTmpFileBlkNode *node) {
    ObSpinLockGuard guard(lock);
    block_indexes.push_back(node->block_.get_block_index());
    return true;
  };

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    threads.emplace_back(generate);
  }
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    threads[i].join();
  }

  ASSERT_EQ(list.size(), MAX_BLOCK_NUM * MAX_THREAD_NUM);
  list.for_each(remove_op);
  ASSERT_EQ(list.size(), MAX_BLOCK_NUM * MAX_THREAD_NUM / 2);
  list.for_each(collect);
  sort(block_indexes.begin(), block_indexes.end());
  for (int64_t i = 1; i < block_indexes.size(); ++i) {
    ASSERT_EQ(block_indexes[i], block_indexes[i - 1] + 2);
  }
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_tmp_file_block_handle_list.log*");
  OB_LOGGER.set_file_name("test_tmp_file_block_handle_list.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
