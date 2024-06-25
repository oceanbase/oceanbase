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
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include <exception>
#define private public
#define protected public
#include "lib/allocator/ob_malloc.h"
#include "storage/multi_data_source/mds_node.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_row.h"
#include "example_user_helper_define.cpp"
#include "storage/tablet/ob_mds_schema_helper.h"
namespace oceanbase {
namespace storage {
namespace mds {
void *DefaultAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void DefaultAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
void *MdsAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
}}}
namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestMdsRowAndMdsCtx: public ::testing::Test
{
public:
  TestMdsRowAndMdsCtx() { ObMdsSchemaHelper::get_instance().init(); };
  virtual ~TestMdsRowAndMdsCtx() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
  static void mds_row_set_element();
  static void two_thread_set_conflict();
  static void get_uncommitted();
  static void get_latest();
  static void get_by_writer();
  static void get_snapshop_until_timeout();
  static void get_snapshop_disgard();
  static MdsRow<ExampleUserData2> row_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsRowAndMdsCtx);
};

MdsRow<ExampleUserData2> TestMdsRowAndMdsCtx::row_;

void TestMdsRowAndMdsCtx::mds_row_set_element() {
  ExampleUserData2 data1(1);
  MdsCtx ctx1(1);
  ASSERT_EQ(OB_SUCCESS, row_.set(data1, ctx1, 0));// copy user data
  UserMdsNode<ExampleUserData2> *user_node = static_cast<UserMdsNode<ExampleUserData2> *>(row_.sorted_list_.list_.list_head_);
  ASSERT_NE(user_node->user_data_.data_.ptr(), data1.data_.ptr());
  auto p_data = data1.data_.ptr();
  ASSERT_EQ(OB_SUCCESS, row_.set(std::move(data1), ctx1, 0));// move user data
  user_node = static_cast<UserMdsNode<ExampleUserData2> *>(row_.sorted_list_.list_.list_head_);
  ASSERT_EQ(user_node->user_data_.data_.ptr(), p_data);
  MdsCtx ctx2(2);
  ExampleUserData2 data2(2);
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, row_.set(data2, ctx2, 0));// 不设超时，报6005
  ASSERT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, row_.set(data2, ctx2, 200_ms));// 设超时，报6003
  ctx1.on_redo(mock_scn(1));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, row_.set(data2, ctx2, 0));// 不设超时，报6005
  ctx1.before_prepare();
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, row_.set(data2, ctx2, 0));// 不设超时，报6005
  ctx1.on_prepare(mock_scn(2));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, row_.set(data2, ctx2, 0));// 不设超时，报6005
  ctx1.on_commit(mock_scn(3));
  ASSERT_EQ(OB_SUCCESS, row_.set(data2, ctx2, 0));
}

void TestMdsRowAndMdsCtx::two_thread_set_conflict() {
  std::thread t1([]() {
    ExampleUserData2 data(6);
    MdsCtx ctx(3);
    ASSERT_EQ(OB_SUCCESS, row_.set(data, ctx, 0));
    ob_usleep(500_ms);
    ctx.on_redo(mock_scn(6));
    ctx.before_prepare();
    ctx.on_prepare(mock_scn(6));
    ctx.on_commit(mock_scn(6));
  });
  std::thread t2([]() {
    ExampleUserData2 data(7);
    MdsCtx ctx(4);
    ob_usleep(100_ms);
    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, row_.set(data, ctx, 0));
    ASSERT_EQ(OB_SUCCESS, row_.set(data, ctx, 1_s));
    ob_usleep(500_ms);
    ctx.on_redo(mock_scn(9));
    ctx.before_prepare();
    ctx.on_prepare(mock_scn(9));
    ctx.on_commit(mock_scn(9));
  });
  t1.join();
  t2.join();
}

void TestMdsRowAndMdsCtx::get_uncommitted() {
  ExampleUserData2 data(12);
  MdsCtx ctx(5);
  ASSERT_EQ(OB_SUCCESS, row_.set(data, ctx, 0));
  MdsCtx ctx2(6);
  int64_t data_size = 0;
  ASSERT_EQ(OB_SUCCESS, row_.get_uncommitted([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, 0));
  ASSERT_EQ(12, data_size);
  ctx.on_redo(mock_scn(10));
  ctx.before_prepare();
  ctx.on_prepare(mock_scn(10));
  ctx.on_commit(mock_scn(10));
}

void TestMdsRowAndMdsCtx::get_latest() {
  int64_t data_size = 0;
  ASSERT_EQ(OB_SUCCESS, row_.get_snapshot([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, share::SCN::max_scn(), 0));
  ExampleUserData2 data(13);
  MdsCtx ctx(7);
  ASSERT_EQ(OB_SUCCESS, row_.set(data, ctx, 0));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, row_.get_snapshot([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, share::SCN::max_scn(), 0));
  ASSERT_EQ(12, data_size);
}

void TestMdsRowAndMdsCtx::get_by_writer() {
  ExampleUserData2 data(13);
  MdsCtx ctx(9);
  int64_t data_size = 0;
  ASSERT_EQ(OB_SUCCESS, row_.get_by_writer([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, 9, mock_scn(6), 0, 0));// read snapshot
  ASSERT_EQ(6, data_size);// read committed version

  ASSERT_EQ(OB_SUCCESS, row_.set(data, ctx, 0));
  ASSERT_EQ(OB_SUCCESS, row_.get_by_writer([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, 9, mock_scn(10)/*not affected*/, 0, 0));
  ASSERT_EQ(13, data_size);// read self write
}

void TestMdsRowAndMdsCtx::get_snapshop_until_timeout() {
  ExampleUserData2 data(20);
  MdsCtx ctx(20);
  int64_t data_size = 0;
  ASSERT_EQ(OB_SUCCESS, row_.set(data, ctx, 0));
  ctx.before_prepare();// block all read operations
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, row_.get_by_writer([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, 10, mock_scn(6), 0, 500_ms));// read snapshot timeout
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, row_.get_snapshot([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, mock_scn(6), 500_ms));// read snapshot timeout

  ctx.on_redo(mock_scn(20));
  ctx.on_prepare(mock_scn(20));
  ctx.on_commit(mock_scn(20));

  ASSERT_EQ(OB_SUCCESS, row_.get_snapshot([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, mock_scn(6), 500_ms));// read snapshot
  ASSERT_EQ(6, data_size);
  ASSERT_EQ(OB_SUCCESS, row_.get_snapshot([&data_size](const ExampleUserData2 &data) {
    data_size = data.data_.length();
    return OB_SUCCESS;
  }, mock_scn(21), 500_ms));// read snapshot
  ASSERT_EQ(20, data_size);
}

void TestMdsRowAndMdsCtx::get_snapshop_disgard() {
   ASSERT_EQ(OB_SNAPSHOT_DISCARDED, row_.get_snapshot([](const ExampleUserData2 &data) {
    return OB_SUCCESS;
  }, mock_scn(1), 500_ms));// read snapshot timeout
}

TEST_F(TestMdsRowAndMdsCtx, mds_row_set_element) { TestMdsRowAndMdsCtx::mds_row_set_element(); }
TEST_F(TestMdsRowAndMdsCtx, two_thread_set_conflict) { TestMdsRowAndMdsCtx::two_thread_set_conflict(); }
TEST_F(TestMdsRowAndMdsCtx, get_uncommitted) { TestMdsRowAndMdsCtx::get_uncommitted(); }
TEST_F(TestMdsRowAndMdsCtx, get_latest) { TestMdsRowAndMdsCtx::get_latest(); }
TEST_F(TestMdsRowAndMdsCtx, get_by_writer) { TestMdsRowAndMdsCtx::get_by_writer(); }
TEST_F(TestMdsRowAndMdsCtx, get_snapshop_until_timeout) { TestMdsRowAndMdsCtx::get_snapshop_until_timeout(); }
TEST_F(TestMdsRowAndMdsCtx, get_snapshop_disgard) { TestMdsRowAndMdsCtx::get_snapshop_disgard(); }

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_row.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_row.log", false);
  logger.set_log_level(OB_LOG_LEVEL_TRACE);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  oceanbase::unittest::TestMdsRowAndMdsCtx::row_.~MdsRow();
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    ret = -1;
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}