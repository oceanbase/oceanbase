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

#define USING_LOG_PREFIX RS
#define FREEZE_INFO_UNITTEST_DEF

#include "share/ob_freeze_info_proxy.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

#include <gtest/gtest.h>
#include <pthread.h>

namespace oceanbase
{
namespace share
{
using namespace common;

class MockFreezeInfoCache : public ObFreezeInfoCache
{
public:
  static const int64_t FROZEN_TIMESTAMP = 1000;
  static const int64_t SCEHAM_VERSION = 1000;
private:
  virtual int try_get_freeze_info_remotely_(int64_t input_frozen_version,
                                            int64_t &frozen_version,
                                            common::ObFreezeStatus &freeze_status,
                                            int64_t &frozen_timestamp,
                                            int64_t &schema_version)
  {
    frozen_version = input_frozen_version;
    freeze_status = COMMIT_SUCCEED;
    frozen_timestamp = frozen_version;
    schema_version = frozen_version;
    SHARE_LOG(INFO, "get_freeze_info remotely");
    return OB_SUCCESS;
  }
};

class TestFreezeInfo : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() { freeze_info_cache_.destroy(); }
  static void *thread_func1(void *arg);
  static void *thread_func2(void *arg);
  static MockFreezeInfoCache freeze_info_cache_;
  static pthread_barrier_t barrier;
};

MockFreezeInfoCache TestFreezeInfo::freeze_info_cache_;
pthread_barrier_t TestFreezeInfo::barrier;

void TestFreezeInfo::SetUp()
{
  int ret = OB_SUCCESS;
  ret = freeze_info_cache_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void *TestFreezeInfo::thread_func1(void *arg)
{
  UNUSED(arg);
  int64_t start_version = 1;
  int64_t end_version = 128;
  int64_t LOOP = 256;
  pthread_barrier_wait(&barrier);
  for (int64_t times = 0; times < LOOP; ++times) {
    for (int64_t version = start_version; version <= end_version; ++version) {
      common::ObFreezeStatus freeze_status = FREEZE_STATUS_MAX;
      int64_t frozen_timestamp = -1;
      int64_t schema_version = -1;
      int64_t frozen_version = 0;
      EXPECT_EQ(OB_SUCCESS, freeze_info_cache_.get_freeze_info(version,
                                                               frozen_version,
                                                               freeze_status,
                                                               frozen_timestamp,
                                                               schema_version));
      EXPECT_EQ(version, frozen_version);
      EXPECT_EQ(COMMIT_SUCCEED, freeze_status);
      EXPECT_EQ(version, frozen_timestamp);
      EXPECT_EQ(version, schema_version);
    }
  }
  return NULL;
}

TEST_F(TestFreezeInfo, common1)
{
  static const int64_t THREAD_NUM = 8;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, pthread_barrier_init(&barrier, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_create(&work_thread[i], NULL, &thread_func1, NULL));
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier));
}

void *TestFreezeInfo::thread_func2(void *arg)
{
  UNUSED(arg);
  int64_t start_version = 128;
  int64_t end_version = 1;
  int64_t LOOP = 256;
  pthread_barrier_wait(&barrier);
  for (int64_t times = 0; times < LOOP; ++times) {
    for (int64_t version = start_version; version >= end_version; --version) {
      common::ObFreezeStatus freeze_status = FREEZE_STATUS_MAX;
      int64_t frozen_timestamp = -1;
      int64_t schema_version = -1;
      int64_t frozen_version = 0;
      EXPECT_EQ(OB_SUCCESS, freeze_info_cache_.get_freeze_info(version,
                                                               frozen_version,
                                                               freeze_status,
                                                               frozen_timestamp,
                                                               schema_version));
      EXPECT_EQ(version, frozen_version);
      EXPECT_EQ(COMMIT_SUCCEED, freeze_status);
      EXPECT_EQ(version, frozen_timestamp);
      EXPECT_EQ(version, schema_version);
    }
  }
  return NULL;
}

TEST_F(TestFreezeInfo, common2)
{
  static const int64_t THREAD_NUM = 8;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, pthread_barrier_init(&barrier, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM / 2; ++i) {
    ASSERT_EQ(0, pthread_create(&work_thread[i], NULL, &thread_func1, NULL));
  }
  for (int64_t i = THREAD_NUM / 2; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_create(&work_thread[i], NULL, &thread_func2, NULL));
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier));
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_freeze_info_cache.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_freeze_info_cache.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
