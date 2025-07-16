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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheTaskRunner : public ::testing::Test
{
public:
  TestSSMicroCacheTaskRunner() {}
  virtual ~TestSSMicroCacheTaskRunner() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheTaskRunner::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheTaskRunner::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheTaskRunner::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  tnt_file_mgr->persist_disk_space_task_.enable_adjust_size_ = false;
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30)));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheTaskRunner::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

TEST_F(TestSSMicroCacheTaskRunner, test_disable_all_task)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_disable_all_task");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;

  ASSERT_EQ(0, cache_stat.task_stat().blk_ckpt_cnt_);
  ObSSDoBlkCheckpointTask &blk_ckpt_task = task_runner.blk_ckpt_task_;
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  ob_usleep(3 * 1000 * 1000L);
  ASSERT_EQ(1, cache_stat.task_stat().blk_ckpt_cnt_);

  task_runner.disable_task();
  const int64_t start_time_s = ObTimeUtility::current_time_s();
  int64_t MAX_RETRY_TIME_S = 120;
  bool is_closed = false;
  do {
    is_closed = task_runner.is_task_closed();
    if (!is_closed) {
      ob_usleep(1000 * 1000);
      LOG_INFO("ss_micro_cache task runner still not closed");
    }
  } while (!is_closed && ObTimeUtility::current_time_s() - start_time_s < MAX_RETRY_TIME_S);
  ASSERT_EQ(true, is_closed);

  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  ob_usleep(3 * 1000 * 1000L);
  ASSERT_EQ(1, cache_stat.task_stat().blk_ckpt_cnt_);
  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = ObTimeUtility::current_time_us();

  task_runner.enable_task();
  ob_usleep(3 * 1000 * 1000L);
  ASSERT_EQ(false, task_runner.is_task_closed());

  blk_ckpt_task.ckpt_op_.blk_ckpt_ctx_.prev_ckpt_us_ = TestSSCommonUtil::get_prev_blk_ckpt_time_us();
  ob_usleep(3 * 1000 * 1000L);
  ASSERT_EQ(2, cache_stat.task_stat().blk_ckpt_cnt_);

  LOG_INFO("TEST_CASE: finish test_disable_all_task");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_task_runner.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_task_runner.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
