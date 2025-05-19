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
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroRangeManager : public ::testing::Test
{
public:
  TestSSMicroRangeManager() = default;
  virtual ~TestSSMicroRangeManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
};

void TestSSMicroRangeManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroRangeManager::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroRangeManager::SetUp()
{}

void TestSSMicroRangeManager::TearDown()
{}

TEST_F(TestSSMicroRangeManager, basic_test)
{
  LOG_INFO("TEST_CASE: start basic_test");
  ObSSMicroRangeManager &micro_range_mgr =  MTL(ObSSMicroCache *)->micro_range_mgr_;
  ASSERT_EQ(true, micro_range_mgr.is_inited());

  const int64_t init_range_cnt = micro_range_mgr.init_range_cnt_;
  const int64_t per_range_len = micro_range_mgr.per_range_len_;
  ObSSMicroRangeInfo **range_info_arr = micro_range_mgr.range_info_arr_;
  ASSERT_NE(nullptr, range_info_arr);
  for (int64_t i = 0; i < init_range_cnt; ++i) {
    ASSERT_NE(nullptr, range_info_arr[i]);
    ASSERT_EQ(true, range_info_arr[i]->is_valid());
    if (i < init_range_cnt - 1) {
      ASSERT_EQ((i + 1) * per_range_len - 1, range_info_arr[i]->end_hval_);
    }
  }
  ASSERT_EQ(SS_MICRO_KEY_MAX_HASH_VAL, range_info_arr[init_range_cnt - 1]->end_hval_);

  ObSSMicroRangeInfoHandle range_info_handle;
  const uint32_t hash_val = per_range_len;
  ASSERT_EQ(false, range_info_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.inner_get_range_info(hash_val, range_info_handle));
  ASSERT_EQ(true, range_info_handle.is_valid());
  ASSERT_EQ(2 * per_range_len - 1, range_info_handle()->end_hval_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_range_manager.log*");
  OB_LOGGER.set_file_name("test_ss_micro_range_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}