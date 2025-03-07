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
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#include "mittest/shared_storage/clean_residual_data.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;

class TestSSMicroCacheIOHelper : public ::testing::Test
{
public:
  TestSSMicroCacheIOHelper() {}
  virtual ~TestSSMicroCacheIOHelper() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheIOHelper::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheIOHelper::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheIOHelper::SetUp()
{
}

void TestSSMicroCacheIOHelper::TearDown()
{
}

TEST_F(TestSSMicroCacheIOHelper, basic_read_write)
{
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager);
  const int64_t micro_cache_file_size = disk_space_manager->get_micro_cache_file_size();
  const int64_t SIZE_2_MB = 2 * 1024 * 1024;
  ASSERT_GT(micro_cache_file_size, SIZE_2_MB * 10);

  // write micro cache file
  const int64_t size = SIZE_2_MB;
  const int64_t offset = lower_align(micro_cache_file_size - SIZE_2_MB, DIO_READ_ALIGN_SIZE);
  char write_buf[size];
  for (int64_t i = 0; i < size; ++i) {
    write_buf[i] = static_cast<char>(ObRandom::rand(0, 128));
  }
  ObSSPhysicalBlock phy_block;
  ObSSPhysicalBlockHandle phy_block_handle;
  phy_block_handle.set_ptr(&phy_block);
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheIOHelper::write_block(offset, size, write_buf, phy_block_handle));

  // read micro cache file, and compare read_buf with write_buf
  char read_buf[size];
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheIOHelper::read_block(offset, size, read_buf, phy_block_handle));
  ASSERT_EQ(0, MEMCMP(read_buf, write_buf, size));
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_io_helper.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_io_helper.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
