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

#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "observer/ob_server.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::storage;
using namespace oceanbase::observer;

class ObMandatoryCalibrateTest : public ::testing::Test
{
public:
  ObMandatoryCalibrateTest() = default;
  virtual ~ObMandatoryCalibrateTest() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void ObMandatoryCalibrateTest::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void ObMandatoryCalibrateTest::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(ObMandatoryCalibrateTest, test_mandatory_calibrate)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  int64_t cur_type_used_size_init = 0;
  int64_t cur_type_used_size_alloc = 0;
  int64_t cur_type_used_size_calibrate = 0;
  int64_t cur_type_used_size_mandatory = 0;
  const int64_t overflow_size = 10 * ObTenantFileManager::MB; // 10MB
  int64_t deviation_threshold = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.get_deviation_threshold(0, 0, 0, deviation_threshold));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
  ASSERT_TRUE(tenant_file_mgr->calibrate_disk_space_task_.run_cnt_ > 1);

  for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
      cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
    if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_type) {
      continue; // skip hot tablet macro block, it is not used in this test
    }
    // 2 times for each cache type, test ordinary->mandatory->ordinary->mandatory,
    // Make sure that after mandatory calibration, it can be correctly calibrated by ordinary
    for (int64_t i = 0; i < 2; i++) {
      cur_type_used_size_init = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(cache_type)].used_;
      LOG_INFO("mandatory_calibrate_test_info", "cur_type_name", get_ss_macro_cache_type_str(cache_type),
              K(cur_type_used_size_init), "cur_type_used_size_init(MB)", cur_type_used_size_init / ObTenantFileManager::MB);
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->alloc_file_size(cache_type, deviation_threshold + overflow_size, ObDiskSpaceType::FILE));
      cur_type_used_size_alloc = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(cache_type)].used_;
      LOG_INFO("mandatory_calibrate_test_info", "cur_type_name", get_ss_macro_cache_type_str(cache_type),
              K(cur_type_used_size_alloc), "cur_type_used_size_alloc(MB)", cur_type_used_size_alloc / ObTenantFileManager::MB);
      // ordinary clibrate
      ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
      // sleep(10);  There is no need to sleep because it is proceeding synchronously
      cur_type_used_size_calibrate = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(cache_type)].used_;
      LOG_INFO("mandatory_calibrate_test_info", "cur_type_name", get_ss_macro_cache_type_str(cache_type),
              K(cur_type_used_size_calibrate), "cur_type_used_size_calibrate(MB)", cur_type_used_size_calibrate / ObTenantFileManager::MB);
      ASSERT_TRUE(cur_type_used_size_calibrate == cur_type_used_size_alloc);
      // mandatory clibrate
      ObCalibrateSSDiskSpaceArg arg;
      arg.tenant_id_ = MTL_ID();
      const ObGlobalContext &gctx = ObServer::get_instance().get_gctx();
      ObCalibrateSSDiskSpaceP calibrate_p(gctx);
      calibrate_p.arg_ = arg;
      ASSERT_EQ(OB_SUCCESS, calibrate_p.process());
      sleep(10);  // need to sleep, because the mandatory calibration is asynchronous, and it takes time to complete
      cur_type_used_size_mandatory = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(cache_type)].used_;
      LOG_INFO("mandatory_calibrate_test_info", "cur_type_name", get_ss_macro_cache_type_str(cache_type),
              K(cur_type_used_size_mandatory), "cur_type_used_size_mandatory(MB)", cur_type_used_size_mandatory / ObTenantFileManager::MB);
      ASSERT_TRUE(cur_type_used_size_mandatory == cur_type_used_size_init);
    }
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_mandatory_calibrate.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_mandatory_calibrate.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
