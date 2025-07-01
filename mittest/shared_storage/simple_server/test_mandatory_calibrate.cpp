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
#include <filesystem>

#define protected public
#define private public
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::observer;

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{
struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

class ObMandatoryCalibrateTest : public ObSimpleClusterTestBase
{
public:
  ObMandatoryCalibrateTest()
      : ObSimpleClusterTestBase("test_mandatory_calibrate", "50G", "50G", "50G"),
        tenant_created_(false),
        run_ctx_()
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant("tt1", "5G", "10G", false/*oracle_mode*/, 8, "2G"));
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
    //   OK(get_curr_simple_server().init_sql_proxy2());
      tenant_created_ = true;
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx_.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
      }
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

public:
    static const uint64_t HOT_TABLET_TABLET_ID = 200005; // tablet_id for HOT_TABLET_MACRO_BLOCK

protected:
  bool tenant_created_;
  TestRunCtx run_ctx_;
};

TEST_F(ObMandatoryCalibrateTest, test_mandatory_calibrate)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  policy_service->update_tablet_status(HOT_TABLET_TABLET_ID, PolicyStatus::HOT); //HOT 0
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  int64_t cur_type_used_size_init = 0;
  int64_t cur_type_used_size_alloc = 0;
  int64_t cur_type_used_size_calibrate = 0;
  int64_t cur_type_used_size_mandatory = 0;
  const int64_t overflow_size = 10 * ObTenantFileManager::MB; // 10MB
  int64_t deviation_threshold = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.get_deviation_threshold(0, 0, deviation_threshold));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
  ASSERT_TRUE(tenant_file_mgr->calibrate_disk_space_task_.run_cnt_ > 1);

  for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
      cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
    // 2 times for each cache type, test ordinary->mandatory->ordinary->mandatory,
    // Make sure that after mandatory calibration, it can be correctly calibrated by ordinary
    for (int64_t i = 0; i < 2; i++) {
      cur_type_used_size_init = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(cache_type)].used_;
      LOG_INFO("mandatory_calibrate_test_info", "cur_type_name", get_ss_macro_cache_type_str(cache_type),
              K(cur_type_used_size_init), "cur_type_used_size_init(MB)", cur_type_used_size_init / ObTenantFileManager::MB);
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->alloc_file_size(cache_type, deviation_threshold + overflow_size));
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

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
    char buf[1000] = {0};
    const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
    databuff_printf(buf, sizeof(buf),
        "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
        oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
        oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
    oceanbase::shared_storage_info = buf;
    oceanbase::unittest::init_log_and_gtest(argc, argv);
    OB_LOGGER.set_log_level("INFO");
    GCONF.ob_startup_mode.set_value("shared_storage");
    GCONF.datafile_size.set_value("100G");
    GCONF.memory_limit.set_value("20G");
    GCONF.system_memory.set_value("5G");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
