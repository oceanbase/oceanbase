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

 #include <gtest/gtest.h>
 #include <sys/stat.h>
 #include <sys/vfs.h>
 #include <sys/types.h>
 #include <gmock/gmock.h>
 #define protected public
 #define private public
 #include "lib/utility/ob_test_util.h"
 #include "mittest/mtlenv/mock_tenant_module_env.h"
 #include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
 #include "storage/shared_storage/ob_disk_space_manager.h"
 #include "storage/shared_storage/ob_dir_manager.h"
 #include "storage/shared_storage/ob_file_manager.h"
 #include "storage/shared_storage/ob_ss_format_util.h"
 #include "storage/blocksstable/ob_macro_block_id.h"
 #include "mittest/shared_storage/clean_residual_data.h"
 #include "storage/shared_storage/ob_ss_reader_writer.h"

 #undef private
 #undef protected

 namespace oceanbase
 {
 namespace storage
 {
 using namespace oceanbase::blocksstable;
 using namespace oceanbase::storage;

 class TestAutoExpandDisk : public ::testing::Test
 {
 public:
 TestAutoExpandDisk() = default;
   virtual ~TestAutoExpandDisk() = default;
   static void SetUpTestCase();
   static void TearDownTestCase();
   virtual void SetUp() override;
   virtual void TearDown() override;

   static const int64_t CONFIG_DATA_DISK_SIZE = 18LL * common::GB;
   static const int64_t INIT_DATA_DISK_SIZE = 10LL * common::GB;
   static const int64_t HIDDEN_SYS_DATA_DISK_CONFIG_SIZE = common::MB;
 };

 void TestAutoExpandDisk::SetUpTestCase()
 {
   GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
   ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
   MTL(tmp_file::ObTenantTmpFileManager *)->stop();
   MTL(tmp_file::ObTenantTmpFileManager *)->wait();
   MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
   OK(OB_SERVER_DISK_SPACE_MGR.update_hidden_sys_data_disk_config_size(HIDDEN_SYS_DATA_DISK_CONFIG_SIZE));
 }

 void TestAutoExpandDisk::TearDownTestCase()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
       LOG_WARN("failed to clean residual data", KR(ret));
   }
   MockTenantModuleEnv::get_instance().destroy();
 }

 void TestAutoExpandDisk::SetUp()
 {
   ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
   ASSERT_NE(nullptr, tenant_disk_space_mgr);
   OK(OB_SERVER_DISK_SPACE_MGR.free(tenant_disk_space_mgr->get_total_disk_size()));
   tenant_disk_space_mgr->stop();
   tenant_disk_space_mgr->wait();
   tenant_disk_space_mgr->destroy();

   omt::ObTenant *tenant = nullptr;
   ASSERT_NE(nullptr, GCTX.omt_);
   OK(GCTX.omt_->get_tenant(MTL_ID(), tenant));
   ASSERT_NE(nullptr, tenant);
   share::ObUnitInfoGetter::ObTenantConfig new_unit;
   OK(new_unit.assign(tenant->get_unit()));
   new_unit.config_.resource_.min_cpu_ = 1;
   new_unit.config_.resource_.max_cpu_ = 1;
   new_unit.config_.resource_.data_disk_size_ = CONFIG_DATA_DISK_SIZE;
   new_unit.actual_data_disk_size_ = INIT_DATA_DISK_SIZE;
   OK(GCTX.omt_->update_tenant_unit(new_unit));

   OK(OB_SERVER_DISK_SPACE_MGR.alloc(INIT_DATA_DISK_SIZE));
   OK(tenant_disk_space_mgr->init(MTL_ID(), INIT_DATA_DISK_SIZE));
   OK(tenant_disk_space_mgr->start());
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = false;
 }

 void TestAutoExpandDisk::TearDown()
 {
 }

 TEST_F(TestAutoExpandDisk, test_auto_expand_disk_task)
 {
   int ret = OB_SUCCESS;
   ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
   int64_t total_disk_size = 17L * 1024L * 1024L * 1024L; // 17GB
   const int64_t datafile_size = OB_SERVER_DISK_SPACE_MGR.get_total_disk_size(); // 20GB
   bool succ_resize = false;
   ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size_(total_disk_size, succ_resize));
   ASSERT_EQ(0, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_size());
   ASSERT_EQ(DataDiskSuggestedOperationType::TYPE::NONE, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_operation());
   ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());
   int64_t reserved_disk_size = OB_SERVER_DISK_SPACE_MGR.get_reserved_disk_size();
   ASSERT_EQ(datafile_size - total_disk_size - reserved_disk_size, OB_SERVER_DISK_SPACE_MGR.get_free_disk_size());

   // disable evict_task
   ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
   ASSERT_NE(nullptr, macro_cache_mgr);
   macro_cache_mgr->evict_task_.is_inited_ = false;

   // 1.alloc TMP_FILE exceeds 90% of macro_cache_size
   ObSSMacroCacheStat cache_stat;
   const int64_t macro_cache_size = tenant_disk_space_mgr->get_allocated_shared_macro_cache_size_nolock_();
   ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
   const int64_t disk_size = macro_cache_size * 91 / 100;
   ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->alloc_file_size(disk_size, ObSSMacroCacheType::TMP_FILE,
                                                                ObDiskSpaceType::FILE));
   bool is_expand = false;
   ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->need_expand_disk_size(is_expand));
   ASSERT_TRUE(is_expand);

   // 2.auto expand data_disk_size
   ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->expand_data_disk_size(ObDiskSpaceManager::DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH));
   total_disk_size += ObDiskSpaceManager::DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH;
   ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

   // 3.check expand data_disk_size success
   ASSERT_EQ(datafile_size - total_disk_size - reserved_disk_size, OB_SERVER_DISK_SPACE_MGR.get_free_disk_size());
   const int64_t cur_expect_expand_disk_size = OB_SERVER_DISK_SPACE_MGR.cal_data_disk_suggested_size_();
   ASSERT_GT(cur_expect_expand_disk_size, OB_SERVER_DISK_SPACE_MGR.get_ss_cache_max_size_());
   ASSERT_EQ(0, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_size());
   ASSERT_EQ(DataDiskSuggestedOperationType::TYPE::NONE, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_operation());

   // 4.check expand datafile_size
   GCONF.datafile_maxsize = 25LL * common::GB; // 25GB
   ASSERT_EQ(OB_SUCCESS, OB_SERVER_DISK_SPACE_MGR.resize(datafile_size + 1));
   ASSERT_GT(OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_size(), 0);
   ASSERT_EQ(DataDiskSuggestedOperationType::TYPE::EXPAND, OB_SERVER_DISK_SPACE_MGR.get_data_disk_suggested_operation());

   // 5. actual_data_disk_size shoud not exceed config_data_disk_size
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = true;
   OK(tenant_disk_space_mgr->check_expand_disk_task_.sync_tenant_data_disk_size_());
   int64_t config_data_disk_size = 0;
   int64_t actual_data_disk_size = 0;
   OK(tenant_disk_space_mgr->get_tenant_unit_data_disk_size(MTL_ID(), config_data_disk_size, actual_data_disk_size));
   ASSERT_GT(config_data_disk_size, actual_data_disk_size);
   OK(tenant_disk_space_mgr->expand_data_disk_size(config_data_disk_size - actual_data_disk_size + 1));
   OK(tenant_disk_space_mgr->check_expand_disk_task_.sync_tenant_data_disk_size_());
   OK(tenant_disk_space_mgr->get_tenant_unit_data_disk_size(MTL_ID(), config_data_disk_size, actual_data_disk_size));
   ASSERT_EQ(config_data_disk_size, actual_data_disk_size);
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = false;

   OK(tenant_disk_space_mgr->free_file_size(disk_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE));
 }

 TEST_F(TestAutoExpandDisk, test_auto_shrink_disk_task)
 {
   ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
   ASSERT_NE(nullptr, tenant_disk_space_mgr);

   int64_t config_data_disk_size = 0;
   int64_t actual_data_disk_size = 0;
   OK(tenant_disk_space_mgr->get_tenant_unit_data_disk_size(MTL_ID(), config_data_disk_size, actual_data_disk_size));
   int64_t min_shrinkable_disk_size = 0;
   OK(tenant_disk_space_mgr->cal_min_shrinkable_disk_size(min_shrinkable_disk_size));
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK", K(config_data_disk_size), K(actual_data_disk_size), K(min_shrinkable_disk_size), KPC(tenant_disk_space_mgr));
   ASSERT_GT(config_data_disk_size, actual_data_disk_size);

   // sys tenant config data disk size = sys_unit config + HIDDEN_SYS_DATA_DISK_CONFIG_SIZE
   // update sys_unit config data disk size to actual_data_disk_size - HIDDEN_SYS_DATA_DISK_CONFIG_SIZE + 1
   // so sys tenant config data disk size = actual_data_disk_size + 1, cannot shrink
   omt::ObTenant *tenant = nullptr;
   ASSERT_NE(nullptr, GCTX.omt_);
   OK(GCTX.omt_->get_tenant(MTL_ID(), tenant));
   ASSERT_NE(nullptr, tenant);
   share::ObUnitInfoGetter::ObTenantConfig new_unit;
   OK(new_unit.assign(tenant->get_unit()));
   new_unit.config_.resource_.data_disk_size_ = actual_data_disk_size - HIDDEN_SYS_DATA_DISK_CONFIG_SIZE + 1;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK new_unit", K(new_unit), K(actual_data_disk_size));
   OK(GCTX.omt_->update_tenant_unit(new_unit));
   bool is_need_shrink = true;
   OK(tenant_disk_space_mgr->need_shrink_disk_size(is_need_shrink));
   ASSERT_FALSE(is_need_shrink);

   // update config data disk size to min_shrinkable_disk_size + 1GB
   // make sure actual_data_disk_size is larger than the new config data disk size
   OK(new_unit.assign(tenant->get_unit()));
   new_unit.config_.resource_.data_disk_size_ = min_shrinkable_disk_size + common::GB - HIDDEN_SYS_DATA_DISK_CONFIG_SIZE;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK new_unit", K(new_unit), K(actual_data_disk_size), K(min_shrinkable_disk_size));
   ASSERT_GT(new_unit.config_.resource_.data_disk_size_, 0);
   ASSERT_LT(new_unit.config_.resource_.data_disk_size_ + HIDDEN_SYS_DATA_DISK_CONFIG_SIZE, actual_data_disk_size);
   OK(GCTX.omt_->update_tenant_unit(new_unit));

   // macro cache used size too large, cannot shrink
   const int64_t macro_cache_size = tenant_disk_space_mgr->get_allocated_shared_macro_cache_size_nolock_();
   ASSERT_GT(macro_cache_size, new_unit.config_.resource_.data_disk_size_);
   OK(tenant_disk_space_mgr->alloc_file_size(macro_cache_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE));
   OK(tenant_disk_space_mgr->need_shrink_disk_size(is_need_shrink));
   ASSERT_TRUE(is_need_shrink);
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = true;
   for (int i = 0; i < 5; i++) {
     OK(tenant_disk_space_mgr->check_expand_disk_task_.check_shrink_disk_size_());
     OK(tenant_disk_space_mgr->check_expand_disk_task_.sync_tenant_data_disk_size_());
   }
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = false;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK 1", K(new_unit), K(actual_data_disk_size), K(min_shrinkable_disk_size), KPC(tenant_disk_space_mgr));
   ASSERT_EQ(tenant_disk_space_mgr->get_total_disk_size(), actual_data_disk_size);
   // directly shrink should failed
   ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, tenant_disk_space_mgr->shrink_data_disk_size(common::MB));

   // free macro cache, should shrink to the config data disk size
   OK(tenant_disk_space_mgr->free_file_size(macro_cache_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE));
   is_need_shrink = false;
   OK(tenant_disk_space_mgr->need_shrink_disk_size(is_need_shrink));
   ASSERT_TRUE(is_need_shrink);
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = true;
   for (int i = 0; i < 5; i++) {
     OK(tenant_disk_space_mgr->check_expand_disk_task_.check_shrink_disk_size_());
     OK(tenant_disk_space_mgr->check_expand_disk_task_.sync_tenant_data_disk_size_());
   }
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = false;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK 2", K(new_unit), K(actual_data_disk_size), K(min_shrinkable_disk_size), KPC(tenant_disk_space_mgr));
   ASSERT_LE(tenant_disk_space_mgr->get_total_disk_size(), new_unit.config_.resource_.data_disk_size_ + HIDDEN_SYS_DATA_DISK_CONFIG_SIZE);
   OK(tenant_disk_space_mgr->need_shrink_disk_size(is_need_shrink));
   ASSERT_FALSE(is_need_shrink);

   // cannot shrink disk size < update config data disk size < min_shrinkable_disk_size
   OK(tenant_disk_space_mgr->cal_min_shrinkable_disk_size(min_shrinkable_disk_size));
   OK(new_unit.assign(tenant->get_unit()));
   new_unit.config_.resource_.data_disk_size_ = tenant_disk_space_mgr->get_micro_cache_file_size() + tenant_disk_space_mgr->get_reserved_disk_size() + common::KB;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK 3", K(new_unit), K(actual_data_disk_size), K(min_shrinkable_disk_size), KPC(tenant_disk_space_mgr));
   ASSERT_LT(new_unit.config_.resource_.data_disk_size_ + HIDDEN_SYS_DATA_DISK_CONFIG_SIZE, tenant_disk_space_mgr->get_total_disk_size());
   ASSERT_LT(new_unit.config_.resource_.data_disk_size_ + HIDDEN_SYS_DATA_DISK_CONFIG_SIZE, min_shrinkable_disk_size);
   OK(GCTX.omt_->update_tenant_unit(new_unit));

   // config data disk size < min_shrinkable_disk_size, can only shrink to min_shrinkable_disk_size
   OK(tenant_disk_space_mgr->need_shrink_disk_size(is_need_shrink));
   ASSERT_TRUE(is_need_shrink);
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = true;
   for (int i = 0; i < 5; i++) {
     OK(tenant_disk_space_mgr->cal_min_shrinkable_disk_size(min_shrinkable_disk_size));
     OK(tenant_disk_space_mgr->check_expand_disk_task_.check_shrink_disk_size_());
     OK(tenant_disk_space_mgr->check_expand_disk_task_.sync_tenant_data_disk_size_());
   }
   tenant_disk_space_mgr->check_expand_disk_task_.is_inited_ = false;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK 4", K(min_shrinkable_disk_size), K(new_unit), KPC(tenant_disk_space_mgr));
   ASSERT_EQ(tenant_disk_space_mgr->get_total_disk_size(), MAX(min_shrinkable_disk_size, new_unit.config_.resource_.data_disk_size_ + HIDDEN_SYS_DATA_DISK_CONFIG_SIZE));

   // update config data disk size < cannot shrink disk size
   tenant_disk_space_mgr->unit_config_data_disk_size_ = tenant_disk_space_mgr->get_micro_cache_file_size() + tenant_disk_space_mgr->get_reserved_disk_size() - common::MB;
   LOG_INFO("TEST_AUTO_SHRINK_DISK_TASK 4", K(new_unit), K(actual_data_disk_size), K(min_shrinkable_disk_size), KPC(tenant_disk_space_mgr));
   ASSERT_EQ(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, tenant_disk_space_mgr->cal_min_shrinkable_disk_size(min_shrinkable_disk_size));
 }

 } // namespace storage
 } // namespace oceanbase

 int main(int argc, char **argv)
 {
   int ret = 0;
   system("rm -f ./test_auto_expand_disk.log*");
   OB_LOGGER.set_file_name("test_auto_expand_disk.log", true);
   OB_LOGGER.set_log_level("INFO");
   testing::InitGoogleTest(&argc, argv);
   return RUN_ALL_TESTS();
 }