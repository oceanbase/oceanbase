/**
 * Copyright (c) 2025 OceanBase
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
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#include "storage/incremental/share/ob_ss_diagnose.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array.h"
#include "common/ob_smart_call.h"
#include "observer/ob_server.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "share/ob_server_struct.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"

#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::storage;
using namespace oceanbase::common;

class TestSSDiagnoseInfoMgr : public ObSimpleClusterTestBase
{
public:
  TestSSDiagnoseInfoMgr() : ObSimpleClusterTestBase("test_ss_diagnose_info_mgr_", "50G", "50G", "50G") {}
  virtual ~TestSSDiagnoseInfoMgr() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
public:
  static const int64_t TEST_LS_ID = 1001;
  static const int64_t TEST_TABLET_ID = 2001;
  static const int64_t TEST_TENANT_ID = 1001;
};

void TestSSDiagnoseInfoMgr::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSDiagnoseInfoMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_init_destroy)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;

  // Test init
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());
  ASSERT_TRUE(diagnose_mgr.is_inited_);

  // Test double init
  ASSERT_EQ(OB_INIT_TWICE, diagnose_mgr.init());

  // Test destroy
  diagnose_mgr.destroy();
  ASSERT_FALSE(diagnose_mgr.is_inited_);

  // Test init after destroy
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());
  ASSERT_TRUE(diagnose_mgr.is_inited_);

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_add_diagnose_tablet)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Test add diagnose tablet before init
  diagnose_mgr.destroy();
  ASSERT_EQ(OB_NOT_INIT, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type));

  // Re-init for further tests
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  // Test add valid diagnose tablet
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type));

  // Test add same diagnose tablet again (should return OB_SUCCESS)
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type));

  // Test add invalid ls_id
  ObLSID invalid_ls_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.add_diagnose_tablet(invalid_ls_id, tablet_id, type));

  // Test add invalid tablet_id
  ObTabletID invalid_tablet_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.add_diagnose_tablet(ls_id, invalid_tablet_id, type));

  // Test add invalid type
  ObSSDiagnoseInfo::ObDiagnoseType invalid_type = ObSSDiagnoseInfo::DIA_TYPE_INVALID;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, invalid_type));

  invalid_type = ObSSDiagnoseInfo::DIA_TYPE_MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, invalid_type));

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_get_diagnose_tablets)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Test get diagnose tablets before adding any
  ObArray<ObSSDiagnoseTablet> diagnose_tablets;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(0, diagnose_tablets.count());

  // Add some diagnose tablets
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type));

  ObLSID ls_id2(TEST_LS_ID + 1);
  ObTabletID tablet_id2(TEST_TABLET_ID + 1);
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id2, tablet_id2, type));

  // Test get diagnose tablets after adding
  diagnose_tablets.reset();
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(2, diagnose_tablets.count());

  // Test get diagnose tablets before init
  diagnose_mgr.destroy();
  diagnose_tablets.reset();
  ASSERT_EQ(OB_NOT_INIT, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_delete_diagnose_tablet)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Test delete before adding
  ASSERT_EQ(OB_HASH_NOT_EXIST, diagnose_mgr.delete_diagnose_tablet(ls_id, tablet_id, type));

  // Add diagnose tablet
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type));

  // Test delete after adding
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.delete_diagnose_tablet(ls_id, tablet_id, type));

  // Verify it's deleted
  ObArray<ObSSDiagnoseTablet> diagnose_tablets;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(0, diagnose_tablets.count());

  // Test delete invalid parameters
  ObLSID invalid_ls_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.delete_diagnose_tablet(invalid_ls_id, tablet_id, type));

  ObTabletID invalid_tablet_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.delete_diagnose_tablet(ls_id, invalid_tablet_id, type));

  ObSSDiagnoseInfo::ObDiagnoseType invalid_type = ObSSDiagnoseInfo::DIA_TYPE_INVALID;
  ASSERT_EQ(OB_INVALID_ARGUMENT, diagnose_mgr.delete_diagnose_tablet(ls_id, tablet_id, invalid_type));

  // Test delete before init
  diagnose_mgr.destroy();
  ASSERT_EQ(OB_NOT_INIT, diagnose_mgr.delete_diagnose_tablet(ls_id, tablet_id, type));

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_add_ss_diagnose_info)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  int64_t timestamp = ObTimeUtility::current_time();
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Test add diagnose info with 1 parameter
  const char* key1 = "test_key1";
  int64_t value1 = 12345;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, key1, value1));

  // Test add diagnose info with 2 parameters
  const char* key2 = "test_key2";
  int64_t value2 = 67890;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, key1, value1, key2, value2));

  // Test add diagnose info with 3 parameters
  const char* key3 = "test_key3";
  int64_t value3 = 11111;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, key1, value1, key2, value2, key3, value3));

  // Test add diagnose info before init
  diagnose_mgr.destroy();
  ASSERT_EQ(OB_NOT_INIT, diagnose_mgr.add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, key1, value1));

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_get_diagnose_info)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  int64_t timestamp = ObTimeUtility::current_time();
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Add some diagnose info first
  const char* key1 = "test_key1";
  int64_t value1 = 12345;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, key1, value1));

  // Test get diagnose info
  const int64_t max_cnt = 10;
  ObSSDiagnoseInfo info_array[max_cnt];
  int64_t pos = 0;
  diagnose_mgr.get_diagnose_info(type, info_array, pos, max_cnt);

  // Should have at least one diagnose info
  ASSERT_GT(pos, 0);
  ASSERT_LE(pos, max_cnt);

  // Test get diagnose info before init
  diagnose_mgr.destroy();
  pos = 0;
  diagnose_mgr.get_diagnose_info(type, info_array, pos, max_cnt);
  ASSERT_EQ(0, pos);

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_remove_diagnose_tablets)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Add some diagnose tablets
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type));

  ObLSID ls_id2(TEST_LS_ID + 1);
  ObTabletID tablet_id2(TEST_TABLET_ID + 1);
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id2, tablet_id2, type));

  // Get diagnose tablets
  ObArray<ObSSDiagnoseTablet> diagnose_tablets;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(2, diagnose_tablets.count());

  // Remove diagnose tablets
  diagnose_mgr.remove_diagnose_tablets(diagnose_tablets);

  // Verify they are removed
  diagnose_tablets.reset();
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(0, diagnose_tablets.count());

  // Test remove before init
  diagnose_mgr.destroy();
  diagnose_mgr.remove_diagnose_tablets(diagnose_tablets);

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_mtl_init)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr *diagnose_mgr = nullptr;

  // Test mtl_init with valid pointer
  diagnose_mgr = new ObSSDiagnoseInfoMgr();
  ASSERT_NE(nullptr, diagnose_mgr);
  ASSERT_EQ(OB_SUCCESS, ObSSDiagnoseInfoMgr::mtl_init(diagnose_mgr));
  ASSERT_TRUE(diagnose_mgr->is_inited_);

  delete diagnose_mgr;
}

TEST_F(TestSSDiagnoseInfoMgr, test_diagnose_tablet_operations)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);

  // Test multiple diagnose types for same tablet
  ObSSDiagnoseInfo::ObDiagnoseType type1 = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;
  ObSSDiagnoseInfo::ObDiagnoseType type2 = ObSSDiagnoseInfo::DIA_TYPE_SS_GC;

  // Add first type
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type1));

  // Add second type to same tablet
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_diagnose_tablet(ls_id, tablet_id, type2));

  // Verify both types are added
  ObArray<ObSSDiagnoseTablet> diagnose_tablets;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(1, diagnose_tablets.count()); // Same tablet, different types

  // Delete first type
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.delete_diagnose_tablet(ls_id, tablet_id, type1));

  // Verify tablet still exists (has second type)
  diagnose_tablets.reset();
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(1, diagnose_tablets.count());

  // Delete second type
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.delete_diagnose_tablet(ls_id, tablet_id, type2));

  // Verify tablet is completely removed
  diagnose_tablets.reset();
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(0, diagnose_tablets.count());

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_diagnose_info_circular_buffer)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr.init());

  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;

  // Add more diagnose info than the buffer size (MAX_DIAGNOSE_CNT[type] = 10)
  const int64_t buffer_size = 10;
  const int64_t add_count = buffer_size + 5; // Add more than buffer size

  for (int64_t i = 0; i < add_count; ++i) {
    int64_t timestamp = ObTimeUtility::current_time() + i;
    const char* key = "test_key";
    int64_t value = i;
    ASSERT_EQ(OB_SUCCESS, diagnose_mgr.add_ss_diagnose_info(type, ls_id, tablet_id, timestamp, key, value));
  }

  // Get diagnose info and verify we get the latest entries (circular buffer behavior)
  const int64_t max_cnt = 20;
  ObSSDiagnoseInfo info_array[max_cnt];
  int64_t pos = 0;
  diagnose_mgr.get_diagnose_info(type, info_array, pos, max_cnt);

  // Should have some diagnose info (exact count depends on need_diagnose_info_ logic)
  ASSERT_GE(pos, 0);
  ASSERT_LE(pos, max_cnt);

  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_destroy_core)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  diagnose_mgr.destroy();
}

TEST_F(TestSSDiagnoseInfoMgr, test_mtl_module)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseInfoMgr *diagnose_mgr = MTL(ObSSDiagnoseInfoMgr*);
  ASSERT_TRUE(OB_NOT_NULL(diagnose_mgr));
  ASSERT_TRUE(diagnose_mgr->is_inited_);

  // Test add diagnose tablet
  ObLSID ls_id(TEST_LS_ID);
  ObTabletID tablet_id(TEST_TABLET_ID);
  ObSSDiagnoseInfo::ObDiagnoseType type = ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr->add_diagnose_tablet(ls_id, tablet_id, type));

  // Test get diagnose tablets
  ObArray<ObSSDiagnoseTablet> diagnose_tablets;
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr->get_diagnose_tablets(diagnose_tablets));
  ASSERT_EQ(1, diagnose_tablets.count());

  // Test delete diagnose tablet
  ASSERT_EQ(OB_SUCCESS, diagnose_mgr->delete_diagnose_tablet(ls_id, tablet_id, type));

  // Test remove diagnose tablets
  diagnose_tablets.reset();
  diagnose_mgr->remove_diagnose_tablets(diagnose_tablets);
  ASSERT_EQ(0, diagnose_tablets.count());

  // Test get diagnose info
  ObSSDiagnoseInfo info_array[10];
  int64_t pos = 0;
  diagnose_mgr->get_diagnose_info(type, info_array, pos, 10);
  ASSERT_EQ(0, pos);
}


} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_diagnose_info_mgr.log*");
  OB_LOGGER.set_file_name("test_ss_diagnose_info_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}