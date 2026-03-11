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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include "test_backup.h"
#include "share/ob_device_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/incremental/sslog/ob_sslog_define.h"
#endif
#define private public
#define protected public

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::storage;

namespace oceanbase {
namespace backup {

#ifdef OB_BUILD_SHARED_STORAGE

class TestSSBackupExternInfoMgr : public ::testing::Test {
public:
  TestSSBackupExternInfoMgr();
  virtual ~TestSSBackupExternInfoMgr();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

private:
  void inner_init_();
  void clean_env_();
  void make_ss_ls_meta_info_(ObBackupSSLSMetaInfo &info, ObIAllocator &allocator);

protected:
  share::ObBackupDest backup_set_dest_;
  share::ObLSID ls_id_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(TestSSBackupExternInfoMgr);
};

TestSSBackupExternInfoMgr::TestSSBackupExternInfoMgr()
    : backup_set_dest_(), ls_id_(), test_dir_(""), test_dir_uri_("")
{}

TestSSBackupExternInfoMgr::~TestSSBackupExternInfoMgr()
{}

void TestSSBackupExternInfoMgr::SetUpTestCase()
{
  ObTenantBase *tenant_base = new share::ObTenantBase(OB_SYS_TENANT_ID);
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(OB_SYS_TENANT_ID, 0)) {
    malloc->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
  }
  tenant_base->init();
  ObTenantEnv::set_tenant(tenant_base);
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_base->set(io_service);
  ObTenantEnv::set_tenant(tenant_base);
}

void TestSSBackupExternInfoMgr::TearDownTestCase()
{
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
}

void TestSSBackupExternInfoMgr::SetUp()
{
  inner_init_();
}

void TestSSBackupExternInfoMgr::TearDown()
{
  clean_env_();
}

void TestSSBackupExternInfoMgr::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_),
                        "%s/test_ss_backup_extern_info_mgr", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  clean_env_();
  ret = backup_set_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = util.mkdir(test_dir_uri_, backup_set_dest_.get_storage_info());
  EXPECT_EQ(OB_SUCCESS, ret);
  ls_id_ = ObLSID(1001);
}

void TestSSBackupExternInfoMgr::clean_env_()
{
  char cmd[OB_MAX_URI_LENGTH + 16];
  (void)databuff_printf(cmd, sizeof(cmd), "rm -rf %s*", test_dir_);
  (void)system(cmd);
}

// Populate info with deterministic test strings, allocated from allocator.
void TestSSBackupExternInfoMgr::make_ss_ls_meta_info_(
    ObBackupSSLSMetaInfo &info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  info.raw_ls_meta_row_.type_ = sslog::ObSSLogMetaType::SSLOG_LS_META;
  ret = ob_write_string(allocator, ObString::make_string("test_meta_key"),
                        info.raw_ls_meta_row_.meta_key_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ob_write_string(allocator, ObString::make_string("test_meta_value"),
                        info.raw_ls_meta_row_.meta_value_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ob_write_string(allocator, ObString::make_string("test_extra_info"),
                        info.raw_ls_meta_row_.extra_info_);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Basic write → read round-trip: verify all fields survive serialization.
TEST_F(TestSSBackupExternInfoMgr, read_write_ls_meta_info)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t dest_id = 1;

  ObArenaAllocator write_allocator;
  ObBackupSSLSMetaInfo write_info;
  make_ss_ls_meta_info_(write_info, write_allocator);

  ObSSExternLSMetaMgr mgr;
  ret = mgr.init(backup_set_dest_, ls_id_, turn_id, retry_id, dest_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mgr.write_ls_meta_info(write_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator read_allocator;
  ObBackupSSLSMetaInfo read_info;
  ret = mgr.read_ls_meta_info(read_allocator, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(write_info.raw_ls_meta_row_.type_, read_info.raw_ls_meta_row_.type_);
  EXPECT_EQ(0, write_info.raw_ls_meta_row_.meta_key_.compare(
                   read_info.raw_ls_meta_row_.meta_key_));
  EXPECT_EQ(0, write_info.raw_ls_meta_row_.meta_value_.compare(
                   read_info.raw_ls_meta_row_.meta_value_));
  EXPECT_EQ(0, write_info.raw_ls_meta_row_.extra_info_.compare(
                   read_info.raw_ls_meta_row_.extra_info_));
}

// Shared allocator pattern: mirrors merge_ss_ls_meta_infos_ usage.
// A single allocator services multiple read_ls_meta_info calls; the results
// are pushed (shallow-copied) into an array and must remain valid while the
// allocator is alive.
TEST_F(TestSSBackupExternInfoMgr, allocator_lifetime_shared_reuse)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t dest_id = 1;

  // Write two different LS meta files (reusing same ls_id with different turn_ids).
  ObLSID ls_id_a(2001);
  ObLSID ls_id_b(2002);

  {
    ObArenaAllocator wa;
    ObBackupSSLSMetaInfo wi;
    make_ss_ls_meta_info_(wi, wa);

    ObSSExternLSMetaMgr mgr_a;
    ASSERT_EQ(OB_SUCCESS, mgr_a.init(backup_set_dest_, ls_id_a, turn_id, retry_id, dest_id));
    ASSERT_EQ(OB_SUCCESS, mgr_a.write_ls_meta_info(wi));

    ObSSExternLSMetaMgr mgr_b;
    ASSERT_EQ(OB_SUCCESS, mgr_b.init(backup_set_dest_, ls_id_b, turn_id, retry_id, dest_id));
    ASSERT_EQ(OB_SUCCESS, mgr_b.write_ls_meta_info(wi));
  }

  // Read both back using a single shared allocator, push into array.
  ObArenaAllocator shared_allocator;
  ObSArray<ObBackupSSLSMetaInfo> infos;

  {
    ObSSExternLSMetaMgr mgr_a;
    ASSERT_EQ(OB_SUCCESS, mgr_a.init(backup_set_dest_, ls_id_a, turn_id, retry_id, dest_id));
    ObBackupSSLSMetaInfo info_a;
    ASSERT_EQ(OB_SUCCESS, mgr_a.read_ls_meta_info(shared_allocator, info_a));
    ASSERT_EQ(OB_SUCCESS, infos.push_back(info_a));
  }
  {
    ObSSExternLSMetaMgr mgr_b;
    ASSERT_EQ(OB_SUCCESS, mgr_b.init(backup_set_dest_, ls_id_b, turn_id, retry_id, dest_id));
    ObBackupSSLSMetaInfo info_b;
    ASSERT_EQ(OB_SUCCESS, mgr_b.read_ls_meta_info(shared_allocator, info_b));
    ASSERT_EQ(OB_SUCCESS, infos.push_back(info_b));
  }

  // Verify both elements are accessible while shared_allocator is alive.
  ASSERT_EQ(2, infos.count());
  EXPECT_EQ(sslog::ObSSLogMetaType::SSLOG_LS_META, infos.at(0).raw_ls_meta_row_.type_);
  EXPECT_EQ(sslog::ObSSLogMetaType::SSLOG_LS_META, infos.at(1).raw_ls_meta_row_.type_);
  EXPECT_EQ(0, infos.at(0).raw_ls_meta_row_.meta_key_.compare(
                   ObString::make_string("test_meta_key")));
  EXPECT_EQ(0, infos.at(0).raw_ls_meta_row_.meta_value_.compare(
                   ObString::make_string("test_meta_value")));
  EXPECT_EQ(0, infos.at(0).raw_ls_meta_row_.extra_info_.compare(
                   ObString::make_string("test_extra_info")));
  EXPECT_EQ(0, infos.at(1).raw_ls_meta_row_.meta_key_.compare(
                   ObString::make_string("test_meta_key")));
  EXPECT_EQ(0, infos.at(1).raw_ls_meta_row_.meta_value_.compare(
                   ObString::make_string("test_meta_value")));
  EXPECT_EQ(0, infos.at(1).raw_ls_meta_row_.extra_info_.compare(
                   ObString::make_string("test_extra_info")));
}

// Reading a non-existent file must return OB_OBJECT_NOT_EXIST.
TEST_F(TestSSBackupExternInfoMgr, read_nonexistent_file)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t dest_id = 1;
  ObLSID nonexistent_ls(9999);

  ObSSExternLSMetaMgr mgr;
  ret = mgr.init(backup_set_dest_, nonexistent_ls, turn_id, retry_id, dest_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator allocator;
  ObBackupSSLSMetaInfo info;
  ret = mgr.read_ls_meta_info(allocator, info);
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ret);
}

#endif  // OB_BUILD_SHARED_STORAGE

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  (void)system("rm -f test_ss_backup_extern_info_mgr.log*");
  OB_LOGGER.set_file_name("test_ss_backup_extern_info_mgr.log", true);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
