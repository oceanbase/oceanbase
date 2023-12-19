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
#include <string>
#include <gtest/gtest.h>
#include "test_backup.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#define private public
#define protected public

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::logservice;

namespace oceanbase {
namespace backup {

class TestBackupExternInfoMgr : public ::testing::Test {
public:
  TestBackupExternInfoMgr();
  virtual ~TestBackupExternInfoMgr();
  virtual void SetUp();
  virtual void TearDown();

private:
  void inner_init_();
  void clean_env_();
  void make_ls_meta_package_(ObBackupLSMetaInfo &ls_meta_info);

protected:
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(TestBackupExternInfoMgr);
};

TestBackupExternInfoMgr::TestBackupExternInfoMgr()
    : backup_dest_(), tenant_id_(OB_INVALID_ID), backup_set_desc_(), ls_id_(), test_dir_(""), test_dir_uri_("")
{}

TestBackupExternInfoMgr::~TestBackupExternInfoMgr()
{}

void TestBackupExternInfoMgr::SetUp()
{
  inner_init_();
}

void TestBackupExternInfoMgr::TearDown()
{}

void TestBackupExternInfoMgr::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_extern_info_mgr", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  clean_env_();
  ret = backup_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = util.mkdir(test_dir_uri_, backup_dest_.get_storage_info());
  EXPECT_EQ(OB_SUCCESS, ret);
  tenant_id_ = 1002;
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  ls_id_ = ObLSID(1001);
}

void TestBackupExternInfoMgr::clean_env_()
{
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

void TestBackupExternInfoMgr::make_ls_meta_package_(ObBackupLSMetaInfo &ls_meta_info)
{
  ls_meta_info.ls_meta_package_.ls_meta_.tenant_id_ = tenant_id_;
  ls_meta_info.ls_meta_package_.ls_meta_.ls_id_ = ls_id_;
  ls_meta_info.ls_meta_package_.ls_meta_.migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
  ls_meta_info.ls_meta_package_.ls_meta_.gc_state_ = LSGCState::NORMAL;
  ls_meta_info.ls_meta_package_.ls_meta_.restore_status_ = ObLSRestoreStatus(ObLSRestoreStatus::Status::NONE);
  ls_meta_info.ls_meta_package_.palf_meta_.prev_log_info_.lsn_.val_ = 1;
  ls_meta_info.ls_meta_package_.palf_meta_.curr_lsn_.val_ = 2;
}

static bool cmp_backup_ls_meta(const ObBackupLSMetaInfo &lhs, const ObBackupLSMetaInfo &rhs)
{
  return lhs.ls_meta_package_.ls_meta_.tenant_id_ == rhs.ls_meta_package_.ls_meta_.tenant_id_ &&
         lhs.ls_meta_package_.ls_meta_.ls_id_ == rhs.ls_meta_package_.ls_meta_.ls_id_ &&
         lhs.ls_meta_package_.ls_meta_.migration_status_ == rhs.ls_meta_package_.ls_meta_.migration_status_ &&
         lhs.ls_meta_package_.ls_meta_.gc_state_ == rhs.ls_meta_package_.ls_meta_.gc_state_ &&
         lhs.ls_meta_package_.ls_meta_.restore_status_ == rhs.ls_meta_package_.ls_meta_.restore_status_ &&
         lhs.ls_meta_package_.palf_meta_.prev_log_info_.lsn_.val_ ==
             rhs.ls_meta_package_.palf_meta_.prev_log_info_.lsn_.val_ &&
         lhs.ls_meta_package_.palf_meta_.curr_lsn_.val_ == rhs.ls_meta_package_.palf_meta_.curr_lsn_.val_;
}

TEST_F(TestBackupExternInfoMgr, read_write_ls_meta_info)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObBackupLSMetaInfo write_info;
  ObBackupLSMetaInfo read_info;
  make_ls_meta_package_(write_info);
  ObExternLSMetaMgr mgr;
  int64_t retry_id = 0;
  int64_t turn_id = 1;
  ret = mgr.init(backup_dest_, backup_set_desc_, ls_id_, turn_id, retry_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = mgr.write_ls_meta_info(write_info);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = mgr.read_ls_meta_info(read_info);
  EXPECT_EQ(OB_SUCCESS, ret);
  bool is_equal = cmp_backup_ls_meta(write_info, read_info);
  EXPECT_TRUE(is_equal);
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_extern_info_mgr.log*");
  OB_LOGGER.set_file_name("test_backup_extern_info_mgr.log", true);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
