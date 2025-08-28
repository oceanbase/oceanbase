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
#define private public
#define protected public

#include "share/backup/ob_backup_path.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace backup
{

class TestBackupPath : public ::testing::Test {
public:
  TestBackupPath();
  virtual ~TestBackupPath();
  virtual void SetUp();
  virtual void TearDown();

private:
  void inner_init_();

protected:
  ObBackupDest backup_dest_;
  int64_t task_id_;
  int64_t incarnation_;
  int64_t tenant_id_;
  int64_t dest_id_;
  ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t file_id_;
  common::ObInOutBandwidthThrottle throttle_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(TestBackupPath);
};

TestBackupPath::TestBackupPath()
    : backup_dest_(),
      incarnation_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(0),
      backup_set_desc_(),
      ls_id_(),
      backup_data_type_(),
      turn_id_(-1),
      retry_id_(-1),
      file_id_(-1),
      test_dir_(""),
      test_dir_uri_("")
{}

TestBackupPath::~TestBackupPath()
{}

void TestBackupPath::SetUp()
{
  inner_init_();
}

void TestBackupPath::inner_init_()
{
  int ret = OB_SUCCESS;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s", "obbackup");
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file:///%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = backup_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  task_id_ = 1;
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  backup_data_type_.set_user_data_backup();
  incarnation_ = 1;
  tenant_id_ = 1;
  dest_id_ = 1;
  ls_id_ = ObLSID(1001);
  turn_id_ = 1;
  retry_id_ = 0;
  file_id_ = 0;
}

void TestBackupPath::TearDown()
{
}

// TODO(yangyi.yyy): test backup tenant path

TEST_F(TestBackupPath, get_ls_macro_block_index_backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path1;
  
  ret = ObBackupPathUtilV_4_3_2::get_ls_macro_block_index_backup_path(
    backup_dest_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, backup_path1);

  EXPECT_EQ(OB_SUCCESS, ret);
  
  const char *str = "file:///obbackup/backup_set_1_full/logstream_1001/user_data_turn_1_retry_0/macro_block_index.obbak";

  int cmp_result = std::strcmp(backup_path1.get_ptr(), str);

  EXPECT_EQ(cmp_result, 0);
}

TEST_F(TestBackupPath, get_tenant_macro_block_index_backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path1;
  backup_data_type_.set_major_data_backup();
  ret = ObBackupPathUtilV_4_3_2::get_tenant_macro_block_index_backup_path(
    backup_dest_, backup_set_desc_, backup_data_type_, turn_id_, retry_id_, backup_path1);

  EXPECT_EQ(OB_SUCCESS, ret);

  const char *str = "file:///obbackup/backup_set_1_full/infos/major_data_info_turn_1/tenant_major_data_macro_block_index.0.obbak";

  int cmp_result = std::strcmp(backup_path1.get_ptr(), str);

  EXPECT_EQ(cmp_result, 0);
}

TEST_F(TestBackupPath, get_intermediate_layer_index_backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path1;

  ObBackupIntermediateTreeType index_tree_type = ObBackupIntermediateTreeType::BACKUP_INDEX_TREE;
  
  ret = ObBackupPathUtilV_4_3_2::get_intermediate_layer_index_backup_path(
    backup_dest_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, file_id_, index_tree_type, backup_path1);

  EXPECT_EQ(OB_SUCCESS, ret);
  
  const char *str1 = "file:///obbackup/backup_set_1_full/logstream_1001/user_data_turn_1_retry_0/index_tree.0.obbak";

  int cmp_result = std::strcmp(backup_path1.get_ptr(), str1);

  EXPECT_EQ(cmp_result, 0);

  share::ObBackupPath backup_path2;

  ObBackupIntermediateTreeType meta_tree_type = ObBackupIntermediateTreeType::BACKUP_META_TREE;
  
  ret = ObBackupPathUtilV_4_3_2::get_intermediate_layer_index_backup_path(
    backup_dest_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, file_id_, meta_tree_type, backup_path2);

  EXPECT_EQ(OB_SUCCESS, ret);
  
  const char *str2 = "file:///obbackup/backup_set_1_full/logstream_1001/user_data_turn_1_retry_0/meta_tree.0.obbak";

  cmp_result = std::strcmp(backup_path2.get_ptr(), str2);

  EXPECT_EQ(cmp_result, 0);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_path.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_path.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

