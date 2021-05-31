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

#define USING_LOG_PREFIX SHARE

#include "share/backup/ob_backup_path.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace share;

TEST(ObBackupPathUtil, get_cluster_clog_backup_info_path)
{
  ObBackupPath path;
  ObClusterBackupDest dest;
  const char* bnackup_dest = "file:///root_backup_dir";
  const char* cluster_name = "cluster_name";
  const uint64_t cluster_id = 1;
  const uint64_t incarnation = 1;
  ASSERT_EQ(OB_SUCCESS, dest.set(bnackup_dest, cluster_name, cluster_id, incarnation));
  const char* expect_path = "file:///root_backup_dir/cluster_name/1/incarnation_1/cluster_clog_backup_info";
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_cluster_clog_backup_info_path(dest, path));
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));
}

TEST(ObBackupPathUtil, trim_right_backslash)
{
  ObBackupPath path;
  const char* backup_root_path = "oss://root_backup_dir//";
  const char* expect_path = "oss://root_backup_dir";
  ASSERT_EQ(OB_SUCCESS, path.init(backup_root_path));
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  path.reset();
  backup_root_path = "oss://root_backup_dir//affea1/";
  expect_path = "oss://root_backup_dir//affea1";
  ASSERT_EQ(OB_SUCCESS, path.init(backup_root_path));
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));
}

TEST(ObBackupPathUtil, base_data_path)
{
  ObBackupPath path;
  const char* backup_root_path = "oss://root_backup_dir";
  const char* cluster_name = "cluster_name";
  const uint64_t cluster_id = 1;
  const uint64_t incarnation = 1;
  const uint64_t tenant_id = 1002;
  const uint64_t full_backup_set_id = 8;
  const uint64_t inc_backup_set_id = 9;
  const uint64_t table_id = 1100611139453888LL;
  const uint64_t part_id = 1152921509170249728LL;
  const uint64_t task_id = 12345;
  const uint64_t sub_task_id = 33;
  const uint64_t retry_cnt0 = 0;
  const uint64_t retry_cnt1 = 1;
  const char* full_backup_set_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8";
  const char* inc_backup_set_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                    "backup_9";

  const char* meta_index_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                "backup_9/meta_index_file_12345";
  const char* meta_file_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                               "backup_9/meta_file_12345";

  const char* pg_data_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                             "data/1100611139453888/1152921509170249728";
  const char* sstable_macro_index_path0 = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                          "data/1100611139453888/1152921509170249728/sstable_macro_index_9";
  const char* sstable_macro_index_path1 = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                          "data/1100611139453888/1152921509170249728/sstable_macro_index_9.1";
  const char* macro_block_index_path0 = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                        "data/1100611139453888/1152921509170249728/macro_block_index_9";
  const char* macro_block_index_path1 = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                        "data/1100611139453888/1152921509170249728/macro_block_index_9.1";
  const char* macro_block_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
                                 "data/1100611139453888/1152921509170249728/macro_block_9.33";

  ObBackupBaseDataPathInfo test_backup_path_info;
  test_backup_path_info.dest_.set(backup_root_path, cluster_name, cluster_id, incarnation);
  test_backup_path_info.tenant_id_ = tenant_id;
  test_backup_path_info.full_backup_set_id_ = full_backup_set_id;
  test_backup_path_info.inc_backup_set_id_ = inc_backup_set_id;
  LOG_INFO("dump path", K(test_backup_path_info));
  ASSERT_EQ(true, test_backup_path_info.is_valid());

  const char* expect_path = NULL;
  int ret = 0;
  // backup set
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_tenant_data_full_backup_set_path(test_backup_path_info, path));
  expect_path = full_backup_set_path;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();

  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_tenant_data_inc_backup_set_path(test_backup_path_info, path));
  expect_path = inc_backup_set_path;
  ret = path.get_obstr().compare(expect_path);
  ASSERT_EQ(0, ret);
  if (0 != ret) {
    LOG_ERROR("dump path", K(path), K(expect_path));
  }
  path.reset();

  // meta
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_tenant_data_meta_index_path(test_backup_path_info, task_id, path));
  expect_path = meta_index_path;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();

  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_tenant_data_meta_file_path(test_backup_path_info, task_id, path));
  expect_path = meta_file_path;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();

  // data
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_tenant_pg_data_path(test_backup_path_info, table_id, part_id, path));
  expect_path = pg_data_path;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();

  ASSERT_EQ(OB_SUCCESS,
      ObBackupPathUtil::get_sstable_macro_index_path(test_backup_path_info, table_id, part_id, retry_cnt0, path));
  expect_path = sstable_macro_index_path0;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();
  ASSERT_EQ(OB_SUCCESS,
      ObBackupPathUtil::get_sstable_macro_index_path(test_backup_path_info, table_id, part_id, retry_cnt1, path));
  expect_path = sstable_macro_index_path1;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();

  ASSERT_EQ(OB_SUCCESS,
      ObBackupPathUtil::get_macro_block_index_path(test_backup_path_info, table_id, part_id, retry_cnt0, path));
  expect_path = macro_block_index_path0;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();
  ASSERT_EQ(OB_SUCCESS,
      ObBackupPathUtil::get_macro_block_index_path(test_backup_path_info, table_id, part_id, retry_cnt1, path));
  expect_path = macro_block_index_path1;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();

  ASSERT_EQ(OB_SUCCESS,
      ObBackupPathUtil::get_macro_block_file_path(
          test_backup_path_info, table_id, part_id, test_backup_path_info.inc_backup_set_id_, sub_task_id, path));
  expect_path = macro_block_path;
  ret = path.get_obstr().compare(expect_path);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, ret);
  path.reset();
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
