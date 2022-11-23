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

TEST(ObBackupPathUtil, trim_right_backslash)
{
  ObBackupPath path;
  const char *backup_root_path = "oss://root_backup_dir//";
  const char *expect_path = "oss://root_backup_dir";
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
  const char *backup_root_path = "oss://root_backup_dir?host=xxx&access_id=xxx&access_key=xxx";
  const char *cluster_name = "cluster_name";
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
  const char *full_backup_set_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8";
  const char *inc_backup_set_path  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "backup_9";

  const char *meta_index_path  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "backup_9/meta_index_file_12345";
  const char *meta_file_path  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "backup_9/meta_file_12345";

  const char *pg_data_path  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "data/1100611139453888/1152921509170249728";
  const char *sstable_macro_index_path0  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "data/1100611139453888/1152921509170249728/sstable_macro_index_9";
  const char *sstable_macro_index_path1  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "data/1100611139453888/1152921509170249728/sstable_macro_index_9.1";
  const char *macro_block_index_path0  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "data/1100611139453888/1152921509170249728/macro_block_index_9";
  const char *macro_block_index_path1  = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "data/1100611139453888/1152921509170249728/macro_block_index_9.1";
  const char *macro_block_path = "oss://root_backup_dir/cluster_name/1/incarnation_1/1002/data/backup_set_8/"
      "data/1100611139453888/1152921509170249728/macro_block_9.33";
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
