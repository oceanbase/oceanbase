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

#include <gtest/gtest.h>
#include "lib/container/ob_array_array.h"
#include "lib/container/ob_array_iterator.h"
#define private public
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/backup/ob_backup_struct.h"
#include "rootserver/ob_backup_data_clean.h"
#undef private
#include <algorithm>
#include "lib/hash/ob_hashmap.h"
using namespace oceanbase;
using namespace common;
using namespace share;

TEST(ObBackupUtils, check_is_tmp_file)
{
  bool is_tmp_file = false;
  ObString file_name_1 = "file_name.tmp.123456";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_1, is_tmp_file));
  ASSERT_TRUE(is_tmp_file);
  ObString file_name_2 = "file_name.TMP.123456";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_2, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_3 = "file_name.tmp";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_3, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_4 = "file_name.tmp.";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_4, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_5 = "file_name";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_5, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_6 = "file_name.123456";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_6, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
}

TEST(ObSimpleBackupSetPath, simple_path_disk)
{
  ObSimpleBackupSetPath simple_path;
  simple_path.backup_set_id_ = 1;
  simple_path.backup_dest_ = "file:///root_backup_dir";
  LOG_INFO("simple_path", K(simple_path.get_simple_path()), K(simple_path.get_storage_info()));
  ASSERT_EQ(OB_SUCCESS, simple_path.get_simple_path().case_compare("file:///root_backup_dir"));
  ASSERT_EQ(OB_SUCCESS, simple_path.get_storage_info().case_compare(""));
}

TEST(ObSimpleBackupSetPath, simple_path_oss)
{
  ObSimpleBackupSetPath simple_path;
  simple_path.backup_set_id_ = 1;
  simple_path.backup_dest_ =
      "oss://backup_dir/?host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222";
  LOG_INFO("simple_path", K(simple_path.get_simple_path()), K(simple_path.get_storage_info()));
  ASSERT_EQ(OB_SUCCESS, simple_path.get_simple_path().case_compare("oss://backup_dir/"));
  ASSERT_EQ(OB_SUCCESS,
      simple_path.get_storage_info().case_compare(
          "host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222"));
}

TEST(ObBackupDest, disk)
{
  const char* backup_test = "file:///root_backup_dir";
  ObBackupDest dest;
  ASSERT_EQ(OB_SUCCESS, dest.set(backup_test));
  LOG_INFO("dump backup dest", K(dest));
  ASSERT_EQ(0, strcmp(dest.root_path_, "file:///root_backup_dir"));
  ASSERT_EQ(0, strcmp(dest.storage_info_, ""));
}

TEST(ObBackupDest, oss)
{
  const char* backup_test =
      "oss://backup_dir/?host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222";
  ObBackupDest dest;
  ASSERT_EQ(OB_SUCCESS, dest.set(backup_test));
  LOG_INFO("dump backup dest", K(dest));
  ASSERT_EQ(0, strcmp(dest.root_path_, "oss://backup_dir/"));
  ASSERT_EQ(0, strcmp(dest.storage_info_, "host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222"));
}

int check_backup_dest_opt(const ObBackupDestOpt& opt, int64_t log_archive_checkpoint_interval, int64_t recovery_window,
    int64_t piece_switch_interval, int64_t backup_copies, bool auto_delete_obsolete_backup,
    bool auto_touch_reserved_backup)
{
  int ret = OB_SUCCESS;

  if (opt.log_archive_checkpoint_interval_ != log_archive_checkpoint_interval) {
    ret = OB_ERR_SYS;
    LOG_WARN("not match", K(log_archive_checkpoint_interval), K(opt));
  } else if (opt.recovery_window_ != recovery_window) {
    ret = OB_ERR_SYS;
    LOG_WARN("not match", K(recovery_window), K(opt));
  } else if (opt.piece_switch_interval_ != piece_switch_interval) {
    ret = OB_ERR_SYS;
    LOG_WARN("not match", K(piece_switch_interval), K(opt));
  } else if (opt.backup_copies_ != backup_copies) {
    ret = OB_ERR_SYS;
    LOG_WARN("not match", K(backup_copies), K(opt));
  } else if (opt.auto_delete_obsolete_backup_ != auto_delete_obsolete_backup) {
    ret = OB_ERR_SYS;
    LOG_WARN("not match", K(auto_delete_obsolete_backup), K(opt));
  } else if (opt.auto_touch_reserved_backup_ != auto_touch_reserved_backup) {
    ret = OB_ERR_SYS;
    LOG_WARN("not match", K(auto_touch_reserved_backup), K(opt));
  }

  return ret;
}

TEST(ObBackupDestOpt, normal)
{
  const char* backup_dest_opt_str =
      "recovery_window=7d&auto_delete_obsolete_backup=true&backup_copies=2&log_archive_piece_switch_interval=1d";
  const char* backup_backup_dest_opt_str = "recovery_window=14d&auto_delete_obsolete_backup=true";
  ObBackupDestOpt backup_dest_opt;
  ObBackupDestOpt backup_backup_dest_opt;
  const bool global_auto_delete = false;
  const int64_t global_recovery_window = 0;
  const int64_t global_checkpoint_interval = 120 * 1000LL * 1000;  // default config 120s
  const int64_t global_backup_checkpoint_interval = 0;

  ASSERT_EQ(OB_SUCCESS,
      backup_dest_opt.init(false /*is_backup_backup*/,
          backup_dest_opt_str,
          global_auto_delete,
          global_recovery_window,
          global_checkpoint_interval,
          false));
  ASSERT_EQ(OB_SUCCESS,
      check_backup_dest_opt(backup_dest_opt,
          global_checkpoint_interval,
          7 * 24 * 3600LL * 1000 * 1000 /*recovery_window*/,
          1 * 24 * 3600LL * 1000 * 1000 /*piece_switch_interval*/,
          2 /*backup_copies*/,
          true /*auto delete*/,
          false /*auto touch*/));

  ASSERT_EQ(OB_SUCCESS,
      backup_backup_dest_opt.init(true /*is_backup_backup*/,
          backup_backup_dest_opt_str,
          global_auto_delete,
          global_recovery_window,
          global_backup_checkpoint_interval,
          false));
  ASSERT_EQ(OB_SUCCESS,
      check_backup_dest_opt(backup_backup_dest_opt,
          0,
          14 * 24 * 3600LL * 1000 * 1000 /*recovery_window*/,
          0 /*piece_switch_interval*/,
          0 /*backup_copies*/,
          true /*auto delete*/,
          false /*auto touch*/));
}

TEST(ObBackupDestOpt, backup_dest_all)
{
  ObBackupDestOpt backup_dest_opt;

  ASSERT_EQ(OB_SUCCESS,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d&auto_delete_obsolete_backup=true&log_archive_piece_"
          "switch_interval=3d&backup_copies=3",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));

  ASSERT_EQ(OB_SUCCESS,
      check_backup_dest_opt(backup_dest_opt,
          10 * 60 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          30 * 24 * 3600LL * 1000 * 1000 /*recovery_window*/,
          3 * 24 * 3600LL * 1000 * 1000 /*piece_switch_interval*/,
          3 /*backup_copies*/,
          true /*auto delete*/,
          false /*auto touch*/));
}

TEST(ObBackupDestOpt, backup_dest_empty)
{
  ObBackupDestOpt backup_dest_opt;

  ASSERT_EQ(OB_SUCCESS,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));

  ASSERT_EQ(OB_SUCCESS,
      check_backup_dest_opt(backup_dest_opt,
          2 * 60 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          0 /*recovery_window*/,
          0 /*piece_switch_interval*/,
          0 /*backup_copies*/,
          0 /*auto delete*/,
          false /*auto touch*/));
}

TEST(ObBackupDestOpt, backup_dest_invalid_format)
{
  ObBackupDestOpt backup_dest_opt;

  // invalid key checkpoint_interval
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "checkpoint_interval=10m&recovery_window=30d&auto_delete_obsolete_backup=true&log_archive_piece_switch_"
          "interval=3d&backup_copies=3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));

  // has blank space
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d "
          "&auto_delete_obsolete_backup=true&log_archive_piece_switch_interval=3d&backup_copies=3&auto_update_reserved_"
          "backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));

  // recovery_window has no value
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=&auto_delete_obsolete_backup=true&log_archive_piece_"
          "switch_interval=3d&backup_copies=3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));

  // recovery_window has invalid value
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=a&auto_delete_obsolete_backup=true&log_archive_piece_"
          "switch_interval=3d&backup_copies=3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));

  // auto_delete_obsolete_backup has invalid value
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=7d&auto_delete_obsolete_backup=a&log_archive_piece_"
          "switch_interval=3d&backup_copies=3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObBackupDestOpt, backup_dest_invalid_delete_and_touch)
{
  ObBackupDestOpt backup_dest_opt;
  ASSERT_EQ(OB_NOT_SUPPORTED,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d&auto_delete_obsolete_backup=true&log_archive_piece_"
          "switch_interval=3d&backup_copies=3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObBackupDestOpt, backup_dest_invalid_copies)
{
  ObBackupDestOpt backup_dest_opt;
  ASSERT_EQ(OB_NOT_SUPPORTED,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d&auto_delete_obsolete_backup=true&log_archive_piece_"
          "switch_interval=3d&backup_copies=-1",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObBackupDestOpt, backup_dest_invalid_recovery_window)
{
  ObBackupDestOpt backup_dest_opt;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=-1d&log_archive_piece_switch_interval=3d&backup_copies="
          "3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObBackupDestOpt, backup_dest_invalid_piece_switch)
{
  ObBackupDestOpt backup_dest_opt;
  ASSERT_EQ(OB_NOT_SUPPORTED,
      backup_dest_opt.init(false /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d&log_archive_piece_switch_interval=1s&backup_copies="
          "3&auto_update_reserved_backup_timestamp=true",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObBackupDestOpt, backup_backup_dest_invalid_copies)
{
  ObBackupDestOpt backup_dest_opt;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(true /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d&auto_delete_obsolete_backup=true&backup_copies=1",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObBackupDestOpt, backup_backup_dest_invalid_piece_switch)
{
  ObBackupDestOpt backup_dest_opt;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      backup_dest_opt.init(true /*is_backup_backup*/,
          "log_archive_checkpoint_interval=10m&recovery_window=30d&auto_delete_obsolete_backup=true&log_archive_piece_"
          "switch_interval=3d&backup_copies=0",
          false /*global_auto_delete*/,
          0 /*global_recovery_window*/,
          120 * 1000LL * 1000LL /*global_checkpoint_interval*/,
          false));
}

TEST(ObLogArchiveBackupInfoMgr, log_archive_info_cmp)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr::CompareBackupPieceInfo cmp(ret);
  ObBackupPieceInfo piece_info;
  ObArray<ObBackupPieceInfo> piece_infos;
  ObArray<ObBackupPieceInfo> piece_infos_copy1;
  ObArray<ObBackupPieceInfo> piece_infos_copy2;
  ObArray<ObBackupPieceInfo> piece_infos_copy3;

  ret = piece_info.backup_dest_.assign("file::///oceanbase/");

  ASSERT_EQ(OB_SUCCESS, ret);
  piece_info.checkpoint_ts_ = 1621652028364222;
  piece_info.compatible_ = ObTenantLogArchiveStatus::COMPATIBLE_VERSION_2;
  piece_info.create_date_ = 20210523;
  piece_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
  piece_info.key_.tenant_id_ = 1;
  piece_info.max_ts_ = 1621652028364222;
  piece_info.start_piece_id_ = 0;
  piece_info.start_ts_ = 0;
  piece_info.status_ = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE;

  //mock array with same incarnation, round, copy id
  for (int64_t i = 0; OB_SUCC(ret) && i < 10; ++i) {
    piece_info.key_.copy_id_ = 0;
    piece_info.key_.incarnation_ = 1;
    piece_info.key_.round_id_ = 1;
    piece_info.key_.backup_piece_id_ = i;
    ret = piece_infos.push_back(piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  std::sort(piece_infos.begin(), piece_infos.end(), cmp);

  for (int64_t i = 0; OB_SUCC(ret) && i < piece_infos.count(); ++i) {
    const ObBackupPieceInfo &tmp_piece_info = piece_infos.at(i);
    ASSERT_EQ(tmp_piece_info.key_.backup_piece_id_, i);
  }
  piece_infos.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < 10; ++i) {
    piece_info.key_.copy_id_ = 1;
    piece_info.key_.incarnation_ = 1;
    piece_info.key_.round_id_ = 1+i;
    piece_info.key_.backup_piece_id_ = i;
    ret = piece_infos_copy1.push_back(piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < 9; ++i) {
    piece_info.key_.copy_id_ = 2;
    piece_info.key_.incarnation_ = 1;
    piece_info.key_.round_id_ = 1+i;
    piece_info.key_.backup_piece_id_ = i;
    ret = piece_infos_copy2.push_back(piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }


  for (int64_t i = 0; OB_SUCC(ret) && i < 7; ++i) {
    piece_info.key_.copy_id_ = 3;
    piece_info.key_.incarnation_ = 1;
    piece_info.key_.round_id_ = 1+i;
    piece_info.key_.backup_piece_id_ = i;
    ret = piece_infos_copy3.push_back(piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  //add to piece infos
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_infos_copy3.count(); ++i) {
    const ObBackupPieceInfo &tmp_piece_info = piece_infos_copy3.at(i);
    ret = piece_infos.push_back(tmp_piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < piece_infos_copy1.count(); ++i) {
    const ObBackupPieceInfo &tmp_piece_info = piece_infos_copy1.at(i);
    ret = piece_infos.push_back(tmp_piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }


  for (int64_t i = 0; OB_SUCC(ret) && i < piece_infos_copy2.count(); ++i) {
    const ObBackupPieceInfo &tmp_piece_info = piece_infos_copy2.at(i);
    ret = piece_infos.push_back(tmp_piece_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  std::sort(piece_infos.begin(), piece_infos.end(), cmp);

  for (int64_t i = 0; OB_SUCC(ret) && i < 10; ++i) {
    const ObBackupPieceInfo &tmp_piece_info = piece_infos.at(i);
    ASSERT_EQ(tmp_piece_info.key_.backup_piece_id_, i);
    ASSERT_EQ(tmp_piece_info.key_.round_id_, i + 1);
    ASSERT_EQ(tmp_piece_info.key_.copy_id_, 1);
  }

  ret = piece_infos.push_back(piece_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(piece_infos.begin(), piece_infos.end(), cmp);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

}


TEST(ObLogArchiveBackupInfoMgr, log_archive_info_cmp2)
{
  int ret = OB_SUCCESS;
  ObArray<ObLogArchiveBackupInfo> log_infos;
  ObLogArchiveBackupInfo log_info;
  log_info.status_.start_ts_ = 1;
  log_info.status_.checkpoint_ts_ = 0;
  log_info.status_.round_ = 1;
  ret = log_infos.push_back(log_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  log_info.reset();
  log_info.status_.start_ts_ = 1;
  log_info.status_.checkpoint_ts_ = 0;
  log_info.status_.round_ = 2;
  ret = log_infos.push_back(log_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  log_info.reset();
  log_info.status_.start_ts_ = 1;
  log_info.status_.checkpoint_ts_ = 0;
  log_info.status_.round_ = 3;
  ret = log_infos.push_back(log_info);
  ASSERT_EQ(OB_SUCCESS, ret);


  typedef ObArray<ObLogArchiveBackupInfo>::iterator ArrayIter;
  rootserver::ObBackupDataClean::CompareLogArchiveSnapshotVersion cmp;
  ArrayIter iter = std::lower_bound(log_infos.begin(),
                                    log_infos.end(),
                                    1,
                                    cmp);
  if (iter == log_infos.end()) {
    --iter;
  } else if (iter != log_infos.begin()
      && iter->status_.start_ts_ > 1) {
    --iter;
  }
  log_info = *iter;
  LOG_INFO("log info", K(log_info));

}


TEST(ObBackupDataClean, duplicate_task)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObArray<ObTenantBackupTaskInfo> task_infos;

  //mock task info
  //array backup set id is : 1, 2, 1, 2
  task_info.tenant_id_ = OB_SYS_TENANT_ID;
  task_info.incarnation_ = 1;
  task_info.backup_set_id_ = 1;
  task_info.copy_id_ = 0;
  task_info.status_ = ObTenantBackupTaskInfo::FINISH;
  ret = task_infos.push_back(task_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_info.reset();
  task_info.tenant_id_ = OB_SYS_TENANT_ID;
  task_info.incarnation_ = 1;
  task_info.backup_set_id_ = 2;
  ret = task_infos.push_back(task_info);
  task_info.copy_id_ = 0;
  task_info.status_ = ObTenantBackupTaskInfo::FINISH;
  ASSERT_EQ(OB_SUCCESS, ret);
  task_info.reset();
  task_info.tenant_id_ = OB_SYS_TENANT_ID;
  task_info.incarnation_ = 1;
  task_info.backup_set_id_ = 1;
  task_info.copy_id_ = 0;
  task_info.status_ = ObTenantBackupTaskInfo::DOING;
  ret = task_infos.push_back(task_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_info.reset();
  task_info.tenant_id_ = OB_SYS_TENANT_ID;
  task_info.incarnation_ = 1;
  task_info.backup_set_id_ = 2;
  task_info.copy_id_ = 0;
  task_info.status_ = ObTenantBackupTaskInfo::DOING;
  ret = task_infos.push_back(task_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  rootserver::ObBackupDataClean data_clean;
  data_clean.is_inited_ = true;
  ret = data_clean.duplicate_task_info(task_infos);
  ASSERT_EQ(2, task_infos.count());
  ASSERT_EQ(1, task_infos.at(0).backup_set_id_);
  ASSERT_EQ(ObTenantBackupTaskInfo::DOING, task_infos.at(0).status_);
  ASSERT_EQ(2, task_infos.at(1).backup_set_id_);
  ASSERT_EQ(ObTenantBackupTaskInfo::DOING, task_infos.at(1).status_);

}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
