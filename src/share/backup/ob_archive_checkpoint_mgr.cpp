// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE
#include "ob_archive_checkpoint_mgr.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_path.h"

using namespace oceanbase;
using namespace share;

bool ObGetMaxCheckpointOp::is_valid() const
{
  return max_checkpoint_scn_ >= 0
      && file_name_ != nullptr
      && type_ <= ObBackupFileSuffix::BACKUP
      && type_ >= ObBackupFileSuffix::NONE;
}

int ObGetMaxCheckpointOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name) || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, d_name is null", K(ret));
  } else {
    uint64_t checkpoint_scn = 0;
    if (OB_FAIL(ObBackupPath::parse_checkpoint(entry->d_name, file_name_, type_, checkpoint_scn))) {
      OB_LOG(WARN, "failed to get checkpoint scn", K(ret), KP(entry->d_name));
    } else if (checkpoint_scn > max_checkpoint_scn_) {
      max_checkpoint_scn_ = checkpoint_scn;
    }
  }
  return ret;
}

bool ObDelHisCheckpointFileOp::is_valid() const
{
  return checkpoint_scn_ >= 0
      && !path_.is_empty()
      && file_name_ != nullptr
      && type_ <= ObBackupFileSuffix::BACKUP
      && type_ >= ObBackupFileSuffix::NONE
      && storage_info_ != nullptr;
}

int ObDelHisCheckpointFileOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name) || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, d_name is null", K(ret));
  } else {
    uint64_t checkpoint_scn = 0;
    ObBackupPath full_path = path_;
    ObBackupIoAdapter io_util;
    if (OB_FAIL(ObBackupPath::parse_checkpoint(entry->d_name, file_name_, type_, checkpoint_scn))) {
      OB_LOG(WARN, "failed to get checkpoint scn", K(ret), KP(entry->d_name));
    } else if (checkpoint_scn >= checkpoint_scn_ || 0 == checkpoint_scn) {
      //do nothing
    } else if (OB_FAIL(full_path.join_checkpoint_info_file(file_name_, checkpoint_scn, type_))) {
      OB_LOG(WARN, "failed to set full path for del file",
          K(ret), K(checkpoint_scn), KP(file_name_), K(type_));
    } else if (OB_FAIL(io_util.del_file(full_path.get_ptr(), storage_info_))) {
      OB_LOG(WARN, "failed to delete file", K(ret), K(full_path));
    }
  }
  return ret;
}

/**
 * ------------------------------ObArchiveCheckpointMgr---------------------
 */
int ObArchiveCheckpointMgr::init(
    const ObBackupPath &path,
    const char *file_name,
    const ObBackupFileSuffix &type,
    const ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObArchiveCheckpointMgr init twice", K(ret));
  } else if (path.is_empty()
      || OB_ISNULL(file_name)
      || type > ObBackupFileSuffix::BACKUP
      || type < ObBackupFileSuffix::NONE
      || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(path), KP(file_name), K(type));
  } else {
    path_ = path;
    file_name_ = file_name;
    type_ = type;
    storage_info_ = storage_info;
    is_inited_ = true;
  }
  return ret;
}

bool ObArchiveCheckpointMgr::is_valid() const
{
  return !path_.is_empty()
      && type_ <= ObBackupFileSuffix::BACKUP
      && type_ >= ObBackupFileSuffix::NONE
      && file_name_ != nullptr
      && storage_info_ != nullptr;
}

void ObArchiveCheckpointMgr::reset()
{
  is_inited_ = false;
  path_.reset();
  file_name_ = nullptr;
  type_ = ObBackupFileSuffix::NONE;
  storage_info_ = nullptr;
}

int ObArchiveCheckpointMgr::check_is_tagging_(const ObBackupStorageInfo *storage_info, bool &is_tagging) const
{
  int ret = OB_SUCCESS;
  is_tagging = false;
  if (OB_STORAGE_OSS == storage_info_ -> device_type_) {
    //TODO(zhixing.yh) Adapt the analytic interface in storage_info
  }
  return ret;
}

int ObArchiveCheckpointMgr::write(const uint64_t checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t max_checkpoint_scn = 0;
  ObBackupPath full_path = path_; //checkpoint scn file path
  ObBackupPath dir_path = path_; //checkpoint dir file path
  ObBackupIoAdapter io_util;
  bool is_tagging = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Archive checkpoint mgr not init", K(ret));
  } else if (checkpoint_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(checkpoint_scn));
  } else if (OB_FAIL(check_is_tagging_(storage_info_, is_tagging))) {
    LOG_WARN("failed to judge delete mode", K(ret));
  } else if (OB_FAIL(full_path.join_checkpoint_info_file(file_name_, checkpoint_scn, type_))) {
    LOG_WARN("failed to get piece checkpoint file path",
        K(ret), K(checkpoint_scn), KP(file_name_), K(full_path), K(type_));
  } else if (OB_FAIL(write_checkpoint_file_(full_path))) {
    LOG_WARN("failed to write checkpoint file", K(ret), K(full_path));
  }
  //if the delete mode is not 'tagging', need to list files for deleting smaller checkpoint scn files
  if (OB_SUCC(ret) && !is_tagging && OB_TMP_FAIL(del_history_files_(dir_path, checkpoint_scn))) {
    LOG_WARN("failed to delete files", K(ret), K(dir_path), K(checkpoint_scn), K(tmp_ret));
  }

  return ret;
}

int ObArchiveCheckpointMgr::read(uint64_t &max_checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  ObArray<common::ObString> file_names;
  common::ObArenaAllocator allocator;
  ObBackupPath checkpoint_dir_path = path_;
  max_checkpoint_scn = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Archive checkpoint mgr not init", K(ret));
  } else if (OB_FAIL(get_max_checkpoint_scn_(checkpoint_dir_path, max_checkpoint_scn))) {
    LOG_WARN("failed to get max checkpoint scn", K(ret), K(checkpoint_dir_path));
  }
  return ret;
}

int ObArchiveCheckpointMgr::get_max_checkpoint_scn_(
    const ObBackupPath &path,
    uint64_t &max_checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  max_checkpoint_scn = 0;
  ObGetMaxCheckpointOp max_checkpoint_op(max_checkpoint_scn, file_name_, type_);
  if (OB_FAIL(io_util.list_files(path.get_ptr(), storage_info_, max_checkpoint_op))) {
    LOG_WARN("failed to get max checkpoint scn", K(ret), K(path));
  }
  return ret;
}

int ObArchiveCheckpointMgr::del_history_files_(
    const ObBackupPath &dir_path,
    const uint64_t write_checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  ObDelHisCheckpointFileOp del_his_file_op(write_checkpoint_scn, dir_path, file_name_, type_, storage_info_);
  if (OB_FAIL(io_util.list_files(dir_path.get_ptr(), storage_info_, del_his_file_op))) {
    LOG_WARN("failed to del history checkpoint file",
        K(ret), K(dir_path), K(write_checkpoint_scn), K(path_), KP(file_name_), K(type_));
  }
  return ret;
}

int ObArchiveCheckpointMgr::write_checkpoint_file_(const ObBackupPath &path) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBackupIoAdapter io_util;
  char buf = '\0';
  const int64_t buf_size = 0;
  if (OB_FAIL(io_util.mk_parent_dir(path.get_ptr(), storage_info_))) {
    LOG_WARN("failed to mk dir.", K(ret), K(path));
  } else if (OB_FAIL(io_util.write_single_file(path.get_ptr(), storage_info_, &buf, buf_size))) {
    LOG_WARN("failed to write single file.", K(ret), K(path));
  } else {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      FLOG_INFO("succeed to write checkpoint file.", K(path));
    }
  }
  return ret;
}