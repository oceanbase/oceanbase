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
#include "ob_backup_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/ob_define.h"

using namespace oceanbase;
using namespace common;
using namespace share;

ObBackupPath::ObBackupPath()
  : cur_pos_(0)
{
  path_[0] = '\0';
}

ObBackupPath::~ObBackupPath()
{
  reset();
}

void ObBackupPath::reset()
{
  path_[0] = '\0';
  cur_pos_ = 0;
}


int ObBackupPath::trim_right_backslash()
{
  int ret = OB_SUCCESS;
  for (int64_t pos = cur_pos_ - 1; pos >= 0; --pos) {
    if (path_[pos] == '/') {
      path_[pos] = '\0';
      --cur_pos_;
    } else {
      break;
    }
  }

  return ret;
}

int ObBackupPath::init(const common::ObString &backup_root_path)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ != 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  }  else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "%.*s",
      backup_root_path.length(), backup_root_path.ptr()))) {
    OB_LOG(WARN, "fail to backup_root_path", K(ret), K(backup_root_path));
  } else if (OB_FAIL(trim_right_backslash())) {
    OB_LOG(WARN, "fail to trim_right_backslash", K(ret));
  }

  return ret;
}

int ObBackupPath::join_incarnation(const uint64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_,
      "/%s_%lu", OB_STR_INCARNATION, incarnation))) {
    LOG_WARN("failed to join incarnation", K(ret), K(incarnation), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    OB_LOG(WARN, "fail to trim_right_backslash", K(ret));
  }

  return ret;
}

int ObBackupPath::add_backup_suffix(const ObBackupFileSuffix &type)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else {
    switch (type) {
      case ARCHIVE: {
        if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "%s", OB_ARCHIVE_SUFFIX))) {
          LOG_WARN("failed to add archive suffix", K(ret));
        }
        break;
      }
      case BACKUP: {
        if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "%s", OB_BACKUP_SUFFIX))) {
          LOG_WARN("failed to add backup suffix", K(ret));
        }
        break;
      }
      case NONE: {
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknow backup file suffix type", K(ret), K(type));
      }
    }

  }
  return ret;
}
int ObBackupPath::join(const common::ObString &str_path, const ObBackupFileSuffix &type)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ <= 0) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret), K(*this));
  } else if (str_path.length() <= 0 ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(str_path));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_,
      "/%.*s", str_path.length(), str_path.ptr()))) {
    LOG_WARN("failed to join path", K(ret), K(str_path), K(*this));
  } else if (OB_FAIL(add_backup_suffix(type))) {
    LOG_WARN("failed to add backup file suffix", K(ret), K(type), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    OB_LOG(WARN, "fail to trim_right_backslash", K(ret));
  }
  return ret;

}

int ObBackupPath::join(const uint64_t int_path, const ObBackupFileSuffix &type)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ <= 0) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_,
      "/%lu", int_path))) {
    LOG_WARN("failed to join int path", K(ret), K(int_path), K(*this));
  } else if (OB_FAIL(add_backup_suffix(type))) {
    LOG_WARN("failed to add backup file suffix", K(ret), K(type), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    OB_LOG(WARN, "fail to trim_right_backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join(const int64_t v, const ObBackupFileSuffix &type)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ <= 0) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", KR(ret), K(*this));
  } else if (OB_UNLIKELY(v < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(v));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%ld", v))) {
    LOG_WARN("failed to databuff_printf", KR(ret), K(*this));
  } else if (OB_FAIL(add_backup_suffix(type))) {
    LOG_WARN("failed to add backup file suffix", K(ret), K(type), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("fail to trim_right_backslash", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObBackupPath::join_tenant_incarnation(const uint64_t tenant_id, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_,
                 sizeof(path_),
                 cur_pos_,
                 "/%s_%lu_%s_%lu",
                 OB_STR_TENANT,
                 tenant_id,
                 OB_STR_INCARNATION,
                 incarnation))) {
    LOG_WARN("failed to join incarnation", K(ret), K(tenant_id), K(incarnation), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_round_piece(const int64_t round, const int64_t piece_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%lu_%lu", round, piece_id))) {
    LOG_WARN("failed to join round piece", K(ret), K(round), K(piece_id));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_backup_set(const ObBackupSetDesc &backup_set_desc)
{
  int ret = OB_SUCCESS;
  const char *backup_type = NULL;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (!backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_desc));
  } else if (backup_set_desc.backup_type_.is_full_backup()) {
    backup_type = OB_STR_FULL_BACKUP;
  } else {
    backup_type = OB_STR_INC_BACKUP;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(path_,
                 sizeof(path_),
                 cur_pos_,
                 "/%s_%ld_%s",
                 OB_STR_BACKUP_SET,
                 backup_set_desc.backup_set_id_,
                 backup_type))) {
    LOG_WARN("failed to join full_backup_set", K(ret), K(backup_set_desc), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_complement_log()
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%s", OB_STR_COMPLEMENT_LOG))) {
    LOG_WARN("failed to join complement log", K(ret), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%s_%ld", OB_STR_LS, ls_id.id()))) {
    LOG_WARN("failed to join incarnation", K(ret), K(ls_id), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_macro_data_dir(
    const ObBackupDataType &backup_type, const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  const char *backup_data_type = NULL;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (turn_id < 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(turn_id), K(retry_id));
  } else if (backup_type.is_major_backup()) {
    backup_data_type = OB_STR_MAJOR_BACKUP;
  } else if (backup_type.is_minor_backup()) {
    backup_data_type = OB_STR_MINOR_BACKUP;
  } else {
    backup_data_type = OB_STR_SYS_BACKUP;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(path_,
                 sizeof(path_),
                 cur_pos_,
                 "/%s_%s_%ld_%s_%ld",
                 backup_data_type,
                 OB_STR_TURN,
                 turn_id,
                 OB_STR_RETRY,
                 retry_id))) {
    LOG_WARN("failed to join macro data dir", K(ret), K(turn_id), K(retry_id), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_macro_data_file(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "%s.%ld", OB_STR_BACKUP_MACRO_BLOCK_DATA, file_id))) {
    LOG_WARN("failed to join macro block data file", K(ret), K(file_id), K(*this));
  } else if (OB_FAIL(join(file_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join file_name", K(ret), K(file_name));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_tablet_info_file(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(
                 databuff_printf(file_name, sizeof(file_name), "%s.%ld", OB_STR_TABLET_INFO, file_id))) {
    LOG_WARN("failed to join macro block data file", K(ret), K(file_id), K(*this));
  } else if (OB_FAIL(join(file_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join file_name", K(ret), K(file_name));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_data_info_turn(const share::ObBackupDataType &type, const int64_t turn_id)
{
  int ret = OB_SUCCESS;
  const char *type_str = "";
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (type.is_minor_backup()) {
    type_str = "minor";
  } else if (type.is_major_backup()) {
    type_str = "major";
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%s_%s_%ld", type_str, OB_STR_DATA_INTO_TURN, turn_id))) {
    LOG_WARN("failed to join macro block data file", K(ret), K(turn_id), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_data_info_turn_v_4_1_x(const int64_t turn_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%s_%ld", OB_STR_DATA_INTO_TURN, turn_id))) {
    LOG_WARN("failed to join macro block data file", K(ret), K(turn_id), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_meta_info_turn_and_retry(const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, 
      "/%s_%ld_%s_%ld", OB_STR_META_INFO_TURN, turn_id, OB_STR_RETRY, retry_id))) {
    LOG_WARN("failed to join info retry", K(ret), K(retry_id), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_tenant_macro_range_index_file(const ObBackupDataType &backup_type, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  const char *backup_data_type = NULL;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (backup_type.is_major_backup()) {
    backup_data_type = OB_STR_MAJOR_BACKUP;
  } else if (backup_type.is_minor_backup()) {
    backup_data_type = OB_STR_MINOR_BACKUP;
  } else {
    backup_data_type = OB_STR_SYS_BACKUP;
  }
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(file_name,
                 sizeof(file_name),
                 "%s_%s_%s.%ld",
                 OB_STR_TENANT,
                 backup_data_type,
                 OB_STR_BACKUP_MACRO_RANGE_INDEX,
                 retry_id))) {
    LOG_WARN("failed to join macro range index file", K(ret), K(backup_type), K(*this));
  } else if (OB_FAIL(join(file_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join file_name", K(ret), K(file_name));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

int ObBackupPath::join_tenant_meta_index_file(const ObBackupDataType &backup_type, const int64_t retry_id, const bool is_sec_meta)
{
  int ret = OB_SUCCESS;
  const char *backup_data_type = NULL;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (backup_type.is_major_backup()) {
    backup_data_type = OB_STR_MAJOR_BACKUP;
  } else if (backup_type.is_minor_backup()) {
    backup_data_type = OB_STR_MINOR_BACKUP;
  } else {
    backup_data_type = OB_STR_SYS_BACKUP;
  }
  // meta_index corresponds to tablet meta index and sstable meta index
  // sec_meta_index corresponds to macro block id mapping meta index, which will be removed later
  const char *meta_file_name = is_sec_meta ? OB_STR_BACKUP_SEC_META_INDEX : OB_STR_BACKUP_META_INDEX;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(*this));
  } else if (OB_FAIL(databuff_printf(
                 file_name, sizeof(file_name), "%s_%s_%s.%ld",
                 OB_STR_TENANT, backup_data_type, meta_file_name, retry_id))) {
    LOG_WARN("failed to join tenant meta index file", K(ret), K(backup_type), K(*this));
  } else if (OB_FAIL(join(file_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join file_name", K(ret), K(file_name));
  } else if (OB_FAIL(trim_right_backslash())) {
    LOG_WARN("failed to trim right backslash", K(ret));
  }
  return ret;
}

// param case: file_name -> 'checkpoint_info', checkpoint -> 1632889932327676777, type -> ARCHIVE
// result : oss://backup/[file_name].[checkpoint].obarc
int ObBackupPath::join_checkpoint_info_file(const common::ObString &file_name, const uint64_t checkpoint, const ObBackupFileSuffix &type)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (file_name.length() <= 0 ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(file_name));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, "/%.*s", file_name.length(), file_name.ptr()))) {
    LOG_WARN("failed to join file name", K(ret), K(file_name), K(*this));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), cur_pos_, ".%lu", checkpoint))) {
    LOG_WARN("failed to join checkpoint", K(ret), K(checkpoint), K(*this));
  } else if (OB_FAIL(add_backup_suffix(type))) {
    LOG_WARN("failed to add backup file suffix", K(ret), K(type), K(*this));
  } else if (OB_FAIL(trim_right_backslash())) {
    OB_LOG(WARN, "fail to trim_right_backslash", K(ret));
  }
  return ret;
}

// param case: entry_d_name -> 'checkpoint_info.1678226622262333112.obarc', file_name -> 'checkpoint_info', type -> ARCHIVE
// result : checkpoint -> 1678226622262333112
int ObBackupPath::parse_checkpoint(const common::ObString &entry_d_name, const common::ObString &file_name, const ObBackupFileSuffix &type, uint64_t &checkpoint)
{
  int ret = OB_SUCCESS;
  checkpoint = 0;
  ObBackupPath tmp_path; //format string for sscanf
  char tmp_file_name[OB_MAX_FILE_NAME_LENGTH] = { 0 };
  if (entry_d_name.length() <= 0 || file_name.length() <= 0 || type > ObBackupFileSuffix::BACKUP || type < ObBackupFileSuffix::NONE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(entry_d_name), K(file_name));
  } else if (OB_FAIL(databuff_printf(tmp_file_name, sizeof(tmp_file_name), "%s.%%lu", file_name.ptr()))) {
    LOG_WARN("failed to join tmp file name", K(ret), K(file_name));
  } else if (OB_FAIL(tmp_path.init(tmp_file_name))) {
    LOG_WARN("failed to init tmp path", K(ret), K(tmp_file_name));
  } else if (OB_FAIL(tmp_path.add_backup_suffix(type))) {
    LOG_WARN("failed to add backup file suffix", K(ret), K(type), K(tmp_path));
  } else if (OB_FAIL(tmp_path.trim_right_backslash())) {
    OB_LOG(WARN, "fail to trim_right_backslash", K(ret));
  } else if (1 == sscanf(entry_d_name.ptr(), tmp_path.get_ptr(), &checkpoint)) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      OB_LOG(INFO, "succeed to get checkpoint scn", K(ret), K(entry_d_name), K(checkpoint), K(tmp_path));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to get checkpoint", K(ret), K(entry_d_name), K(file_name), K(type), K(checkpoint), K(tmp_path));
  }
  return ret;
}

common::ObString ObBackupPath::get_obstr() const
{
  return ObString(cur_pos_, path_);
}

ObBackupPath &ObBackupPath::operator =(const ObBackupPath &path)
{
  if (this != &path) {

    if (path.is_empty()) {
      cur_pos_ = 0;
      path_[0] = '\0';
    } else {
      int64_t path_length = path.length();
      MEMCPY(path_, path.get_ptr(), path_length);
      cur_pos_ = path_length;
      path_[path_length] = '\0';
    }
  }
  return *this;
}

uint64_t ObBackupPath::hash() const
{
  uint64_t seed = 0;
  seed = murmurhash(path_, static_cast<int32_t>(strlen(path_)), seed);
  return seed;
}

bool ObBackupPath::operator ==(const ObBackupPath &path) const
{
  return (0 == STRCMP(path_, path.get_ptr()) && (cur_pos_ == path.length()));
}

int ObBackupPathUtil::get_tenant_data_backup_set_placeholder_path_(
    const uint64_t backup_set_id,
    const ObBackupType backup_type,
    const SCN &min_restore_scn,
    const ObString &suffix, 
    ObBackupPath &path) 
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char min_restore_ts_buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  const char *backup_type_str = backup_type.is_full_backup() ? "full" : "inc";
  char backup_set_placeholder_name[OB_MAX_BACKUP_PATH_LENGTH];
  if (OB_FAIL(backup_scn_to_time_tag(min_restore_scn, min_restore_ts_buff, sizeof(min_restore_ts_buff), pos))) {
    LOG_WARN("fail to format time tag", K(ret), K(min_restore_scn));
  } else if (OB_FAIL(databuff_printf(backup_set_placeholder_name, sizeof(backup_set_placeholder_name), 
      "backup_set_%lu_%s_%s_%s", backup_set_id, backup_type_str, suffix.ptr(), min_restore_ts_buff))) {
    LOG_WARN("failed to format backup set placeholder name", K(ret), K(path));
  } else if (OB_FAIL(path.join(backup_set_placeholder_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to backup set placeholder name", K(ret), K(backup_set_placeholder_name));
  }
  return ret;
}

// file:///obbackup/backup_sets
int ObBackupPathUtil::get_backup_sets_dir_path(const share::ObBackupDest &backup_tenant_dest,
    ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (OB_FAIL(path.init(backup_tenant_dest.get_root_path()))) {
    LOG_WARN("failed to init path", K(ret));
  } else if (OB_FAIL(path.join(OB_STR_BACKUP_SETS, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join backup set", K(ret), K(backup_tenant_dest));
  }
  return ret;
}

// file:///backup/backup_sets/backup_set_xx_xxx_start.obbak
int ObBackupPathUtil::get_backup_set_placeholder_start_path(
    const share::ObBackupDest &backup_tenant_dest, const share::ObBackupSetDesc &backup_set_desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  char backup_set_name[OB_MAX_BACKUP_PATH_LENGTH];
  if (OB_FAIL(get_backup_sets_dir_path(backup_tenant_dest, backup_path))) {
    LOG_WARN("failed to get tenant backup sets dir path", K(ret), K(backup_tenant_dest));
  } else if (backup_set_desc.backup_type_.is_full_backup() 
      && OB_FAIL(databuff_printf(backup_set_name, sizeof(backup_set_name), "backup_set_%lu_full_start", backup_set_desc.backup_set_id_))) {
    LOG_WARN("failed to format backup set name", K(ret), K(backup_tenant_dest));
  } else if (!backup_set_desc.backup_type_.is_full_backup() 
      && OB_FAIL(databuff_printf(backup_set_name, sizeof(backup_set_name), "backup_set_%lu_inc_start", backup_set_desc.backup_set_id_))) {
    LOG_WARN("failed to format backup set name", K(ret), K(backup_tenant_dest));
  } else if (OB_FAIL(backup_path.join(backup_set_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("fail to join backup set name", K(ret), K(backup_set_name));
  }
  return ret;
}

int ObBackupPathUtil::get_backup_set_placeholder_end_success_path(
    const share::ObBackupDest &backup_tenant_dest, const share::ObBackupSetDesc &backup_set_desc,  
    const SCN &min_restore_scn, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObString suffix("end_success");
  if (OB_FAIL(get_backup_sets_dir_path(backup_tenant_dest, backup_path))) {
    LOG_WARN("failed to get tenant backup sets dir path", K(ret), K(backup_tenant_dest));
  } else if (OB_FAIL(get_tenant_data_backup_set_placeholder_path_(
      backup_set_desc.backup_set_id_, backup_set_desc.backup_type_, min_restore_scn, suffix, backup_path))) {
    LOG_WARN("failed to set placeholder path", K(ret), K(backup_path));
  }
  return ret;
}

int ObBackupPathUtil::get_backup_set_placeholder_end_failed_path(
    const share::ObBackupDest &backup_tenant_dest, const share::ObBackupSetDesc &backup_set_desc, 
    const SCN &min_restore_scn, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObString suffix("end_failed");
  if (OB_FAIL(get_backup_sets_dir_path(backup_tenant_dest, backup_path))) {
    LOG_WARN("failed to get tenant backup sets dir path", K(ret), K(backup_tenant_dest));
  } else if (OB_FAIL(get_tenant_data_backup_set_placeholder_path_(
      backup_set_desc.backup_set_id_, backup_set_desc.backup_type_, min_restore_scn, suffix, backup_path))) {
    LOG_WARN("failed to set placeholder path", K(ret), K(backup_path));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/
int ObBackupPathUtil::get_backup_set_dir_path(const share::ObBackupDest &backup_set_dest, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (OB_FAIL(path.init(backup_set_dest.get_root_path()))) {
    LOG_WARN("failed to init path", K(ret));
  } 
  return ret;
}

// file:///obbackup/backup_set_1_full/
int ObBackupPathUtil::get_backup_set_dir_path(const share::ObBackupDest &backup_tenant_dest, 
    const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_backup_set_inner_placeholder_prefix(
    const share::ObBackupSetDesc &backup_set_desc,
    char *placeholder_prefix,
    int64_t length)
{
  int ret = OB_SUCCESS;
  const char *backup_type_str = backup_set_desc.backup_type_.is_full_backup() ? "full" : "inc";
  if (OB_ISNULL(placeholder_prefix)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("placeholder_prefix is null", K(ret));
  } else if (OB_FAIL(databuff_printf(placeholder_prefix, length,
      "backup_set_%lu_%s_", backup_set_desc.backup_set_id_, backup_type_str))) {
    LOG_WARN("failed to format backup set placeholder prefix", K(ret), K(backup_set_desc));
  }

  return ret;
}

// file:///obbackup/backup_set_1_full/backup_set_1_full_xxxx_xxxxx.obbak
int ObBackupPathUtil::get_backup_set_inner_placeholder(
    const share::ObBackupDest &backup_set_dest, const share::ObBackupSetDesc &backup_set_desc, 
    const SCN &replay_scn, const SCN &min_restore_scn,
    share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  char log_replay_ts_buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  char min_restore_ts_buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  const char *backup_type_str = backup_set_desc.backup_type_.is_full_backup() ? "full" : "inc";
  char backup_set_placeholder_name[OB_MAX_BACKUP_PATH_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get tenant data infos dir", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_scn_to_time_tag(replay_scn, log_replay_ts_buff, sizeof(log_replay_ts_buff), pos))) {
    LOG_WARN("fail to format time tag", K(ret), K(replay_scn));
  } else if (OB_FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(backup_scn_to_time_tag(min_restore_scn, min_restore_ts_buff, sizeof(min_restore_ts_buff), pos))) {
    LOG_WARN("fail to format time tag", K(ret), K(min_restore_scn));
  } else if (OB_FAIL(databuff_printf(backup_set_placeholder_name, sizeof(backup_set_placeholder_name), 
      "backup_set_%lu_%s_%s_%s", backup_set_desc.backup_set_id_, backup_type_str, log_replay_ts_buff, min_restore_ts_buff))) {
    LOG_WARN("failed to format backup set placeholder name", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(backup_set_placeholder_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to backup set placeholder name", K(ret), K(backup_set_placeholder_name));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/
int ObBackupPathUtil::get_ls_backup_dir_path(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id, 
    ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_ls(ls_id))) {
    LOG_WARN("failed to join log stream", K(ret), K(ls_id));
  }
  return ret;
}

int ObBackupPathUtil::get_ls_backup_dir_path(const share::ObBackupDest &backup_set_dest,
    const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, desc, path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest), K(desc));
  } else if (OB_FAIL(path.join_ls(ls_id))) {
    LOG_WARN("failed to join log stream", K(ret), K(ls_id));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/meta_info_turn_1/tablet_info.obbak
int ObBackupPathUtil::get_ls_data_tablet_info_path(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const int64_t file_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_backup_dir_path(backup_set_dest, ls_id, path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_meta_info_turn_and_retry(turn_id, retry_id))) {
    LOG_WARN("failed to join info retry", K(ret), K(retry_id));
  } else if (OB_FAIL(path.join_tablet_info_file(file_id))) {
    LOG_WARN("failed to join", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/
int ObBackupPathUtil::get_ls_backup_data_dir_path(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_backup_dir_path(backup_set_dest, ls_id, path))) {
    LOG_WARN(
        "failed to get log stream backup dir path", K(ret), K(backup_set_dest), K(ls_id));
  } else if (OB_FAIL(path.join_macro_data_dir(backup_data_type, turn_id, retry_id))) {
    LOG_WARN("failed to join macro data dir", K(ret), K(backup_data_type), K(turn_id), K(retry_id));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/
int ObBackupPathUtil::get_ls_backup_data_dir_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_ls_backup_data_dir_path(
      backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/macro_block_data.1.obbak
int ObBackupPathUtil::get_macro_block_backup_path(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, const int64_t file_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_backup_data_dir_path(
          backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, path))) {
    LOG_WARN("failed to get log stream backup data dir path",
        K(ret),
        K(backup_set_dest),
        K(ls_id));
  } else if (OB_FAIL(path.join_macro_data_file(file_id))) {
    LOG_WARN("failed to join macro_block_index", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_macro_block_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, const int64_t file_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_macro_block_backup_path(
      backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, file_id, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/macro_range_index.obbak
int ObBackupPathUtil::get_ls_macro_range_index_backup_path(const share::ObBackupDest &backup_set_dest, 
    const share::ObLSID &ls_id, const ObBackupDataType &backup_data_type, const int64_t turn_id, 
    const int64_t retry_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_backup_data_dir_path(
          backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, path))) {
    LOG_WARN("failed to get log stream backup data dir path",
        K(ret),
        K(backup_set_dest),
        K(ls_id));
  } else if (OB_FAIL(path.join(OB_STR_BACKUP_MACRO_RANGE_INDEX, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join macro range index", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_ls_macro_range_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_ls_macro_range_index_backup_path(
      backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/meta_index.obbak
int ObBackupPathUtil::get_ls_meta_index_backup_path(const share::ObBackupDest &backup_set_dest, 
    const share::ObLSID &ls_id, const ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, const bool is_sec_meta, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  const char *meta_file_name = is_sec_meta ? OB_STR_BACKUP_SEC_META_INDEX : OB_STR_BACKUP_META_INDEX;
  if (OB_FAIL(get_ls_backup_data_dir_path(
          backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, path))) {
    LOG_WARN("failed to get log stream backup data dir path",
        K(ret),
        K(backup_set_dest),
        K(ls_id));
  } else if (OB_FAIL(path.join(meta_file_name, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join macro range index", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_ls_meta_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, const bool is_sec_meta, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_ls_meta_index_backup_path(
      backup_set_dest, ls_id, backup_data_type, turn_id, retry_id, is_sec_meta, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/
int ObBackupPathUtil::get_ls_info_dir_path(const share::ObBackupDest &backup_set_dest, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(ObString::make_string("infos"), ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join data", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_ls_info_dir_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_ls_info_dir_path(
      backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/major_data_info_turn_1
int ObBackupPathUtil::get_ls_info_data_info_dir_path(const share::ObBackupDest &backup_set_dest,
    const share::ObBackupDataType &type, const int64_t turn_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(ObString::make_string("infos"), ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join data", K(ret));
  } else if (OB_FAIL(backup_path.join_data_info_turn(type, turn_id))) {
    LOG_WARN("failed to join info turn", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_ls_info_data_info_dir_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, const share::ObBackupDataType &type, const int64_t turn_id,
    share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_ls_info_data_info_dir_path(backup_set_dest, type, turn_id, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/meta_info/
int ObBackupPathUtil::get_tenant_meta_info_dir_path(const share::ObBackupDest &backup_set_dest, 
    share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_META_INFO, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join data", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_tenant_meta_info_dir_path(const share::ObBackupDest &backup_tenant_dest, 
    const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_tenant_meta_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/meta_info/ls_attr_info.1.obbak
int ObBackupPathUtil::get_backup_ls_attr_info_path(const share::ObBackupDest &backup_set_dest,
    const int64_t turn_id, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  char buf[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  if (OB_FAIL(get_tenant_meta_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(databuff_printf(buf, OB_BACKUP_MAX_TIME_STR_LEN, "%s.%ld", OB_STR_LS_ATTR_INFO, turn_id))) {
    LOG_WARN("failed to printf ls attr info", K(ret));
  } else if (OB_FAIL(backup_path.join(buf, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join ls attr info", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/meta_info/ls_meta_infos.obbak
int ObBackupPathUtil::get_ls_meta_infos_path(const share::ObBackupDest &backup_set_dest, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  char buf[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  if (OB_FAIL(get_tenant_meta_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_LS_META_INFOS, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join ls meta infos", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/meta_info/root_key.obbak
int ObBackupPathUtil::get_backup_root_key_path(const share::ObBackupDest &backup_set_dest, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tenant_meta_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_ROOT_KEY, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join ls meta infos", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/tenant_backup_set_infos.obbak
int ObBackupPathUtil::get_tenant_backup_set_infos_path(const share::ObBackupDest &backup_set_dest, 
    ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_TENANT_BACKUP_SET_INFOS, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join data", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_tenant_backup_set_infos_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_tenant_backup_set_infos_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/single_backup_set_info.obbak
int ObBackupPathUtil::get_backup_set_info_path(const share::ObBackupDest &backup_set_dest,
    ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_SINGLE_BACKUP_SET_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join data", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_backup_set_info_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_backup_set_info_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/diagnose_info.obbak
int ObBackupPathUtil::get_diagnose_info_path(const share::ObBackupDest &backup_set_dest,
    ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_DIAGNOSE_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join data", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_diagnose_info_path(const share::ObBackupDest &backup_tenant_dest,
    share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_diagnose_info_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/locality_info.obbak
int ObBackupPathUtil::get_locality_info_path(const share::ObBackupDest &backup_set_dest,
    ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(OB_STR_LOCALITY_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join data", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_locality_info_path(const share::ObBackupDest &backup_tenant_dest,
    share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_locality_info_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/log_stream_1/meta_info_turn_1_retry_0/ls_meta_info.obbak
int ObBackupPathUtil::get_ls_meta_info_backup_path(const share::ObBackupDest &backup_tenant_dest,
    const ObBackupSetDesc &desc, const share::ObLSID &ls_id, const int64_t turn_id, 
    const int64_t retry_id, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_tenant_dest, desc, backup_path))) {
    LOG_WARN("fail to get backup set dier path", K(ret), K(backup_tenant_dest), K(desc));
  } else if (OB_FAIL(backup_path.join_ls(ls_id))) {
    LOG_WARN("fail to join ls", K(ret), K(ls_id));
  } else if (OB_FAIL(backup_path.join_meta_info_turn_and_retry(turn_id, retry_id))) {
    LOG_WARN("failed to join info retry", K(ret), K(retry_id));
  } else if (OB_FAIL(backup_path.join(OB_STR_LS_META_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join info turn", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/logstream_1/xxx_xxx_turn_1_retry_0/macro_range_index.obbak
int ObBackupPathUtil::get_tenant_macro_range_index_backup_path(const share::ObBackupDest &backup_set_dest,
    const ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_data_info_turn(backup_data_type, turn_id))) {
    LOG_WARN("failed to join info turn", K(ret));
  } else if (OB_FAIL(path.join_tenant_macro_range_index_file(backup_data_type, retry_id))) {
    LOG_WARN("failed to join tenant macro range index file", K(ret), K(backup_data_type), K(retry_id));
  }
  return ret;
}

int ObBackupPathUtil::get_tenant_macro_range_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, const share::ObBackupDataType &backup_data_type, const int64_t turn_id,
    const int64_t retry_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_tenant_macro_range_index_backup_path(
      backup_set_dest, backup_data_type, turn_id, retry_id, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/logstream_1/xxx_xxx_turn_1_retry_0/meta_index.obbak
int ObBackupPathUtil::get_tenant_meta_index_backup_path(const share::ObBackupDest &backup_set_dest,
    const ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
    const bool is_sec_meta, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_data_info_turn(backup_data_type, turn_id))) {
    LOG_WARN("failed to join info turn", K(ret));
  } else if (OB_FAIL(path.join_tenant_meta_index_file(backup_data_type, retry_id, is_sec_meta))) {
    LOG_WARN("failed to join tenant macro range index file", K(ret), K(backup_data_type));
  }
  return ret;
}

int ObBackupPathUtil::get_tenant_meta_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, const share::ObBackupDataType &backup_data_type, const int64_t turn_id,
    const int64_t retry_id, const bool is_sec_meta, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_tenant_meta_index_backup_path(
      backup_set_dest, backup_data_type, turn_id, retry_id, is_sec_meta, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file://obbackup/backup_set_1_full/infos/data_info_turn_1/tablet_log_stream_info.obbak
int ObBackupPathUtil::get_backup_data_tablet_ls_info_path(const share::ObBackupDest &backup_set_dest,
    const share::ObBackupDataType &backup_data_type, const uint64_t turn_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_data_info_turn(backup_data_type, turn_id))) {
    LOG_WARN("failed to join info turn", K(ret));
  } else if (OB_FAIL(path.join(OB_STR_TABLET_LOG_STREAM_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join tablet_log_stream_info", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/deleted_tablet_info

int ObBackupPathUtil::get_deleted_tablet_info_path(const share::ObBackupDest &backup_set_dest, share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_dir_path(backup_set_dest, path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join(OB_STR_DELETED_TABLET_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join tablet_log_stream_info", K(ret));
  }
  return ret;
}

// file://obbackup/backup_set_1_full/infos/meta_info/tablet_log_stream_info.obbak
int ObBackupPathUtil::get_backup_data_meta_tablet_ls_info_path(const share::ObBackupDest &backup_set_dest, share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char buf[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  if (OB_FAIL(get_tenant_meta_info_dir_path(backup_set_dest, path))) {
    LOG_WARN("failed to get tenant meta info dir", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, OB_BACKUP_MAX_TIME_STR_LEN, "%s", OB_STR_TABLET_LOG_STREAM_INFO))) {
    LOG_WARN("failed to printf ls meta infos", K(ret));
  } else if (OB_FAIL(path.join(buf, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join ls meta infos", K(ret));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/complement_log/
int ObBackupPathUtil::get_complement_log_dir_path(const share::ObBackupDest &backup_set_dest, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get log stream backup dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join_complement_log())) {
    LOG_WARN("faile to join complement log", K(ret));
  }
  return ret;
}

int ObBackupPathUtil::get_complement_log_dir_path(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_set_dest;
  if (OB_FAIL(construct_backup_set_dest(backup_tenant_dest, desc, backup_set_dest))) {
    LOG_WARN("fail to construct backup set dest", K(ret));
  } else if (OB_FAIL(get_complement_log_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("fail to get ls backup data dir path", K(ret));
  }
  return ret;
}

// file:///obbackup/tenant_1001_incarnation_1/clog/1_1/log_stream_1/
int ObBackupPathUtil::get_ls_log_archive_prefix(const share::ObBackupDest &backup_set_dest, uint64_t tenant_id,
    const int64_t incarnation, const int64_t round, const int64_t piece_id, const share::ObLSID &ls_id,
    ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (OB_FAIL(path.init(backup_set_dest.get_root_path()))) {
    LOG_WARN("failed to init path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_tenant_incarnation(tenant_id, incarnation))) {
    LOG_WARN("failed to join tenant and incarnation", K(ret), K(tenant_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_STR_CLOG), ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join clog", K(ret));
  } else if (OB_FAIL(path.join_round_piece(round, piece_id))) {
    LOG_WARN("failed to join round and piece", K(ret), K(round), K(piece_id));
  } else if (OB_FAIL(path.join_ls(ls_id))) {
    LOG_WARN("failed to join log stream", K(ret), K(ls_id));
  }
  return ret;
}

int ObBackupPathUtil::construct_backup_set_dest(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &backup_desc, share::ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  backup_set_dest.reset();
  if (!backup_tenant_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.init(backup_tenant_dest.get_root_path()))) {
    LOG_WARN("fail to init path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_backup_set(backup_desc))) {
    LOG_WARN("fail to join backup set", K(ret));
  }

  const ObBackupStorageInfo *storage_info = nullptr;
  char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(storage_info = backup_tenant_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage info must not be nullptr", K(ret));
  } else if (OB_FAIL(storage_info->get_storage_info_str(
        storage_info_buf, OB_MAX_BACKUP_STORAGE_INFO_LENGTH))) {
    LOG_WARN("fail to get storage info", K(ret));
  } else if (OB_FAIL(backup_set_dest.set(path.get_ptr(), storage_info_buf))) {
    LOG_WARN("fail to set backup set dest", K(ret), K(path), K(storage_info_buf));
  }
  return ret;
}

int ObBackupPathUtil::construct_backup_complement_log_dest(const share::ObBackupDest &backup_tenant_dest,
    const share::ObBackupSetDesc &backup_desc, share::ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  backup_set_dest.reset();
  if (!backup_tenant_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.init(backup_tenant_dest.get_root_path()))) {
    LOG_WARN("fail to init path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_backup_set(backup_desc))) {
    LOG_WARN("fail to join backup set", K(ret));
  } else if (OB_FAIL(path.join_complement_log())) {
    LOG_WARN("fail to join complement log", K(ret));
  }

  const ObBackupStorageInfo *storage_info = nullptr;
  char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(storage_info = backup_tenant_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage info must not be nullptr", K(ret));
  } else if (OB_FAIL(storage_info->get_storage_info_str(
        storage_info_buf, OB_MAX_BACKUP_STORAGE_INFO_LENGTH))) {
    LOG_WARN("fail to get storage info", K(ret));
  } else if (OB_FAIL(backup_set_dest.set(path.get_ptr(), storage_info_buf))) {
    LOG_WARN("fail to set backup set dest", K(ret), K(path), K(storage_info_buf));
  }
  return ret;
}

int ObBackupPathUtil::construct_backup_complement_log_dest(const share::ObBackupDest &backup_tenant_dest, share::ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  backup_set_dest.reset();
  if (!backup_tenant_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.init(backup_tenant_dest.get_root_path()))) {
    LOG_WARN("fail to init path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_complement_log())) {
    LOG_WARN("fail to join complement log", K(ret));
  }

  const ObBackupStorageInfo *storage_info = nullptr;
  char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(storage_info = backup_tenant_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage info must not be nullptr", K(ret));
  } else if (OB_FAIL(storage_info->get_storage_info_str(
        storage_info_buf, OB_MAX_BACKUP_STORAGE_INFO_LENGTH))) {
    LOG_WARN("fail to get storage info", K(ret));
  } else if (OB_FAIL(backup_set_dest.set(path.get_ptr(), storage_info_buf))) {
    LOG_WARN("fail to set backup set dest", K(ret), K(path), K(storage_info_buf));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/xxx_xxx_turn_1_retry_0/meta_index.obbak
int ObBackupPathUtilV_4_1::get_tenant_meta_index_backup_path(const share::ObBackupDest &backup_set_dest,
    const ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
    const bool is_sec_meta, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_data_info_dir_path(backup_set_dest, turn_id, path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(path.join_tenant_meta_index_file(backup_data_type, retry_id, is_sec_meta))) {
    LOG_WARN("failed to join tenant macro range index file", K(ret), K(backup_data_type));
  }
  return ret;
}
// file:///obbackup/backup_set_1_full/infos/data_info_turn_1/
int ObBackupPathUtilV_4_1::get_ls_info_data_info_dir_path(const share::ObBackupDest &backup_set_dest,
    const int64_t turn_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_path.join(ObString::make_string("infos"), ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join data", K(ret));
  } else if (OB_FAIL(backup_path.join_data_info_turn_v_4_1_x(turn_id))) {
    LOG_WARN("failed to join info turn", K(ret));
  }
  return ret;
}
// file:///obbackup/backup_set_1_full/infos/data_info_turn_1/tablet_to_ls_info.obbak
int ObBackupPathUtilV_4_1::get_backup_data_tablet_ls_info_path(const share::ObBackupDest &backup_set_dest,
    const uint64_t turn_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_info_data_info_dir_path(backup_set_dest, turn_id, path))) {
    LOG_WARN("failed to get ls info data info dir path", K(ret));
  } else if (OB_FAIL(path.join(OB_STR_TABLET_LOG_STREAM_INFO, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to join tablet_log_stream_info", K(ret));
  }
  return ret;
}