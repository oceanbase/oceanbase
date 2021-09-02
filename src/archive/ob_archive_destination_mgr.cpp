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

#define USING_LOG_PREFIX ARCHIVE

#include "ob_archive_destination_mgr.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "share/backup/ob_backup_path.h"
#include "clog/ob_log_define.h"
#include "observer/ob_server_struct.h"
#include "ob_archive_path.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::clog;
namespace oceanbase {
namespace archive {

void ObArchiveDestination::reset()
{
  is_inited_ = false;
  compatible_ = false;
  need_switch_piece_on_beginning_ = false;
  cur_piece_id_ = OB_BACKUP_INVALID_PIECE_ID;
  cur_piece_create_date_ = OB_INVALID_TIMESTAMP;
  cur_index_file_id_ = 0;
  index_file_offset_ = 0;
  cur_data_file_id_ = 0;
  data_file_offset_ = 0;
  data_file_min_log_id_ = OB_INVALID_ID;
  data_file_min_log_ts_ = OB_INVALID_TIMESTAMP;
  force_switch_data_file_ = false;
  force_switch_index_file_ = false;
  is_data_file_valid_ = false;
}

int ObArchiveDestination::init(const common::ObPGKey& pg_key, const int64_t incarnation, const int64_t round,
    const bool need_switch_piece_on_beginning, const int64_t piece_id, const int64_t piece_create_date,
    const bool compatible, const uint64_t cur_index_file_id, const int64_t index_file_offset,
    const uint64_t cur_data_file_id, const int64_t data_file_offset, const int64_t force_switch_data_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(0 > piece_id) || OB_UNLIKELY(OB_INVALID_ARCHIVE_FILE_ID == cur_index_file_id) ||
      OB_UNLIKELY(index_file_offset < 0) || OB_UNLIKELY(OB_INVALID_ARCHIVE_FILE_ID == cur_data_file_id) ||
      OB_UNLIKELY(data_file_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
        KR(ret),
        K(pg_key),
        K(incarnation),
        K(round),
        K(cur_index_file_id),
        K(index_file_offset),
        K(cur_data_file_id),
        K(data_file_offset));
  } else {
    compatible_ = compatible;
    need_switch_piece_on_beginning_ = need_switch_piece_on_beginning;
    cur_piece_id_ = piece_id;
    cur_piece_create_date_ = piece_create_date;
    cur_index_file_id_ = cur_index_file_id;
    index_file_offset_ = index_file_offset;
    cur_data_file_id_ = cur_data_file_id;
    data_file_offset_ = index_file_offset;
    force_switch_data_file_ = force_switch_data_file;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveDestination::init_with_valid_residual_data_file(const common::ObPGKey& pg_key, const int64_t incarnation,
    const int64_t round, const bool need_switch_piece_on_beginning, const int64_t piece_id,
    const int64_t piece_create_date, const bool compatible, const uint64_t cur_index_file_id,
    const int64_t index_file_offset, const uint64_t cur_data_file_id, const int64_t data_file_offset,
    const int64_t force_switch_data_file, const uint64_t min_log_id, const int64_t min_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init(pg_key,
          incarnation,
          round,
          need_switch_piece_on_beginning,
          piece_id,
          piece_create_date,
          compatible,
          cur_index_file_id,
          index_file_offset,
          cur_data_file_id,
          data_file_offset,
          force_switch_data_file))) {
    LOG_WARN("ObArchiveDestination init fail",
        KR(ret),
        K(pg_key),
        K(incarnation),
        K(round),
        K(cur_index_file_id),
        K(index_file_offset),
        K(cur_data_file_id),
        K(data_file_offset));
  } else if (OB_FAIL(set_data_file_record_min_log_info(min_log_id, min_log_ts))) {
    LOG_WARN("set_data_file_record_min_log_info fail", KR(ret), K(min_log_id), K(min_log_ts));
  } else {
    ARCHIVE_LOG(INFO, "init_with_valid_residual_data_file succ", KPC(this), K(pg_key));
  }

  return ret;
}

int ObArchiveDestination::switch_file(
    const ObPGKey& pg_key, const LogArchiveFileType file_type, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(!util.is_valid_file_type(file_type)) ||
             OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(file_type), K(incarnation), K(round));
  } else if (OB_FAIL(update_file_meta_info_(file_type))) {
    LOG_WARN("fail to update file meta info", K(ret), K(pg_key), K(file_type));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to switch file", K(ret), K(pg_key), K(file_type));
  } else {
    LOG_INFO("succ to switch file", K(ret), K(pg_key), K(file_type), K(cur_data_file_id_), K(cur_index_file_id_));
  }
  return ret;
}

int ObArchiveDestination::switch_piece(const ObPGKey& pg_key, const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_), K(pg_key));
  } else if (OB_UNLIKELY(!pg_key.is_valid() ||
                         ((!need_switch_piece_on_beginning_) && (piece_id != (cur_piece_id_ + 1))) ||
                         (need_switch_piece_on_beginning_ && (piece_id != cur_piece_id_)))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", K(ret), KPC(this), K(piece_id));
  } else if (OB_FAIL(update_file_meta_info_(LOG_ARCHIVE_FILE_TYPE_DATA))) {
    LOG_WARN("fail to update file meta info of data file", K(ret), K(pg_key));
  } else if (OB_FAIL(update_file_meta_info_(LOG_ARCHIVE_FILE_TYPE_INDEX))) {
    LOG_WARN("fail to update file meta info of index file", K(ret), K(pg_key));
  } else {
    reset_file_meta_info_();
    if (need_switch_piece_on_beginning_) {
      need_switch_piece_on_beginning_ = false;
      LOG_INFO("success to switch_piece on beginning",
          K(pg_key),
          K(piece_id),
          K(piece_create_date),
          K(cur_data_file_id_),
          K(cur_index_file_id_));
    } else {
      cur_piece_id_ = piece_id;
      cur_piece_create_date_ = piece_create_date;
      LOG_INFO("success to switch_piece",
          K(pg_key),
          K(piece_id),
          K(piece_create_date),
          K(cur_data_file_id_),
          K(cur_index_file_id_));
    }
  }
  return ret;
}

int ObArchiveDestination::update_file_meta_info_(const LogArchiveFileType file_type)
{
  int ret = OB_SUCCESS;
  switch (file_type) {
    case LOG_ARCHIVE_FILE_TYPE_INDEX: {
      ++cur_index_file_id_;
      index_file_offset_ = 0;
      force_switch_index_file_ = false;
      break;
    }
    case LOG_ARCHIVE_FILE_TYPE_DATA: {
      ++cur_data_file_id_;
      data_file_offset_ = 0;
      data_file_min_log_id_ = OB_INVALID_ID;
      data_file_min_log_ts_ = -1;
      is_data_file_valid_ = false;
      force_switch_data_file_ = false;
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid file type", KR(ret), K(file_type));
  }
  return ret;
}

void ObArchiveDestination::reset_file_meta_info_()
{
  cur_data_file_id_ = 1;
  data_file_offset_ = 0;
  cur_index_file_id_ = 1;
  index_file_offset_ = 0;
}

int ObArchiveDestination::update_file_offset(const int64_t buf_len, const LogArchiveFileType file_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf size", KR(ret), K(buf_len));
  } else {
    switch (file_type) {
      case LOG_ARCHIVE_FILE_TYPE_INDEX:
        index_file_offset_ += buf_len;
        break;
      case LOG_ARCHIVE_FILE_TYPE_DATA:
        data_file_offset_ += buf_len;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid file type", KR(ret), K(file_type));
    }
  }
  return ret;
}

int ObArchiveDestination::set_data_file_record_min_log_info(const uint64_t min_log_id, const int64_t min_log_ts)
{
  int ret = OB_SUCCESS;
  if (is_data_file_valid_) {
    // skip
  } else {
    data_file_min_log_id_ = min_log_id;
    data_file_min_log_ts_ = min_log_ts;
    is_data_file_valid_ = true;
  }
  return ret;
}

int ObArchiveDestination::set_file_force_switch(const LogArchiveFileType file_type)
{
  int ret = OB_SUCCESS;
  switch (file_type) {
    case LOG_ARCHIVE_FILE_TYPE_INDEX:
      force_switch_index_file_ = true;
      break;
    case LOG_ARCHIVE_FILE_TYPE_DATA:
      force_switch_data_file_ = true;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid file type", KR(ret), K(file_type));
  }
  return ret;
}

int ObArchiveDestination::get_file_info(const common::ObPGKey& pg_key, const LogArchiveFileType file_type,
    const int64_t incarnation, const int64_t round, bool& compatible, int64_t& offset, const int64_t path_len,
    char* file_path)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil util;
  compatible = compatible_;
  switch (file_type) {
    case LOG_ARCHIVE_FILE_TYPE_INDEX:
      offset = index_file_offset_;
      ret = util.build_archive_file_path(pg_key,
          LOG_ARCHIVE_FILE_TYPE_INDEX,
          cur_index_file_id_,
          incarnation,
          round,
          cur_piece_id_,
          cur_piece_create_date_,
          path_len,
          file_path);
      break;
    case LOG_ARCHIVE_FILE_TYPE_DATA:
      offset = data_file_offset_;
      ret = util.build_archive_file_path(pg_key,
          LOG_ARCHIVE_FILE_TYPE_DATA,
          cur_data_file_id_,
          incarnation,
          round,
          cur_piece_id_,
          cur_piece_create_date_,
          path_len,
          file_path);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid file type", KR(ret), K(file_type));
  }
  return ret;
}

int ObArchiveDestination::get_file_offset(const LogArchiveFileType file_type, int64_t& offset, bool& flag)
{
  int ret = OB_SUCCESS;
  switch (file_type) {
    case LOG_ARCHIVE_FILE_TYPE_INDEX:
      offset = index_file_offset_;
      flag = force_switch_index_file_;
      break;
    case LOG_ARCHIVE_FILE_TYPE_DATA:
      offset = data_file_offset_;
      flag = force_switch_data_file_;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid file type", KR(ret), K(file_type));
  }
  return ret;
}

int ObArchiveDestination::get_data_file_min_log_info(uint64_t& min_log_id, int64_t& min_log_ts, bool& min_log_exist)
{
  int ret = OB_SUCCESS;

  if (is_data_file_valid_ && OB_INVALID_ID == data_file_min_log_id_) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid min_log_id", KR(ret), K(is_data_file_valid_), K(data_file_min_log_id_));
  } else {
    min_log_id = data_file_min_log_id_;
    min_log_ts = data_file_min_log_ts_;
    min_log_exist = is_data_file_valid_;
  }

  return ret;
}

bool ObArchiveDestination::has_current_data_file_been_written() const
{
  return data_file_offset_ > 0;
}

uint64_t ObArchiveDestination::get_last_archived_index_file_id() const
{
  return (index_file_offset_ > 0) ? cur_index_file_id_ : (cur_index_file_id_ - 1);
}

uint64_t ObArchiveDestination::get_last_archived_data_file_id() const
{
  return (data_file_offset_ > 0) ? cur_data_file_id_ : (cur_data_file_id_ - 1);
}

}  // namespace archive
}  // namespace oceanbase
