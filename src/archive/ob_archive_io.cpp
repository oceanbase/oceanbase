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

#include "ob_archive_io.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_archive_util.h"
#include "ob_archive_path.h"
#include "ob_archive_file_utils.h"
#include "share/ob_debug_sync.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
namespace oceanbase {
namespace archive {
ObArchiveIO::ObArchiveIO(const common::ObString& storage_info) : storage_info_(storage_info)
{}

int ObArchiveIO::get_index_file_range(
    const ObPGKey& pg_key, const int64_t incarnation, const int64_t round, uint64_t& min_file_id, uint64_t& max_file_id)
{
  int ret = OB_SUCCESS;
  min_file_id = 0;
  max_file_id = 0;

  if (OB_UNLIKELY(!pg_key.is_valid() || 0 >= incarnation || 0 >= round)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(incarnation), K(round));
  } else if (OB_FAIL(
                 get_file_range_(pg_key, LOG_ARCHIVE_FILE_TYPE_INDEX, incarnation, round, min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "get_file_range_ fail", K(ret), K(pg_key), K(incarnation), K(round));
  }

  return ret;
}

// get data file range by list
int ObArchiveIO::get_data_file_range(
    const ObPGKey& pg_key, const int64_t incarnation, const int64_t round, uint64_t& min_file_id, uint64_t& max_file_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pg_key.is_valid() || 0 >= incarnation || 0 >= round)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(incarnation), K(round));
  } else if (OB_FAIL(
                 get_file_range_(pg_key, LOG_ARCHIVE_FILE_TYPE_DATA, incarnation, round, min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "get_file_range_ fail", K(ret), K(pg_key), K(incarnation), K(round));
  }

  return ret;
}

int ObArchiveIO::get_file_range_(const ObPGKey& pg_key, LogArchiveFileType file_type, const int64_t incarnation,
    const int64_t round, uint64_t& min_file_id, uint64_t& max_file_id)
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH];
  ObArchivePathUtil path_util;
  min_file_id = 0;
  max_file_id = 0;

  if (OB_FAIL(path_util.build_archive_file_prefix(pg_key, file_type, incarnation, round, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_prefix fail", K(ret), K(pg_key), K(incarnation), K(round));
  } else {
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_file_range(uri, storage_info_, min_file_id, max_file_id))) {
      ARCHIVE_LOG(WARN, "get_file_range fail", K(ret), K(pg_key), K(file_type));
    }
  }

  return ret;
}

int ObArchiveIO::get_max_archived_index_info(const ObPGKey& pg_key, const int64_t incarnation, const int64_t round,
    const uint64_t min_index_file_id, const uint64_t max_index_file_id, MaxArchivedIndexInfo& info)
{
  int ret = OB_SUCCESS;
  uint64_t index_file_id = max_index_file_id;
  ObArchivePathUtil path_util;
  ObArchiveFileUtils utils;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(min_index_file_id > max_index_file_id) || OB_UNLIKELY(0 == min_index_file_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid arguments",
        KR(ret),
        K(pg_key),
        K(incarnation),
        K(round),
        K(min_index_file_id),
        K(max_index_file_id));
  } else {
    do {
      char path[MAX_PATH_LENGTH];
      if (OB_FAIL(path_util.build_archive_file_path(
              pg_key, LOG_ARCHIVE_FILE_TYPE_INDEX, index_file_id, incarnation, round, MAX_PATH_LENGTH, path))) {
        ARCHIVE_LOG(WARN,
            "build_archive_file_path fail",
            K(ret),
            K(pg_key),
            K(incarnation),
            K(round),
            K(min_index_file_id),
            K(max_index_file_id),
            K(index_file_id));
      } else {
        ObString uri(path);
        if (OB_FAIL(utils.get_max_archived_info_in_single_index_file(uri, storage_info_, info)) &&
            OB_ENTRY_NOT_EXIST != ret) {
          ARCHIVE_LOG(
              WARN, "get_max_archived_info_in_single_index_file fail", K(pg_key), K(ret), K(uri), K(storage_info_));
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          ARCHIVE_LOG(WARN, "index file not exist", KR(ret), K(pg_key), K(index_file_id), K(uri), K(storage_info_));
          ret = OB_SUCCESS;
        } else {
          ARCHIVE_LOG(INFO,
              "finish extracting archived info from index file",
              K(pg_key),
              K(index_file_id),
              K(info),
              K(uri),
              K(storage_info_));
        }
      }
      index_file_id--;
    } while (OB_SUCC(ret) && index_file_id >= min_index_file_id && (!info.is_index_info_collected_()));
  }

  if (OB_SUCC(ret)) {
    ARCHIVE_LOG(
        INFO, "after get_max_archive_index_info", K(pg_key), K(min_index_file_id), K(max_index_file_id), K(info));
  }

  return ret;
}

int ObArchiveIO::push_log(const char* path, const int64_t data_len, char* data, const bool new_file,
    const bool compatible, const bool is_data_file, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  bool is_write_succ = true;
  bool lock_mode = !compatible;  // use file lock in normal mode, otherwise not use file lock
  StorageOpenMode mode = lock_mode ? StorageOpenMode::CREATE_OPEN_LOCK
                                   : (new_file ? StorageOpenMode::EXCLUSIVE_CREATE : StorageOpenMode::ONLY_OPEN_UNLOCK);
  ObStorageAppender lock_appender;
  ObStorageAppender excl_appender(mode);
  ObStorageAppender* appender = lock_mode ? &lock_appender : &excl_appender;
  ObString uri(path);
  ObStorageAppender::AppenderParam param;

  DEBUG_SYNC(LOG_ARCHIVE_PUSH_LOG);

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_LOG_ARCHIVE_BEFORE_PUSH_LOG_FAILED) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(data_len < 0) || NULL == path) {
    ARCHIVE_LOG(WARN, "invalid argument", K(data), K(data_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_append_param_(is_data_file, epoch, param))) {
    ARCHIVE_LOG(WARN, "get appender param failed", K(ret), K(new_file), K(uri));
  } else if (OB_FAIL(appender->open(uri, storage_info_, param))) {
    ARCHIVE_LOG(WARN, "storage appender open fail", K(ret), K(new_file), K(uri));
  } else if (OB_FAIL(appender->write(data, data_len))) {
    ARCHIVE_LOG(WARN, "storage appender write fail", K(ret), K(uri), K(storage_info_), K(data), K(data_len));
    is_write_succ = false;
  } else if (OB_FAIL(appender->close())) {
    ARCHIVE_LOG(WARN, "storage appender close fail", K(ret), K(uri), K(storage_info_));
  }

  if (!is_write_succ) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = appender->close())) {
      ARCHIVE_LOG(WARN, "storage appender close fail", K(tmp_ret));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_LOG_ARCHIVE_PUSH_LOG_FAILED) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObArchiveIO::check_and_make_dir(const ObPGKey& pg_key, ObString& uri)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(util.mkdir(uri, storage_info_))) {
    ARCHIVE_LOG(WARN, "mkdir fail", K(ret), K(pg_key), K(uri), K(storage_info_));
  } else {
    ARCHIVE_LOG(INFO, "mk dir succ", K(pg_key), K(uri));
  }

  return ret;
}

int ObArchiveIO::check_file_exist(const ObPGKey& pg_key, const char* path, bool& file_exist)
{
  int ret = OB_SUCCESS;
  file_exist = false;
  ObStorageUtil util(false /*need retry*/);
  ObString uri(path);

  if (OB_FAIL(util.is_exist(uri, storage_info_, file_exist))) {
    ARCHIVE_LOG(WARN, "check file is_exist fail", K(ret), K(pg_key), K(uri));
  }

  return ret;
}

int ObArchiveIO::get_append_param_(
    const bool is_data_file, const int64_t epoch, ObStorageAppender::AppenderParam& param)
{
  int ret = OB_SUCCESS;
  param.strategy_ = is_data_file ? ObAppendStrategy::OB_APPEND_USE_SLICE_PUT : ObAppendStrategy::OB_APPEND_USE_OVERRITE;
  // Clog data file use slice put for COS, and open slice version. If backuo storage type is not COS,
  // the following parameters will not work.
  if (ObAppendStrategy::OB_APPEND_USE_SLICE_PUT == param.strategy_) {
    param.version_param_.open_object_version_ = true;
    if (0 >= epoch) {
      ret = OB_ERR_SYS;
      ARCHIVE_LOG(WARN, "epoch is invalid", K(ret), K(epoch), K(is_data_file));
    } else {
      param.version_param_.version_ = epoch;
    }
  } else {
    param.version_param_.open_object_version_ = false;
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
