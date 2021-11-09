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

#include "ob_archive_log_file_store.h"
#include "ob_log_archive_struct.h"
#include "clog/ob_log_reader_interface.h"
#include "ob_archive_file_utils.h"
#include "ob_archive_destination_mgr.h"
#include "ob_archive_path.h"
#include "ob_archive_entry_iterator.h"  // ObArchiveEntryIterator
namespace oceanbase {
using namespace clog;
using namespace common;
using namespace share;

namespace archive {
ObArchiveLogFileStore::ObArchiveLogFileStore() : inited_(false), storage_info_()
{}

ObArchiveLogFileStore::~ObArchiveLogFileStore()
{}

int ObArchiveLogFileStore::init(const share::ObBackupPiecePath& base_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore has been initialized", K(ret));
  } else if (OB_UNLIKELY(base_path.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid base path", K(ret), K(base_path));
  } else if (OB_FAIL(dest.set(base_path.ptr()))) {
    ARCHIVE_LOG(WARN, "set backup_dest fail", K(ret), K(base_path));
  } else if (OB_FAIL(storage_info_.assign(dest.storage_info_))) {
    ARCHIVE_LOG(WARN, "failed to assign storage_info_", K(ret), K(base_path));
  } else if (OB_FAIL(base_path_.assign(dest.root_path_))) {
    ARCHIVE_LOG(WARN, "failed to assign base_path_", K(ret), K(base_path));
  } else {
    inited_ = true;
    ARCHIVE_LOG(INFO, "init archive log file store succ", K(base_path), K(dest));
  }
  return ret;
}

int ObArchiveLogFileStore::init(const char* root_path, const char* storage_info, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t incarnation, const int64_t archive_round,
    const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest dest;
  ObArchivePathUtil util;
  char base_path[MAX_PATH_LENGTH] = {0};
  ObBackupDest backup_dest;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore has been initialized", K(ret));
  } else if (OB_ISNULL(root_path) || OB_ISNULL(storage_info) || OB_ISNULL(cluster_name) ||
             OB_UNLIKELY(cluster_id < 1) ||
             OB_UNLIKELY(OB_INVALID_ID == tenant_id || incarnation < 1 || archive_round < 1)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        K(ret),
        KP(root_path),
        KP(storage_info),
        KP(cluster_name),
        K(cluster_id),
        K(tenant_id),
        K(incarnation),
        K(archive_round));

  } else if (OB_FAIL(backup_dest.set(root_path, storage_info))) {
    ARCHIVE_LOG(WARN, "failed to set backup dest", K(ret), K(root_path), K(storage_info));
  } else if (OB_FAIL(util.build_base_path(backup_dest,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 incarnation,
                 archive_round,
                 piece_id,
                 piece_create_date,
                 MAX_PATH_LENGTH,
                 base_path))) {
    ARCHIVE_LOG(WARN,
        "build_base_path fail",
        K(ret),
        K(root_path),
        K(cluster_name),
        K(cluster_id),
        K(tenant_id),
        K(incarnation),
        K(archive_round));
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    ARCHIVE_LOG(WARN, "failed to assign storage_info_", K(ret));
  } else if (OB_FAIL(base_path_.assign(base_path))) {
    ARCHIVE_LOG(WARN, "failed to assign base_path_", K(ret), K(base_path));
  } else {
    inited_ = true;
    ARCHIVE_LOG(INFO, "init archive log file store succ", K(root_path), K(storage_info));
  }

  return ret;
}

// 1. If the log id can be located in the index file, the smallest data file containing the log id will be returned
// 2. If the log id is greater than the record recorded in the index, return the largest
//    data file recorded in the index file and containing valid data.
//    example: the log id is 100, and five index info exist, they are ([1,30] - 1), ([31, 60] - 2),
//    ([61, 90] - 3), (invalid - 4), (invalid - 5), then file id 4 is returned
int ObArchiveLogFileStore::locate_file_by_log_id(
    const ObPGKey& pg_key, const uint64_t log_id, uint64_t& file_id, bool& archive_file_exist) const
{
  int ret = OB_SUCCESS;
  ObString storage_info(storage_info_.ptr());
  ObArchiveIndexFileInfo max_valid_index_info;
  archive_file_exist = true;
  bool index_file_exist = false;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_id));
  } else if (OB_FAIL(search_log_in_index_files_(pg_key, log_id, file_id, max_valid_index_info, index_file_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ARCHIVE_LOG(WARN, "log id not found in index files", K(ret), K(pg_key), K(log_id));
    } else {
      ARCHIVE_LOG(WARN, "search_log_id_in_index_file_ fail", K(ret), K(pg_key), K(log_id));
    }
  } else {
    ARCHIVE_LOG(INFO, "locate_file_by_log_id  succ", K(pg_key), K(log_id), K(file_id));
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // 1. valid index info not exist
    if (!max_valid_index_info.is_effective()) {
      int tmp_ret = OB_SUCCESS;
      bool exist_file_unrecord = false;
      uint64_t max_file_id = 0;
      if (OB_SUCCESS !=
          (tmp_ret = get_max_data_file_unrecord_(pg_key, storage_info, max_file_id, exist_file_unrecord))) {
        ARCHIVE_LOG(WARN, "get_max_data_file_unrecord_ fail", KR(tmp_ret), K(pg_key), K(storage_info));
        // overwrite ret
        ret = tmp_ret;
      } else if (exist_file_unrecord) {
        file_id = max_file_id;
        ret = OB_SUCCESS;
        ARCHIVE_LOG(INFO, "no valid index info, return max data file id", KR(ret), K(pg_key), K(log_id), K(file_id));
      } else {
        archive_file_exist = false;
      }
    }
    // 2. min log id in index info > the log id
    else if (max_valid_index_info.min_log_id_ > log_id) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR,
          "max_valid_index_info min_log_id_ bigger than log_id",
          KR(ret),
          K(pg_key),
          K(log_id),
          K(max_valid_index_info));
    }
    // 3. max log id in index info < the log id
    else if (max_valid_index_info.max_log_id_ < log_id) {
      ret = OB_SUCCESS;
      file_id = max_valid_index_info.data_file_id_;
      if (max_valid_index_info.max_log_id_ + 1 < log_id) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = check_file_exist_(pg_key, file_id + 1, LOG_ARCHIVE_FILE_TYPE_DATA))) {
          ARCHIVE_LOG(WARN, "check_file_exist_ fail", KR(tmp_ret), K(pg_key), K(max_valid_index_info));
        } else {
          file_id = max_valid_index_info.data_file_id_ + 1;
        }
      }
    }
  }

  return ret;
}

// return the max safe file id to clean
// locate file only with index files, for example locate log id 10 in index file M and data file N,
// then return M-1 and N-1
//
int ObArchiveLogFileStore::locate_file_by_log_id_for_clear(const ObPGKey& pg_key, const uint64_t log_id,
    const int64_t retention_timestamp, uint64_t& index_file_id, uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_index_file_id = 0;
  uint64_t max_index_file_id = 0;
  ObString storage_info(storage_info_.ptr());
  int64_t unused_log_ts = OB_INVALID_TIMESTAMP;
  bool by_log_id = true;
  uint64_t target_index_file_id = 0;
  index_file_id = 0;
  data_file_id = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == log_id) ||
             OB_UNLIKELY(0 >= retention_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_id), K(retention_timestamp));
  } else if (OB_FAIL(get_index_file_id_range(pg_key, min_index_file_id, max_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_FAIL(get_target_index_file_for_clear_(pg_key,
                 log_id,
                 unused_log_ts,
                 by_log_id,
                 retention_timestamp,
                 min_index_file_id,
                 max_index_file_id,
                 target_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_target_index_file_for_clear_ fail", KR(ret), K(pg_key), K(log_id));
  } else if (OB_FAIL(get_safe_data_file_for_clear_(
                 pg_key, log_id, unused_log_ts, by_log_id, retention_timestamp, target_index_file_id, data_file_id))) {
    ARCHIVE_LOG(WARN, "get_safe_data_file_for_clear_ fail", KR(ret), K(pg_key), K(log_id));
  } else {
    index_file_id = target_index_file_id - 1;
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // rewrite ret
    ret = OB_SUCCESS;
    index_file_id = 0;
    data_file_id = 0;
  }

  return ret;
}

int ObArchiveLogFileStore::locate_file_by_log_ts_for_clear(const ObPGKey& pg_key, const int64_t log_ts,
    const int64_t retention_timestamp, uint64_t& index_file_id, uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_index_file_id = 0;
  uint64_t max_index_file_id = 0;
  uint64_t unused_log_id = OB_INVALID_ID;
  bool by_log_id = false;
  uint64_t target_index_file_id = 0;
  index_file_id = 0;
  data_file_id = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == log_ts) ||
             OB_UNLIKELY(0 >= retention_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_ts), K(retention_timestamp));
  } else if (OB_FAIL(get_index_file_id_range(pg_key, min_index_file_id, max_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_FAIL(get_target_index_file_for_clear_(pg_key,
                 unused_log_id,
                 log_ts,
                 by_log_id,
                 retention_timestamp,
                 min_index_file_id,
                 max_index_file_id,
                 target_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_target_index_file_for_clear_ fail", KR(ret), K(pg_key), K(log_ts));
  } else if (OB_FAIL(get_safe_data_file_for_clear_(
                 pg_key, unused_log_id, log_ts, by_log_id, retention_timestamp, target_index_file_id, data_file_id))) {
    ARCHIVE_LOG(WARN, "get_safe_data_file_for_clear_ fail", KR(ret), K(pg_key), K(log_ts));
  } else {
    index_file_id = target_index_file_id - 1;
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    index_file_id = 0;
    data_file_id = 0;
  }
  return ret;
}

int ObArchiveLogFileStore::get_index_file_id_range(
    const ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret), K(pg_key));
  } else if (OB_FAIL(get_file_id_range_(pg_key, LOG_ARCHIVE_FILE_TYPE_INDEX, min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "failed to get_index_file_id_range", K(ret), K(pg_key));
  } else { /*do nothing*/
  }
  return ret;
}

int ObArchiveLogFileStore::get_data_file_id_range(const ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret), K(pg_key));
  } else if (OB_FAIL(get_file_id_range_(pg_key, LOG_ARCHIVE_FILE_TYPE_DATA, min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "failed to get_data_file_id_range", K(ret), K(pg_key));
  } else { /*do nothing*/
  }
  return ret;
}

int ObArchiveLogFileStore::read_data_direct(const ObArchiveReadParam& param, ObReadBuf& rbuf, ObReadRes& res)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  char path[MAX_PATH_LENGTH] = {0};
  ObArchivePathUtil path_util;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret), K(param));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(param));
  } else if (OB_FAIL(path_util.build_file_path(
                 param.pg_key_, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_DATA, param.file_id_, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(param));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_.ptr());
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.range_read(uri, storage_info, rbuf.buf_, param.read_len_, param.offset_, read_size))) {
      ARCHIVE_LOG(WARN, "range_read fail", K(ret), K(uri), K(storage_info), K(param), K(read_size));
    } else {
      res.buf_ = rbuf.buf_;
      res.data_len_ = read_size;
    }
  }

  return ret;
}

// get pg max archived log id and checkpoint ts
int ObArchiveLogFileStore::get_pg_max_archived_info(const ObPGKey& pg_key, const uint64_t real_tenant_id,
    uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts)
{
  int ret = OB_SUCCESS;
  MaxArchivedIndexInfo info;
  uint64_t target_data_file_id = 0;
  bool target_data_file_exist = false;
  bool log_exist = false;

  // 1. get max archived info from index file
  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(get_max_archived_info_from_index_files_(pg_key, info))) {
    ARCHIVE_LOG(WARN, "get_max_archived_info_from_index_files_ fail", K(ret), K(pg_key));
  }
  // index info exist
  else if (info.data_file_collect_) {
    // log info exist in index files
    if (info.is_index_record_collected()) {
      max_log_id = info.max_record_log_id_;
      max_log_ts = info.max_record_log_submit_ts_;
      max_checkpoint_ts = info.max_record_checkpoint_ts_;
      log_exist = true;
    }

    // check if exist data file unrecorded
    target_data_file_id = info.max_record_data_file_id_ + 1;
    if (OB_FAIL(check_file_exist_(pg_key, target_data_file_id, LOG_ARCHIVE_FILE_TYPE_DATA)) &&
        OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "check_file_exist_ fail", K(ret), K(pg_key), K(target_data_file_id));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      target_data_file_exist = false;
    } else {
      target_data_file_exist = true;
    }
  }
  // not index info extract from index files
  else {
    uint64_t min_data_file_id = 0;
    uint64_t max_data_file_id = 0;
    if (OB_FAIL(get_data_file_id_range(pg_key, min_data_file_id, max_data_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_data_file_id_range fail", K(ret), K(pg_key));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ARCHIVE_LOG(WARN, "no data file exist", K(ret), K(pg_key));
      ret = OB_SUCCESS;
      target_data_file_exist = false;
    } else {
      target_data_file_id = max_data_file_id;
      target_data_file_exist = true;
    }
  }

  // 2. get max archived info in unrecorded data file
  if (OB_SUCC(ret) && target_data_file_exist) {
    int tmp_ret = OB_SUCCESS;
    uint64_t tmp_log_id = OB_INVALID_ID;
    int64_t tmp_checkpoint_ts = OB_INVALID_TIMESTAMP;
    int64_t tmp_max_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS !=
            (tmp_ret = get_max_archived_info_from_data_file_(
                 pg_key, real_tenant_id, target_data_file_id, tmp_log_id, tmp_checkpoint_ts, tmp_max_log_ts)) &&
        OB_ENTRY_NOT_EXIST != tmp_ret) {
      ARCHIVE_LOG(WARN, "get_max_archived_info_from_data_file_ fail", K(pg_key), K(target_data_file_id));
      ret = tmp_ret;
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // do nothing
    } else {
      max_log_id = tmp_log_id;
      max_checkpoint_ts = tmp_checkpoint_ts;
      max_log_ts = tmp_max_log_ts;
      log_exist = true;
    }
  }

  if (OB_SUCC(ret) && !log_exist) {
    // get_max_archived_info from archive_pg_key
    if (OB_FAIL(get_max_archived_info_from_archive_key_(pg_key, max_log_id, max_checkpoint_ts, max_log_ts))) {
      ARCHIVE_LOG(WARN, "failed to get_max_archived_info_from_archive_key", K(pg_key));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_file_id_range_(
    const ObPGKey& pg_key, const LogArchiveFileType file_type, uint64_t& min_file_id, uint64_t& max_file_id) const
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH] = {'0'};
  ObArchivePathUtil path_util;
  ObBackupDest backup_dest;

  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(path_util.build_file_prefix(pg_key, base_path_.ptr(), file_type, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_prefix fail", K(ret), K(pg_key));
  } else {
    ObString storage_info(storage_info_.ptr());
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_file_range(uri, storage_info, min_file_id, max_file_id))) {
      ARCHIVE_LOG(WARN, "failed get_file_range", K(uri), K(ret), K(pg_key));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_archive_key_content(const ObPGKey& pg_key, ObArchiveKeyContent& archive_key_content)
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH] = {'0'};
  ObArchivePathUtil path_util;

  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(path_util.build_archive_key_path(pg_key, base_path_.ptr(), MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, " failed to build_archive_key_path", K(ret), K(pg_key));
  } else {
    ObString storage_info(storage_info_.ptr());
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_archived_info_from_archive_key(uri, storage_info, archive_key_content))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ARCHIVE_LOG(INFO, "archive_key file not exist", K(ret), K(pg_key), K(uri));
      } else {
        ARCHIVE_LOG(WARN, "failed to get_archived_info_from_archive_key", K(uri), K(ret), K(pg_key));
      }
    }
  }
  return ret;
}

int ObArchiveLogFileStore::get_max_data_file_unrecord_(
    const ObPGKey& pg_key, ObString& storage_info, uint64_t& file_id, bool& file_exist) const
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH] = {'0'};
  ObArchivePathUtil path_util;

  if (OB_FAIL(
          path_util.build_file_prefix(pg_key, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_DATA, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_prefix fail", K(ret), K(pg_key));
  } else {
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_max_data_file_when_index_not_exist(uri, storage_info, file_id))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        ARCHIVE_LOG(WARN, "get_max_data_file_when_index_not_exist fail", K(ret), K(pg_key), K(file_id));
      } else {
        ret = OB_SUCCESS;
        file_exist = false;
      }
    } else {
      file_exist = true;
    }
  }

  return ret;
}

int ObArchiveLogFileStore::search_log_in_index_files_(const ObPGKey& pg_key, const uint64_t log_id,
    uint64_t& data_file_id, ObArchiveIndexFileInfo& max_valid_index_info, bool& index_file_exist) const
{
  int ret = OB_SUCCESS;
  bool log_exist = false;
  uint64_t min_file_id = 0;
  uint64_t max_file_id = 0;
  index_file_exist = false;

  if (OB_FAIL(get_index_file_id_range(pg_key, min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_UNLIKELY(min_file_id > max_file_id) || OB_UNLIKELY(0 >= min_file_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid index file id", K(ret), K(pg_key), K(min_file_id), K(max_file_id));
  } else {
    index_file_exist = true;
    uint64_t index_file_id = min_file_id;
    for (; OB_SUCC(ret) && !log_exist && index_file_id <= max_file_id; index_file_id++) {
      if (OB_FAIL(search_log_in_single_index_file_(
              pg_key, log_id, index_file_id, data_file_id, log_exist, max_valid_index_info))) {
        ARCHIVE_LOG(WARN, "search_log_in_single_index_file_ fail", K(ret), K(log_id), K(index_file_id));
      }
    }  // for
  }

  // return OB_ENTRY_NOT_EXIST when log is not found in index files
  if (OB_SUCC(ret) && !log_exist) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObArchiveLogFileStore::search_log_in_single_index_file_(const ObPGKey& pg_key, const uint64_t log_id,
    const uint64_t index_file_id, uint64_t& data_file_id, bool& log_exist,
    ObArchiveIndexFileInfo& max_valid_index_info) const
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH];
  ObArchivePathUtil path_util;

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_INDEX, index_file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(index_file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_.ptr());
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.search_log_in_single_index_file(
            uri, storage_info, log_id, data_file_id, log_exist, max_valid_index_info))) {
      ARCHIVE_LOG(WARN, "search_log_in_single_index_file fail", K(ret), K(pg_key), K(log_id), K(index_file_id));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::check_file_exist_(
    const ObPGKey& pg_key, const uint64_t file_id, const LogArchiveFileType file_type) const
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH];
  ObArchivePathUtil path_util;
  bool file_exist = false;

  if (OB_FAIL(path_util.build_file_path(pg_key, base_path_.ptr(), file_type, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_.ptr());
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.check_file_exist(uri, storage_info, file_exist))) {
      ARCHIVE_LOG(WARN, "check file is_exist fail", KR(ret), K(pg_key), K(file_id));
    } else if (!file_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_target_index_file_for_clear_(const ObPGKey& pg_key, const uint64_t log_id,
    const int64_t log_ts, const bool by_log_id, const int64_t retention_timestamp, const uint64_t min_index_file_id,
    const uint64_t max_index_file_id, uint64_t& target_index_file_id)
{
  int ret = OB_SUCCESS;
  uint64_t file_id = min_index_file_id;
  ObArchiveIndexFileInfo info;
  bool done = false;
  target_index_file_id = 0;

  for (; file_id <= max_index_file_id && OB_SUCC(ret) && !done; file_id++) {
    bool exist = false;
    if (OB_FAIL(get_max_valid_index_info_in_single_index_file_(pg_key, file_id, info, exist))) {
      ARCHIVE_LOG(WARN, "get_max_archive_info fail", K(ret), K(pg_key), K(file_id));
    } else if (!exist) {
      ARCHIVE_LOG(INFO, "no index info exist", K(pg_key), K(file_id));
    } else {
      target_index_file_id = file_id;
      done = (by_log_id ? info.max_log_id_ >= log_id : info.max_log_submit_ts_ >= log_ts) ||
             info.max_log_submit_ts_ >= retention_timestamp;
    }
  }

  if (OB_SUCC(ret)) {
    if (target_index_file_id > 0) {
      ARCHIVE_LOG(INFO,
          "get_safe_index_file_for_clear_ succ",
          K(pg_key),
          K(log_id),
          K(min_index_file_id),
          K(max_index_file_id),
          K(target_index_file_id),
          K(info));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_safe_data_file_for_clear_(const ObPGKey& pg_key, const uint64_t log_id,
    const int64_t log_ts, const bool by_log_id, const int64_t retention_timestamp, const uint64_t index_file_id,
    uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_INDEX, index_file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(index_file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_.ptr());
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_max_safe_data_file_id(
            uri, storage_info, log_id, log_ts, by_log_id, retention_timestamp, data_file_id))) {
      ARCHIVE_LOG(
          WARN, "get_max_safe_data_file_id fail", K(ret), K(uri), K(storage_info), K(log_id), K(log_ts), K(by_log_id));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_valid_index_info_in_single_index_file_(
    const ObPGKey& pg_key, const uint64_t file_id, ObArchiveIndexFileInfo& info, bool& exist)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};
  ObString storage_info(storage_info_.ptr());
  ObArchiveFileUtils utils;

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_INDEX, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    if (OB_FAIL(utils.get_max_valid_index_info_in_single_index_file(uri, storage_info, info, exist))) {
      ARCHIVE_LOG(WARN, "get_max_valid_index_info_in_single_index_file fail", K(ret), K(pg_key));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archived_info_from_index_files_(const ObPGKey& pg_key, MaxArchivedIndexInfo& info)
{
  int ret = OB_SUCCESS;
  uint64_t min_file_id = 0;
  uint64_t max_file_id = 0;

  if (OB_FAIL(get_index_file_id_range(pg_key, min_file_id, max_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ARCHIVE_LOG(WARN, "not index file exist", KR(ret), K(pg_key));
    ret = OB_SUCCESS;
  } else {
    uint64_t file_id = max_file_id;
    for (; file_id >= min_file_id && OB_SUCC(ret) && !info.is_index_record_collected(); file_id--) {
      if (OB_FAIL(get_max_archived_info_from_single_index_file_(pg_key, file_id, info)) && OB_ENTRY_NOT_EXIST != ret) {
        ARCHIVE_LOG(WARN, "get_max_archive_info fail", KR(ret), K(file_id));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ARCHIVE_LOG(WARN, "index file not exist", KR(ret), K(pg_key), K(file_id));
        ret = OB_SUCCESS;
      } else {
        ARCHIVE_LOG(INFO, "get_max_archived_info_from_single_index_file_ succ", K(pg_key), K(file_id), K(info));
      }
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archived_info_from_single_index_file_(
    const ObPGKey& pg_key, const uint64_t file_id, MaxArchivedIndexInfo& info)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};
  ObArchiveFileUtils utils;
  ObString storage_info(storage_info_.ptr());

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_INDEX, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    if (OB_FAIL(utils.get_max_archived_info_in_single_index_file(uri, storage_info, info))) {
      ARCHIVE_LOG(WARN, "get_max_archived_info_in_single_index_file fail", K(ret), K(pg_key));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archived_info_from_data_file_(const ObPGKey& pg_key, const uint64_t real_tenant_id,
    const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(direct_extract_last_log_(pg_key, real_tenant_id, file_id, max_log_id, max_checkpoint_ts, max_log_ts))) {
    ARCHIVE_LOG(INFO,
        "direct_extract_last_log_ succ",
        K(pg_key),
        K(file_id),
        K(max_log_id),
        K(max_checkpoint_ts),
        K(max_log_ts));
  } else if (OB_SUCC(iterate_max_archived_info_(
                 pg_key, real_tenant_id, file_id, max_log_id, max_checkpoint_ts, max_log_ts))) {
    ARCHIVE_LOG(INFO,
        "iterate_max_archived_info_ succ",
        K(pg_key),
        K(file_id),
        K(max_log_id),
        K(max_checkpoint_ts),
        K(max_log_ts));
  } else {
    ARCHIVE_LOG(WARN, "get_max_archived_info_from_data_file_ fail", K(pg_key), K(file_id));
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archived_info_from_archive_key_(
    const ObPGKey& pg_key, uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts)
{
  int ret = OB_SUCCESS;
  ObArchiveKeyContent archive_key_content;
  if (OB_FAIL(get_archive_key_content(pg_key, archive_key_content)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "failed to get_archive_key_content", K(pg_key), K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // do nothing
  } else if (!archive_key_content.check_integrity()) {
    // override with OB_ENTRY_NOT_EXIST
    ret = OB_ENTRY_NOT_EXIST;
  } else if (archive_key_content.is_first_piece_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    max_log_id = archive_key_content.index_info_.max_log_id_;
    max_checkpoint_ts = archive_key_content.index_info_.max_checkpoint_ts_;
    max_log_ts = archive_key_content.index_info_.max_log_submit_ts_;
  }
  return ret;
}

int ObArchiveLogFileStore::direct_extract_last_log_(const ObPGKey& pg_key, const uint64_t real_tenant_id,
    const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts)
{
  int ret = OB_SUCCESS;
  ObArchiveBlockMeta block_meta;
  clog::ObLogEntry unused_log_entry;
  ObLogArchiveInnerLog unused_inner_log;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};
  UNUSED(real_tenant_id);

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_.ptr(), LOG_ARCHIVE_FILE_TYPE_DATA, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_.ptr());
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.extract_last_log_in_data_file(
            pg_key, file_id, this, uri, storage_info, block_meta, unused_log_entry, unused_inner_log))) {
      ARCHIVE_LOG(WARN, "extract_last_log_in_data_file fail", K(ret), K(pg_key), K(file_id), K(uri), K(storage_info));
    } else {
      max_log_id = block_meta.max_log_id_;
      max_checkpoint_ts = block_meta.max_checkpoint_ts_;
      max_log_ts = block_meta.max_log_submit_ts_;
    }
  }

  return ret;
}

int ObArchiveLogFileStore::iterate_max_archived_info_(const ObPGKey& pg_key, const uint64_t real_tenant_id,
    const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts)
{
  int ret = OB_SUCCESS;
  const int64_t TIMEOUT = 60 * 1000 * 1000L;
  const bool need_limit_bandwidth = false;
  bool exist_log = false;
  ObArchiveEntryIterator iter;
  bool unused_flag = false;
  int64_t unused_accum_checksum = 0;
  UNUSED(real_tenant_id);

  if (OB_FAIL(iter.init(this, pg_key, file_id, 0, TIMEOUT, need_limit_bandwidth))) {
    ARCHIVE_LOG(WARN, "ObArchiveEntryIterator init fail", K(pg_key), K(file_id));
  } else {
    clog::ObLogEntry log_entry;
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = iter.next_entry(log_entry, unused_flag, unused_accum_checksum))) {
      exist_log = true;
    }
  }

  // rewrite ret
  if (OB_ITER_END != ret) {
    ARCHIVE_LOG(WARN, "iterate_max_archived_info_ fail", K(ret), K(pg_key), K(file_id));
  } else if (exist_log) {
    const ObArchiveBlockMeta& block_meta = iter.get_last_block_meta();
    max_log_id = block_meta.max_log_id_;
    max_checkpoint_ts = block_meta.max_checkpoint_ts_;
    max_log_ts = block_meta.max_log_submit_ts_;
    ret = OB_SUCCESS;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

}  // end of namespace archive
}  // end of namespace oceanbase
