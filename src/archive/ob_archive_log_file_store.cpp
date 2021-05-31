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
using namespace share;
using namespace common;
using namespace share;

namespace archive {
ObArchiveLogFileStore::ObArchiveLogFileStore() : inited_(false), storage_info_(), restore_info_()
{}

ObArchiveLogFileStore::~ObArchiveLogFileStore()
{}

int ObArchiveLogFileStore::init_by_restore_info(const ObPhysicalRestoreInfo& restore_info)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore has been initialized", K(ret));
  } else if (OB_UNLIKELY(!restore_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(restore_info));
  } else if (OB_FAIL(dest.set(restore_info.backup_dest_))) {
    ARCHIVE_LOG(WARN, "set backup_dest fail", K(ret), K(restore_info));
  } else if (OB_FAIL(init(dest.root_path_,
                 dest.storage_info_,
                 restore_info.cluster_name_,
                 restore_info.cluster_id_,
                 restore_info.tenant_id_,
                 restore_info.incarnation_,
                 restore_info.log_archive_round_))) {
    ARCHIVE_LOG(WARN, "init fail", K(ret), K(restore_info));
  }
  return ret;
}

int ObArchiveLogFileStore::init(const char* root_path, const char* storage_info, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t incarnation, const int64_t archive_round)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest dest;
  ObArchivePathUtil util;

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
  } else if (OB_FAIL(util.build_base_path(root_path,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 incarnation,
                 archive_round,
                 MAX_PATH_LENGTH,
                 base_path_))) {
    ARCHIVE_LOG(WARN,
        "build_base_path fail",
        K(ret),
        K(root_path),
        K(cluster_name),
        K(cluster_id),
        K(tenant_id),
        K(incarnation),
        K(archive_round),
        K(base_path_));
  } else {
    MEMCPY(storage_info_, storage_info, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
    inited_ = true;
  }

  return ret;
}

// 1. If the log id can be located in the index file, the smallest data file containing the log id will be returned
// 2. If the log id is greater than the record recorded in the index, return the largest
//    data file recorded in the index file and containing valid data.
//    example: the log id is 100, and five index info exist, they are ([1,30] - 1), ([31, 60] - 2),
//    ([61, 90] - 3), (invalid - 4), (invalid - 5), then file id 4 is returned
int ObArchiveLogFileStore::locate_file_by_log_id(const ObPGKey& pg_key, const uint64_t log_id, uint64_t& file_id)
{
  int ret = OB_SUCCESS;
  ObString storage_info(storage_info_);
  // The max valid index info not bigger than log id
  ObArchiveIndexFileInfo max_valid_index_info;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_id));
  } else if (OB_FAIL(search_log_in_index_files_(pg_key, log_id, file_id, max_valid_index_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ARCHIVE_LOG(WARN, "log id not found in index files", K(ret), K(pg_key), K(log_id));
    } else {
      ARCHIVE_LOG(WARN, "search_log_id_in_index_file_ fail", K(ret), K(pg_key), K(log_id));
    }
  } else if (max_valid_index_info.min_log_id_ == log_id) {
  } else {
    ARCHIVE_LOG(INFO, "locate_file_by_log_id  succ", K(pg_key), K(log_id), K(file_id));
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // 1. valid index info not exist
    if (!max_valid_index_info.is_valid()) {
      int tmp_ret = OB_SUCCESS;
      bool exist_file_unrecord = false;
      uint64_t max_file_id = 0;
      if (OB_SUCCESS !=
              (tmp_ret = get_max_data_file_unrecord_(pg_key, storage_info, max_file_id, exist_file_unrecord)) &&
          OB_ERR_UNEXPECTED != tmp_ret) {
        ARCHIVE_LOG(WARN, "get_max_data_file_unrecord_ fail", K(tmp_ret), K(pg_key), K(storage_info));
      } else if (OB_ERR_UNEXPECTED == tmp_ret) {
        // overwrite ret
        ret = tmp_ret;
        ARCHIVE_LOG(ERROR, "get_max_data_file_unrecord_ fail", K(ret), K(pg_key));
      } else if (exist_file_unrecord) {
        file_id = max_file_id;
        ret = OB_SUCCESS;
        ARCHIVE_LOG(INFO, "no valid index info, return max data file id", K(ret), K(pg_key), K(log_id), K(file_id));
      }
    }
    // 2. min log id in index info > the log id
    else if (max_valid_index_info.min_log_id_ > log_id) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(
          ERROR, "max_valid_index_info min_log_id_ bigger than log_id", K(pg_key), K(log_id), K(max_valid_index_info));
    }
    // 3. max log id in index info < the log id
    else if (max_valid_index_info.max_log_id_ < log_id) {
      ret = OB_SUCCESS;
      file_id = max_valid_index_info.data_file_id_;
      if (max_valid_index_info.max_log_id_ + 1 < log_id) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS !=
            (tmp_ret = check_file_exist_(pg_key, max_valid_index_info.data_file_id_ + 1, LOG_ARCHIVE_FILE_TYPE_DATA))) {
          ARCHIVE_LOG(WARN, "check_file_exist_ fail", KR(ret), K(pg_key), K(max_valid_index_info));
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
int ObArchiveLogFileStore::locate_file_by_log_id_for_clear(
    const ObPGKey& pg_key, const uint64_t log_id, uint64_t& index_file_id, uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_index_file_id = 0;
  uint64_t max_index_file_id = 0;
  ObString storage_info(storage_info_);
  int64_t unused_log_ts = OB_INVALID_TIMESTAMP;
  bool by_log_id = true;
  uint64_t target_index_file_id = 0;
  index_file_id = 0;
  data_file_id = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_id));
  } else if (OB_FAIL(get_index_file_id_range(pg_key, min_index_file_id, max_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_FAIL(get_target_index_file_for_clear_(pg_key,
                 log_id,
                 unused_log_ts,
                 by_log_id,
                 min_index_file_id,
                 max_index_file_id,
                 target_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_target_index_file_for_clear_ fail", KR(ret), K(pg_key), K(log_id));
  } else if (OB_FAIL(get_safe_data_file_for_clear_(
                 pg_key, log_id, unused_log_ts, by_log_id, target_index_file_id, data_file_id))) {
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

int ObArchiveLogFileStore::locate_file_by_log_ts_for_clear(
    const ObPGKey& pg_key, const int64_t log_ts, uint64_t& index_file_id, uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_index_file_id = 0;
  uint64_t max_index_file_id = 0;
  ObString storage_info(storage_info_);
  uint64_t unused_log_id = OB_INVALID_ID;
  bool by_log_id = false;
  uint64_t target_index_file_id = 0;
  index_file_id = 0;
  data_file_id = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_ts));
  } else if (OB_FAIL(get_index_file_id_range(pg_key, min_index_file_id, max_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_FAIL(get_target_index_file_for_clear_(pg_key,
                 unused_log_id,
                 log_ts,
                 by_log_id,
                 min_index_file_id,
                 max_index_file_id,
                 target_index_file_id))) {
    ARCHIVE_LOG(WARN, "get_target_index_file_for_clear_ fail", KR(ret), K(pg_key), K(log_ts));
  } else if (OB_FAIL(get_safe_data_file_for_clear_(
                 pg_key, unused_log_id, log_ts, by_log_id, target_index_file_id, data_file_id))) {
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

int ObArchiveLogFileStore::get_index_file_id_range(const ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id)
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
  char path[MAX_PATH_LENGTH];
  ObArchivePathUtil path_util;
  ObString storage_info;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret), K(param));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(param));
  } else if (OB_FAIL(path_util.build_file_path(
                 param.pg_key_, base_path_, LOG_ARCHIVE_FILE_TYPE_DATA, param.file_id_, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(param));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_);
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
int ObArchiveLogFileStore::get_pg_max_archived_info(
    const ObPGKey& pg_key, uint64_t& max_log_id, int64_t& max_checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObArchiveIndexFileInfo info;
  bool info_exist = false;
  uint64_t target_data_file_id = 0;
  bool target_data_file_exist = false;
  bool log_exist = false;

  // 1. get max archived info from index file
  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(get_max_archived_index_info_(pg_key, info, info_exist))) {
    ARCHIVE_LOG(WARN, "get_max_archived_index_info_ fail", K(ret), K(pg_key));
  } else if (!info_exist) {
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
  } else {
    log_exist = true;
    max_log_id = info.max_log_id_;
    max_checkpoint_ts = info.max_checkpoint_ts_;
    target_data_file_id = info.data_file_id_ + 1;
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

  // 2. get max archived info in unrecorded data file
  if (OB_SUCC(ret) && target_data_file_exist) {
    int tmp_ret = OB_SUCCESS;
    uint64_t tmp_log_id = OB_INVALID_ID;
    int64_t tmp_checkpoint_ts = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS != (tmp_ret = get_max_archived_info_from_data_file_(
                           pg_key, target_data_file_id, tmp_log_id, tmp_checkpoint_ts)) &&
        OB_ENTRY_NOT_EXIST != tmp_ret) {
      ARCHIVE_LOG(WARN, "get_max_archived_info_from_data_file_ fail", K(pg_key), K(target_data_file_id));
      ret = tmp_ret;
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // do nothing
    } else {
      max_log_id = tmp_log_id;
      max_checkpoint_ts = tmp_checkpoint_ts;
      log_exist = true;
    }
  }

  if (OB_SUCC(ret) && !log_exist) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObArchiveLogFileStore::get_file_id_range_(
    const ObPGKey& pg_key, const LogArchiveFileType file_type, uint64_t& min_file_id, uint64_t& max_file_id)
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH] = {'0'};
  ObString storage_info(storage_info_);
  ObArchivePathUtil path_util;

  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(path_util.build_file_prefix(pg_key, base_path_, file_type, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_prefix fail", K(ret), K(pg_key));
  } else {
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_file_range(uri, storage_info, min_file_id, max_file_id))) {
      ARCHIVE_LOG(WARN, "failed get_file_range", K(uri), K(ret), K(pg_key));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_data_file_unrecord_(
    const ObPGKey& pg_key, ObString& storage_info, uint64_t& file_id, bool& file_exist)
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH] = {'0'};
  ObArchivePathUtil path_util;

  if (OB_FAIL(path_util.build_file_prefix(pg_key, base_path_, LOG_ARCHIVE_FILE_TYPE_DATA, MAX_PATH_LENGTH, path))) {
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

int ObArchiveLogFileStore::search_log_in_index_files_(
    const ObPGKey& pg_key, const uint64_t log_id, uint64_t& data_file_id, ObArchiveIndexFileInfo& max_valid_index_info)
{
  int ret = OB_SUCCESS;
  bool log_exist = false;
  uint64_t min_file_id = 0;
  uint64_t max_file_id = 0;

  if (OB_FAIL(get_index_file_id_range(pg_key, min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_UNLIKELY(min_file_id > max_file_id) || OB_UNLIKELY(0 >= min_file_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid index file id", K(ret), K(pg_key), K(min_file_id), K(max_file_id));
  } else {
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
    const uint64_t index_file_id, uint64_t& data_file_id, bool& log_exist, ObArchiveIndexFileInfo& max_valid_index_info)
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH];
  ObArchivePathUtil path_util;

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_, LOG_ARCHIVE_FILE_TYPE_INDEX, index_file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(index_file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.search_log_in_single_index_file(
            uri, storage_info, log_id, data_file_id, log_exist, max_valid_index_info))) {
      ARCHIVE_LOG(WARN, "search_log_in_single_index_file fail", K(ret), K(pg_key), K(log_id), K(index_file_id));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::check_file_exist_(
    const ObPGKey& pg_key, const uint64_t file_id, const LogArchiveFileType file_type)
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_LENGTH];
  ObArchivePathUtil path_util;
  bool file_exist = false;

  if (OB_FAIL(path_util.build_file_path(pg_key, base_path_, file_type, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_);
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
    const int64_t log_ts, const bool by_log_id, const uint64_t min_index_file_id, const uint64_t max_index_file_id,
    uint64_t& target_index_file_id)
{
  int ret = OB_SUCCESS;
  uint64_t file_id = min_index_file_id;
  ObArchiveIndexFileInfo info;
  bool done = false;
  target_index_file_id = 0;

  for (; file_id <= max_index_file_id && OB_SUCC(ret) && !done; file_id++) {
    bool exist = false;
    if (OB_FAIL(get_max_archive_info_in_single_index_file_(pg_key, file_id, info, exist))) {
      ARCHIVE_LOG(WARN, "get_max_archive_info fail", K(ret), K(pg_key), K(file_id));
    } else if (!exist) {
      ARCHIVE_LOG(INFO, "no index info exist", K(pg_key), K(file_id));
    } else {
      target_index_file_id = file_id;
      done = by_log_id ? info.max_log_id_ >= log_id : info.max_log_submit_ts_ >= log_ts;
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
    const int64_t log_ts, const bool by_log_id, const uint64_t index_file_id, uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};

  if (OB_FAIL(path_util.build_file_path(
          pg_key, base_path_, LOG_ARCHIVE_FILE_TYPE_INDEX, index_file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(index_file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_max_safe_data_file_id(uri, storage_info, log_id, log_ts, by_log_id, data_file_id))) {
      ARCHIVE_LOG(
          WARN, "get_max_safe_data_file_id fail", K(ret), K(uri), K(storage_info), K(log_id), K(log_ts), K(by_log_id));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archive_info_in_single_index_file_(
    const ObPGKey& pg_key, const uint64_t file_id, ObArchiveIndexFileInfo& info, bool& exist)
{
  int ret = OB_SUCCESS;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};
  ObString storage_info(storage_info_);
  ObArchiveFileUtils utils;

  if (OB_FAIL(
          path_util.build_file_path(pg_key, base_path_, LOG_ARCHIVE_FILE_TYPE_INDEX, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    if (OB_FAIL(utils.get_max_index_info_in_single_file(uri, storage_info, info, exist))) {
      ARCHIVE_LOG(WARN, "get_max_index_info_in_single_file fail", K(ret), K(pg_key));
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archived_index_info_(
    const ObPGKey& pg_key, ObArchiveIndexFileInfo& info, bool& exist)
{
  int ret = OB_SUCCESS;
  uint64_t min_file_id = 0;
  uint64_t max_file_id = 0;
  exist = false;

  if (OB_FAIL(get_index_file_id_range(pg_key, min_file_id, max_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_index_file_id_range fail", K(ret), K(pg_key));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    exist = false;
    ret = OB_SUCCESS;
  } else {
    uint64_t file_id = max_file_id;
    for (; file_id >= min_file_id && OB_SUCC(ret) && !exist; file_id--) {
      if (OB_FAIL(get_max_archive_info_in_single_index_file_(pg_key, file_id, info, exist))) {
        ARCHIVE_LOG(WARN, "get_max_archive_info fail", K(ret), K(file_id));
      } else if (!exist) {
        ARCHIVE_LOG(INFO, "no index info exist", K(pg_key), K(file_id));
      } else {
        ARCHIVE_LOG(INFO, "get_max_archived_index_info_ succ", K(pg_key), K(file_id), K(info));
      }
    }
  }

  return ret;
}

int ObArchiveLogFileStore::get_max_archived_info_from_data_file_(
    const ObPGKey& pg_key, const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(direct_extract_last_log_(pg_key, file_id, max_log_id, max_checkpoint_ts))) {
    ARCHIVE_LOG(INFO, "direct_extract_last_log_ succ", K(pg_key), K(file_id), K(max_log_id), K(max_checkpoint_ts));
  } else if (OB_SUCC(iterate_max_archived_info_(pg_key, file_id, max_log_id, max_checkpoint_ts))) {
    ARCHIVE_LOG(INFO, "iterate_max_archived_info_ succ", K(pg_key), K(file_id), K(max_log_id), K(max_checkpoint_ts));
  } else {
    ARCHIVE_LOG(WARN, "get_max_archived_info_from_data_file_ fail", K(pg_key), K(file_id));
  }

  return ret;
}

int ObArchiveLogFileStore::direct_extract_last_log_(
    const ObPGKey& pg_key, const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObArchiveBlockMeta block_meta;
  clog::ObLogEntry unused_log_entry;
  ObLogArchiveInnerLog unused_inner_log;
  ObArchivePathUtil path_util;
  char path[MAX_PATH_LENGTH] = {0};

  if (OB_FAIL(
          path_util.build_file_path(pg_key, base_path_, LOG_ARCHIVE_FILE_TYPE_DATA, file_id, MAX_PATH_LENGTH, path))) {
    ARCHIVE_LOG(WARN, "build_file_path fail", K(ret), K(pg_key), K(file_id));
  } else {
    ObString uri(path);
    ObString storage_info(storage_info_);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.extract_last_log_in_data_file(
            pg_key, file_id, this, uri, storage_info, block_meta, unused_log_entry, unused_inner_log))) {
      ARCHIVE_LOG(WARN, "extract_last_log_in_data_file fail", K(ret), K(pg_key), K(file_id), K(uri), K(storage_info));
    } else {
      max_log_id = block_meta.max_log_id_;
      max_checkpoint_ts = block_meta.max_checkpoint_ts_;
    }
  }

  return ret;
}

int ObArchiveLogFileStore::iterate_max_archived_info_(
    const ObPGKey& pg_key, const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts)
{
  int ret = OB_SUCCESS;
  const int64_t TIMEOUT = 60 * 1000 * 1000L;
  const bool need_limit_bandwidth = false;
  bool exist_log = false;
  ObArchiveEntryIterator iter;

  if (OB_FAIL(iter.init(this, pg_key, file_id, 0, TIMEOUT, need_limit_bandwidth))) {
    ARCHIVE_LOG(WARN, "ObArchiveEntryIterator init fail", K(pg_key), K(file_id));
  } else {
    clog::ObLogEntry log_entry;
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = iter.next_entry(log_entry))) {
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
    ret = OB_SUCCESS;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

}  // end of namespace archive
}  // end of namespace oceanbase
