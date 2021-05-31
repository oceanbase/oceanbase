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

#include "ob_archive_file_utils.h"
#include "lib/restore/ob_storage.h"
#include "ob_log_archive_struct.h"
#include "ob_archive_util.h"
#include "clog/ob_log_type.h"
#include "ob_archive_log_file_store.h"  // ObIArchiveLogFileStore
#include "ob_archive_entry_iterator.h"

namespace oceanbase {
using namespace common;
using namespace clog;
namespace archive {
int ObArchiveFileUtils::get_file_range(
    const ObString& prefix, const ObString& storage_info, uint64_t& min_file_id, uint64_t& max_file_id)
{
  int ret = OB_SUCCESS;
  bool file_match = false;
  int64_t file_num = 0;
  min_file_id = 0;
  max_file_id = 0;
  ObStorageUtil util(false /*need retry*/);
  ObArray<common::ObString> file_names;
  ObArenaAllocator allocator;

  if (OB_FAIL(util.list_files(prefix, storage_info, allocator, file_names))) {
    ARCHIVE_LOG(WARN, "list_files fail", K(ret), K(prefix));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < file_names.count(); idx++) {
      ObString file_name = file_names.at(idx);
      uint64_t tmp_file_id = 0;
      file_match = false;
      if (OB_FAIL(extract_file_id_(file_name, tmp_file_id, file_match))) {
        ARCHIVE_LOG(WARN, "extract_file_id_ fail", K(ret), K(file_name), K(tmp_file_id));
      } else if (OB_UNLIKELY(!file_match)) {
        // file_name not match, skip
      } else {
        ARCHIVE_LOG(DEBUG, "extract_file_id_ succ", K(file_name), K(tmp_file_id));
        min_file_id = min_file_id == 0 ? tmp_file_id : std::min(min_file_id, tmp_file_id);
        max_file_id = std::max(max_file_id, tmp_file_id);
        file_num++;
      }
    }  // for
  }

  if (OB_SUCC(ret) && 0 == file_num) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(DEBUG, "no files", K(prefix));
  }

  return ret;
}

int ObArchiveFileUtils::get_file_length(const ObString& uri, const ObString& storage_info, int64_t& file_len)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(util.get_file_length(uri, storage_info, file_len))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri), K(storage_info));
    } else {
      // rewrite ret
      ret = OB_ENTRY_NOT_EXIST;
      ARCHIVE_LOG(INFO, "get_file_length fail", K(ret), K(uri), K(storage_info));
    }
  }

  return ret;
}

int ObArchiveFileUtils::read_single_file(
    const ObString& uri, const ObString& storage_info, char* buf, const int64_t text_file_length, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(util.read_single_file(uri, storage_info, buf, text_file_length, read_size))) {
    ARCHIVE_LOG(WARN, "read_single_file fail", K(ret), K(text_file_length), K(uri));
  }

  return ret;
}

int ObArchiveFileUtils::search_log_in_single_index_file(const ObString& uri, const ObString& storage_info,
    const uint64_t log_id, uint64_t& data_file_id, bool& log_exist, ObArchiveIndexFileInfo& max_valid_index_info)
{
  int ret = OB_SUCCESS;
  int64_t file_len = 0;
  int64_t read_size = 0;
  char* buf = NULL;
  log_exist = false;

  if (OB_FAIL(get_file_length(uri, storage_info, file_len))) {
    ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri));
  } else if (OB_UNLIKELY(0 > file_len)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file len", KR(ret), K(uri), K(file_len));
  } else if (OB_UNLIKELY(0 == file_len)) {
    ARCHIVE_LOG(INFO, "index file is empty", K(uri), K(storage_info));
  } else if (OB_ISNULL(buf = static_cast<char*>(ob_archive_malloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate fail", K(ret), K(file_len));
  } else if (OB_FAIL(read_single_file(uri, storage_info, buf, file_len, read_size))) {
    ARCHIVE_LOG(WARN, "read_text_file fail", K(ret), K(uri));
  } else if (OB_FAIL(locate_log_in_single_index_file_(
                 log_id, buf, file_len, data_file_id, log_exist, max_valid_index_info))) {
    ARCHIVE_LOG(WARN, "locate_log_in_single_index_file fail", K(ret), K(data_file_id), K(log_id));
  }

  if (NULL != buf) {
    ob_archive_free(buf);
    buf = NULL;
  }

  return ret;
}

int ObArchiveFileUtils::get_server_start_archive_ts(
    const ObString& uri, const ObString& storage_info, int64_t& archive_round, int64_t& start_ts)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t file_len = 0;
  int64_t read_size = 0;
  int64_t pos = 0;
  ObArchiveStartTimestamp start_timestamp;

  if (OB_FAIL(get_file_length(uri, storage_info, file_len))) {
    ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri));
  } else if (OB_UNLIKELY(0 > file_len)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file len", KR(ret), K(uri), K(file_len));
  } else if (OB_UNLIKELY(0 == file_len)) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(WARN, "index file is empty", K(ret), K(uri));
  } else if (OB_ISNULL(buf = static_cast<char*>(ob_archive_malloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate fail", K(ret), K(file_len));
  } else if (OB_FAIL(read_single_file(uri, storage_info, buf, file_len, read_size))) {
    ARCHIVE_LOG(WARN, "read_text_file fail", K(ret), K(uri));
  } else if (OB_FAIL(start_timestamp.deserialize(buf, file_len, pos))) {
    ARCHIVE_LOG(WARN, "deserialize fail", K(ret), K(buf), K(file_len), K(uri), K(storage_info));
  } else if (!start_timestamp.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "ObArchiveStartTimestamp is invalid",
        K(ret),
        K(uri),
        K(storage_info),
        K(start_timestamp),
        K(file_len),
        K(read_size),
        K(buf));
  } else {
    archive_round = start_timestamp.archive_round_;
    start_ts = start_timestamp.timestamp_;
  }

  if (NULL != buf) {
    ob_archive_free(buf);
    buf = NULL;
  }

  return ret;
}

int ObArchiveFileUtils::range_read(const ObString& uri, const ObString& storage_info, char* buf, const int64_t buf_size,
    const int64_t offset, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need retry*/);
  read_size = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_size) || OB_UNLIKELY(0 > offset)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(uri), K(storage_info), K(buf), K(buf_size), K(offset));
  } else if (OB_FAIL(util.read_part_file(uri, storage_info, buf, buf_size, offset, read_size))) {
    ARCHIVE_LOG(
        WARN, "read_part_file fail", KR(ret), K(uri), K(storage_info), K(buf), K(buf_size), K(offset), K(read_size));
  }

  return ret;
}

int ObArchiveFileUtils::check_file_exist(const ObString& uri, const ObString& storage_info, bool& file_exist)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need retry*/);
  file_exist = false;

  if (OB_FAIL(util.is_exist(uri, storage_info, file_exist))) {
    ARCHIVE_LOG(WARN, "check file is_exist fail", K(ret), K(uri));
  }

  return ret;
}

int ObArchiveFileUtils::create_file(const ObString& uri, const ObString& storage_info)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need_retry*/);
  char buf[1] = "";
  const int64_t empty_file_length = 0;
  if (OB_FAIL(util.write_single_file(uri, storage_info, buf, empty_file_length))) {
    ARCHIVE_LOG(WARN, "failed to create file", KR(ret), K(uri), K(storage_info));
  }
  return ret;
}

// when valid index file(data file has integrated log) is not exist,
// get max data file with this interface
int ObArchiveFileUtils::get_max_data_file_when_index_not_exist(
    const ObString& prefix, const ObString& storage_info, uint64_t& file_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_data_file_id = 0;
  uint64_t max_data_file_id = 0;

  if (OB_FAIL(get_file_range(prefix, storage_info, min_data_file_id, max_data_file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_file_range fail", K(ret), K(prefix), K(storage_info));
    } else {
      ARCHIVE_LOG(INFO, "no unrecorded data file exist", K(ret), K(prefix), K(storage_info));
    }
  } else if (OB_UNLIKELY(0 == max_data_file_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(
        WARN, "invalid data file id", K(ret), K(prefix), K(storage_info), K(min_data_file_id), K(max_data_file_id));
  } else {
    file_id = max_data_file_id;
  }

  return ret;
}

int ObArchiveFileUtils::get_max_index_info_in_single_file(
    const ObString& uri, const ObString& storage_info, ObArchiveIndexFileInfo& info, bool& exist)
{
  int ret = OB_SUCCESS;
  int64_t file_len = 0;
  int64_t read_size = 0;
  char* buf = NULL;
  int64_t record_num = 0;
  int64_t info_size = 0;
  int64_t pos = 0;
  exist = false;

  if (OB_FAIL(get_file_length(uri, storage_info, file_len))) {
    ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri));
  } else if (OB_UNLIKELY(0 > file_len)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file len", KR(ret), K(uri), K(file_len));
  } else if (OB_UNLIKELY(0 == file_len)) {
    ARCHIVE_LOG(INFO, "index file is empty", K(uri));
  } else if (OB_ISNULL(buf = static_cast<char*>(ob_archive_malloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate fail", K(ret), K(file_len));
  } else if (OB_FAIL(read_single_file(uri, storage_info, buf, file_len, read_size))) {
    ARCHIVE_LOG(WARN, "read_text_file fail", K(ret), K(uri));
  } else if (OB_FAIL(info.get_real_record_length(buf, file_len, info_size))) {
    ARCHIVE_LOG(WARN, "get_real_record_length fail", K(ret), K(buf), K(file_len));
  } else if (OB_UNLIKELY(0 == info_size)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "info_size is zero", KR(ret), K(uri), K(storage_info), K(info_size));
  } else if (OB_UNLIKELY(0 >= (record_num = file_len / info_size))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "no index record exist",
        KR(ret),
        K(uri),
        K(storage_info),
        K(file_len),
        K(info_size),
        K(record_num),
        K(buf));
  } else {
    int64_t index = record_num - 1;
    for (; OB_SUCC(ret) && !exist && index >= 0; index--) {
      info.reset();
      pos = index * info_size;
      if (OB_FAIL(info.deserialize(buf, file_len, pos))) {
        ARCHIVE_LOG(ERROR, "deserialize fail", K(ret), K(buf), K(file_len), K(index), K(record_num), K(pos));
      } else if (OB_UNLIKELY(!info.is_valid())) {
        // skip
        ARCHIVE_LOG(WARN, "invalid index info, skip it", KP(buf), K(file_len), K(info));
      } else {
        exist = true;
      }
    }
  }

  if (NULL != buf) {
    ob_archive_free(buf);
    buf = NULL;
  }

  return ret;
}

// get max safe data file id for clean,
// base: valid index file exist
int ObArchiveFileUtils::get_max_safe_data_file_id(const ObString& uri, const ObString& storage_info,
    const uint64_t log_id, const int64_t log_ts, const bool by_log_id, uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t file_len = 0;
  int64_t read_size = 0;
  int64_t pos = 0;
  bool done = false;
  ObArchiveIndexFileInfo info;
  data_file_id = 0;

  if (OB_FAIL(get_file_length(uri, storage_info, file_len))) {
    ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri));
  } else if (OB_UNLIKELY(0 > file_len)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file len", KR(ret), K(uri), K(file_len));
  } else if (OB_UNLIKELY(0 == file_len)) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(WARN, "index file is empty", K(ret), K(uri));
  } else if (OB_ISNULL(buf = static_cast<char*>(ob_archive_malloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate fail", K(ret), K(file_len));
  } else if (OB_FAIL(read_single_file(uri, storage_info, buf, file_len, read_size))) {
    ARCHIVE_LOG(WARN, "read_text_file fail", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && pos < file_len && !done) {
      info.reset();
      if (OB_FAIL(info.deserialize(buf, file_len, pos))) {
        ARCHIVE_LOG(WARN,
            "deserialize fail",
            KR(ret),
            K(uri),
            K(storage_info),
            K(buf),
            K(file_len),
            K(pos),
            K(log_id),
            K(log_ts),
            K(by_log_id));
        ret = OB_SUCCESS;
        done = true;
      } else if (OB_UNLIKELY(!info.is_valid())) {
        // skip
        ARCHIVE_LOG(WARN, "invalid index info, skip it", K(uri), K(info));
      } else {
        bool cmp = by_log_id ? info.max_log_id_ >= log_id : info.max_log_submit_ts_ >= log_ts;
        if (cmp) {
          done = true;
          data_file_id = (0 == data_file_id) ? info.data_file_id_ - 1 : data_file_id;
        } else {
          data_file_id = info.data_file_id_;
        }
      }
    }
  }

  if (NULL != buf) {
    ob_archive_free(buf);
    buf = NULL;
  }

  return ret;
}

//================== private functions ========================================
int ObArchiveFileUtils::extract_file_id_(const ObString& file_name, uint64_t& file_id, bool& match)
{
  int ret = OB_SUCCESS;
  const int64_t length = file_name.length();
  const char* str = file_name.ptr();
  uint64_t tmp_file_id = 0;
  match = true;
  file_id = 0;

  if (OB_UNLIKELY(file_name.empty()) || OB_ISNULL(str) || OB_UNLIKELY('\0' == (*str))) {
    match = false;
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(file_name));
  } else if (OB_UNLIKELY('0' == (*str))) {
    match = false;
  } else {
    for (int64_t i = 0; match && i < length; i++) {
      if (!isdigit(str[i]) && '\0' != str[i]) {
        match = false;
      } else if ('\0' == str[i]) {
        break;
      } else {
        tmp_file_id = tmp_file_id * 10 + (str[i] - '0');
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!match) {
      ARCHIVE_LOG(WARN, "invalid file_name", K(file_name));
    } else {
      file_id = tmp_file_id;
    }
  }

  return ret;
}

int ObArchiveFileUtils::read_max_archived_info_(char* buf, const int64_t buf_len, MaxArchivedIndexInfo& archive_info)
{
  int ret = OB_SUCCESS;
  int64_t info_size = 0;
  int64_t record_num = 0;
  int64_t pos = 0;
  ObArchiveIndexFileInfo index_info;

  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(buf), K(buf_len), K(info_size));
  } else if (OB_FAIL(index_info.get_real_record_length(buf, buf_len, info_size))) {
    ARCHIVE_LOG(WARN, "get_real_record_length fail", K(ret), K(buf), K(buf_len));
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(0 >= info_size) || OB_UNLIKELY(0 >= (record_num = buf_len / info_size))) {
    ARCHIVE_LOG(INFO, "no whole record exist", K(buf), K(buf_len), K(info_size));
    // no record, skip
  } else {
    int64_t index = record_num - 1;
    for (; OB_SUCC(ret) && !archive_info.is_index_info_collected_() && index >= 0; index--) {
      index_info.reset();
      pos = index * info_size;
      if (OB_FAIL(index_info.deserialize(buf, buf_len, pos))) {
        ARCHIVE_LOG(ERROR, "deserialize fail", K(ret), K(buf), K(buf_len), K(index), K(record_num), K(pos));
      } else {
        archive_info.set_data_file_id(index_info.data_file_id_);
        if (OB_UNLIKELY(!index_info.is_valid())) {
          // skip
          ARCHIVE_LOG(WARN, "invalid index info, skip it", KP(buf), K(buf_len), K(index_info));
        } else if (OB_FAIL(archive_info.set_round_start_info(index_info))) {
          ARCHIVE_LOG(ERROR, "failed to set_round_start_info", K(index_info), KR(ret));
        } else if (OB_FAIL(archive_info.set_log_info(index_info.max_log_id_,
                       index_info.max_checkpoint_ts_,
                       index_info.max_log_submit_ts_,
                       index_info.clog_epoch_id_,
                       index_info.accum_checksum_))) {
          ARCHIVE_LOG(ERROR, "failed to set_log_info", K(index_info), KR(ret));
        } else {
          ARCHIVE_LOG(INFO, "succ to set max_archive_index_info", K(index_info), K(archive_info), KR(ret));
        }
      }  // else
    }    // for
  }

  return ret;
}

// locate log in single index file,
// base: index file exist
int ObArchiveFileUtils::locate_log_in_single_index_file_(const uint64_t log_id, char* data, const int64_t data_len,
    uint64_t& file_id, bool& log_exist, ObArchiveIndexFileInfo& max_valid_index_info)
{
  int ret = OB_SUCCESS;
  ObArchiveIndexFileInfo info;

  if (OB_ISNULL(data) || OB_UNLIKELY(0 == log_id) || OB_UNLIKELY(0 >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(log_id), K(data), K(data_len));
  } else {
    int64_t file_len = data_len;
    int64_t pos = 0;
    while (OB_SUCC(ret) && pos < file_len && !log_exist) {
      info.reset();
      if (OB_FAIL(info.deserialize(data, data_len, pos))) {
        ARCHIVE_LOG(WARN, "deserialize fail", K(ret), K(data), K(data_len), K(pos));
        ret = OB_SUCCESS;
        break;
      } else if (OB_UNLIKELY(!info.is_valid())) {
        // skip
        ARCHIVE_LOG(WARN, "invalid index info, skip it", K(info));
      } else {
        max_valid_index_info = info;
        if (info.min_log_id_ <= log_id && info.max_log_id_ >= log_id) {
          file_id = info.data_file_id_;
          log_exist = true;
        }
      }
    }
  }

  return ret;
}

int ObArchiveFileUtils::get_max_archived_info_in_single_index_file(
    const ObString& uri, const ObString& storage_info, MaxArchivedIndexInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t file_len = 0;
  char* buf = NULL;
  int64_t read_size = 0;

  if (OB_FAIL(get_file_length(uri, storage_info, file_len))) {
    ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri));
  } else if (OB_UNLIKELY(0 > file_len)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file len", KR(ret), K(uri), K(file_len));
  } else if (OB_UNLIKELY(0 == file_len)) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(INFO, "index file is empty", K(ret), K(uri));
  } else if (OB_ISNULL(buf = static_cast<char*>(ob_archive_malloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate fail", K(ret), K(uri), K(file_len));
  } else if (OB_FAIL(read_single_file(uri, storage_info, buf, file_len, read_size))) {
    ARCHIVE_LOG(WARN, "read_text_file fail", K(ret));
  } else if (OB_FAIL(read_max_archived_info_(buf, file_len, info))) {
    ARCHIVE_LOG(WARN, "read_max_archived_info_ fail", K(ret), K(buf), K(file_len));
  }

  if (NULL != buf) {
    ob_archive_free(buf);
    buf = NULL;
  }

  return ret;
}

int ObArchiveFileUtils::get_first_log(const ObPGKey& pg_key, const uint64_t file_id, ObIArchiveLogFileStore* file_store,
    uint64_t& log_id, clog::ObLogType& log_type)
{
  int ret = OB_SUCCESS;
  ObArchiveBlockMeta block_meta;
  clog::ObLogEntry log_entry;
  ObLogArchiveInnerLog inner_log;

  if (OB_UNLIKELY(!pg_key.is_valid() || OB_UNLIKELY(0 >= file_id) || OB_ISNULL(file_store))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key), K(file_id), K(file_store));
  } else if (OB_FAIL(extract_first_log_in_data_file(pg_key, file_id, file_store, block_meta, log_entry, inner_log))) {
    ARCHIVE_LOG(WARN, "extract first log", KR(ret), K(pg_key), K(file_id));
  } else {
    const ObLogEntryHeader& log_header = log_entry.get_header();
    log_id = log_header.get_log_id();
    log_type = log_header.get_log_type();
  }

  return ret;
}

int ObArchiveFileUtils::extract_first_log_in_data_file(const ObPGKey& pg_key, const uint64_t file_id,
    ObIArchiveLogFileStore* file_store, ObArchiveBlockMeta& block_meta, clog::ObLogEntry& log_entry,
    ObLogArchiveInnerLog& inner_log)
{
  int ret = OB_SUCCESS;
  const int64_t offset = 0;

  if (OB_FAIL(extract_log_info_from_specific_block_(
          pg_key, file_id, file_store, offset, block_meta, log_entry, inner_log))) {
    ARCHIVE_LOG(WARN, "extract from specific block fail", KR(ret), K(pg_key), K(file_id));
  }

  return ret;
}

// extract last log info in specified data file
// ATTENTION: maybe the last block in file is not atomic, this function wiil be failed
int ObArchiveFileUtils::extract_last_log_in_data_file(const ObPGKey& pg_key, const uint64_t file_id,
    ObIArchiveLogFileStore* file_store, const ObString& uri, const ObString& storage_info,
    ObArchiveBlockMeta& block_meta, clog::ObLogEntry& log_entry, ObLogArchiveInnerLog& inner_log)
{
  int ret = OB_SUCCESS;
  int64_t file_len = 0;
  char offset_buf[4] = {'\0'};
  const int64_t offset_size = ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
  int64_t offset_pos = 0;
  int32_t offset = -1;
  int64_t block_len = -1;
  int64_t read_size = 0;

  if (OB_UNLIKELY(!pg_key.is_valid() || OB_UNLIKELY(0 >= file_id) || OB_ISNULL(file_store))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key), K(file_id), K(file_store));
  } else if (OB_FAIL(get_file_length(uri, storage_info, file_len))) {
    // 1. get file length
    ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri));
  } else if (OB_UNLIKELY(offset_size > file_len)) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(INFO, "data file is empty", K(ret), K(uri));
  } else if (OB_FAIL(range_read(uri, storage_info, offset_buf, offset_size, file_len - offset_size, read_size))) {
    // 2. read last block offset(last four bytes in data file)
    ARCHIVE_LOG(WARN, "range_read fail", KR(ret), K(uri), K(storage_info), K(offset_size), K(file_len));
  } else if (OB_FAIL(serialization::decode_i32(offset_buf, offset_size, offset_pos, &offset))) {
    // 3. decode the last offset
    ARCHIVE_LOG(WARN, "decode_i32 fail", KR(ret), K(uri), K(storage_info), K(offset_buf));
  } else if (OB_UNLIKELY(0 > offset)) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(WARN,
        "offset smaller than zero",
        KR(ret),
        K(uri),
        K(storage_info),
        K(offset_buf),
        K(offset_size),
        K(file_len),
        K(offset));
  } else if (OB_UNLIKELY(0 >= (block_len = file_len - offset_size - offset))) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(WARN,
        "block_len not bigger than zero",
        KR(ret),
        K(uri),
        K(storage_info),
        K(offset_buf),
        K(offset_size),
        K(file_len),
        K(offset_size),
        K(offset),
        K(block_len));
  } else if (OB_FAIL(extract_log_info_from_specific_block_(
                 pg_key, file_id, file_store, offset, block_meta, log_entry, inner_log))) {
    // 4. extract last info info
    ARCHIVE_LOG(WARN, "extract from specific block fail", KR(ret), K(pg_key), K(file_id), K(offset));
  }

  return ret;
}

int ObArchiveFileUtils::extract_log_info_from_specific_block_(const ObPGKey& pg_key, const uint64_t file_id,
    ObIArchiveLogFileStore* file_store, const int64_t offset, ObArchiveBlockMeta& block_meta,
    clog::ObLogEntry& log_entry, ObLogArchiveInnerLog& inner_log)
{
  int ret = OB_SUCCESS;
  const bool ignore_batch_commit_flag = true;
  const int64_t UNUSED_TIME_OUT = 60 * 1000 * 1000L;
  const bool need_limit_bandwidth = false;
  ObArchiveEntryIterator iter;

  if (OB_UNLIKELY(0 >= file_id || 0 > offset) || OB_ISNULL(file_store)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(file_id), K(offset), K(file_store));
  } else if (OB_FAIL(iter.init(file_store, pg_key, file_id, offset, UNUSED_TIME_OUT, need_limit_bandwidth))) {
    ARCHIVE_LOG(WARN, "iter init fail", KR(ret), K(pg_key), K(file_id), K(offset));
  } else if (OB_FAIL(iter.next_entry(log_entry))) {
    ARCHIVE_LOG(WARN, "iter next_entry fail", KR(ret));
  } else if (OB_UNLIKELY(!log_entry.check_integrity(ignore_batch_commit_flag))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "check_integrity fail", KR(ret), K(pg_key), K(file_id), K(offset), K(log_entry));
  } else {
    const ObLogEntryHeader& log_header = log_entry.get_header();
    const clog::ObLogType log_type = log_header.get_log_type();
    block_meta = iter.get_last_block_meta();
    if (is_archive_kickoff_log(log_type)) {
      int64_t log_pos = 0;
      if (OB_FAIL(inner_log.deserialize(log_entry.get_buf(), log_header.get_data_len(), log_pos))) {
        ARCHIVE_LOG(WARN, "ObLogArchiveInnerLog deserialize fail", KR(ret), K(log_entry));
      }
    }
  }

  return ret;
}
}  // namespace archive
}  // namespace oceanbase
