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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_FILE_UTILS_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_FILE_UTILS_H_

#include "common/ob_partition_key.h"
#include "ob_log_archive_struct.h"
#include "clog/ob_log_entry.h"
#include "clog/ob_log_type.h"

namespace oceanbase {
namespace archive {
using oceanbase::clog::ObLogArchiveInnerLog;
using oceanbase::common::ObPGKey;
using oceanbase::common::ObString;
class ObIArchiveLogFileStore;
class ObArchiveFileUtils {
  static const int64_t MAX_PATH_LENGTH = OB_MAX_ARCHIVE_PATH_LENGTH;

public:
  // parameter below:
  // uri: pg archive absolute path
  // storage_info: authentication info
  //
  // get min & max file of specified partition
  int get_file_range(
      const ObString& prefix, const ObString& storage_info, uint64_t& min_file_id, uint64_t& max_file_id);

  // get file length of specified file
  int get_file_length(const ObString& uri, const ObString& storage_info, int64_t& file_len);

  // read whole content of specified file
  int read_single_file(
      const ObString& uri, const ObString& storage_info, char* buf, const int64_t text_file_length, int64_t& read_size);

  // search specified log in an index file with log id
  int search_log_in_single_index_file(const ObString& uri, const ObString& storage_info, const uint64_t log_id,
      uint64_t& data_file_id, bool& log_exist, ObArchiveIndexFileInfo& max_valid_index_info);

  // get the sever start ts of the server, where server start ts is unique for each server
  int get_server_start_archive_ts(
      const ObString& uri, const ObString& storage_info, int64_t& archive_round, int64_t& start_ts);

  // read specified size content of the specified file
  int range_read(const ObString& uri, const ObString& storage_info, char* buf, const int64_t buf_size,
      const int64_t offset, int64_t& read_size);

  // check file if exist or not
  int check_file_exist(const ObString& uri, const ObString& storage_info, bool& file_exist);

  // create empty file with specified uri
  int create_file(const ObString& uri, const ObString& storage_info);

  // get the max data file id with listing method
  int get_max_data_file_when_index_not_exist(const ObString& prefix, const ObString& storage_info, uint64_t& file_id);

  // get the max archived info from an index file
  // archived info include max archived log id/submit_ts/checkpoint ts/epoch_id/acc_checksum
  int get_max_archived_info_in_single_index_file(
      const ObString& uri, const ObString& storage_info, MaxArchivedIndexInfo& info);

  //
  int get_first_log(const ObPGKey& pg_key, const uint64_t file_id, ObIArchiveLogFileStore* file_store, uint64_t& log_id,
      clog::ObLogType& log_type);

  int extract_first_log_in_data_file(const ObPGKey& pg_key, const uint64_t file_id, ObIArchiveLogFileStore* file_store,
      ObArchiveBlockMeta& block_meta, clog::ObLogEntry& log_entry, ObLogArchiveInnerLog& inner_log);

  int extract_last_log_in_data_file(const ObPGKey& pg_key, const uint64_t file_id, ObIArchiveLogFileStore* file_store,
      const ObString& uri, const ObString& storage_info, ObArchiveBlockMeta& block_meta, clog::ObLogEntry& log_entry,
      ObLogArchiveInnerLog& inner_log);

  int get_max_index_info_in_single_file(
      const ObString& uri, const ObString& storage_info, ObArchiveIndexFileInfo& info, bool& exist);

  int get_max_safe_data_file_id(const ObString& uri, const ObString& storage_info, const uint64_t log_id,
      const int64_t log_ts, const bool by_log_id, uint64_t& data_file_id);

private:
  int extract_file_id_(const ObString& file_name, uint64_t& file_id, bool& match);

  int locate_log_in_single_index_file_(const uint64_t log_id, char* data, const int64_t data_len, uint64_t& file_id,
      bool& log_exist, ObArchiveIndexFileInfo& max_valid_index_info);

  int read_max_archived_info_(char* buf, const int64_t buf_len, MaxArchivedIndexInfo& archive_info);

  int extract_log_info_from_specific_block_(const ObPGKey& pg_key, const uint64_t file_id,
      ObIArchiveLogFileStore* file_store, const int64_t offset, ObArchiveBlockMeta& block_meta,
      clog::ObLogEntry& log_entry, ObLogArchiveInnerLog& inner_log);
};

}  // namespace archive
}  // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_FILE_UTILS_H_ */
