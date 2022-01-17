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

#ifndef OCEANBASE_ARCHIVE_CLEAR_LOG_H_
#define OCEANBASE_ARCHIVE_CLEAR_LOG_H_

#include "share/backup/ob_backup_struct.h"
#include "ob_archive_log_file_store.h"

namespace oceanbase {
namespace archive {
class ObArchiveClear {
public:
  /// get max boundary files to clean by log_id and retention_timestamp
  ///
  /// @param dest                 archive file basic path
  /// @param log_id               log's id >= this log_id must be retain
  /// @param retention_timestamp  logs' submit_timestamp >= retention_timestamp must be retain
  int get_clean_max_clog_file_id_by_log_id(const share::ObClusterBackupDest& dest, const int64_t archive_round,
      const int64_t piece_id, const int64_t piece_create_date, const common::ObPGKey& pg_key, const uint64_t log_id,
      const int64_t retention_timestamp, uint64_t& index_file_id, uint64_t& data_file_id);

  /// get max boundary files to clean by log_ts and retention_timestamp
  ///
  /// @param dest                 archive file basic path
  /// @param log_ts               log's ts >= this log_id must be retain
  /// @param retention_timestamp  log's submit_timestamp >= retention_timestamp must be retain
  int get_clean_max_clog_file_id_by_log_ts(const share::ObClusterBackupDest& dest, const int64_t archive_round,
      const int64_t piece_id, const int64_t piece_create_date, const common::ObPGKey& pg_key, const int64_t log_ts,
      const int64_t retention_timestamp, uint64_t& index_file_id, uint64_t& data_file_id);
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_CLEAR_LOG_H_ */
