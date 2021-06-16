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
  int get_clean_max_clog_file_id_by_log_id(const share::ObClusterBackupDest& dest, const int64_t archive_round,
      const common::ObPGKey& pg_key, const uint64_t log_id, uint64_t& index_file_id, uint64_t& data_file_id);

  int get_clean_max_clog_file_id_by_log_ts(const share::ObClusterBackupDest& dest, const int64_t archive_round,
      const common::ObPGKey& pg_key, const int64_t log_ts, uint64_t& index_file_id, uint64_t& data_file_id);
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_CLEAR_LOG_H_ */
