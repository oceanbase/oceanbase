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

#include "ob_archive_clear.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace archive {
int ObArchiveClear::get_clean_max_clog_file_id_by_log_id(const share::ObClusterBackupDest& dest,
    const int64_t archive_round, const ObPGKey& pg_key, const uint64_t log_id, uint64_t& index_file_id,
    uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  ObArchiveLogFileStore file_store;

  if (OB_UNLIKELY(!dest.is_valid()) || OB_UNLIKELY(0 >= archive_round) || OB_UNLIKELY(OB_INVALID_ID == log_id) ||
      OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(dest), K(archive_round), K(pg_key), K(log_id));
  } else if (OB_FAIL(file_store.init(dest.dest_.root_path_,
                 dest.dest_.storage_info_,
                 dest.cluster_name_,
                 dest.cluster_id_,
                 pg_key.get_tenant_id(),
                 dest.incarnation_,
                 archive_round))) {
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore init fail", K(ret), K(dest), K(archive_round), K(pg_key), K(log_id));
  } else if (OB_FAIL(file_store.locate_file_by_log_id_for_clear(pg_key, log_id, index_file_id, data_file_id))) {
    ARCHIVE_LOG(WARN, "locate_file_by_log_id_for_clear fail", K(ret), K(pg_key), K(log_id));
  } else {
    ARCHIVE_LOG(INFO,
        "locate_file_by_log_id_for_clear succ",
        K(dest),
        K(archive_round),
        K(pg_key),
        K(log_id),
        K(index_file_id),
        K(data_file_id));
  }

  return ret;
}

int ObArchiveClear::get_clean_max_clog_file_id_by_log_ts(const share::ObClusterBackupDest& dest,
    const int64_t archive_round, const ObPGKey& pg_key, const int64_t log_ts, uint64_t& index_file_id,
    uint64_t& data_file_id)
{
  int ret = OB_SUCCESS;
  ObArchiveLogFileStore file_store;

  if (OB_UNLIKELY(!dest.is_valid()) || OB_UNLIKELY(0 >= archive_round) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == log_ts) ||
      OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(dest), K(archive_round), K(pg_key), K(log_ts));
  } else if (OB_FAIL(file_store.init(dest.dest_.root_path_,
                 dest.dest_.storage_info_,
                 dest.cluster_name_,
                 dest.cluster_id_,
                 pg_key.get_tenant_id(),
                 dest.incarnation_,
                 archive_round))) {
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore init fail", K(ret), K(dest), K(archive_round), K(pg_key), K(log_ts));
  } else if (OB_FAIL(file_store.locate_file_by_log_ts_for_clear(pg_key, log_ts, index_file_id, data_file_id))) {
    ARCHIVE_LOG(WARN, "locate_file_by_log_ts_for_clear fail", K(ret), K(pg_key), K(log_ts));
  } else {
    ARCHIVE_LOG(INFO,
        "locate_file_by_log_id_for_clear succ",
        K(dest),
        K(archive_round),
        K(pg_key),
        K(log_ts),
        K(index_file_id),
        K(data_file_id));
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
