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

#include "clog/ob_log_define.h"
#include "common/ob_partition_key.h"
#include "ob_log_archive_struct.h"

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_PATH_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_PATH_H_
namespace oceanbase {
namespace archive {

class ObArchivePathUtil {
public:
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/
  int build_base_path(const char* root_path, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t incarnantion, const int64_t archive_round, const int64_t path_buf_len,
      char* base_path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/TYPE/table_id/partition-id/
  // TYPE: data/index
  int build_file_prefix(const common::ObPGKey& pg_key, const char* base_path, const LogArchiveFileType file_type,
      const int64_t path_buf_len, char* dest_path_buf);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/TYPE/table_id/partition-id/
  // TYPE: data/index
  int build_archive_file_prefix(const common::ObPGKey& pg_key, const LogArchiveFileType file_type,
      const int64_t incarnation, const int64_t round, const int64_t path_buf_len, char* dest_path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/TYPE/table_id/partition-id/file_id
  // TYPE: data/index
  int build_archive_file_path(const common::ObPGKey& pg_key, const LogArchiveFileType file_type, const uint64_t file_id,
      const int64_t incarnation, const int64_t round, const int64_t path_buf_len, char* dest_path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/TYPE/table_id/partition-id/file_id
  // TYPE: data/index
  int build_file_path(const common::ObPGKey& pg_key, const char* base_path, const LogArchiveFileType file_type,
      const uint64_t file_id, const int64_t path_buf_len, char* dest_path_buf);
  // only two kinds of archive files, data/index
  bool is_valid_file_type(const LogArchiveFileType file_type);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/archive_key/
  int build_archive_key_prefix(const int64_t incarnation, const int64_t round, const uint64_t tenant_id,
      const int64_t path_buf_len, char* path, const int64_t storage_info_len, char* info);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/archive_key/ARCHIVE_KEY
  // ARCHIVE_KEY: tableID_partitionID
  int build_archive_key_path(const common::ObPGKey& pkey, const int64_t incarnation, const int64_t round,
      const int64_t path_buf_len, char* path, const int64_t storage_info_len, char* info);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/clog_info/
  int build_server_start_archive_prefix(const int64_t incarnation, const int64_t round, const int64_t path_buf_len,
      char* path, const int64_t storage_info_len, char* info);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/clog_info/SERVER_START_TS
  // SERVER_START_TS: serverID_ip_port
  int build_server_start_archive_path(const int64_t incarnation, const int64_t round, const int64_t path_buf_len,
      char* dest_path, const int64_t storage_info_len, char* info);

private:
  int build_archive_file_prefix_(const common::ObPGKey& pg_key, const LogArchiveFileType file_type,
      const int64_t incarnation, const int64_t round, const int64_t path_buf_len, char* dest_path, int64_t& pos);
  int build_file_prefix_(const common::ObPGKey& pg_key, const char* base_path, const LogArchiveFileType file_type,
      const int64_t path_buf_len, char* dest_path_buf, int64_t& pos);
  const char* get_file_prefix_with_type_(const LogArchiveFileType file_type);
  int build_archive_key_prefix_(const int64_t incarnation, const int64_t round, const uint64_t tenant_id,
      const int64_t path_buf_len, char* dest_path, int64_t& pos, const int64_t storage_info_len, char* storage_info);
  int build_server_start_archive_prefix_(const int64_t incarnation, const int64_t round, const int64_t path_buf_len,
      char* dest_path, int64_t& pos, const int64_t storage_info_len, char* info);
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_PATH_H_ */
