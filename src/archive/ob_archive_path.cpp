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

#include "observer/ob_server_struct.h"        // GCTX
#include "observer/ob_server.h"               // ObServer
#include "share/backup/ob_backup_info_mgr.h"  // ObLogArchiveBackupInfo
#include "share/backup/ob_backup_path.h"      // ObBackupPathUtil
#include "ob_archive_path.h"

using namespace oceanbase::share;
namespace oceanbase {
namespace archive {
//======================== public functions ==========================//

int ObArchivePathUtil::build_base_path(const char* root_path, const char* cluster_name, const int64_t cluster_id,
    const uint64_t tenant_id, const int64_t incarnantion, const int64_t archive_round, const int64_t path_buf_len,
    char* base_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObClusterBackupDest dest;
  ObBackupPath path;

  if (OB_ISNULL(root_path) || OB_ISNULL(cluster_name) || OB_UNLIKELY(0 >= cluster_id) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(0 >= incarnantion) ||
      OB_UNLIKELY(0 >= archive_round) || OB_UNLIKELY(0 >= path_buf_len) || OB_ISNULL(base_path)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(root_path),
        K(cluster_name),
        K(cluster_id),
        K(tenant_id),
        K(incarnantion),
        K(archive_round),
        K(path_buf_len),
        K(base_path));
  } else if (OB_FAIL(dest.set(root_path, cluster_name, cluster_id, incarnantion))) {
    ARCHIVE_LOG(WARN, "dest set fail", KR(ret), K(root_path), K(cluster_name), K(cluster_id), K(incarnantion));
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_prefix_path(dest, tenant_id, archive_round, path))) {
    ARCHIVE_LOG(WARN, "get_cluster_clog_prefix_path fail", KR(ret), K(dest), K(path));
  } else if (OB_FAIL(databuff_printf(
                 base_path, path_buf_len, pos, "%.*s", static_cast<int32_t>(path.length()), path.get_ptr()))) {
    ARCHIVE_LOG(WARN,
        "databuff_printf fail",
        KR(ret),
        K(base_path),
        K(path),
        K(cluster_name),
        K(root_path),
        K(cluster_id),
        K(tenant_id),
        K(incarnantion),
        K(archive_round));
  } else {
    ARCHIVE_LOG(DEBUG, "build_base_path succ", K(base_path));
  }

  return ret;
}

int ObArchivePathUtil::build_file_prefix(const ObPGKey& pg_key, const char* base_path,
    const LogArchiveFileType file_type, const int64_t path_buf_len, char* dest_path_buf)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(!is_valid_file_type(file_type)) ||
      OB_UNLIKELY(0 >= path_buf_len) || OB_ISNULL(dest_path_buf) || OB_ISNULL(base_path)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(
        WARN, "invalid argument", KR(ret), K(pg_key), K(file_type), K(path_buf_len), K(dest_path_buf), K(base_path));
  } else if (OB_FAIL(build_file_prefix_(pg_key, base_path, file_type, path_buf_len, dest_path_buf, pos))) {
    ARCHIVE_LOG(WARN, "build_file_prefix_ fail", KR(ret), K(pg_key));
  } else {
    ARCHIVE_LOG(DEBUG, "build_file_prefix succ", K(pg_key), K(file_type), K(dest_path_buf));
  }

  return ret;
}

int ObArchivePathUtil::build_file_prefix_(const ObPGKey& pg_key, const char* base_path,
    const LogArchiveFileType file_type, const int64_t path_buf_len, char* dest_path_buf, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();

  if (OB_FAIL(databuff_printf(dest_path_buf,
          path_buf_len,
          pos,
          "%s/%s/%lu/%ld",
          base_path,
          get_file_prefix_with_type_(file_type),
          table_id,
          partition_id))) {
    ARCHIVE_LOG(WARN, "databuff_printf fail", KR(ret), K(base_path), K(pg_key), K(file_type));
  }

  return ret;
}

int ObArchivePathUtil::build_file_path(const ObPGKey& pg_key, const char* base_path, const LogArchiveFileType file_type,
    const uint64_t file_id, const int64_t path_buf_len, char* dest_path_buf)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(!is_valid_file_type(file_type)) || OB_UNLIKELY(0 == file_id) ||
      OB_ISNULL(dest_path_buf) || OB_ISNULL(base_path)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key), K(file_type), K(file_id), K(dest_path_buf), K(base_path));
  } else if (OB_FAIL(build_file_prefix_(pg_key, base_path, file_type, path_buf_len, dest_path_buf, pos))) {
    ARCHIVE_LOG(WARN, "build_file_prefix_ fail", KR(ret), K(pg_key));
  } else if (OB_FAIL(databuff_printf(dest_path_buf, path_buf_len, pos, "/%lu", file_id))) {
    ARCHIVE_LOG(WARN, "databuff_printf fail", KR(ret), K(pg_key), K(file_type), K(file_id));
  } else {
    ARCHIVE_LOG(DEBUG, "build_file_path succ", K(pg_key), K(file_type), K(file_id), K(dest_path_buf));
  }

  return ret;
}

int ObArchivePathUtil::build_archive_file_prefix(const ObPGKey& pg_key, const LogArchiveFileType file_type,
    const int64_t incarnation, const int64_t round, const int64_t path_buf_len, char* dest_path)
{
  int ret = OB_SUCCESS;
  int64_t unused_pos = 0;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(0 >= path_buf_len) || OB_ISNULL(dest_path) || OB_UNLIKELY(!is_valid_file_type(file_type))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(pg_key),
        K(file_type),
        K(incarnation),
        K(round),
        K(path_buf_len),
        K(dest_path));
  } else if (OB_FAIL(build_archive_file_prefix_(
                 pg_key, file_type, incarnation, round, path_buf_len, dest_path, unused_pos))) {
    ARCHIVE_LOG(WARN, "build_archive_file_prefix_ fail", KR(ret), K(file_type), K(incarnation), K(round));
  }

  return ret;
}

int ObArchivePathUtil::build_archive_file_prefix_(const ObPGKey& pg_key, const LogArchiveFileType file_type,
    const int64_t incarnation, const int64_t round, const int64_t path_buf_len, char* dest_path, int64_t& pos)
{
  int ret = OB_SUCCESS;
  char base_path[OB_MAX_BACKUP_PATH_LENGTH];

  int64_t cluster_id = GCTX.config_->cluster_id;
  const char* cluster_name = GCTX.config_->cluster;
  const uint64_t tenant_id = pg_key.get_tenant_id();
  ObLogArchiveBackupInfo info;

  if (OB_ISNULL(dest_path)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "dest_path is NULL", KR(ret), K(dest_path));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
    ARCHIVE_LOG(WARN, "get_log_archive_backup_info fail", KR(ret), K(pg_key));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "ObLogArchiveBackupInfo is not valid", KR(ret), K(pg_key), K(info));
  } else if (incarnation != info.status_.incarnation_ || round != info.status_.round_) {
    ret = OB_BACKUP_INFO_NOT_MATCH;
    ARCHIVE_LOG(WARN, "work in different incarnation or round, wait to stop");
  } else if (OB_FAIL(build_base_path(info.backup_dest_,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 info.status_.incarnation_,
                 info.status_.round_,
                 path_buf_len,
                 base_path))) {
    ARCHIVE_LOG(WARN, "build_base_path fail", KR(ret), K(pg_key));
  } else if (OB_FAIL(build_file_prefix_(pg_key, base_path, file_type, path_buf_len, dest_path, pos))) {
    ARCHIVE_LOG(WARN, "build_file_prefix_ fail", KR(ret), K(pg_key), K(file_type), K(pos));
  }

  return ret;
}

int ObArchivePathUtil::build_archive_file_path(const ObPGKey& pg_key, const LogArchiveFileType file_type,
    const uint64_t file_id, const int64_t incarnation, const int64_t round, const int64_t path_buf_len, char* dest_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(0 >= incarnation) ||
      OB_UNLIKELY(0 >= round) || OB_UNLIKELY(0 >= path_buf_len) || OB_ISNULL(dest_path) ||
      OB_UNLIKELY(!is_valid_file_type(file_type))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(pg_key),
        K(file_type),
        K(incarnation),
        K(round),
        K(path_buf_len),
        K(dest_path));
  } else if (OB_FAIL(build_archive_file_prefix_(pg_key, file_type, incarnation, round, path_buf_len, dest_path, pos))) {
    ARCHIVE_LOG(WARN, "build_archive_file_prefix_ fail", KR(ret), K(file_type), K(incarnation), K(round));
  } else if (OB_FAIL(databuff_printf(dest_path, path_buf_len, pos, "/%lu", file_id))) {
    ARCHIVE_LOG(WARN, "build archive file path fail", KR(ret), K(pg_key), K(file_type), K(file_id));
  }

  return ret;
}

bool ObArchivePathUtil::is_valid_file_type(const LogArchiveFileType file_type)
{
  return LOG_ARCHIVE_FILE_TYPE_INDEX == file_type || LOG_ARCHIVE_FILE_TYPE_DATA == file_type;
}

const char* ObArchivePathUtil::get_file_prefix_with_type_(const LogArchiveFileType file_type)
{
  const char* prefix = "invalid";
  switch (file_type) {
    case LOG_ARCHIVE_FILE_TYPE_INDEX: {
      prefix = "index";
      break;
    }
    case LOG_ARCHIVE_FILE_TYPE_DATA: {
      prefix = "data";
      break;
    }
    default:
      prefix = "invalid";
      ARCHIVE_LOG(ERROR, "invalid file type", K(file_type));
  }
  return prefix;
}

int ObArchivePathUtil::build_archive_key_prefix(const int64_t incarnation, const int64_t round,
    const uint64_t tenant_id, const int64_t path_buf_len, char* path, const int64_t storage_info_len, char* info)
{
  int ret = OB_SUCCESS;
  int64_t unused_pos = 0;

  if (OB_ISNULL(path) || OB_ISNULL(info) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(0 >= path_buf_len) || OB_UNLIKELY(0 >= storage_info_len) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(path),
        K(info),
        K(incarnation),
        K(round),
        K(path_buf_len),
        K(storage_info_len),
        K(tenant_id));
  } else if (OB_FAIL(build_archive_key_prefix_(
                 incarnation, round, tenant_id, path_buf_len, path, unused_pos, storage_info_len, info))) {
    ARCHIVE_LOG(WARN, "build_archive_key_prefix_ fail", KR(ret), K(incarnation), K(round));
  } else {
    ARCHIVE_LOG(INFO, "build_archive_key_prefix succ", K(incarnation), K(round), K(path), K(info));
  }

  return ret;
}

int ObArchivePathUtil::build_archive_key_path(const ObPGKey& pkey, const int64_t incarnation, const int64_t round,
    const int64_t path_buf_len, char* path, const int64_t storage_info_len, char* info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const uint64_t tenant_id = pkey.get_tenant_id();
  const uint64_t table_id = pkey.get_table_id();
  const int64_t partition_id = pkey.get_partition_id();

  if (OB_ISNULL(path) || OB_ISNULL(info) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(0 >= path_buf_len) ||
      OB_UNLIKELY(0 >= storage_info_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(path),
        K(info),
        K(incarnation),
        K(round),
        K(path_buf_len),
        K(storage_info_len),
        K(tenant_id));
  } else if (OB_FAIL(build_archive_key_prefix_(
                 incarnation, round, tenant_id, path_buf_len, path, pos, storage_info_len, info))) {
    ARCHIVE_LOG(WARN, "build_archive_key_prefix_ fail", KR(ret), K(incarnation), K(round), K(pkey));
  } else if (OB_FAIL(databuff_printf(path, path_buf_len, pos, "/%lu_%ld", table_id, partition_id))) {
    ARCHIVE_LOG(WARN, "databuff_printf fail", KR(ret), K(pkey), K(path), K(pos));
  } else {
    ARCHIVE_LOG(INFO, "build_archive_key_path succ", K(pkey), K(path), K(info));
  }

  return ret;
}

int ObArchivePathUtil::build_archive_key_prefix_(const int64_t incarnation, const int64_t round,
    const uint64_t tenant_id, const int64_t path_buf_len, char* dest_path, int64_t& pos, const int64_t storage_info_len,
    char* storage_info)
{
  int ret = OB_SUCCESS;

  int64_t cluster_id = GCTX.config_->cluster_id;
  const char* cluster_name = GCTX.config_->cluster;
  ObLogArchiveBackupInfo info;
  ObClusterBackupDest dest;
  ObBackupPath path;

  if (OB_ISNULL(dest_path)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "dest_path is NULL", KR(ret), K(dest_path));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
    ARCHIVE_LOG(WARN, "get_log_archive_backup_info fail", KR(ret));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "ObLogArchiveBackupInfo is not valid", KR(ret), K(info));
  } else if (incarnation != info.status_.incarnation_ || round != info.status_.round_) {
    ret = OB_BACKUP_INFO_NOT_MATCH;
    ARCHIVE_LOG(WARN, "work in different incarnation or round, wait to stop");
  } else if (OB_FAIL(dest.set(info.backup_dest_, cluster_name, cluster_id, info.status_.incarnation_))) {
    ARCHIVE_LOG(WARN, "ObBackupDest set fail", KR(ret), K(info), K(cluster_name), K(cluster_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_prefix(dest, tenant_id, round, path))) {
    ARCHIVE_LOG(WARN, "get_clog_archive_key_prefix fail", KR(ret), K(incarnation), K(round), K(info));
  } else if (OB_FAIL(databuff_printf(dest_path, path_buf_len, pos, "%s", path.get_ptr()))) {
    ARCHIVE_LOG(WARN, "databuff_printf fail", KR(ret), K(incarnation), K(round), K(tenant_id));
  } else {
    strncpy(storage_info, dest.dest_.storage_info_, storage_info_len);
  }

  return ret;
}

int ObArchivePathUtil::build_server_start_archive_prefix(const int64_t incarnation, const int64_t round,
    const int64_t path_buf_len, char* path, const int64_t storage_info_len, char* info)
{
  int ret = OB_SUCCESS;
  int64_t unused_pos = 0;

  if (OB_ISNULL(path) || OB_ISNULL(info) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(0 >= path_buf_len) || OB_UNLIKELY(0 >= storage_info_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(path), K(info), K(incarnation), K(round));
  } else if (OB_FAIL(build_server_start_archive_prefix_(
                 incarnation, round, path_buf_len, path, unused_pos, storage_info_len, info))) {
    ARCHIVE_LOG(WARN, "build_server_start_archive_prefix_ fail", KR(ret), K(incarnation), K(round));
  } else {
    ARCHIVE_LOG(INFO, "build_server_start_archive_prefix succ", K(incarnation), K(round), K(path), K(info));
  }

  return ret;
}

// base_path/clog_info
int ObArchivePathUtil::build_server_start_archive_prefix_(const int64_t incarnation, const int64_t round,
    const int64_t path_buf_len, char* dest_path, int64_t& pos, const int64_t storage_info_len, char* storage_info)
{
  int ret = OB_SUCCESS;
  const char* cluster_name = GCTX.config_->cluster;
  int64_t cluster_id = GCTX.config_->cluster_id;
  ObLogArchiveBackupInfo info;
  ObClusterBackupDest dest;
  ObBackupPath path;

  if (OB_ISNULL(dest_path)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(dest_path));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
    ARCHIVE_LOG(WARN, "get_log_archive_backup_info fail", KR(ret));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObLogArchiveBackupInfo is not valid", KR(ret), K(info));
  } else if (incarnation != info.status_.incarnation_ || round != info.status_.round_) {
    ret = OB_BACKUP_INFO_NOT_MATCH;
    ARCHIVE_LOG(WARN, "backup info not match", KR(ret), K(incarnation), K(round), K(info));
  } else if (OB_FAIL(dest.set(info.backup_dest_, cluster_name, cluster_id, info.status_.incarnation_))) {
    ARCHIVE_LOG(WARN, "ObBackupDest set fail", KR(ret), K(info));
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info(dest, path))) {
    ARCHIVE_LOG(WARN, "get_cluster_prefix_path fail", KR(ret), K(dest), K(path));
  } else if (OB_FAIL(databuff_printf(dest_path, path_buf_len, pos, "%s", path.get_ptr()))) {
    ARCHIVE_LOG(WARN, "databuff_printf fail", KR(ret), K(dest_path), K(path));
  } else {
    strncpy(storage_info, dest.dest_.storage_info_, storage_info_len);
  }

  return ret;
}

// base_path/clog_info/id_ip_port
int ObArchivePathUtil::build_server_start_archive_path(const int64_t incarnation, const int64_t round,
    const int64_t path_buf_len, char* dest_path, const int64_t storage_info_len, char* info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t server_id = GCTX.server_id_;
  char ip[common::OB_IP_STR_BUFF];
  const ObAddr addr = observer::ObServer::get_instance().get_self();

  if (OB_ISNULL(dest_path) || OB_ISNULL(info) || OB_UNLIKELY(0 >= incarnation) || OB_UNLIKELY(0 >= round) ||
      OB_UNLIKELY(0 >= path_buf_len) || OB_UNLIKELY(0 >= storage_info_len)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(dest_path),
        K(incarnation),
        K(round),
        K(path_buf_len),
        K(storage_info_len));
  } else if (OB_UNLIKELY(!addr.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "ip_to_string fail", KR(ret), K(addr));
  } else if (OB_FAIL(build_server_start_archive_prefix_(
                 incarnation, round, path_buf_len, dest_path, pos, storage_info_len, info))) {
    ARCHIVE_LOG(WARN, "build_server_start_archive_prefix_ fail", KR(ret));
  } else if (OB_FAIL(databuff_printf(dest_path, path_buf_len, pos, "/%ld_%s_%d", server_id, ip, addr.get_port()))) {
    ARCHIVE_LOG(WARN, "databuff_printf fail", KR(ret), K(dest_path), K(addr), K(server_id));
  } else {
    ARCHIVE_LOG(INFO, "build_server_start_archive_path succ", K(incarnation), K(round), K(dest_path), K(info));
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
