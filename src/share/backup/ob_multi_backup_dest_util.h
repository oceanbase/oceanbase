// Copyright 2021 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#ifndef OCEANBASE_SHARE_BACKUP_OB_MULTI_BACKUP_DEST_UTIL_H_
#define OCEANBASE_SHARE_BACKUP_OB_MULTI_BACKUP_DEST_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace share {

enum ObMultiBackupPathType {
  BACKUP_PATH_CLUSTER_LEVEL,
  BACKUP_PATH_SINGLE_DIR,
  BACKUP_PATH_MAX,
};

struct ObCmpSetPathCopyIdSmaller {
  bool operator()(const share::ObSimpleBackupSetPath& lhs, const share::ObSimpleBackupSetPath& rhs);
};

struct ObCmpPiecePathCopyIdSmaller {
  bool operator()(const share::ObSimpleBackupPiecePath& lhs, const share::ObSimpleBackupPiecePath& rhs);
};

struct ObCmpBackupPieceInfoBackupPieceId {
  bool operator()(const share::ObBackupPieceInfo& lhs, const share::ObBackupPieceInfo& rhs);
};

struct ObCmpBackupSetInfoBackupSetId {
  bool operator()(const share::ObBackupSetFileInfo& lhs, const share::ObBackupSetFileInfo& rhs);
};

class ObMultiBackupDestUtil {
public:
  ObMultiBackupDestUtil() = default;
  ~ObMultiBackupDestUtil() = default;
  static int parse_multi_uri(const common::ObString& multi_uri, common::ObArenaAllocator& allocator,
      common::ObArray<common::ObString>& uri_list);
  static int check_all_path_is_same_type(
      const common::ObArray<common::ObString>& path_list, bool& is_same, ObMultiBackupPathType& type);
  static int get_backup_tenant_id(const common::ObArray<common::ObString>& url_list, const ObMultiBackupPathType& type,
      const common::ObString& cluster_name, const int64_t cluster_id, const common::ObString& tenant_name,
      const int64_t restore_timestamp, uint64_t& tenant_id);
  static int get_multi_backup_path_list(const bool is_preview, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t restore_timestamp, const common::ObArray<common::ObString>& list,
      common::ObArray<ObSimpleBackupSetPath>& set_list, common::ObArray<ObSimpleBackupPiecePath>& piece_list);
  static int filter_duplicate_path_list(
      common::ObArray<ObSimpleBackupSetPath>& set_list, common::ObArray<ObSimpleBackupPiecePath>& piece_list);
  static int check_multi_path_is_complete(const int64_t restore_timestamp,
      common::ObArray<share::ObBackupSetFileInfo>& set_info_list,
      common::ObArray<share::ObBackupPieceInfo>& piece_info_list, bool& is_complete);

private:
  static int get_backup_dest_list(
      const common::ObArray<common::ObString>& path_list, common::ObArray<ObBackupPathString>& dest_list);
  static int get_backup_set_info_path(const common::ObString& user_path, ObBackupPath& backup_set_path);
  static int get_backup_piece_info_path(const common::ObString& user_path, ObBackupPath& backup_piece_path);
  static int get_path_type(
      const common::ObString& path, const common::ObString& storage_info, ObMultiBackupPathType& type);
  static int inner_get_backup_tenant_id_from_set_or_piece(
      const common::ObArray<common::ObString>& path_list, const int64_t restore_timestamp, uint64_t& tenant_id);
  static int inner_get_backup_tenant_id_from_set_info(
      const common::ObString& url, const int64_t restore_timestamp, uint64_t& tenant_id);
  static int inner_get_backup_tenant_id_from_piece_info(
      const common::ObString& url, const int64_t restore_timestamp, uint64_t& tenant_id);
  static int inner_get_backup_tenant_id_from_tenant_name_info(const common::ObString& cluster_name,
      const int64_t cluster_id, const common::ObString& url, const common::ObString& tenant_name,
      const int64_t restore_timestamp, uint64_t& tenant_id);
  static int get_cluster_backup_dest(const ObBackupDest& backup_dest, const char* cluster_name,
      const int64_t cluster_id, share::ObClusterBackupDest& cluster_backup_dest);
  static int get_backup_set_list(const bool is_preview, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t restore_timestamp, const common::ObString& backup_dest_str,
      common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version, int64_t& start_replay_log_ts);
  static int do_get_backup_set_list(const bool is_preview, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t restore_timestamp, const ObBackupDest& backup_dest,
      common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version, int64_t& start_replay_log_ts);
  static int do_get_backup_set_list_from_cluster_level(const bool is_preview, const char* cluster_name,
      const int64_t cluster_id, const uint64_t tenant_id, const int64_t restore_timestamp,
      const ObBackupDest& backup_dest, common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version,
      int64_t& start_replay_log_ts);
  static int do_inner_get_backup_set_list(const char* cluster_name, const int64_t cluster_id,
      const int64_t restore_timestamp, const ObArray<ObBackupSetFileInfo>& file_infos,
      common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version, int64_t& start_replay_log_ts);
  static int get_backup_piece_list(const bool is_preview, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t snapshot_version, const int64_t start_replay_log_ts,
      const int64_t restore_timestamp, const common::ObString& backup_dest_str,
      common::ObArray<ObSimpleBackupPiecePath>& path_list);
  static int do_get_backup_piece_list(const bool is_preview, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t snapshot_version, const int64_t start_replay_log_ts,
      const int64_t restore_timestamp, const ObBackupDest& backup_dest,
      common::ObArray<ObSimpleBackupPiecePath>& path_list);
  static int do_get_backup_piece_list_from_cluster_level(const bool is_preview, const char* cluster_name,
      const int64_t cluster_id, const uint64_t tenant_id, const int64_t snapshot_version,
      const int64_t start_replay_log_ts, const int64_t restore_timestamp, const ObBackupDest& backup_dest,
      common::ObArray<ObSimpleBackupPiecePath>& path_list);
  static int do_inner_get_backup_piece_list(const char* cluster_name, const int64_t cluster_id,
      const int64_t snapshot_version, const int64_t start_replay_log_ts, const int64_t restore_timestamp,
      const ObArray<ObBackupPieceInfo>& piece_array, common::ObArray<ObSimpleBackupPiecePath>& path_list);
  static int may_need_replace_active_piece_info(const ObClusterBackupDest& dest, const common::ObString& storage_info,
      common::ObArray<ObBackupPieceInfo>& piece_array);
  static int check_backup_path_is_backup_backup(const char* cluster_name, const int64_t cluster_id,
      const common::ObString& root_path, const common::ObString& storage_info, const uint64_t tenant_id,
      bool& is_backup_backup);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiBackupDestUtil);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
