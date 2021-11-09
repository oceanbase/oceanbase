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

#ifndef SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_
#define SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "ob_backup_info_mgr.h"
#include "ob_backup_struct.h"
#include "ob_backup_path.h"

namespace oceanbase {
namespace share {
class ObIBackupLeaseService;

class ObExternLogArchiveBackupInfo final {
  static const uint8_t VERSION = 1;
  static const uint8_t LOG_ARCHIVE_BACKUP_INFO_FILE_VERSION = 1;
  static const uint8_t LOG_ARCHIVE_BACKUP_INFO_CONTENT_VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternLogArchiveBackupInfo();
  ~ObExternLogArchiveBackupInfo();

  void reset();
  bool is_valid() const;
  int64_t get_write_buf_size() const;
  int write_buf(char *buf, const int64_t buf_len, int64_t &pos) const;
  int read_buf(const char *buf, const int64_t buf_len);

  int update(const ObTenantLogArchiveStatus &status);
  int get_last(ObTenantLogArchiveStatus &status);
  int get_log_archive_status(const int64_t restore_timestamp, ObTenantLogArchiveStatus &status);
  int get_log_archive_status(common::ObIArray<ObTenantLogArchiveStatus> &status_array);
  int mark_log_archive_deleted(const common::ObIArray<int64_t> &round_ids);
  int delete_marked_log_archive_info(const common::ObIArray<int64_t> &round_ids);
  bool is_empty() const
  {
    return status_array_.empty();
  }

  TO_STRING_KV(K_(status_array));

private:
  common::ObSArray<ObTenantLogArchiveStatus> status_array_;
  DISALLOW_COPY_AND_ASSIGN(ObExternLogArchiveBackupInfo);
};

// used to manager backup piece file buf
// avg piece info maybe 200 bytes, assume 1 piece for 1 day, 730KB for 10 years
class ObExternalBackupPieceInfo final {
  static const uint8_t VERSION = 1;
  static const uint8_t LOG_ARCHIVE_BACKUP_PIECE_FILE_VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternalBackupPieceInfo();
  ~ObExternalBackupPieceInfo();

  void reset();
  bool is_valid() const;
  int64_t get_write_buf_size() const;
  int write_buf(char *buf, const int64_t buf_len, int64_t &pos) const;
  int read_buf(const char *buf, const int64_t buf_len);

  int update(const share::ObBackupPieceInfo &piece);  // update or insert
  int get_piece_array(common::ObIArray<share::ObBackupPieceInfo> &piece_array);
  int mark_deleting(const common::ObIArray<share::ObBackupPieceInfoKey> &piece_keys);
  int mark_deleted(const common::ObIArray<share::ObBackupPieceInfoKey> &piece_keys);
  bool is_all_piece_info_deleted() const;

  TO_STRING_KV(K_(piece_array));

private:
  common::ObSArray<share::ObBackupPieceInfo> piece_array_;
  DISALLOW_COPY_AND_ASSIGN(ObExternalBackupPieceInfo);
};

class ObLogArchiveBackupInfoMgr final {
  static const uint8_t LOG_ARCHIVE_BACKUP_SINGLE_PIECE_FILE_VERSION = 1;

public:
  ObLogArchiveBackupInfoMgr();
  ~ObLogArchiveBackupInfoMgr();

  int set_copy_id(const int64_t copy_id);
  int set_backup_backup()
  {
    is_backup_backup_ = true;
    return OB_SUCCESS;
  }

  int check_sys_log_archive_status(common::ObISQLClient &sql_client);
  int get_log_archive_backup_info(common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id,
      const ObBackupInnerTableVersion &version, ObLogArchiveBackupInfo &info);
  int get_log_archive_backup_info_compatible(
      common::ObISQLClient &sql_client, const uint64_t tenant_id, ObLogArchiveBackupInfo &info);
  int get_non_frozen_backup_piece(common::ObISQLClient &sql_client, const bool for_update,
      const ObLogArchiveBackupInfo &info, ObNonFrozenBackupPieceInfo &non_frozen_piece);
  int get_non_frozen_backup_piece(common::ObISQLClient &sql_client, const bool for_update,
      const share::ObBackupPieceInfoKey &cur_key, share::ObNonFrozenBackupPieceInfo &piece);
  int get_backup_piece(common::ObISQLClient &sql_client, const bool for_update, const share::ObBackupPieceInfoKey &key,
      share::ObBackupPieceInfo &piece);
  int get_log_archive_backup_backup_info(
      common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo &info);
  int update_log_archive_backup_info(
      common::ObISQLClient &sql_client, const ObBackupInnerTableVersion &version, const ObLogArchiveBackupInfo &info);
  int update_backup_piece(ObMySQLTransaction &trans, const ObNonFrozenBackupPieceInfo &non_frozen_piece);
  int update_backup_piece(common::ObISQLClient &sql_client, const share::ObBackupPieceInfo &piece);
  int get_log_archive_checkpoint(common::ObISQLClient &sql_client, const uint64_t tenant_id, int64_t &checkpoint_ts);
  int get_create_tenant_timestamp(common::ObISQLClient &sql_client, const uint64_t tenant_id, int64_t &create_ts);

  int update_log_archive_status_history(common::ObISQLClient &sql_proxy, const ObLogArchiveBackupInfo &info,
      const int64_t inner_table_version, share::ObIBackupLeaseService &backup_lease_service);
  int delete_tenant_log_archive_status_v2(common::ObISQLClient &sql_proxy, const uint64_t tenant_id);
  int delete_tenant_log_archive_status_v1(common::ObISQLClient &sql_proxy, const uint64_t tenant_id);
  int get_all_active_log_archive_tenants(common::ObISQLClient &sql_proxy, common::ObIArray<uint64_t> &tenant_ids);

  int get_last_extern_log_archive_backup_info(
      const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id, ObTenantLogArchiveStatus &last_status);

  int update_extern_log_archive_backup_info(
      const ObLogArchiveBackupInfo &info, share::ObIBackupLeaseService &backup_lease_service);
  int update_external_backup_piece(
      const share::ObNonFrozenBackupPieceInfo &piece, share::ObIBackupLeaseService &backup_lease_service);
  int update_external_backup_piece(const ObBackupPieceInfo &piece, share::ObIBackupLeaseService &backup_lease_service);
  int update_external_backup_backup_piece(const share::ObClusterBackupDest &cluster_backup_dest,
      const ObBackupPieceInfo &piece, share::ObIBackupLeaseService &backup_lease_service);
  int update_external_single_backup_piece_info(
      const ObBackupPieceInfo &piece, share::ObIBackupLeaseService &backup_lease_service);
  int read_external_single_backup_piece_info(const ObBackupPath &path, const ObString &storage_info,
      ObBackupPieceInfo &piece, share::ObIBackupLeaseService &backup_lease_service);
  int read_extern_log_archive_backup_info(
      const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id, ObExternLogArchiveBackupInfo &info);

  int get_log_archive_history_info(common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const int64_t archive_round, const int64_t copy_id, const bool for_update,
      ObLogArchiveBackupInfo &archive_backup_info);
  int get_log_archive_history_infos(common::ObISQLClient &sql_client, const uint64_t tenant_id, const bool for_update,
      common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int get_all_log_archive_history_infos(
      common::ObISQLClient &sql_client, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int get_same_round_log_archive_history_infos(common::ObISQLClient &sql_client, const int64_t round,
      const bool for_update, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int mark_log_archive_history_info_deleted(const ObLogArchiveBackupInfo &info, common::ObISQLClient &sql_client);
  int delete_log_archive_info(const ObLogArchiveBackupInfo &info, common::ObISQLClient &sql_client);
  int delete_all_backup_backup_log_archive_info(common::ObISQLClient &sql_client);
  int delete_backup_backup_log_archive_info(const uint64_t tenant_id, common::ObISQLClient &sql_client);

  int mark_extern_log_archive_backup_info_deleted(const ObClusterBackupDest &cluster_backup_dest,
      const uint64_t tenant_id, const common::ObIArray<int64_t> &round_ids,
      share::ObIBackupLeaseService &backup_lease_service);
  int delete_marked_extern_log_archive_backup_info(const ObClusterBackupDest &current_backup_dest,
      const uint64_t tenant_id, const common::ObIArray<int64_t> &round_ids, bool &is_empty,
      share::ObIBackupLeaseService &backup_lease_service);

  // for backup backup
  int get_all_backup_backup_log_archive_status(
      common::ObISQLClient &sql_client, const bool for_update, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int get_backup_backup_log_archive_round_list(common::ObISQLClient &sql_client, const int64_t log_archive_round,
      const bool for_update, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int get_all_same_round_in_progress_backup_info(common::ObISQLClient &sql_client, const int64_t round,
      common::ObIArray<share::ObLogArchiveBackupInfo> &info_list);
  int get_all_same_round_log_archive_infos(common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const int64_t round, common::ObIArray<share::ObLogArchiveBackupInfo> &info_list);
  int get_backup_piece_list(common::ObISQLClient &sql_client, const uint64_t tenant_id, const int64_t copy_id,
      common::ObIArray<share::ObBackupPieceInfo> &info_list);
  int check_has_round_mode_archive_in_dest(common::ObISQLClient &sql_client, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, bool &has_round_mode);
  int check_has_piece_mode_archive_in_dest(common::ObISQLClient &sql_client, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, bool &has_piece_mode);
  int get_backup_piece_tenant_list(common::ObISQLClient &sql_client, const int64_t backup_piece_id,
      const int64_t copy_id, common::ObIArray<share::ObBackupPieceInfo> &info_list);
  int get_backup_piece_copy_list(const int64_t incarnation, const uint64_t tenant_id, const int64_t round_id,
      const int64_t piece_id, const ObBackupBackupCopyIdLevel copy_id_level, common::ObISQLClient &sql_client,
      common::ObIArray<share::ObBackupPieceInfo> &info_list);
  int get_all_cluster_level_backup_piece_copy_count(const int64_t incarnation, const uint64_t tenant_id,
      const int64_t round_id, const int64_t piece_id, common::ObISQLClient &sql_client, int64_t &copy_count);
  int get_max_backup_piece(common::ObISQLClient &sql_client, const int64_t incarnation, const uint64_t tenant_id,
      const int64_t copy_id, share::ObBackupPieceInfo &piece);
  int get_max_frozen_backup_piece(common::ObISQLClient &sql_client, const int64_t incarnation, const uint64_t tenant_id,
      const int64_t copy_id, share::ObBackupPieceInfo &piece);
  int get_last_piece_in_round(common::ObISQLClient &sql_client, const int64_t incarnation, const uint64_t tenant_id,
      const int64_t round_id, share::ObBackupPieceInfo &piece);
  int get_external_backup_piece_info(const share::ObBackupPath &path, const common::ObString &storage_info,
      share::ObExternalBackupPieceInfo &info, share::ObIBackupLeaseService &backup_lease_service);
  int sync_backup_backup_piece_info(const uint64_t tenant_id, const ObClusterBackupDest &src_backup_dest,
      const ObClusterBackupDest &dst_backup_dest, share::ObIBackupLeaseService &backup_lease_service);
  // for backup data clean
  int get_round_backup_piece_infos(common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id,
      const int64_t incarnation, const int64_t log_archive_round, const bool is_backup_backup,
      common::ObIArray<share::ObBackupPieceInfo> &piece_infos);
  int get_backup_log_archive_history_infos(common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const bool for_update, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int get_backup_log_archive_info_from_original_piece_infos(common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const bool for_update, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int get_backup_log_archive_info_from_piece_info(common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const int64_t piece_id, const int64_t copy_id, const bool for_update, ObLogArchiveBackupInfo &info);

  int get_original_backup_log_piece_infos(common::ObISQLClient &sql_client, const bool for_update,
      const uint64_t tenant_id, common::ObIArray<share::ObBackupPieceInfo> &piece_infos);
  int get_backup_piece(common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id,
      const int64_t backup_piece_id, const int64_t copy_id, ObBackupPieceInfo &piece_info);
  int get_tenant_ids_with_piece_id(common::ObISQLClient &sql_client, const int64_t backup_piece_id,
      const int64_t copy_id, common::ObIArray<uint64_t> &tenant_ids);
  int get_tenant_ids_with_round_id(common::ObISQLClient &sql_client, const bool for_update,
      const int64_t backup_round_id, const int64_t copy_id, common::ObIArray<uint64_t> &tenant_ids);
  int get_backup_tenant_ids_with_snapshot(common::ObISQLClient &sql_client, const int64_t snapshot_version,
      const bool is_backup_backup, common::ObIArray<uint64_t> &tenant_ids);
  int mark_extern_backup_piece_deleting(const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id,
      const common::ObIArray<share::ObBackupPieceInfoKey> &piece_keys, const bool is_backup_backup,
      share::ObIBackupLeaseService &backup_lease_service);
  int mark_extern_backup_piece_deleted(const ObClusterBackupDest &current_backup_dest, const uint64_t tenant_id,
      const common::ObIArray<share::ObBackupPieceInfoKey> &piece_keys, const bool is_backup_backup,
      bool &is_all_deleted, share::ObIBackupLeaseService &backup_lease_service);
  int get_extern_backup_info_path(
      const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id, share::ObBackupPath &path);
  int get_external_backup_piece_path(const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id,
      const bool is_backup_backup, share::ObBackupPath &path);
  int delete_extern_backup_info_file(const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id);
  int update_extern_backup_info_file_timestamp(
      const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id);
  int delete_extern_backup_piece_file(const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id,
      const bool is_backup_backup, share::ObIBackupLeaseService &backup_lease_service);
  int update_extern_backup_piece_file_timestamp(
      const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id, const bool is_backup_backup);
  int get_tenant_backup_piece_infos(common::ObISQLClient &sql_client, const int64_t incarnation,
      const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id,
      common::ObIArray<share::ObBackupPieceInfo> &piece_infos);
  int get_max_backup_piece_id_in_backup_dest(const share::ObBackupBackupCopyIdLevel copy_id_level,
      const share::ObBackupDest &backup_dest, const uint64_t tenant_id, common::ObISQLClient &sql_client,
      int64_t &piece_id);
  int get_min_available_backup_piece_id_in_backup_dest(const share::ObBackupDest &backup_dest,
      const int64_t incarnation, const uint64_t tenant_id, const int64_t copy_id, common::ObISQLClient &sql_client,
      int64_t &piece_id);
  int check_has_incomplete_file_info_smaller_than_backup_piece_id(const int64_t incarnation, const uint64_t tenant_id,
      const int64_t backup_piece_id, const share::ObBackupDest &backup_dest, common::ObISQLClient &sql_client,
      bool &has_incomplet_before);
  int get_tenant_backup_piece_infos_with_file_status(common::ObISQLClient &sql_client, const int64_t incarnation,
      const uint64_t tenant_id, const ObBackupFileStatus::STATUS &file_status, const bool is_backup_backup,
      common::ObIArray<share::ObBackupPieceInfo> &piece_infos);

private:
  int parse_backup_backup_log_archive_status_(sqlclient::ObMySQLResult &result, ObLogArchiveBackupInfo &info);
  int parse_his_backup_backup_log_archive_status_(sqlclient::ObMySQLResult &result, ObLogArchiveBackupInfo &info);
  int get_log_archive_backup_info_with_lock_(
      common::ObISQLClient &sql_client, const uint64_t tenant_id, ObLogArchiveBackupInfo &info);
  int get_log_archive_status_(
      common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo &info);
  int parse_log_archive_status_result_(common::sqlclient::ObMySQLResult &result, ObLogArchiveBackupInfo &info);
  int parse_log_archive_status_(common::sqlclient::ObMySQLResult &result, ObLogArchiveBackupInfo &info);
  int get_non_frozen_backup_piece_(common::ObISQLClient &sql_client, const bool for_update,
      const share::ObBackupPieceInfoKey &cur_key, ObNonFrozenBackupPieceInfo &non_frozen_piece);
  int get_backup_piece_list_(common::ObISQLClient &proxy, const common::ObSqlString &sql,
      common::ObIArray<share::ObBackupPieceInfo> &piece_list);
  int parse_backup_piece_(sqlclient::ObMySQLResult &result, ObBackupPieceInfo &info);
  int update_log_archive_backup_info_(common::ObISQLClient &sql_client, const ObLogArchiveBackupInfo &info);
  int update_log_archive_status_(common::ObISQLClient &sql_client, const ObLogArchiveBackupInfo &info);

  int get_extern_backup_info_path_(
      const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id, share::ObBackupPath &path);
  int inner_get_log_archvie_history_infos(
      sqlclient::ObMySQLResult &result, common::ObIArray<ObLogArchiveBackupInfo> &infos);
  int parse_log_archvie_history_status_(common::sqlclient::ObMySQLResult &result, ObLogArchiveBackupInfo &info);

  int write_extern_log_archive_backup_info(const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id,
      const ObExternLogArchiveBackupInfo &info, share::ObIBackupLeaseService &backup_lease_service);
  int inner_read_extern_log_archive_backup_info(
      const ObBackupPath &path, const char *storage_info, ObExternLogArchiveBackupInfo &info);
  const char *get_cur_table_name_() const;
  const char *get_his_table_name_() const;
  // 备份备份的任务都记录在系统租户的表中
  uint64_t get_real_tenant_id(const uint64_t tenant_id) const;
  int update_external_backup_piece_(
      const share::ObBackupPieceInfo &piece, bool is_backup_backup, share::ObIBackupLeaseService &backup_lease_service);
  int update_external_backup_piece_(const share::ObNonFrozenBackupPieceInfo &piece, bool is_backup_backup,
      share::ObIBackupLeaseService &backup_lease_service);
  int update_external_backup_piece_(const share::ObClusterBackupDest &cluster_backup_dest,
      const share::ObBackupPieceInfo piece, bool is_backup_backup, share::ObIBackupLeaseService &backup_lease_service);
  int update_external_backup_piece_(const share::ObClusterBackupDest &cluster_backup_dest,
      const share::ObNonFrozenBackupPieceInfo &piece, bool is_backup_backup,
      share::ObIBackupLeaseService &backup_lease_service);
  int inner_read_external_backup_piece_info_(const ObBackupPath &path, const char *storage_info,
      share::ObExternalBackupPieceInfo &info, share::ObIBackupLeaseService &backup_lease_service);
  int inner_write_extern_log_archive_backup_piece_info_(const ObBackupPath &path, const char *storage_info,
      const share::ObExternalBackupPieceInfo &info, share::ObIBackupLeaseService &backup_lease_service);
  int get_external_backup_piece_path_(const ObClusterBackupDest &cluster_backup_dest, const uint64_t tenant_id,
      const bool is_backup_backup, share::ObBackupPath &path);
  int write_external_single_backup_piece_info_(
      const ObBackupPieceInfo &piece, share::ObIBackupLeaseService &backup_lease_service);
  int trans_log_archive_info_from_piece_info_(
      const ObBackupPieceInfo &piece_info, ObLogArchiveBackupInfo &log_archive_info);

  int get_log_archive_backup_info_v1_(
      common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo &info);
  int get_log_archive_backup_info_v2_(
      common::ObISQLClient &sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo &info);

  int update_log_archive_backup_info_v1_(common::ObISQLClient &sql_client, const ObLogArchiveBackupInfo &info);
  int update_log_archive_backup_info_v2_(common::ObISQLClient &sql_client, const ObLogArchiveBackupInfo &info);

private:
  struct CompareBackupPieceInfo {
    explicit CompareBackupPieceInfo(int32_t &result) : result_(result)
    {}
    bool operator()(const share::ObBackupPieceInfo &lhs, const share::ObBackupPieceInfo &rhs);
    int32_t &result_;
  };

private:
  bool is_backup_backup_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveBackupInfoMgr);
};

}  // namespace share
}  // namespace oceanbase

#endif /* SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_ */
