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
  int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const;
  int read_buf(const char* buf, const int64_t buf_len);

  int update(const ObTenantLogArchiveStatus& status);
  int get_last(ObTenantLogArchiveStatus& status);
  int get_log_archive_status(const int64_t restore_timestamp, ObTenantLogArchiveStatus& status);
  int get_log_archive_status(common::ObIArray<ObTenantLogArchiveStatus>& status_array);
  int mark_log_archive_deleted(const int64_t max_clog_delete_snapshot);
  int delete_marked_log_archive_info();

  TO_STRING_KV(K_(status_array));

private:
  common::ObSArray<ObTenantLogArchiveStatus> status_array_;
  DISALLOW_COPY_AND_ASSIGN(ObExternLogArchiveBackupInfo);
};

class ObLogArchiveBackupInfoMgr final {
public:
  ObLogArchiveBackupInfoMgr();
  ~ObLogArchiveBackupInfoMgr();

  int get_log_archive_backup_info(
      common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info);

  int update_log_archive_backup_info(common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info);
  int get_log_archive_checkpoint(common::ObISQLClient& sql_client, const uint64_t tenant_id, int64_t& checkpoint_ts);
  int is_doing_log_archive(common::ObISQLClient& sql_client, const uint64_t tenant_id, bool& doing);

  int update_log_archive_status_history(common::ObMySQLProxy& sql_proxy, const ObLogArchiveBackupInfo& info,
      share::ObIBackupLeaseService& backup_lease_service);

  int get_last_extern_log_archive_backup_info(
      const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, ObTenantLogArchiveStatus& last_status);

  int update_extern_log_archive_backup_info(
      const ObLogArchiveBackupInfo& info, share::ObIBackupLeaseService& backup_lease_service);

  int read_extern_log_archive_backup_info(
      const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, ObExternLogArchiveBackupInfo& info);

  int get_log_archive_history_info(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      const int64_t archive_round, const bool for_update, ObLogArchiveBackupInfo& info);
  int get_log_archvie_history_infos(common::ObISQLClient& sql_client, const uint64_t tenant_id, const bool for_update,
      common::ObIArray<ObLogArchiveBackupInfo>& infos);
  int mark_log_archive_history_info_deleted(const ObLogArchiveBackupInfo& info, common::ObISQLClient& sql_client);
  int delete_marked_log_archive_history_infos(const uint64_t tenant_id, common::ObISQLClient& sql_client);

  int delete_log_archvie_history_infos(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      const int64_t incarnation, const int64_t log_archive_round);
  int mark_extern_log_archive_backup_info_deleted(const ObClusterBackupDest& cluster_backup_dest,
      const uint64_t tenant_id, const int64_t max_delete_clog_snapshot,
      share::ObIBackupLeaseService& backup_lease_service);
  int delete_marked_extern_log_archive_backup_info(const ObClusterBackupDest& cluster_backup_dest,
      const uint64_t tenant_id, share::ObIBackupLeaseService& backup_lease_service);

private:
  int get_log_archive_backup_info_with_lock_(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, ObLogArchiveBackupInfo& info);
  int get_log_archive_status_(
      common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info);
  int parse_log_archive_status_(common::sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info);
  int update_log_archive_backup_info_(common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info);
  int update_log_archive_status_(common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info);

  int get_extern_backup_info_path_(
      const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, share::ObBackupPath& path);
  int inner_get_log_archvie_history_infos(
      sqlclient::ObMySQLResult& result, common::ObIArray<ObLogArchiveBackupInfo>& infos);
  int parse_log_archvie_history_status_(common::sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info);

  int write_extern_log_archive_backup_info(const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id,
      const ObExternLogArchiveBackupInfo& info, share::ObIBackupLeaseService& backup_lease_service);
  int inner_read_extern_log_archive_backup_info(
      const ObBackupPath& path, const char* storage_info, ObExternLogArchiveBackupInfo& info);
  const char* get_cur_table_name_() const;
  const char* get_his_table_name_() const;

private:
  bool deal_with_copy_;
  int64_t copy_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveBackupInfoMgr);
};

}  // namespace share
}  // namespace oceanbase

#endif /* SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_ */
