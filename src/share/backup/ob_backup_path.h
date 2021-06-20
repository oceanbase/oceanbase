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

#ifndef SRC_SHARE_BACKUP_OB_BACKUP_PATH_H_
#define SRC_SHARE_BACKUP_OB_BACKUP_PATH_H_

#include "ob_backup_struct.h"
namespace oceanbase {
namespace share {

struct ObBackupPath final {
public:
  ObBackupPath();
  ~ObBackupPath();
  void reset();
  int trim_right_backslash();

  int init(const common::ObString& backup_root_path);
  int join_incarnation(const uint64_t incarnation);
  int join(const common::ObString& path);
  int join(const uint64_t int_path);
  int join(const int64_t v);
  int join_full_backup_set(const int64_t backup_set_id);
  int join_inc_backup_set(const int64_t backup_set_id);
  int join_meta_index(const int64_t task_id);
  int join_meta_file(const int64_t task_id);
  int join_macro_block_file(const int64_t backup_set_id, const int64_t sub_task_id);
  int join_macro_block_index(const int64_t backup_set_id, const int64_t retry_cnt = 0);
  int join_sstable_macro_index(const int64_t backup_set_id, const int64_t retry_cnt = 0);
  bool is_empty() const
  {
    return 0 == cur_pos_;
  }

  const char* get_ptr() const
  {
    return path_;
  }
  int64_t length() const
  {
    return cur_pos_;
  }
  common::ObString get_obstr() const;
  bool operator==(const ObBackupPath& path) const;
  ObBackupPath& operator=(const ObBackupPath& path);
  uint64_t hash() const;
  TO_STRING_KV(K_(cur_pos), K_(path));

private:
  int64_t cur_pos_;
  char path_[OB_MAX_BACKUP_PATH_LENGTH];
};

struct ObBackupPathUtil {
  // oss:/backup/cluster_name/cluster_id/incarnation_1/
  static int get_cluster_prefix_path(const ObClusterBackupDest& dest, ObBackupPath& path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/cluster_clog_backup_info
  static int get_cluster_clog_backup_info_path(const ObClusterBackupDest& dest, ObBackupPath& path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/tenant_clog_backup_info
  static int get_tenant_clog_backup_info_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, ObBackupPath& path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round
  static int get_cluster_clog_prefix_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round, ObBackupPath& path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/clog/round/mount_file
  static int get_tenant_clog_mount_file_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round, ObBackupPath& path);
  // oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/data/backup_set_1
  static int get_tenant_data_full_backup_set_path(const ObBackupBaseDataPathInfo& path_info, ObBackupPath& path);
  // "oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/data/backup_set_1/backup_2"
  static int get_tenant_data_inc_backup_set_path(const ObBackupBaseDataPathInfo& path_info, ObBackupPath& path);
  // assert task_id is not duplicate
  // "oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/data/backup_set_1/backup_2/sys_meta_index_file_1"
  static int get_tenant_data_meta_index_path(
      const ObBackupBaseDataPathInfo& path_info, const int64_t task_id, ObBackupPath& path);
  static int get_tenant_data_meta_file_path(
      const ObBackupBaseDataPathInfo& path_info, const int64_t task_id, ObBackupPath& path);
  // "oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/data/backup_set_1/data/table_id/part_id"
  static int get_tenant_pg_data_path(
      const ObBackupBaseDataPathInfo& path_info, const int64_t table_id, const int64_t part_id, ObBackupPath& path);
  static int get_sstable_macro_index_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t retry_cnt, ObBackupPath& path);
  static int get_macro_block_index_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t retry_cnt, ObBackupPath& path);
  static int get_macro_block_file_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t backup_set_id, const int64_t sub_task_id, ObBackupPath& path);
  static int get_major_macro_block_file_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t backup_set_id, const int64_t sub_task_id, ObBackupPath& path);
  static int get_minor_macro_block_file_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t backup_set_id, const int64_t backup_tsk_id, const int64_t sub_task_id,
      ObBackupPath& path);
  static int get_cluster_data_backup_info_path(const ObClusterBackupDest& dest, ObBackupPath& path);
  static int get_tenant_data_backup_info_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, ObBackupPath& path);
  static int get_tenant_backup_set_info_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t full_backup_set_id, ObBackupPath& path);
  static int get_tenant_sys_pg_list_path(const ObClusterBackupDest& dest, const uint64_t tenant_id,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, ObBackupPath& path);
  static int get_tenant_normal_pg_list_path(const ObClusterBackupDest& dest, const uint64_t tenant_id,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, ObBackupPath& path);
  static int get_tenant_info_path(const ObClusterBackupDest& dest, ObBackupPath& path);
  // "oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_name_info"
  static int get_tenant_name_info_path(const ObClusterBackupDest& dest, ObBackupPath& path);
  static int get_tenant_locality_info_path(const ObClusterBackupDest& dest, const uint64_t tenant_id,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, ObBackupPath& path);
  static int get_tenant_backup_diagnose_path(const ObClusterBackupDest& dest, const uint64_t tenant_id,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, ObBackupPath& path);
  static int get_table_clog_data_path(const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round,
      const int64_t table_id, const int64_t part_id, ObBackupPath& path);
  static int get_table_clog_index_path(const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round,
      const int64_t table_id, const int64_t part_id, ObBackupPath& path);
  static int get_tenant_table_data_path(
      const ObBackupBaseDataPathInfo& path_info, const int64_t table_id, ObBackupPath& path);
  static int get_tenant_clog_data_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round, ObBackupPath& path);
  static int get_tenant_clog_index_path(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round, ObBackupPath& path);
  static int get_tenant_backup_data_path(const ObClusterBackupDest& dest, const uint64_t tenant_id, ObBackupPath& path);
  static int get_tenant_clog_path(const ObClusterBackupDest& dest, const uint64_t tenant_id, ObBackupPath& path);
  static int get_tenant_path(const ObClusterBackupDest& dest, const uint64_t tenant_id, ObBackupPath& path);
  static int get_cluster_clog_info(const ObClusterBackupDest& dest, ObBackupPath& path);
  static int get_clog_archive_key_prefix(
      const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t round, ObBackupPath& path);

  // 3.x new backup format
  // "oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/data/backup_set_1/data/table_id/part_id/major_data/"
  static int get_tenant_pg_major_data_path(
      const ObBackupBaseDataPathInfo& path_info, const int64_t table_id, const int64_t part_id, ObBackupPath& path);
  // "oss:/backup/cluster_name/cluster_id/incarnation_1/tenant_id/data/backup_set_1/data/table_id/part_id/minor_data/task_id/"
  static int get_tenant_pg_minor_data_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t task_id, ObBackupPath& path);
  static int get_major_macro_block_index_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t retry_cnt, ObBackupPath& path);
  static int get_minor_macro_block_index_path(const ObBackupBaseDataPathInfo& path_info, const int64_t table_id,
      const int64_t part_id, const int64_t task_id, const int64_t retry_cnt, ObBackupPath& path);
};

class ObBackupMountFile final {
public:
  static int get_mount_file_path(
      const ObLogArchiveBackupInfo& backup_info, ObClusterBackupDest& cluster_dest, share::ObBackupPath& path);
  static int create_mount_file(const ObLogArchiveBackupInfo& info);
  static int check_mount_file(const ObLogArchiveBackupInfo& info);
  static int need_check_mount_file(const ObLogArchiveBackupInfo& info, bool& need_check);

private:
  ObBackupMountFile();
  DISALLOW_COPY_AND_ASSIGN(ObBackupMountFile);
};
}  // namespace share
}  // namespace oceanbase
#endif /* SRC_SHARE_BACKUP_OB_BACKUP_INFO_H_ */
