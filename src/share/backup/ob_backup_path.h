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
#include "share/ob_ls_id.h"
namespace oceanbase
{
namespace share
{

enum ObBackupFileSuffix
{
  NONE = 0, // files with suffix or dir
  ARCHIVE = 1, // archive file
  BACKUP = 2, // backup file
};

struct ObBackupPath final
{
public:
  ObBackupPath();
  ~ObBackupPath();
  void reset();
  int trim_right_backslash();

  int init(const common::ObString &backup_root_path);
  int join_incarnation(const uint64_t incarnation);
  int join(const common::ObString &path, const ObBackupFileSuffix &type);
  int join(const uint64_t int_path, const ObBackupFileSuffix &type);
  int join(const int64_t v, const ObBackupFileSuffix &type);
  bool is_empty() const { return 0 == cur_pos_; }

  int join_tenant_incarnation(const uint64_t tenant_id, const int64_t incarnation);
  int join_round_piece(const int64_t round, const int64_t piece_id);
  int join_backup_set(const share::ObBackupSetDesc &backup_set_desc);
  int join_ls(const share::ObLSID &ls_id);
  int join_complement_log();
  int join_macro_data_dir(const share::ObBackupDataType &type, const int64_t turn_id, const int64_t retry_id);
  int join_macro_data_file(const int64_t file_id);
  int join_tablet_info_file(const int64_t file_id);
  int join_data_info_turn(const share::ObBackupDataType &type, const int64_t turn_id);
  int join_data_info_turn_v_4_1_x(const int64_t turn_id);
  int join_meta_info_turn_and_retry(const int64_t turn_id, const int64_t retry_id);
  int join_tenant_macro_range_index_file(const share::ObBackupDataType &type, const int64_t retry_id);
  int join_tenant_meta_index_file(const share::ObBackupDataType &type, const int64_t retry_id, const bool is_sec_meta);
  int join_checkpoint_info_file(const common::ObString &path, const uint64_t checkpoint, const ObBackupFileSuffix &type);
  int join_table_list_dir();
  int join_table_list_part_file(const share::SCN &scn, const int64_t part_no);
  int join_table_list_meta_info_file(const share::SCN &scn);
  static int parse_checkpoint(const char *entry_d_name, const common::ObString &file_name, const ObBackupFileSuffix &type, uint64_t &checkpoint);
  static int parse_partial_table_list_file_name(const char *entry_d_name, const share::SCN &scn, int64_t &part_no);
  static int parse_table_list_meta_file_name(const char *entry_d_name, share::SCN &scn);
  int add_backup_suffix(const ObBackupFileSuffix &type);

  const char *get_ptr() const { return path_; }
  int64_t length() const { return cur_pos_; }
  int64_t capacity() const { return sizeof(path_); }
  common::ObString get_obstr() const;
  bool operator ==(const ObBackupPath &path) const;
  ObBackupPath &operator=(const ObBackupPath &path);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  TO_STRING_KV(K_(cur_pos), K_(path));
private:
  int64_t cur_pos_;
  char path_[OB_MAX_BACKUP_PATH_LENGTH];
};

struct ObBackupPathUtil
{
  // 4.0 new backup format
  // file:///obbackup/backup_sets
  static int get_backup_sets_dir_path(const share::ObBackupDest &backup_tenant_dest,
      share::ObBackupPath &path);

  // file:///backup/backup_sets/backup_set_1_[full|inc]_start_20220601T120000.obbak
  static int get_backup_set_placeholder_start_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &backup_set_desc, share::ObBackupPath &backup_path);

  // file:///backup/backup_sets/backup_set_1_[full|inc]_end_success_20220601T120000.obbak
  static int get_backup_set_placeholder_end_success_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &backup_set_desc,
      const SCN &min_restore_scn, share::ObBackupPath &backup_path);

  // file:///backup/backup_sets/backup_set_1_[full|inc]_end_failed_20220601T120000.obbak
  static int get_backup_set_placeholder_end_failed_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &backup_set_desc, 
      const SCN &min_restore_scn, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/
  static int get_backup_set_dir_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);

  static int get_backup_set_dir_path(const share::ObBackupDest &backup_tenant_dest, 
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);
  // backup_set_1_full_
  static int get_backup_set_inner_placeholder_prefix(
      const share::ObBackupSetDesc &backup_set_desc,
      char *placeholder_prefix,
      int64_t length);

  // file:///obbackup/backup_set_1_full/backup_set_1_full_xxxx_xxxxx
  static int get_backup_set_inner_placeholder(const share::ObBackupDest &backup_set_dest,
      const share::ObBackupSetDesc &backup_set_desc, const SCN &replay_scn,
      const SCN &min_restore_scn, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/log_stream_1/
  static int get_ls_backup_dir_path(const share::ObBackupDest &backup_set_dest,
       const share::ObLSID &ls_id, share::ObBackupPath &backup_path);
  static int get_ls_backup_dir_path(const share::ObBackupDest &backup_set_dest,
       const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/log_stream_1/meta_info_turn_1/tablet_info.obbak
  static int get_ls_data_tablet_info_path(const share::ObBackupDest &backup_set_dest,
      const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const int64_t file_id,
      share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/
  static int get_ls_backup_data_dir_path(const share::ObBackupDest &backup_set_dest,
      const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, share::ObBackupPath &backup_path);

  static int get_ls_backup_data_dir_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/macro_block_data.1.obbak
  static int get_macro_block_backup_path(const share::ObBackupDest &backup_set_dest,
      const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, const int64_t file_id, share::ObBackupPath &backup_path);
  
  static int get_macro_block_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, const int64_t file_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/macro_range_index.obbak
  static int get_ls_macro_range_index_backup_path(const share::ObBackupDest &backup_set_dest,
      const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, share::ObBackupPath &backup_path);

  static int get_ls_macro_range_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/log_stream_1/major_data_turn_1_retry_0/meta_index.obbak
  static int get_ls_meta_index_backup_path(const share::ObBackupDest &backup_set_dest,
      const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, const bool is_sec_meta, share::ObBackupPath &backup_path);

  static int get_ls_meta_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, const bool is_sec_meta, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/infos/
  static int get_ls_info_dir_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);

  static int get_ls_info_dir_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/infos/major_data_info_turn_1
  static int get_ls_info_data_info_dir_path(const share::ObBackupDest &backup_set_dest,
      const share::ObBackupDataType &type, const int64_t turn_id, share::ObBackupPath &backup_path);

  static int get_ls_info_data_info_dir_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObBackupDataType &type, const int64_t turn_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/infos/meta_info/
  static int get_tenant_meta_info_dir_path(const share::ObBackupDest &backup_set_dest, 
      share::ObBackupPath &backup_path);

  static int get_tenant_meta_info_dir_path(const share::ObBackupDest &backup_tenant_dest, 
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/infos/meta_info/ls_attr_info.1.obbak
  static int get_backup_ls_attr_info_path(const share::ObBackupDest &backup_set_dest,
      const int64_t turn_id, share::ObBackupPath &backup_path);  

  static int get_backup_ls_attr_info_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const int64_t turn_id, share::ObBackupPath &backup_path); 

  // file:///obbackup/backup_set_1_full/infos/meta_info/ls_meta_infos.obbak
  static int get_ls_meta_infos_path(const share::ObBackupDest &backup_set_dest, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/infos/meta_info/root_key.obbak
  static int get_backup_root_key_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/tenant_backup_set_infos.obbak
  static int get_tenant_backup_set_infos_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);  

  static int get_tenant_backup_set_infos_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);  

 // file:///obbackup/backup_set_1_full/single_backup_set_info.obbak
  static int get_backup_set_info_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);  

  static int get_backup_set_info_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);  

  // file:///obbackup/backup_set_1_full/infos/diagnose_info.obbak
  static int get_diagnose_info_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);  

  static int get_diagnose_info_path(const share::ObBackupDest &backup_tenant_dest,
      share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);  

  // file:///obbackup/backup_set_1_full/infos/locality_info.obbak
  static int get_locality_info_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);  

  static int get_locality_info_path(const share::ObBackupDest &backup_tenant_dest,
      share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);  

  // file:///obbackup/backup_set_1_full/log_stream_1/meta_info_turn_1_retry_0/ls_meta_info.obbak
  static int get_ls_meta_info_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObLSID &ls_id, const int64_t turn_id,
      const int64_t retry_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/logstream_1/xxx_xxx_turn_1_retry_0/macro_range_index.obbak
  static int get_tenant_macro_range_index_backup_path(const share::ObBackupDest &backup_set_dest,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      share::ObBackupPath &backup_path);

  static int get_tenant_macro_range_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObBackupDataType &backup_data_type, const int64_t turn_id,
      const int64_t retry_id, share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/logstream_1/xxx_xxx_turn_1_retry_0/meta_index.obbak
  static int get_tenant_meta_index_backup_path(const share::ObBackupDest &backup_set_dest,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      const bool is_sec_meta, share::ObBackupPath &backup_path);

  static int get_tenant_meta_index_backup_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, const share::ObBackupDataType &backup_data_type, const int64_t turn_id,
      const int64_t retry_id, const bool is_sec_meta, share::ObBackupPath &backup_path);

  // file://obbackup/backup_set_1_full/infos/data_info_turn_1/tablet_log_stream_info.obbak
  static int get_backup_data_tablet_ls_info_path(const share::ObBackupDest &backup_set_dest,
      const share::ObBackupDataType &backup_data_type, const uint64_t turn_id, share::ObBackupPath &path);

  // file://obbackup/backup_set_1_full/infos/meta_info/tablet_log_stream_info
  static int get_backup_data_meta_tablet_ls_info_path(const share::ObBackupDest &backup_set_dest, share::ObBackupPath &path);

  // file:///obbackup/backup_set_1_full/infos/deleted_tablet_info
  static int get_deleted_tablet_info_path(const share::ObBackupDest &backup_set_dest, share::ObBackupPath &path);

  // file:///obbackup/backup_set_1_full/complement_log/
  static int get_complement_log_dir_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);

  static int get_complement_log_dir_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);

  // file:///obbackup/tenant_1001_incarnation_1/clog/1_1/log_stream_1/
  static int get_ls_log_archive_prefix(const share::ObBackupDest &backup_set_dest, uint64_t tenant_id,
      const int64_t incarnation, const int64_t round, const int64_t piece_id, const share::ObLSID &ls_id,
      share::ObBackupPath &backup_path);

  // file:///obbackup/backup_set_1_full/infos/table_list/
  static int get_table_list_dir_path(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &desc, share::ObBackupPath &backup_path);
  static int get_table_list_dir_path(const share::ObBackupDest &backup_set_dest,
      share::ObBackupPath &backup_path);
  // file:///obbackup/backup_set_1_full/infos/table_list/table_list_meta_info.[scn].obbak
  static int get_table_list_meta_path(const share::ObBackupDest &backup_set_dest,
      const share::SCN &scn, share::ObBackupPath &path);
  // file:///obbackup/backup_set_1_full/infos/table_list/table_list.[scn].[part_no].obbak
  static int get_table_list_part_file_path(const share::ObBackupDest &backup_set_dest,
      const share::SCN &scn, const int64_t part_no, share::ObBackupPath &path);
  static int construct_backup_set_dest(const share::ObBackupDest &backup_tenant_dest, 
      const share::ObBackupSetDesc &backup_desc, share::ObBackupDest &backup_set_dest);
  static int construct_backup_complement_log_dest(const share::ObBackupDest &backup_tenant_dest,
      const share::ObBackupSetDesc &backup_desc, share::ObBackupDest &backup_set_dest);
  static int construct_backup_complement_log_dest(const share::ObBackupDest &backup_tenant_dest,
      share::ObBackupDest &backup_set_dest);

private:
  static int get_tenant_data_backup_set_placeholder_path_(
      const uint64_t backup_set_id,
      const ObBackupType backup_type,
      const SCN &min_restore_scn,
      const ObString &suffix, 
      share::ObBackupPath &path);
};

struct ObBackupPathUtilV_4_1
{
// 4.0 and 4.1 old format backup path
  static int get_tenant_meta_index_backup_path(const share::ObBackupDest &backup_set_dest,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      const bool is_sec_meta, share::ObBackupPath &backup_path);
  static int get_ls_info_data_info_dir_path(const share::ObBackupDest &backup_set_dest,
      const int64_t turn_id, share::ObBackupPath &backup_path);
  static int get_backup_data_tablet_ls_info_path(const share::ObBackupDest &backup_set_dest,
      const uint64_t turn_id, ObBackupPath &path);
};

}//share
}//oceanbase
#endif /* SRC_SHARE_BACKUP_OB_BACKUP_INFO_H_ */
