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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_VALIDATE_INFO_COLLECTOR_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_VALIDATE_INFO_COLLECTOR_H_

#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_archive_store.h"

namespace oceanbase
{
namespace rootserver
{

/**
 * Used for encapsulating the logic of collecting general backup set and archived log verification information.
 */
class ObBackupValidateInfoCollector
{
public:
  ObBackupValidateInfoCollector();
  ~ObBackupValidateInfoCollector() { reset(); }
  void reset();

  int init(
      common::ObMySQLProxy &sql_proxy,
      share::ObBackupValidateJobAttr &job_attr,
      const int64_t backup_dest_id,
      const int64_t archive_dest_id);
  int collect_backup_set_info(
      common::ObArray<share::ObBackupSetFileDesc> &set_list);
  int collect_archive_piece_info(
      common::ObArray<share::ObPieceKey> &piece_list);
  int collect_complement_log_piece_info(
      const common::ObArray<share::ObBackupSetFileDesc> &set_list,
      common::hash::ObHashMap<int64_t, common::ObArray<share::ObPieceKey>> &complement_piece_map);
private:
  int need_skip_backup_set_(
      const share::ObBackupSetFileDesc &backup_set_info,
      bool &is_skip) const;
  int get_backup_set_info_from_inner_table(common::ObArray<share::ObBackupSetFileDesc> &set_list);
  int get_archive_piece_info_from_inner_table(
      common::ObArray<share::ObPieceKey> &piece_list);
  int collect_backup_set_info_from_path(
      common::ObArray<share::ObBackupSetFileDesc> &set_list);
  int collect_archive_piece_info_from_path(
      common::ObArray<share::ObPieceKey> &piece_list);
  int collect_complement_log_piece_keys_(
      const share::ObBackupSetFileDesc &backup_set_info,
      common::ObArray<share::ObPieceKey> &piece_keys);
  int collect_piece_keys_from_file_list_(
      const share::ObBackupDest &complement_dest,
      common::ObArray<share::ObPieceKey> &piece_keys);
  int get_complement_log_backup_dest_(
      const share::ObBackupSetFileDesc &backup_set_info,
      share::ObBackupDest &complement_dest);
  int filter_and_sort_backup_sets(
      const common::ObArray<share::ObBackupSetFileDesc> &all_set_list,
      common::ObArray<share::ObBackupSetFileDesc> &set_list);
  int filter_and_sort_archive_pieces(
      const common::ObArray<share::ObPieceKey> &all_piece_list,
      common::ObArray<share::ObPieceKey> &piece_list);
  int get_backup_set_info_by_desc_(
      const share::ObBackupSetDesc &set_desc,
      storage::ObExternBackupSetInfoDesc &backup_set_info);
  int check_piece_support_basic_validate_(
      const share::ObArchiveStore &archive_store,
      const share::ObBackupPath &piece_path,
      bool &supported_basic_validate) const;
  struct CompareBackupSetInfo
  {
    bool operator()(const share::ObBackupSetFileDesc &lhs, const share::ObBackupSetFileDesc &rhs) const
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };
  struct CompareArchivePieceKey
  {
    bool operator()(const share::ObPieceKey &lhs, const share::ObPieceKey &rhs) const
    {
      if (lhs.dest_id_ != rhs.dest_id_) {
        return lhs.dest_id_ < rhs.dest_id_;
      } else if (lhs.round_id_ != rhs.round_id_) {
        return lhs.round_id_ < rhs.round_id_;
      } else {
        return lhs.piece_id_ < rhs.piece_id_;
      }
    }
  };

private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  share::ObBackupValidateJobAttr *job_attr_;
  uint64_t tenant_id_;
  int64_t backup_dest_id_;
  int64_t archive_dest_id_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateInfoCollector);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_VALIDATE_INFO_COLLECTOR_H_