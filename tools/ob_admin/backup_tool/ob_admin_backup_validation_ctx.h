/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OB_ADMIN_BACKUP_VALIDATION_CTX_H_
#define OB_ADMIN_BACKUP_VALIDATION_CTX_H_
#include "ob_admin_backup_validation_executor.h"
#include "share/backup/ob_archive_store.h"
#include "storage/backup/ob_backup_data_store.h"
namespace oceanbase
{
namespace tools
{
// memory issue
// lock free
struct ObAdminBackupValidationStat final
{
  ObAdminBackupValidationStat()
      : scheduled_tablet_count_(0), succeed_tablet_count_(0), scheduled_macro_block_count_(0),
        succeed_macro_block_count_(0), scheduled_piece_count_(0), succeed_piece_count_(0),
        scheduled_lsn_range_count_(0), succeed_lsn_range_count_(0)
  {
  }
  ~ObAdminBackupValidationStat() {}
  inline int add_scheduled_tablet_count_(int64_t count)
  {
    ATOMIC_AAF(&scheduled_tablet_count_, count);
    return OB_SUCCESS;
  }
  inline int add_succeed_tablet_count_(int64_t count)
  {
    ATOMIC_AAF(&succeed_tablet_count_, count);
    return OB_SUCCESS;
  }
  inline int add_scheduled_macro_block_count_(int64_t count)
  {
    ATOMIC_AAF(&scheduled_macro_block_count_, count);
    return OB_SUCCESS;
  }
  inline int add_succeed_macro_block_count_(int64_t count)
  {
    ATOMIC_AAF(&succeed_macro_block_count_, count);
    return OB_SUCCESS;
  }

  inline int add_scheduled_piece_count_(int64_t count)
  {
    ATOMIC_AAF(&scheduled_piece_count_, count);
    return OB_SUCCESS;
  }
  inline int add_succeed_piece_count_(int64_t count)
  {
    ATOMIC_AAF(&succeed_piece_count_, count);
    return OB_SUCCESS;
  }
  inline int add_scheduled_lsn_range_count_(int64_t count)
  {
    ATOMIC_AAF(&scheduled_lsn_range_count_, count);
    return OB_SUCCESS;
  }
  inline int add_succeed_lsn_range_count_(int64_t count)
  {
    ATOMIC_AAF(&succeed_lsn_range_count_, count);
    return OB_SUCCESS;
  }
  int64_t scheduled_tablet_count_;
  int64_t succeed_tablet_count_;
  int64_t scheduled_macro_block_count_;
  int64_t succeed_macro_block_count_;
  int64_t scheduled_piece_count_;
  int64_t succeed_piece_count_;
  int64_t scheduled_lsn_range_count_;
  int64_t succeed_lsn_range_count_;
  TO_STRING_KV(K(scheduled_tablet_count_), K(succeed_tablet_count_),
               K(scheduled_macro_block_count_), K(succeed_macro_block_count_),
               K(scheduled_piece_count_), K(succeed_piece_count_), K(scheduled_lsn_range_count_),
               K(succeed_lsn_range_count_));
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupValidationStat);
};
struct ObAdminTabletAttr final
{
  ObAdminTabletAttr();
  ~ObAdminTabletAttr();
  share::ObLSID ls_id_;
  share::ObBackupDataType data_type_;
  backup::ObBackupMetaIndex *sstable_meta_index_;
  backup::ObBackupMetaIndex *tablet_meta_index_;
  backup::ObBackupMetaIndex *macro_block_id_mappings_meta_index_;
  backup::ObBackupMacroBlockIDMappingsMeta *id_mappings_meta_;
  TO_STRING_KV(KP(tablet_meta_index_));
  DISALLOW_COPY_AND_ASSIGN(ObAdminTabletAttr);
};
struct ObAdminLSAttr final
{
  ObAdminLSAttr();
  ~ObAdminLSAttr();
  enum ObAdminLSType { INVALID = 0, NORMAL = 1, DELETED = 2, POST_CONSTRUCTED = 3 };
  ObAdminLSType ls_type_;
  storage::ObLSMetaPackage ls_meta_package_;
  common::hash::ObHashMap<common::ObTabletID, ObAdminTabletAttr *> sys_tablet_map_;
  ObSingleLSInfoDesc *single_ls_info_desc_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminLSAttr);
};
struct ObAdminBackupSetAttr final
{
  ObAdminBackupSetAttr();
  ~ObAdminBackupSetAttr();
  storage::ObBackupDataStore *backup_set_store_;
  storage::ObExternBackupSetInfoDesc *backup_set_info_desc_;
  common::hash::ObHashMap<share::ObLSID, ObAdminLSAttr *> ls_map_;
  ObArray<common::ObTabletID> minor_tablet_id_; // not incluing sys
  ObArray<common::ObTabletID> major_tablet_id_; // not incluing sys
  common::hash::ObHashMap<common::ObTabletID, ObAdminTabletAttr *> minor_tablet_map_;
  common::hash::ObHashMap<common::ObTabletID, ObAdminTabletAttr *> major_tablet_map_;
  ObAdminBackupValidationStat stat_;
  TO_STRING_KV(KP(backup_set_store_));
  int fetch_next_tablet_group(common::ObArray<common::ObArray<ObAdminTabletAttr *>> &tablet_group,
                              int64_t &scheduled_cnt);
  bool is_all_tablet_done();

private:
  bool ls_inner_tablet_done_;
  int64_t minor_tablet_pos_; // [0, minor_tablet_pos_) already done
  int64_t major_tablet_pos_; // [0, major_tablet_pos_) already done
  ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupSetAttr);
};
struct ObAdminBackupPieceAttr final
{
  ObAdminBackupPieceAttr();
  ~ObAdminBackupPieceAttr();
  share::ObArchiveStore *backup_piece_store_;
  share::ObSinglePieceDesc *backup_piece_info_desc_;
  common::hash::ObHashMap<share::ObLSID, ObAdminLSAttr *> ls_map_;
  ObAdminBackupValidationStat stat_;
  TO_STRING_KV(KP(backup_piece_store_));
  int split_lsn_range(
      ObArray<std::pair<share::ObLSID, std::pair<palf::LSN, palf::LSN>>> &lsn_range_array);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupPieceAttr);
};
struct ObAdminBackupValidationCtx final
{
  ObAdminBackupValidationCtx(ObArenaAllocator &arena);
  ~ObAdminBackupValidationCtx();
  int limit_and_sleep(const int64_t bytes);
  int set_io_bandwidth(const int64_t bandwidth);
  void print_log_archive_validation_status();
  void print_data_backup_validation_status();
  int go_abort(const char *fail_file, const char *fail_reason);
  // stateful, no call twice
  int add_backup_set(int64_t backup_set_id);
  int get_backup_set_attr(int64_t backup_set_id, ObAdminBackupSetAttr *&backup_set_attr);
  int add_ls(int64_t backup_set_id, const share::ObLSID &ls_id);
  int get_ls_attr(int64_t backup_set_id, const share::ObLSID &ls_id, ObAdminLSAttr *&ls_attr);
  int add_tablet(int64_t backup_set_id, const share::ObLSID &ls_id,
                 const share::ObBackupDataType &data_type, const common::ObTabletID &tablet_id);
  int get_tablet_attr(int64_t backup_set_id, const share::ObLSID &ls_id,
                      const share::ObBackupDataType &data_type, const common::ObTabletID &tablet_id,
                      ObAdminTabletAttr *&tablet_attr);
  int add_backup_piece(const share::ObPieceKey &backup_piece_key);
  int get_backup_piece_attr(const share::ObPieceKey &backup_piece_key,
                            ObAdminBackupPieceAttr *&backup_piece_attr);
  int add_ls(const share::ObPieceKey &backup_piece_key, const share::ObLSID &ls_id);
  int get_ls_attr(const share::ObPieceKey &backup_piece_key, const share::ObLSID &ls_id,
                  ObAdminLSAttr *&ls_attr);
  // fill before validation
  ObAdminBackupValidationType validation_type_;
  share::ObBackupDest *log_archive_dest_;
  share::ObBackupDest *data_backup_dest_;
  common::ObArray<share::ObBackupDest *> backup_piece_path_array_;
  common::ObArray<share::ObBackupDest *> backup_set_path_array_;
  common::ObArray<share::ObPieceKey> backup_piece_key_array_;
  common::ObArray<int64_t> backup_set_id_array_;
  blocksstable::ObMacroBlockCheckLevel mb_check_level_;
  // fill during validation
  bool aborted_;
  common::hash::ObHashMap<int64_t, ObAdminBackupSetAttr *> backup_set_map_;
  common::hash::ObHashMap<share::ObPieceKey, ObAdminBackupPieceAttr *> backup_piece_map_;
  common::ObArray<int64_t> processing_backup_set_id_array_;
  common::ObArray<share::ObPieceKey> processing_backup_piece_key_array_;
  // TODO: give a sql_proxy
  common::ObMySQLProxy* sql_proxy_;
  ObAdminBackupValidationStat global_stat_;
  common::ObSafeArenaAllocator allocator_;

private:
  char *fail_file_;
  char *fail_reason_;
  obsys::ObRWLock lock_;
  ObBandwidthThrottle throttle_;
  int64_t last_active_time_;
  const char states_icon_[4] = {'|', '\\', '-', '/'};
  int states_icon_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupValidationCtx);
};
}; // namespace tools
}; // namespace oceanbase
#endif