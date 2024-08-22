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
struct ObAdminPieceKey final
{
  int64_t backup_set_id_;
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;

  ObAdminPieceKey()
  {
    backup_set_id_ = 0;
    dest_id_ = 0;
    round_id_ = 0;
    piece_id_ = 0;
  }
  ObAdminPieceKey(const int64_t dest_id, const int64_t round_id, const int64_t piece_id)
      : backup_set_id_(0), dest_id_(dest_id), round_id_(round_id), piece_id_(piece_id)
  {
  }
  ObAdminPieceKey(const int64_t backup_set_id, const int64_t dest_id, const int64_t round_id,
                  const int64_t piece_id)
      : backup_set_id_(backup_set_id), dest_id_(dest_id), round_id_(round_id), piece_id_(piece_id)
  {
  }
  ObAdminPieceKey(const share::ObPieceKey &other)
      : backup_set_id_(0), dest_id_(other.dest_id_), round_id_(other.round_id_),
        piece_id_(other.piece_id_)
  {
  }
  ObAdminPieceKey(const ObAdminPieceKey &other)
      : backup_set_id_(other.backup_set_id_), dest_id_(other.dest_id_), round_id_(other.round_id_),
        piece_id_(other.piece_id_)
  {
  }

  uint64_t hash() const;

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  void reset()
  {
    backup_set_id_ = 0;
    dest_id_ = 0;
    round_id_ = 0;
    piece_id_ = 0;
  }
  share::ObPieceKey to_piece_key() const
  {
    share::ObPieceKey piece_key;
    piece_key.dest_id_ = dest_id_;
    piece_key.round_id_ = round_id_;
    piece_key.piece_id_ = piece_id_;
    return piece_key;
  }
  void operator=(const ObAdminPieceKey &other)
  {
    backup_set_id_ = other.backup_set_id_;
    dest_id_ = other.dest_id_;
    round_id_ = other.round_id_;
    piece_id_ = other.piece_id_;
  }

  void operator=(const share::ObPieceKey &other)
  {
    backup_set_id_ = 0;
    dest_id_ = other.dest_id_;
    round_id_ = other.round_id_;
    piece_id_ = other.piece_id_;
  }

  bool operator==(const ObAdminPieceKey &other) const
  {
    return backup_set_id_ == other.backup_set_id_ && dest_id_ == other.dest_id_
           && round_id_ == other.round_id_ && piece_id_ == other.piece_id_;
  }

  bool operator!=(const ObAdminPieceKey &other) const { return !(*this == other); }

  bool operator<(const ObAdminPieceKey &other) const
  {
    bool ret = false;
    if (backup_set_id_ < other.backup_set_id_) {
      ret = true;
    } else if (round_id_ < other.round_id_) {
      ret = true;
    } else if (round_id_ == other.round_id_ && piece_id_ < other.piece_id_) {
      ret = true;
    }
    return ret;
  }

  TO_STRING_KV(K_(backup_set_id), K_(dest_id), K_(round_id), K_(piece_id));
};
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

  inline int64_t get_scheduled_tablet_count() const
  {
    return ATOMIC_LOAD(&scheduled_tablet_count_);
  }
  inline int64_t get_succeed_tablet_count() const { return ATOMIC_LOAD(&succeed_tablet_count_); }
  inline int64_t get_scheduled_macro_block_count() const
  {
    return ATOMIC_LOAD(&scheduled_macro_block_count_);
  }
  inline int64_t get_succeed_macro_block_count() const
  {
    return ATOMIC_LOAD(&succeed_macro_block_count_);
  }
  inline int64_t get_scheduled_piece_count() const { return ATOMIC_LOAD(&scheduled_piece_count_); }
  inline int64_t get_succeed_piece_count() const { return ATOMIC_LOAD(&succeed_piece_count_); }
  inline int64_t get_scheduled_lsn_range_count() const
  {
    return ATOMIC_LOAD(&scheduled_lsn_range_count_);
  }
  inline int64_t get_succeed_lsn_range_count() const
  {
    return ATOMIC_LOAD(&succeed_lsn_range_count_);
  }

private:
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
struct ObAdminBackupTabletValidationAttr final
{
  ObAdminBackupTabletValidationAttr();
  ~ObAdminBackupTabletValidationAttr();
  share::ObLSID ls_id_;
  share::ObBackupDataType data_type_;
  backup::ObBackupMetaIndex *sstable_meta_index_;
  backup::ObBackupMetaIndex *tablet_meta_index_;
  backup::ObBackupMetaIndex *macro_block_id_mappings_meta_index_;
  backup::ObBackupMacroBlockIDMappingsMeta *id_mappings_meta_;
  TO_STRING_KV(KP(tablet_meta_index_));
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupTabletValidationAttr);
};
struct ObAdminBackupLSValidationAttr final
{
  ObAdminBackupLSValidationAttr();
  ~ObAdminBackupLSValidationAttr();
  enum class ObAdminLSType { INVALID = 0, NORMAL = 1, DELETED = 2, POST_CONSTRUCTED = 3 };
  ObAdminLSType ls_type_;
  storage::ObLSMetaPackage ls_meta_package_;
  common::hash::ObHashMap<common::ObTabletID, ObAdminBackupTabletValidationAttr *> sys_tablet_map_;
  ObSingleLSInfoDesc *single_ls_info_desc_;
  int init();
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupLSValidationAttr);
};
struct ObAdminBackupSetValidationAttr final
{
  ObAdminBackupSetValidationAttr();
  ~ObAdminBackupSetValidationAttr();
  storage::ObBackupDataStore *backup_set_store_;
  storage::ObExternBackupSetInfoDesc *backup_set_info_desc_;
  common::hash::ObHashMap<share::ObLSID, ObAdminBackupLSValidationAttr *> ls_map_;
  ObArray<common::ObTabletID> user_tablet_id_; // not incluing sys
  common::hash::ObHashMap<common::ObTabletID, ObAdminBackupTabletValidationAttr *>
      minor_tablet_map_;
  common::hash::ObHashMap<common::ObTabletID, ObAdminBackupTabletValidationAttr *>
      major_tablet_map_;
  ObAdminBackupValidationStat stat_;
  TO_STRING_KV(KP(backup_set_store_));
  int init();
  int fetch_next_tablet_group(
      common::ObArray<common::ObArray<ObAdminBackupTabletValidationAttr *>> &tablet_group,
      int64_t &scheduled_cnt);
  bool is_all_tablet_done();

private:
  bool ls_inner_tablet_done_;
  int64_t user_tablet_pos_; // [0, user_tablet_pos_) already done
  ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupSetValidationAttr);
};
struct ObAdminBackupPieceValidationAttr final
{
  ObAdminBackupPieceValidationAttr();
  ~ObAdminBackupPieceValidationAttr();
  share::ObArchiveStore *backup_piece_store_;
  share::ObSinglePieceDesc *backup_piece_info_desc_;
  common::hash::ObHashMap<share::ObLSID, ObAdminBackupLSValidationAttr *> ls_map_;
  ObAdminBackupValidationStat stat_;
  TO_STRING_KV(KP(backup_piece_store_));
  int init();
  int split_lsn_range(
      ObArray<std::pair<share::ObLSID, std::pair<palf::LSN, palf::LSN>>> &lsn_range_array);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupPieceValidationAttr);
};
struct ObAdminBackupValidationCtx final
{
  explicit ObAdminBackupValidationCtx(ObArenaAllocator &arena);
  ~ObAdminBackupValidationCtx();
  int init();
  int limit_and_sleep(const int64_t bytes);
  int set_io_bandwidth(const int64_t bandwidth);
  void print_log_archive_validation_status();
  void print_data_backup_validation_status();
  int go_abort(const char *fail_file, const char *fail_reason);
  // stateful, no call twice
  int add_backup_set(int64_t backup_set_id);
  int get_backup_set_attr(int64_t backup_set_id, ObAdminBackupSetValidationAttr *&backup_set_attr);
  int add_ls(int64_t backup_set_id, const share::ObLSID &ls_id);
  int get_ls_attr(int64_t backup_set_id, const share::ObLSID &ls_id,
                  ObAdminBackupLSValidationAttr *&ls_attr);
  int add_tablet(int64_t backup_set_id, const share::ObLSID &ls_id,
                 const share::ObBackupDataType &data_type, const common::ObTabletID &tablet_id);
  int get_tablet_attr(int64_t backup_set_id, const share::ObLSID &ls_id,
                      const share::ObBackupDataType &data_type, const common::ObTabletID &tablet_id,
                      ObAdminBackupTabletValidationAttr *&tablet_attr);
  int add_backup_piece(const ObAdminPieceKey &backup_piece_key);
  int get_backup_piece_attr(const ObAdminPieceKey &backup_piece_key,
                            ObAdminBackupPieceValidationAttr *&backup_piece_attr);
  int add_ls(const ObAdminPieceKey &backup_piece_key, const share::ObLSID &ls_id);
  int get_ls_attr(const ObAdminPieceKey &backup_piece_key, const share::ObLSID &ls_id,
                  ObAdminBackupLSValidationAttr *&ls_attr);
  // fill before validation
  ObAdminBackupValidationType validation_type_;
  share::ObBackupDest *log_archive_dest_;
  share::ObBackupDest *data_backup_dest_;
  common::ObArray<share::ObBackupDest *> backup_piece_path_array_;
  common::ObArray<share::ObBackupDest *> backup_set_path_array_;
  common::ObArray<ObAdminPieceKey> backup_piece_key_array_;
  common::ObArray<int64_t> backup_set_id_array_;
  blocksstable::ObMacroBlockCheckLevel mb_check_level_;
  // fill during validation
  bool aborted_;
  common::hash::ObHashMap<int64_t, ObAdminBackupSetValidationAttr *> backup_set_map_;
  common::hash::ObHashMap<ObAdminPieceKey, ObAdminBackupPieceValidationAttr *> backup_piece_map_;
  common::ObArray<int64_t> processing_backup_set_id_array_;
  common::ObArray<ObAdminPieceKey> processing_backup_piece_key_array_;
  // TODO: give a sql_proxy
  common::ObMySQLProxy *sql_proxy_;
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