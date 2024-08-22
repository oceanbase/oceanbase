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
#include "ob_admin_backup_validation_ctx.h"
#define CLEAR_LINE "\033[1K\033[0G"
namespace oceanbase
{
namespace tools
{
uint64_t ObAdminPieceKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&backup_set_id_, sizeof(backup_set_id_), hash_val);
  hash_val = murmurhash(&dest_id_, sizeof(dest_id_), hash_val);
  hash_val = murmurhash(&round_id_, sizeof(round_id_), hash_val);
  hash_val = murmurhash(&piece_id_, sizeof(piece_id_), hash_val);
  return hash_val;
}
ObAdminBackupTabletValidationAttr::ObAdminBackupTabletValidationAttr()
    : ls_id_(), data_type_(), sstable_meta_index_(nullptr), tablet_meta_index_(nullptr),
      macro_block_id_mappings_meta_index_(nullptr), id_mappings_meta_(nullptr)
{
}
ObAdminBackupTabletValidationAttr::~ObAdminBackupTabletValidationAttr()
{
  if (OB_NOT_NULL(sstable_meta_index_)) {
    sstable_meta_index_->~ObBackupMetaIndex();
    sstable_meta_index_ = nullptr;
  }
  if (OB_NOT_NULL(tablet_meta_index_)) {
    tablet_meta_index_->~ObBackupMetaIndex();
    tablet_meta_index_ = nullptr;
  }
  if (OB_NOT_NULL(macro_block_id_mappings_meta_index_)) {
    macro_block_id_mappings_meta_index_->~ObBackupMetaIndex();
    macro_block_id_mappings_meta_index_ = nullptr;
  }
  if (OB_NOT_NULL(id_mappings_meta_)) {
    id_mappings_meta_->~ObBackupMacroBlockIDMappingsMeta();
    id_mappings_meta_ = nullptr;
  }
}
ObAdminBackupLSValidationAttr::ObAdminBackupLSValidationAttr()
    : ls_type_(ObAdminLSType::INVALID), ls_meta_package_(), single_ls_info_desc_(nullptr)
{
}
ObAdminBackupLSValidationAttr::~ObAdminBackupLSValidationAttr()
{
  FOREACH(iter, sys_tablet_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupTabletValidationAttr();
      iter->second = nullptr;
    }
  }
  sys_tablet_map_.destroy();
  if (OB_NOT_NULL(single_ls_info_desc_)) {
    single_ls_info_desc_->~ObSingleLSInfoDesc();
    single_ls_info_desc_ = nullptr;
  }
}
int ObAdminBackupLSValidationAttr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sys_tablet_map_.create(
          ObAdminBackupValidationExecutor::DEFAULT_BACKUP_TABLET_BUCKET_NUM, ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create sys tablet map", K(ret));
  }
  return ret;
}
ObAdminBackupSetValidationAttr::ObAdminBackupSetValidationAttr()
    : backup_set_store_(nullptr), backup_set_info_desc_(nullptr), ls_inner_tablet_done_(false),
      user_tablet_pos_(0), lock_(ObLatchIds::BACKUP_LOCK)
{
}
ObAdminBackupSetValidationAttr::~ObAdminBackupSetValidationAttr()
{
  if (OB_NOT_NULL(backup_set_store_)) {
    backup_set_store_->~ObBackupDataStore();
    backup_set_store_ = nullptr;
  }
  if (OB_NOT_NULL(backup_set_info_desc_)) {
    backup_set_info_desc_->~ObExternBackupSetInfoDesc();
    backup_set_info_desc_ = nullptr;
  }
  FOREACH(iter, ls_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupLSValidationAttr();
      iter->second = nullptr;
    }
  }
  ls_map_.destroy();
  FOREACH(iter, minor_tablet_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupTabletValidationAttr();
      iter->second = nullptr;
    }
  }
  minor_tablet_map_.destroy();
  FOREACH(iter, major_tablet_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupTabletValidationAttr();
      iter->second = nullptr;
    }
  }
  major_tablet_map_.destroy();
}
int ObAdminBackupSetValidationAttr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_map_.create(ObAdminBackupValidationExecutor::DEFAULT_BACKUP_LS_BUCKET_NUM,
                             ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create ls map", K(ret));
  } else if (OB_FAIL(minor_tablet_map_.create(
                 ObAdminBackupValidationExecutor::DEFAULT_BACKUP_TABLET_BUCKET_NUM,
                 ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create minor tablet map", K(ret));
  } else if (OB_FAIL(major_tablet_map_.create(
                 ObAdminBackupValidationExecutor::DEFAULT_BACKUP_TABLET_BUCKET_NUM,
                 ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create major tablet map", K(ret));
  }
  return ret;
}
int ObAdminBackupSetValidationAttr::fetch_next_tablet_group(
    common::ObArray<common::ObArray<ObAdminBackupTabletValidationAttr *>> &tablet_group,
    int64_t &scheduled_cnt)
{
  ObSpinLockGuard guard(lock_);
  tablet_group.reset();
  int ret = OB_SUCCESS;
  scheduled_cnt = 0;
  if (ls_inner_tablet_done_ && user_tablet_pos_ >= user_tablet_id_.count()) {
    ret = OB_ITER_END;
    STORAGE_LOG(INFO, "all tablet done", K(ret));
  } else {
    common::ObArray<ObAdminBackupTabletValidationAttr *> inner_group;
    if (!ls_inner_tablet_done_) {
      FOREACH_X(ls_map_iter, ls_map_, OB_SUCC(ret))
      {
        inner_group.reuse();
        if (OB_ISNULL(ls_map_iter->second)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "ls map iter is null", K(ret));
        } else if (ls_map_iter->second->ls_type_
                   != ObAdminBackupLSValidationAttr::ObAdminLSType::NORMAL) {
          STORAGE_LOG(INFO, "abnormal ls detected, maybe post-construct, skip inner tablet",
                      K(ls_map_iter->second->ls_meta_package_));
        } else {
          FOREACH_X(sys_tablet_map_iter, ls_map_iter->second->sys_tablet_map_, OB_SUCC(ret))
          {
            if (OB_FAIL(inner_group.push_back(sys_tablet_map_iter->second))) {
              STORAGE_LOG(WARN, "failed to push back tablet attr", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(tablet_group.push_back(inner_group))) {
              STORAGE_LOG(WARN, "failed to push back tablet group", K(ret));
            } else {
              scheduled_cnt += inner_group.count();
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ls_inner_tablet_done_ = true;
      }
    }
    while (OB_SUCC(ret)
           && scheduled_cnt + 2 <= ObAdminBackupValidationExecutor::MAX_TABLET_BATCH_COUNT
           && user_tablet_pos_ < user_tablet_id_.count()) {
      ObAdminBackupTabletValidationAttr *minor_tablet_attr = nullptr;
      ObAdminBackupTabletValidationAttr *major_tablet_attr = nullptr;
      inner_group.reuse();
      if (OB_FAIL(minor_tablet_map_.get_refactored(user_tablet_id_.at(user_tablet_pos_),
                                                   minor_tablet_attr))) {
        STORAGE_LOG(WARN, "failed to get tablet attr", K(ret),
                    K(user_tablet_id_.at(user_tablet_pos_)));
      } else if (OB_FAIL(major_tablet_map_.get_refactored(user_tablet_id_.at(user_tablet_pos_),
                                                          major_tablet_attr))) {
        STORAGE_LOG(WARN, "failed to get tablet attr", K(ret),
                    K(user_tablet_id_.at(user_tablet_pos_)));
      } else if (OB_ISNULL(minor_tablet_attr->tablet_meta_index_)
                 || OB_ISNULL(major_tablet_attr->tablet_meta_index_)) {
        // incompelete tablet, should already marked as skipped
        STORAGE_LOG(WARN, "incompelete tablet, skip validation", K(ret));
      } else if (OB_FAIL(inner_group.push_back(minor_tablet_attr))) {
        STORAGE_LOG(WARN, "failed to push back tablet attr", K(ret));
      } else if (OB_FAIL(inner_group.push_back(major_tablet_attr))) {
        STORAGE_LOG(WARN, "failed to push back tablet attr", K(ret));
      } else if (OB_FAIL(tablet_group.push_back(inner_group))) {
        STORAGE_LOG(WARN, "failed to push back tablet group", K(ret));
      } else {
        scheduled_cnt += inner_group.count();
      }
      ++user_tablet_pos_;
    }

    if (OB_SUCC(ret) && ls_inner_tablet_done_ && user_tablet_pos_ >= user_tablet_id_.count()) {
      ret = OB_ITER_END;
      STORAGE_LOG(INFO, "all tablet done", K(ret));
    }
    STORAGE_LOG(INFO, "succeed to fetch next tablet group", K(ret), K(ls_inner_tablet_done_),
                K(user_tablet_pos_), K(user_tablet_id_.count()));
  }
  return ret;
}
bool ObAdminBackupSetValidationAttr::is_all_tablet_done()
{
  ObSpinLockGuard guard(lock_);
  return ls_inner_tablet_done_ && user_tablet_pos_ >= user_tablet_id_.count();
}
ObAdminBackupPieceValidationAttr::ObAdminBackupPieceValidationAttr() : backup_piece_store_(nullptr)
{
}
ObAdminBackupPieceValidationAttr::~ObAdminBackupPieceValidationAttr()
{
  if (OB_NOT_NULL(backup_piece_store_)) {
    backup_piece_store_->~ObArchiveStore();
    backup_piece_store_ = nullptr;
  }
  if (OB_NOT_NULL(backup_piece_info_desc_)) {
    backup_piece_info_desc_->~ObSinglePieceDesc();
    backup_piece_info_desc_ = nullptr;
  }
  FOREACH(iter, ls_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupLSValidationAttr();
      iter->second = nullptr;
    }
  }
}
int ObAdminBackupPieceValidationAttr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_map_.create(ObAdminBackupValidationExecutor::DEFAULT_BACKUP_LS_BUCKET_NUM,
                             ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create ls map", K(ret));
  }
  return ret;
}
int ObAdminBackupPieceValidationAttr::split_lsn_range(
    ObArray<std::pair<share::ObLSID, std::pair<palf::LSN, palf::LSN>>> &lsn_range_array)
{
  int ret = OB_SUCCESS;
  FOREACH_X(ls_map_iter, ls_map_, OB_SUCC(ret))
  {
    const share::ObLSID ls_id = ls_map_iter->second->single_ls_info_desc_->ls_id_;
    palf::LSN start_lsn(ls_map_iter->second->single_ls_info_desc_->min_lsn_);
    const palf::LSN end_lsn(ls_map_iter->second->single_ls_info_desc_->max_lsn_);
    if (backup_piece_info_desc_->piece_.is_active()) {
      if (OB_FAIL(lsn_range_array.push_back(
              std::make_pair(ls_id, std::make_pair(start_lsn, end_lsn))))) {
        STORAGE_LOG(WARN, "failed to push back lsn range", K(ret));
        break;
      } else {
        continue;
      }
    }
    while (OB_SUCC(ret) && start_lsn < end_lsn) {
      palf::LSN partial_lsn = start_lsn + palf::PALF_BLOCK_SIZE;
      if (partial_lsn > end_lsn) {
        partial_lsn = end_lsn;
      }
      if (OB_FAIL(lsn_range_array.push_back(
              std::make_pair(ls_id, std::make_pair(start_lsn, partial_lsn))))) {
        STORAGE_LOG(WARN, "failed to push back lsn range", K(ret));
      } else {
        start_lsn = partial_lsn;
      }
    }
  }
  return ret;
}
ObAdminBackupValidationCtx::ObAdminBackupValidationCtx(ObArenaAllocator &arena)
    : aborted_(false), sql_proxy_(nullptr), global_stat_(), allocator_(arena), throttle_(),
      states_icon_pos_(0)
{
}
ObAdminBackupValidationCtx::~ObAdminBackupValidationCtx()
{
  if (OB_NOT_NULL(log_archive_dest_)) {
    log_archive_dest_->~ObBackupDest();
    log_archive_dest_ = nullptr;
  }
  if (OB_NOT_NULL(data_backup_dest_)) {
    data_backup_dest_->~ObBackupDest();
    data_backup_dest_ = nullptr;
  }
  for (int64_t i = 0; i < backup_piece_path_array_.count(); i++) {
    if (OB_NOT_NULL(backup_piece_path_array_.at(i))) {
      backup_piece_path_array_.at(i)->~ObBackupDest();
      backup_piece_path_array_.at(i) = nullptr;
    }
  }
  for (int64_t i = 0; i < backup_set_path_array_.count(); i++) {
    if (OB_NOT_NULL(backup_set_path_array_.at(i))) {
      backup_set_path_array_.at(i)->~ObBackupDest();
      backup_set_path_array_.at(i) = nullptr;
    }
  }
  FOREACH(iter, backup_set_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupSetValidationAttr();
      iter->second = nullptr;
    }
  }
  backup_set_map_.destroy();
  FOREACH(iter, backup_piece_map_)
  {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObAdminBackupPieceValidationAttr();
      iter->second = nullptr;
    }
  }
  backup_piece_map_.destroy();
  throttle_.destroy();
}
int ObAdminBackupValidationCtx::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(throttle_.init(INT64_MAX /*default io_bandwidth*/))) {
    STORAGE_LOG(WARN, "failed to init throttle", K(ret));
  } else if (OB_FAIL(backup_set_map_.create(
                 ObAdminBackupValidationExecutor::DEFAULT_BACKUP_SET_BUCKET_NUM,
                 ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create backup set map", K(ret));
  } else if (OB_FAIL(backup_piece_map_.create(
                 ObAdminBackupValidationExecutor::DEFAULT_BACKUP_PIECE_BUCKET_NUM,
                 ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create backup piece map", K(ret));
  }
  return ret;
}
int ObAdminBackupValidationCtx::limit_and_sleep(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  if (OB_FAIL(throttle_.limit_and_sleep(bytes, last_active_time_, INT64_MAX, sleep_us))) {
    STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(bytes));
  } else {
    ATOMIC_SET(&last_active_time_, ObTimeUtility::current_time());
  }
  return ret;
}
int ObAdminBackupValidationCtx::set_io_bandwidth(const int64_t bandwidth)
{
  return throttle_.set_rate(bandwidth * 1000 * 1000 / 8);
}
void ObAdminBackupValidationCtx::print_log_archive_validation_status()
{
  obsys::ObRLockGuard guard(lock_);
  if (!aborted_) {
    if (0 == global_stat_.get_scheduled_lsn_range_count()) {
      printf(CLEAR_LINE);
      printf("%c Validating Meta info of Backup pieces", states_icon_[states_icon_pos_]);
    } else {
      printf(CLEAR_LINE);
      printf("%c Total Pieces(Scheduled/Succeed): %ld/%ld, Total LSN "
             "Range(Scheduled/Succeed): %ld/%ld",
             states_icon_[states_icon_pos_], global_stat_.get_scheduled_piece_count(),
             global_stat_.get_succeed_piece_count(), global_stat_.get_scheduled_lsn_range_count(),
             global_stat_.get_succeed_lsn_range_count());
    }
  } else {
    printf(CLEAR_LINE);
    printf("%c Corrupted File Found: %s, Maybe due to %s", states_icon_[states_icon_pos_],
           fail_file_, fail_reason_);
  }
  states_icon_pos_ = (states_icon_pos_ + 1) % sizeof(states_icon_);
  fflush(stdout);
}
void ObAdminBackupValidationCtx::print_data_backup_validation_status()
{
  obsys::ObRLockGuard guard(lock_);
  if (!aborted_) {
    if (0 == global_stat_.get_scheduled_macro_block_count()) {
      printf(CLEAR_LINE);
      printf("%c Validating Meta info of Backup sets", states_icon_[states_icon_pos_]);
    } else {
      printf(CLEAR_LINE);
      printf("%c Total Tablets(Scheduled/Succeed): %ld/%ld, Total Marco "
             "Blocks(Scheduled/Succeed): %ld/%ld",
             states_icon_[states_icon_pos_], global_stat_.get_scheduled_tablet_count(),
             global_stat_.get_succeed_tablet_count(),
             global_stat_.get_scheduled_macro_block_count(),
             global_stat_.get_succeed_macro_block_count());
    }
  } else {
    printf(CLEAR_LINE);
    printf("%c Corrupted File Found: %s, Maybe due to %s", states_icon_[states_icon_pos_],
           fail_file_, fail_reason_);
  }
  states_icon_pos_ = (states_icon_pos_ + 1) % sizeof(states_icon_);
  fflush(stdout);
}
int ObAdminBackupValidationCtx::go_abort(const char *fail_file, const char *fail_reason)
{
  obsys::ObWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (!aborted_) {
    aborted_ = true;
    if (OB_ISNULL(fail_file_ = static_cast<char *>(allocator_.alloc(strlen(fail_file) + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } else if (OB_ISNULL(fail_reason_
                         = static_cast<char *>(allocator_.alloc(strlen(fail_reason) + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } else {
      MEMCPY(fail_file_, fail_file, strlen(fail_file) + 1);
      MEMCPY(fail_reason_, fail_reason, strlen(fail_reason) + 1);
    }
  }
  return ret;
}
int ObAdminBackupValidationCtx::add_backup_set(int64_t backup_set_id)
{
  obsys::ObWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupSetValidationAttr *backup_set_attr = nullptr;
  if (OB_FAIL(backup_set_map_.get_refactored(backup_set_id, backup_set_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // not exist, it's good
      if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupSetValidationAttr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(backup_set_attr = new (alc_ptr) ObAdminBackupSetValidationAttr())) {
      } else if (OB_FAIL(backup_set_attr->init())) {
        STORAGE_LOG(WARN, "fail to init backup set attr", K(ret));
      } else if (OB_FAIL(backup_set_map_.set_refactored(backup_set_id, backup_set_attr))) {
        STORAGE_LOG(WARN, "fail to set backup set id", K(ret), K(backup_set_id));
      } else if (OB_FAIL(processing_backup_set_id_array_.push_back(backup_set_id))) {
        STORAGE_LOG(WARN, "fail to push back backup set id", K(ret), K(backup_set_id));
      } else {
        backup_set_attr = nullptr; // moved success
        STORAGE_LOG(INFO, "succeed to add backup set id", K(ret), K(backup_set_id));
      }
      if (OB_NOT_NULL(backup_set_attr)) {
        backup_set_attr->~ObAdminBackupSetValidationAttr();
        backup_set_attr = nullptr;
      }
    }
  } else {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "backup set id already exist", K(ret), K(backup_set_id));
  }
  return ret;
}
int ObAdminBackupValidationCtx::get_backup_set_attr(
    int64_t backup_set_id, ObAdminBackupSetValidationAttr *&backup_set_attr)
{
  obsys::ObRLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_set_map_.get_refactored(backup_set_id, backup_set_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "fail to get backup set attr", K(ret), K(backup_set_id));
    } else {
      STORAGE_LOG(WARN, "unexpected fail to get backup set id", K(ret), K(backup_set_id));
    }
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "unexpected null backup set attr", K(ret), K(backup_set_id));
  }
  return ret;
}
int ObAdminBackupValidationCtx::add_ls(int64_t backup_set_id, const share::ObLSID &ls_id)
{
  obsys::ObWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupSetValidationAttr *backup_set_attr = nullptr;
  ObAdminBackupLSValidationAttr *ls_attr = nullptr;
  if (OB_FAIL(get_backup_set_attr(backup_set_id, backup_set_attr))) {
  } else if (OB_FAIL(backup_set_attr->ls_map_.get_refactored(ls_id, ls_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // not exist, it's good
      if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupLSValidationAttr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(ls_attr = new (alc_ptr) ObAdminBackupLSValidationAttr())) {
      } else if (OB_FAIL(ls_attr->init())) {
        STORAGE_LOG(WARN, "fail to init ls attr", K(ret));
      } else if (OB_FAIL(backup_set_attr->ls_map_.set_refactored(ls_id, ls_attr))) {
        STORAGE_LOG(WARN, "fail to set ls id", K(ret), K(ls_id));
      } else {
        ls_attr = nullptr;
        STORAGE_LOG(INFO, "succeed to add ls id", K(ret), K(ls_id));
      }
      if (OB_NOT_NULL(ls_attr)) {
        ls_attr->~ObAdminBackupLSValidationAttr();
        ls_attr = nullptr;
      }
    } else {
      STORAGE_LOG(WARN, "unexpected fail to get ls id", K(ret), K(ls_id));
    }
  } else {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "ls id already exist", K(ret), K(ls_id));
  }
  return ret;
}
int ObAdminBackupValidationCtx::get_ls_attr(int64_t backup_set_id, const share::ObLSID &ls_id,
                                            ObAdminBackupLSValidationAttr *&ls_attr)
{
  obsys::ObRLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  ObAdminBackupSetValidationAttr *backup_set_attr = nullptr;
  if (OB_FAIL(get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "unexpected fail to get backup set id", K(ret), K(backup_set_id));
  } else if (OB_FAIL(backup_set_attr->ls_map_.get_refactored(ls_id, ls_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "fail to get ls attr", K(ret), K(backup_set_id), K(ls_id));
    } else {
      STORAGE_LOG(WARN, "unexpected fail to get ls id", K(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls_attr)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "unexpected null ls attr", K(ret), K(backup_set_id), K(ls_id));
  }
  return ret;
}
int ObAdminBackupValidationCtx::add_tablet(int64_t backup_set_id, const share::ObLSID &ls_id,
                                           const share::ObBackupDataType &data_type,
                                           const common::ObTabletID &tablet_id)
{
  obsys::ObWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupSetValidationAttr *backup_set_attr = nullptr;
  ObAdminBackupLSValidationAttr *ls_attr = nullptr;
  ObAdminBackupTabletValidationAttr *tablet_attr = nullptr;
  if (OB_FAIL(get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "unexpected fail to get backup set attr", K(ret), K(backup_set_id));
  } else if (OB_FAIL(get_ls_attr(backup_set_id, ls_id, ls_attr))) {
    STORAGE_LOG(WARN, "unexpected fail to get ls attr", K(ret), K(backup_set_id), K(ls_id));
  } else if (ObAdminBackupLSValidationAttr::ObAdminLSType::DELETED == ls_attr->ls_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be deleted", K(ret), K(backup_set_id), K(ls_id));
  } else {
    switch (data_type.type_) {
    case share::ObBackupDataType::BACKUP_SYS: {
      if (OB_FAIL(ls_attr->sys_tablet_map_.get_refactored(tablet_id, tablet_attr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // not exist, it's
          // good
          if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupTabletValidationAttr)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
          } else if (FALSE_IT(tablet_attr = new (alc_ptr) ObAdminBackupTabletValidationAttr())) {
          } else if (FALSE_IT(tablet_attr->ls_id_ = ls_id)) {
          } else if (FALSE_IT(tablet_attr->data_type_ = data_type)) {
          } else if (OB_FAIL(ls_attr->sys_tablet_map_.set_refactored(tablet_id, tablet_attr))) {
            STORAGE_LOG(WARN, "fail to set tablet attr", K(ret), K(tablet_id));
          } else if (OB_FAIL(global_stat_.add_scheduled_tablet_count_(1))) {
            STORAGE_LOG(WARN, "fail to add scheduled tablet count", K(ret));
          } else if (OB_FAIL(backup_set_attr->stat_.add_scheduled_tablet_count_(1))) {
            STORAGE_LOG(WARN, "fail to add scheduled tablet count", K(ret));
          } else {
            tablet_attr = nullptr;
            STORAGE_LOG(DEBUG, "succeed to add tablet", K(ret), K(tablet_id));
          }
          if (OB_NOT_NULL(tablet_attr)) {
            tablet_attr->~ObAdminBackupTabletValidationAttr();
            tablet_attr = nullptr;
          }
        } else {
          STORAGE_LOG(WARN, "unexpected fail to get tablet attr", K(ret), K(tablet_id));
        }
      } else {
        ret = OB_ENTRY_EXIST;
        STORAGE_LOG(WARN, "tablet id already exist", K(ret), K(tablet_id));
      }
      break;
    }
    case share::ObBackupDataType::BACKUP_MINOR: {
      if (OB_FAIL(backup_set_attr->minor_tablet_map_.get_refactored(tablet_id, tablet_attr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // not exist, it's good
          if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupTabletValidationAttr)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
          } else if (FALSE_IT(tablet_attr = new (alc_ptr) ObAdminBackupTabletValidationAttr())) {
          } else if (FALSE_IT(tablet_attr->ls_id_ = ls_id)) {
          } else if (FALSE_IT(tablet_attr->data_type_ = data_type)) {
          } else if (OB_FAIL(backup_set_attr->minor_tablet_map_.set_refactored(tablet_id,
                                                                               tablet_attr))) {
            STORAGE_LOG(WARN, "fail to set tablet attr", K(ret), K(tablet_id));
          } else if (OB_FAIL(backup_set_attr->user_tablet_id_.push_back(tablet_id))) {
            STORAGE_LOG(WARN, "fail to push back tablet id", K(ret), K(tablet_id));
          } else if (OB_FAIL(global_stat_.add_scheduled_tablet_count_(1))) {
            STORAGE_LOG(WARN, "fail to add scheduled tablet count", K(ret));
          } else if (OB_FAIL(backup_set_attr->stat_.add_scheduled_tablet_count_(1))) {
            STORAGE_LOG(WARN, "fail to add scheduled tablet count", K(ret));
          } else {
            tablet_attr = nullptr;
            STORAGE_LOG(DEBUG, "succeed to add tablet id", K(ret), K(tablet_id));
          }
        } else {
          STORAGE_LOG(WARN, "unexpected fail to get tablet id", K(ret), K(tablet_id));
        }
      } else {
        ret = OB_ENTRY_EXIST;
        STORAGE_LOG(WARN, "tablet id already exist", K(ret), K(tablet_id));
      }
      break;
    }
    case share::ObBackupDataType::BACKUP_MAJOR: {
      if (OB_FAIL(backup_set_attr->major_tablet_map_.get_refactored(tablet_id, tablet_attr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // not exist, it's good
          if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupTabletValidationAttr)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
          } else if (FALSE_IT(tablet_attr = new (alc_ptr) ObAdminBackupTabletValidationAttr())) {
          } else if (FALSE_IT(tablet_attr->ls_id_ = ls_id)) {
          } else if (FALSE_IT(tablet_attr->data_type_ = data_type)) {
          } else if (OB_FAIL(backup_set_attr->major_tablet_map_.set_refactored(tablet_id,
                                                                               tablet_attr))) {
            STORAGE_LOG(WARN, "fail to set tablet id", K(ret), K(tablet_id));
          } else if (OB_FAIL(global_stat_.add_scheduled_tablet_count_(1))) {
            STORAGE_LOG(WARN, "fail to add scheduled tablet count", K(ret));
          } else if (OB_FAIL(backup_set_attr->stat_.add_scheduled_tablet_count_(1))) {
            STORAGE_LOG(WARN, "fail to add scheduled tablet count", K(ret));
          } else {
            tablet_attr = nullptr;
            STORAGE_LOG(DEBUG, "succeed to add tablet id", K(ret), K(tablet_id));
          }
        } else {
          STORAGE_LOG(WARN, "unexpected fail to get tablet id", K(ret), K(tablet_id));
        }
      } else {
        ret = OB_ENTRY_EXIST;
        STORAGE_LOG(WARN, "tablet id already exist", K(ret), K(tablet_id));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected data type", K(ret), K(data_type));
      break;
    }
    }
  }

  return ret;
}
int ObAdminBackupValidationCtx::get_tablet_attr(int64_t backup_set_id, const share::ObLSID &ls_id,
                                                const share::ObBackupDataType &data_type,
                                                const common::ObTabletID &tablet_id,
                                                ObAdminBackupTabletValidationAttr *&tablet_attr)
{
  obsys::ObRLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  ObAdminBackupSetValidationAttr *backup_set_attr = nullptr;
  ObAdminBackupLSValidationAttr *ls_attr = nullptr;
  if (OB_FAIL(get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "unexpected fail to get backup set attr", K(ret), K(backup_set_id));
  } else if (OB_FAIL(get_ls_attr(backup_set_id, ls_id, ls_attr))) {
    STORAGE_LOG(WARN, "unexpected fail to get ls attr", K(ret), K(backup_set_id), K(ls_id));
  } else {
    switch (data_type.type_) {
    case share::ObBackupDataType::BACKUP_SYS: {
      if (OB_FAIL(ls_attr->sys_tablet_map_.get_refactored(tablet_id, tablet_attr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          STORAGE_LOG(WARN, "fail to get tablet attr", K(ret), K(tablet_id));
        } else {
          STORAGE_LOG(WARN, "unexpected fail to get tablet attr", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(tablet_attr)) {
        ret = OB_ERR_NULL_VALUE;
        STORAGE_LOG(WARN, "unexpected null tablet attr", K(ret), K(tablet_id));
      }
      break;
    }
    case share::ObBackupDataType::BACKUP_MINOR: {
      if (OB_FAIL(backup_set_attr->minor_tablet_map_.get_refactored(tablet_id, tablet_attr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          STORAGE_LOG(WARN, "fail to get tablet attr", K(ret), K(tablet_id));
        } else {
          STORAGE_LOG(WARN, "unexpected fail to get tablet attr", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(tablet_attr)) {
        ret = OB_ERR_NULL_VALUE;
        STORAGE_LOG(WARN, "unexpected null tablet attr", K(ret), K(tablet_id));
      } else if (tablet_attr->ls_id_ != ls_id) {
        // transfer detected
        STORAGE_LOG(WARN, "unmatched ls id maybe transfered", K(ret), K(ls_id));
      }
      break;
    }
    case share::ObBackupDataType::BACKUP_MAJOR: {
      if (OB_FAIL(backup_set_attr->major_tablet_map_.get_refactored(tablet_id, tablet_attr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          STORAGE_LOG(WARN, "fail to get tablet attr", K(ret), K(tablet_id));
        } else {
          STORAGE_LOG(WARN, "unexpected fail to get tablet attr", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(tablet_attr)) {
        ret = OB_ERR_NULL_VALUE;
        STORAGE_LOG(WARN, "unexpected null tablet attr", K(ret), K(tablet_id));
      } else if (tablet_attr->ls_id_ != ls_id) {
        // transfer detected
        STORAGE_LOG(WARN, "unmatched ls id maybe transfered", K(ret), K(ls_id));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected data type", K(ret), K(data_type));
      break;
    }
    }
  }

  return ret;
}
int ObAdminBackupValidationCtx::add_backup_piece(const ObAdminPieceKey &backup_piece_key)
{
  obsys::ObWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupPieceValidationAttr *backup_piece_attr = nullptr;
  if (OB_FAIL(backup_piece_map_.get_refactored(backup_piece_key, backup_piece_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // not exist, it's good
      if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupPieceValidationAttr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(backup_piece_attr = new (alc_ptr) ObAdminBackupPieceValidationAttr())) {
      } else if (OB_FAIL(backup_piece_attr->init())) {
        STORAGE_LOG(WARN, "fail to init backup piece attr", K(ret));
      } else if (OB_FAIL(backup_piece_map_.set_refactored(backup_piece_key, backup_piece_attr))) {
        STORAGE_LOG(WARN, "fail to set backup piece key", K(ret), K(backup_piece_key));
      } else if (OB_FAIL(processing_backup_piece_key_array_.push_back(backup_piece_key))) {
        STORAGE_LOG(WARN, "fail to push back backup piece key", K(ret), K(backup_piece_key));
      } else if (OB_FAIL(global_stat_.add_scheduled_piece_count_(1))) {
        STORAGE_LOG(WARN, "fail to add scheduled piece count", K(ret));
      } else {
        backup_piece_attr = nullptr;
        STORAGE_LOG(INFO, "succeed to add backup piece key", K(ret), K(backup_piece_key));
      }
      if (OB_NOT_NULL(backup_piece_attr)) {
        backup_piece_attr->~ObAdminBackupPieceValidationAttr();
      }
    }
  } else {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "unexpected to get backup piece attr", K(ret), K(backup_piece_key));
  }
  return ret;
}
int ObAdminBackupValidationCtx::get_backup_piece_attr(
    const ObAdminPieceKey &backup_piece_key, ObAdminBackupPieceValidationAttr *&backup_piece_attr)
{
  obsys::ObRLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_piece_map_.get_refactored(backup_piece_key, backup_piece_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "fail to get backup piece attr", K(ret), K(backup_piece_key));
    } else {
      STORAGE_LOG(WARN, "unexpected fail to get backup piece attr", K(ret), K(backup_piece_key));
    }
  } else if (OB_ISNULL(backup_piece_attr)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "unexpected null backup piece attr", K(ret), K(backup_piece_key));
  }
  return ret;
}
int ObAdminBackupValidationCtx::add_ls(const ObAdminPieceKey &backup_piece_key,
                                       const share::ObLSID &ls_id)
{
  obsys::ObWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupPieceValidationAttr *backup_piece_attr = nullptr;
  ObAdminBackupLSValidationAttr *ls_attr = nullptr;
  if (OB_FAIL(get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
  } else if (OB_FAIL(backup_piece_attr->ls_map_.get_refactored(ls_id, ls_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // not exist, it's good
      if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupLSValidationAttr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(ls_attr = new (alc_ptr) ObAdminBackupLSValidationAttr())) {
      } else if (OB_FAIL(ls_attr->init())) {
        STORAGE_LOG(WARN, "fail to init ls attr", K(ret));
      } else if (OB_FAIL(backup_piece_attr->ls_map_.set_refactored(ls_id, ls_attr))) {
        STORAGE_LOG(WARN, "fail to set ls id", K(ret), K(ls_id));
      } else {
        ls_attr = nullptr;
        STORAGE_LOG(INFO, "succeed to add ls id", K(ret), K(ls_id));
      }
      if (OB_NOT_NULL(ls_attr)) {
        ls_attr->~ObAdminBackupLSValidationAttr();
      }
    }
  } else {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "unexpected to get ls attr", K(ret), K(ls_id));
  }
  return ret;
}
int ObAdminBackupValidationCtx::get_ls_attr(const ObAdminPieceKey &backup_piece_key,
                                            const share::ObLSID &ls_id,
                                            ObAdminBackupLSValidationAttr *&ls_attr)
{
  obsys::ObRLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  ObAdminBackupPieceValidationAttr *backup_piece_attr = nullptr;
  if (OB_FAIL(get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
  } else if (OB_FAIL(backup_piece_attr->ls_map_.get_refactored(ls_id, ls_attr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "fail to get ls attr", K(ret), K(backup_piece_key), K(ls_id));
    } else {
      STORAGE_LOG(WARN, "unexpected fail to get ls attr", K(ret), K(ls_id));
    }
  } else if (nullptr == ls_attr) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "unexpected null ls attr", K(ret), K(backup_piece_key), K(ls_id));
  }
  return ret;
}
}; // namespace tools
}; // namespace oceanbase