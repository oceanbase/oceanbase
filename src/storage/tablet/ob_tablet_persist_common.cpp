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
#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_persist_common.h"

#include "storage/tablet/ob_tablet.h"
#include "share/ob_upgrade_utils.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"

namespace oceanbase
{
namespace storage
{
// ================================================
//        ObSSTransferSrcTabletBlockInfo
// ================================================
ObSSTransferSrcTabletBlockInfo::~ObSSTransferSrcTabletBlockInfo()
{
  src_tablet_meta_block_set_.reuse();
}

int ObSSTransferSrcTabletBlockInfo::init(const ObTablet &src_tablet)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SrcTabletBlocks"));
  const int64_t shared_block_bucket_num = ObBlockInfoSet::SHARED_BLOCK_BUCKET_NUM;
  ObTabletMacroInfo *macro_info = nullptr;
  ObMacroInfoIterator macro_iter;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!src_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src tablet", K(ret), K(src_tablet));
  } else if (OB_FAIL(src_tablet.load_macro_info(/* ls_epoch */0,
                                                allocator,
                                                macro_info))) {
    LOG_WARN("failed to load macro info", K(ret), K(src_tablet));
  } else if (OB_ISNULL(macro_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null macro info", K(ret), KP(macro_info));
  } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
    LOG_WARN("failed to init macro iterator", K(ret),
      KPC(macro_info));
  } else if (OB_FAIL(src_tablet_meta_block_set_.create(shared_block_bucket_num,
                                                       "SrcTabletBlocks",
                                                       "ObBlockSetNode",
                                                       MTL_ID()))) {
    LOG_WARN("failed to create src_tablet_meta_block_set", K(ret), K(shared_block_bucket_num));
  } else {
    ObTabletBlockInfo block_info;

    while (OB_SUCC(ret)) {
      block_info.reset();

      if (OB_FAIL(macro_iter.get_next(block_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next block info", K(ret), K(block_info));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(add_block_info_if_need_(block_info))) {
        LOG_WARN("failed to add block info if need", K(ret), K(block_info));
      }
    }
  }

  if (OB_NOT_NULL(macro_info)) {
    macro_info->reset();
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTransferSrcTabletBlockInfo::merge_to(/* out */ ObBlockInfoSet &out_block_info_set) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObBlockInfoSet::TabletMacroSet &shared_meta_block_info_set = out_block_info_set.shared_meta_block_info_set_;

    for (ObBlockInfoSet::SetIterator iter = src_tablet_meta_block_set_.begin();
        OB_SUCC(ret) && iter != src_tablet_meta_block_set_.end();
        ++iter) {
      const blocksstable::MacroBlockId &block_id = iter->first;

      if (OB_FAIL(shared_meta_block_info_set.set_refactored(block_id, /* overwrite */ 0))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("failed to insert block_id", K(ret), K(block_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSSTransferSrcTabletBlockInfo::add_block_info_if_need_(const ObTabletBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  const blocksstable::MacroBlockId &block_id = block_info.macro_id();

  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid block_id", K(ret), K(block_id));
  } else if (!block_id.is_shared_sub_meta()) {
    // do nothing
  } else if (OB_FAIL(src_tablet_meta_block_set_.set_refactored(block_id, /* overwrite */ 0))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("failed to insert block_id", K(ret), K(block_info));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// ================================================
//             ObTabletPersisterParam
// ================================================
ObTabletPersisterParam::ObTabletPersisterParam(
  const int64_t data_version,
  const share::ObLSID ls_id,
  const int64_t ls_epoch,
  const ObTabletID tablet_id,
  const int32_t private_transfer_epoch,
  const int64_t meta_version)
  : data_version_(data_version),
    ls_id_(ls_id),
    ls_epoch_(ls_epoch),
    tablet_id_(tablet_id),
    private_transfer_epoch_(private_transfer_epoch),
    snapshot_version_(0),
    start_macro_seq_(0),
    ddl_redo_callback_(nullptr),
    ddl_finish_callback_(nullptr)
#ifdef OB_BUILD_SHARED_STORAGE
    , op_handle_(nullptr),
    file_(nullptr),
    update_reason_(ObMetaUpdateReason::INVALID_META_UPDATE_REASON),
    sstable_op_id_(0),
    reorganization_scn_(0),
    meta_version_(meta_version),
    src_tablet_block_info_(nullptr)
#endif
{
  if (!share::ObUpgradeChecker::check_data_version_exist(data_version)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid compat data version, please register it to ob_upgrade_utils.cpp if need support.", K(ret), KDV(data_version));
    ob_abort();
  }
}

// shared_major tablet meta persistence
ObTabletPersisterParam::ObTabletPersisterParam(
  const uint64_t data_version,
  const ObTabletID tablet_id,
  const int32_t private_transfer_epoch,
  const int64_t snapshot_version,
  const int64_t start_macro_seq,
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback,
  blocksstable::ObIMacroBlockFlushCallback *ddl_finish_callback)
  : data_version_(data_version),
    ls_id_(),
    ls_epoch_(0),
    tablet_id_(tablet_id),
    private_transfer_epoch_(private_transfer_epoch),
    snapshot_version_(snapshot_version),
    start_macro_seq_(start_macro_seq),
    ddl_redo_callback_(ddl_redo_callback),
    ddl_finish_callback_(ddl_finish_callback)
#ifdef OB_BUILD_SHARED_STORAGE
    , op_handle_(nullptr),
    file_(nullptr),
    update_reason_(ObMetaUpdateReason::INVALID_META_UPDATE_REASON),
    sstable_op_id_(0),
    reorganization_scn_(0),
    meta_version_(0),
    src_tablet_block_info_(nullptr)
#endif
{
  if (!share::ObUpgradeChecker::check_data_version_exist(data_version)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid compat data version, please register it to ob_upgrade_utils.cpp if need support.", K(ret), KDV(data_version));
    ob_abort();
  }
}

#ifdef OB_BUILD_SHARED_STORAGE
ObTabletPersisterParam::ObTabletPersisterParam(
  const uint64_t data_version,
  const share::ObLSID ls_id,
  const ObTabletID tablet_id,
  const ObMetaUpdateReason update_reason,
  const int64_t sstable_op_id,
  const int64_t start_macro_seq,
  ObAtomicOpHandle<ObAtomicTabletMetaOp> *handle,
  ObAtomicTabletMetaFile *file,
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback,
  blocksstable::ObIMacroBlockFlushCallback *ddl_finish_callback,
  const int64_t reorganization_scn,
  const ObSSTransferSrcTabletBlockInfo *src_tablet_block_info)
  : data_version_(data_version),
    ls_id_(ls_id),
    ls_epoch_(0),
    tablet_id_(tablet_id),
    private_transfer_epoch_(OB_INVALID_TRANSFER_SEQ),
    snapshot_version_(0),
    start_macro_seq_(start_macro_seq),
    ddl_redo_callback_(ddl_redo_callback),
    ddl_finish_callback_(ddl_finish_callback),
    op_handle_(handle),
    file_(file),
    update_reason_(update_reason),
    sstable_op_id_(sstable_op_id),
    reorganization_scn_(reorganization_scn),
    meta_version_(0),
    src_tablet_block_info_(src_tablet_block_info)
{
  if (!share::ObUpgradeChecker::check_data_version_exist(data_version)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid compat data version, please register it to ob_upgrade_utils.cpp if need support.", K(ret), KDV(data_version));
    ob_abort();
  }
}
#endif

bool ObTabletPersisterParam::is_valid() const
{
  bool valid = tablet_id_.is_valid();
  if (!valid) {
  } else if (is_major_shared_object()) { // shared_major
    valid = 0 == ls_epoch_ && start_macro_seq_ >= 0;
  #ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_inc_shared_object()) { // inc_shared
    valid = OB_NOT_NULL(op_handle_) && OB_NOT_NULL(file_) && is_valid_update_reason(update_reason_);
  #endif
  } else {// private
    valid = ls_id_.is_valid() && ls_epoch_ >= 0 && 0 == start_macro_seq_;
  #ifdef OB_BUILD_SHARED_STORAGE
    valid = valid && meta_version_ >= 0;
  #endif
  }
  return valid;
}

int ObTabletPersisterParam::get_op_id(int64_t &op_id) const
{
#ifndef OB_BUILD_SHARED_STORAGE
  return OB_NOT_SUPPORTED;
#else
  int ret = OB_SUCCESS;
  op_id = -1;
  ObAtomicTabletMetaOp *op = nullptr;
  if (!is_inc_shared_object()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for non inc shared object", K(ret), KPC(this));
  } else if (OB_ISNULL(op = op_handle_->get_atomic_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null atomic op", K(ret), KPC(op_handle_));
  } else {
    op_id = op->get_op_id();
  }
  return ret;
#endif
}

// ================================================
//               ObTabletTransformArg
// ================================================
ObTabletTransformArg::ObTabletTransformArg()
  : rowkey_read_info_ptr_(nullptr),
    tablet_macro_info_ptr_(nullptr),
    tablet_meta_(),
    table_store_addr_(),
    storage_schema_addr_(),
    tablet_macro_info_addr_(),
    ddl_kvs_(nullptr),
    ddl_kv_count_(0),
    memtable_count_(0),
    new_table_store_ptr_(nullptr),
    table_store_cache_()
{
  MEMSET(memtables_, 0x0, sizeof(memtables_));
}

ObTabletTransformArg::~ObTabletTransformArg()
{
  reset();
}

void ObTabletTransformArg::reset()
{
  rowkey_read_info_ptr_ = nullptr;
  tablet_macro_info_ptr_ = nullptr;
  tablet_meta_.reset();
  table_store_addr_.reset();
  storage_schema_addr_.reset();
  tablet_macro_info_addr_.reset();
  ddl_kvs_ = nullptr;
  ddl_kv_count_ = 0;
  for (int64_t i = 0; i < MAX_MEMSTORE_CNT; ++i) {
    memtables_[i] = nullptr;
  }
  memtable_count_ = 0;
  new_table_store_ptr_ = nullptr;
  table_store_cache_.reset();
}

bool ObTabletTransformArg::is_valid() const
{
  return table_store_addr_.is_none() ^ (nullptr != rowkey_read_info_ptr_)
      && tablet_meta_.is_valid()
      && table_store_addr_.is_valid()
      && storage_schema_addr_.is_valid()
      && tablet_macro_info_addr_.is_valid();
}

// ================================================
//             ObSSTablePersistWrapper
// ================================================

bool ObSSTablePersistWrapper::is_valid() const
{
  return nullptr != sstable_
      && sstable_->is_sstable()
      && sstable_->is_valid();
}

int ObSSTablePersistWrapper::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("wrapper is unexpected not valid", K(ret));
  } else if (OB_FAIL(sstable_->serialize_full_table(data_version_, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize full sstable", K(ret), KPC(sstable_));
  }
  return ret;
}

int64_t ObSSTablePersistWrapper::get_serialize_size() const
{
  int64_t len = 0;
  if (OB_UNLIKELY(!is_valid())) {
    // do nothing
  } else {
    len = sstable_->get_full_serialize_size(data_version_);
  }
  return len;
}

// ================================================
//                ObSharedBlockIndex
// ================================================
int ObSharedBlockIndex::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(shared_macro_id_.hash(hash_val))) {
    STORAGE_LOG(WARN, "fail to calculate macro id's hash value", K(ret), K(shared_macro_id_));
  } else {
    hash_val *= nested_offset_;
  }
  return ret;
}

OB_INLINE bool ObSharedBlockIndex::operator ==(const ObSharedBlockIndex &other) const
{
  return other.shared_macro_id_ == shared_macro_id_ && other.nested_offset_ == nested_offset_;
}


// ================================================
//            ObTabletBlockInfoSetBuilder
// ================================================
int ObTabletBlockInfoSetBuilder::convert_macro_info_map(
    const SharedMacroMap &shared_macro_map,
    ObBlockInfoSet::TabletMacroMap &aggregated_info_map)
{
  int ret = OB_SUCCESS;
  ObSharedBlockIndex shared_blk_index;
  int64_t occupy_size = 0;
  int64_t accumulated_size = 0;
  for (ConstSharedMacroIter iter = shared_macro_map.begin(); OB_SUCC(ret) && iter != shared_macro_map.end(); ++iter) {
    shared_blk_index = iter->first;
    occupy_size = iter->second;
    accumulated_size = 0;
    if (OB_FAIL(aggregated_info_map.get_refactored(shared_blk_index.shared_macro_id_, accumulated_size))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get accumulated size", K(ret), K(shared_blk_index));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(aggregated_info_map.set_refactored(
        shared_blk_index.shared_macro_id_,
        accumulated_size + occupy_size,
        1/*whether to overwrite*/))) {
      STORAGE_LOG(WARN, "fail to update aggregated info map", K(ret), K(shared_blk_index), K(accumulated_size), K(occupy_size));
    }
  }
  return ret;
}

int ObTabletBlockInfoSetBuilder::copy_sstable_macro_info(
    const blocksstable::ObSSTable &sstable,
    SharedMacroMap &shared_macro_map,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    STORAGE_LOG(WARN, "fail to get sstable meta handle", K(ret), K(sstable));
  } else if (sstable.is_small_sstable() && OB_FAIL(copy_shared_macro_info(
      meta_handle.get_sstable_meta().get_macro_info(),
      shared_macro_map,
      block_info_set.meta_block_info_set_,
      block_info_set.backup_block_info_set_))) {
    STORAGE_LOG(WARN, "fail to copy shared macro's info", K(ret), K(meta_handle.get_sstable_meta().get_macro_info()));
  } else if (!sstable.is_small_sstable()
      && OB_FAIL(copy_data_macro_ids(meta_handle.get_sstable_meta().get_macro_info(),
                                      block_info_set))) {
    STORAGE_LOG(WARN, "fail to copy tablet's data macro ids", K(ret), K(meta_handle.get_sstable_meta().get_macro_info()));
  }
  return ret;
}

int ObTabletBlockInfoSetBuilder::copy_shared_macro_info(
    const blocksstable::ObSSTableMacroInfo &macro_info,
    SharedMacroMap &shared_macro_map,
    ObBlockInfoSet::TabletMacroSet &meta_id_set,
    ObBlockInfoSet::TabletMacroSet &backup_id_set)
{
  int ret = OB_SUCCESS;
  ObMacroIdIterator iter;
  MacroBlockId macro_id;
  if (OB_FAIL(macro_info.get_data_block_iter(iter))) {
    STORAGE_LOG(WARN, "fail to get data block iterator", K(ret));
  } else if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
    STORAGE_LOG(WARN, "fail to get shared macro id", K(ret), K(iter));
  } else {
    ObSharedBlockIndex block_idx(macro_id, macro_info.get_nested_offset());
    if (OB_FAIL(shared_macro_map.set_refactored(block_idx, macro_info.get_nested_size(), 0/*whether to overwrite*/))) {
      if (OB_HASH_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to push shared macro info into map", K(ret), K(macro_id), K(macro_info));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  iter.reset();
  ObBlockInfoSet::TabletMacroSet dummy_data_id_set;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(macro_info.get_other_block_iter(iter))) {
    STORAGE_LOG(WARN, "fail to get other block iterator", K(ret));
  } else if (OB_FAIL(do_copy_ids_(iter,
                                 dummy_data_id_set,
                                 meta_id_set,
                                 backup_id_set))) {
    LOG_WARN("fail to copy other block ids", K(ret));
  }
  return ret;
}

int ObTabletBlockInfoSetBuilder::copy_data_macro_ids(
    const blocksstable::ObSSTableMacroInfo &macro_info,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObMacroIdIterator iter;
  MacroBlockId macro_id;

  if (OB_FAIL(macro_info.get_all_block_iter(iter))) {
    LOG_WARN("fail to get data block iterator", K(ret));
  } else if (OB_FAIL(do_copy_ids_(iter,
                                 block_info_set.data_block_info_set_,
                                 block_info_set.meta_block_info_set_,
                                 block_info_set.backup_block_info_set_))) {
    LOG_WARN("fail to copy data block ids", K(ret), K(iter));
  }
  return ret;
}

int ObTabletBlockInfoSetBuilder::do_copy_ids_(
    blocksstable::ObMacroIdIterator &iter,
    ObBlockInfoSet::TabletMacroSet &data_id_set,
    ObBlockInfoSet::TabletMacroSet &meta_id_set,
    ObBlockInfoSet::TabletMacroSet &backup_id_set)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObMacroIdIterator::Type block_type = ObMacroIdIterator::Type::MAX;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next_macro_id(macro_id, block_type))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next macro id", K(ret), K(macro_id));
      }
    } else if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block_id", K(ret), K(macro_id));
    } else if (OB_UNLIKELY(!ObMacroIdIterator::is_block_type_valid(block_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block_type", K(ret), K(macro_id), K(block_type));
    } else if (macro_id.is_backup_id()) {
      if (OB_FAIL(backup_id_set.set_refactored(macro_id, 0 /*whether to overwrite*/))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to push macro id into set", K(ret), K(macro_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    } else {
      ObBlockInfoSet::TabletMacroSet &id_set = ObMacroIdIterator::is_meta_block_type(block_type) ? meta_id_set : data_id_set;
      if (OB_FAIL(id_set.set_refactored(macro_id, 0 /*whether to overwrite*/))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to push macro id into set", K(ret), K(macro_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return OB_ITER_END == ret ? OB_SUCCESS : ret;
}

// ================================================
//            ObSSTableMetaPersistCtx
// ================================================
int ObSSTableMetaPersistCtx::init(
    const int64_t ctx_id,
    const int64_t map_bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", KR(ret));
  } else if (OB_FAIL(shared_macro_map_.create(map_bucket_cnt, "ObBlockInfoMap", "SharedBlkNode", MTL_ID()))) {
    STORAGE_LOG(WARN, "fail to create shared macro map", K(ret));
  } else {
    tables_.set_attr(lib::ObMemAttr(MTL_ID(), "PerstTables", ctx_id));
    write_infos_.set_attr(lib::ObMemAttr(MTL_ID(), "SSTWriteReqs", ctx_id));
    is_inited_ = true;
  }
  return ret;
}

// ================================================
//                ObMultiTimeStats
// ================================================

ObMultiTimeStats::TimeStats::TimeStats(const char *owner)
   : owner_(owner),
     start_ts_(ObTimeUtility::current_time()),
     last_ts_(start_ts_),
     click_count_(0),
     has_extra_info_(false)
{
  memset(click_, 0, sizeof(click_));
  memset(click_str_, 0, sizeof(click_str_));
}

void ObMultiTimeStats::TimeStats::click(const char *step_name)
{
  const int64_t cur_ts = ObTimeUtility::current_time();
  if (OB_LIKELY(click_count_ < MAX_CLICK_COUNT)) {
    click_str_[click_count_] = step_name;
    click_[click_count_++] = (int32_t)(cur_ts - last_ts_);
    last_ts_ = cur_ts;
  }
}

int64_t ObMultiTimeStats::TimeStats::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t i = 0;
  ret = databuff_printf(buf, buf_len, pos, "owner:'%s' total=%ldus%s",
      owner_, last_ts_ - start_ts_, click_count_ > 0 ? ", time_dist: " : "");

  if (OB_SUCC(ret) && click_count_ > 0) {
    ret = databuff_printf(buf, buf_len, pos, "%s=%dus", click_str_[0], click_[0]);
  }
  for (int i = 1; OB_SUCC(ret) && i < click_count_; i++) {
    ret = databuff_printf(buf, buf_len, pos, ", %s=%dus", click_str_[i], click_[i]);
  }
  if (OB_SUCC(ret)) {
    if (has_extra_info_) {
      ret = databuff_printf(buf, buf_len, pos, " %s:%s", "extra_info", extra_info_);
    }
  }
  if (OB_FAIL(ret)) {
    pos = 0;
  }
  return pos;
}

int ObMultiTimeStats::TimeStats::set_extra_info(const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  va_list args;
  va_start(args, fmt);

  if (OB_FAIL(databuff_vprintf(extra_info_, MAX_EXTRA_INFO_LENGTH, pos, fmt, args))) {
    LOG_WARN("fail to databuff_vprintf", K(ret));
  } else {
    has_extra_info_ = true;
  }
  va_end(args);

  return ret;
}

ObMultiTimeStats::ObMultiTimeStats(
  ObArenaAllocator *allocator,
  const int64_t auto_print_threshold)
  : allocator_(allocator),
    stats_(nullptr),
    stats_count_(0),
    auto_print_threshold_(auto_print_threshold)
{
}

ObMultiTimeStats::~ObMultiTimeStats()
{
  if (auto_print_threshold_ >= 0) {
    int64_t total_cost_time = 0;
    for (int64_t i = 0; i < stats_count_; ++i) {
      total_cost_time += stats_[i].get_total_time();
    }
    if (total_cost_time > auto_print_threshold_) {
      FLOG_INFO("[TABLET PERSIST TIME STATS] cost too much time", KPC(this));
    }
  }
  for (int64_t i = 0; i < stats_count_; i++) {
    stats_[i].~TimeStats();
  }
  stats_count_ = 0;
  stats_ = nullptr;
}

int ObMultiTimeStats::acquire_stats(const char *owner, ObMultiTimeStats::TimeStats *&stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stats_count_ > MAX_STATS_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many time stats", K(ret), K(stats_count_));
  } else if (OB_ISNULL(stats_) &&
      OB_ISNULL(stats_ = reinterpret_cast<TimeStats*>(allocator_->alloc(sizeof(TimeStats) *  MAX_STATS_CNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    new (&stats_[stats_count_]) TimeStats(owner);
    stats = &stats_[stats_count_];
    stats_count_++;
  }
  return ret;
}

int64_t ObMultiTimeStats::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < stats_count_; i++) {
    databuff_printf(buf, buf_len, pos, "stats[%ld]: ", i);
    BUF_PRINTO(stats_[i]);
    if (i != stats_count_-1) {
      J_NEWLINE();
    }
  }
  J_OBJ_END();
  return pos;
}

// ================================================
//            ObSSTableMetaPersistHelper
// ================================================
int ObSSTableMetaPersistHelper::IWriteOperator::do_write(const ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!written_addrs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the result from the previous write op are still remaining(forget to reset before write?)",
      K(ret), K(written_addrs_));
  }
  return ret;
}

int ObSSTableMetaPersistHelper::IWriteOperator::fill_write_info(
    const ObTabletPersisterParam &param,
    const ObSSTable &sstable,
    common::ObIAllocator &allocator,
    /* out */ ObSharedObjectWriteInfo &write_info) const
{
  int ret = OB_SUCCESS;
  write_info.reset();

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid persister param", K(ret), K(param));
  } else {
    ObTabletPersistCommon::SSTablePersistWrapper wrapper(param.data_version_, &sstable);

    if (OB_FAIL(ObTabletPersistCommon::fill_write_info(param,
                                                       &wrapper,
                                                       allocator,
                                                       write_info))) {
      STORAGE_LOG(WARN, "failed to fill sstable write info", K(ret), K(param), K(sstable),
        K(write_info));
    }
  }
  return ret;
}

int ObSSTableMetaPersistHelper::register_write_op(ObSSTableMetaPersistHelper::IWriteOperator *sst_write_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == sst_write_op)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null sst_write_op", K(ret), KP(sst_write_op));
  } else if (OB_UNLIKELY(!sst_write_op->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid sst_write_op", K(ret), KPC(sst_write_op));
  } else {
    sst_write_op_ = sst_write_op;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableMetaPersistHelper::do_persist_all_sstables(
    const ObTabletTableStore &old_table_store,
    common::ObArenaAllocator &new_table_store_allocator,
    /*out*/ ObMultiTimeStats &multi_stats,
    /*out*/ ObTabletTableStore &new_table_store,
    /*out*/ int64_t &sst_meta_size_aligned)
{
  int ret = OB_SUCCESS;
  sst_meta_size_aligned = 0;

  ObTableStoreIterator sstable_iter;
  const blocksstable::ObMajorChecksumInfo &major_ckm_info = old_table_store.get_major_ckm_info();
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
#ifdef ERRSIM
  const int64_t large_co_sstable_threshold_config = GCONF.errsim_large_co_sstable_threshold;
  const int64_t large_co_sstable_threshold = 0 == large_co_sstable_threshold_config ? ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE : large_co_sstable_threshold_config;
#else
  const int64_t large_co_sstable_threshold = ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE;
#endif
  new_table_store.reset();
  ObITable *itable = nullptr;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  ObBlockInfoSet &block_info_set = ctx_.block_info_set_;

  if (OB_UNLIKELY(!is_ready_for_persist_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "helper is not ready for persist", K(ret), KPC(this));
  } else if (FALSE_IT(sst_write_op_->reset())) {
  } else if (OB_FAIL(multi_stats.acquire_stats("fetch_and_persist_sstable", time_stats))) {
    STORAGE_LOG(WARN, "failed to acquire stats", K(ret));
  } else if (OB_FAIL(old_table_store.get_all_sstable(sstable_iter))) {
    STORAGE_LOG(WARN, "failed to get all sstable iterator", K(ret), K(old_table_store));
  } else if (FALSE_IT(time_stats->click("get_all_sst_iter"))) {
  }

  if (OB_SUCC(ret)) {
    // iterate all sstable and do persist
    while (OB_SUCC(ret) && OB_SUCC(sstable_iter.get_next(itable))) {
      ObSSTable *sstable = nullptr;
      ObCOSSTableV2 *out_co_sstable = nullptr;
      UNUSED(out_co_sstable);
      // clean result of last write.
      if (OB_ISNULL(itable) || OB_UNLIKELY(!itable->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null itable", K(ret), KP(itable));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(itable))) {
      } else if (OB_FAIL(inner_do_persist_single_sst_(*sstable,
                                                      /*skip_normal_sst*/ false,
                                                      out_co_sstable))) {
        STORAGE_LOG(WARN, "failed to persist sstable", K(ret), KPC(this));
      }
    }
    // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (FAILEDx(time_stats->set_extra_info("%s:%ld,%s:%ld,%s:%ld,%s:%ld",
      "large_co_sst_cnt", ctx_.large_co_sstable_cnt_,
      "small_co_sst_cnt", ctx_.small_co_sstable_cnt_,
      "cg_sst_cnt", ctx_.cg_sstable_cnt_,
      "normal_sst_cnt", ctx_.normal_sstable_cnt_))) {
    LOG_WARN("fail to set time stats extra info", K(ret));
  } else if (FALSE_IT(time_stats->click("fill_all_sstable_write_info"))) {
  }
  // update block info set
  else if (OB_FAIL(ObTabletBlockInfoSetBuilder::convert_macro_info_map(ctx_.shared_macro_map_, block_info_set.clustered_data_block_info_map_))) {
    STORAGE_LOG(WARN, "failed to convert shared data block info map", K(ret));
  }
  // handle write reqs(if need)
  else if (ctx_.write_infos_.count() > 0 && OB_FAIL(sst_write_op_->do_write(ctx_.write_infos_))) {
    STORAGE_LOG(WARN, "failed to batch write sstable", K(ret), KPC_(sst_write_op), K_(ctx_.write_infos));
  } else if (OB_UNLIKELY(ctx_.write_infos_.count() != sst_write_op_->get_written_addrs().count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "write result mismatch with write requests", K(ret), K(ctx_.write_infos_.count()),
      K(sst_write_op_->get_written_addrs().count()));
  } else if (FALSE_IT(time_stats->click("batch_write_sst_write_infos"))) {
  }
  // init new table store
  else if (OB_FAIL(new_table_store.init(new_table_store_allocator,
                                          ctx_.tables_,
                                          sst_write_op_->get_written_addrs(),
                                          major_ckm_info))) {
    STORAGE_LOG(WARN, "failed to init new table store", K(ret), K(ctx_.tables_), K(sst_write_op_->get_written_addrs()));
  } else {
    // calculate written bytes(aligned by 4K)
    time_stats->click("init_new_table_store");
    const common::ObIArray<ObMetaDiskAddr> &addrs = sst_write_op_->get_written_addrs();
    for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); i++) {
      int64_t size = 0;
      if (OB_FAIL(addrs.at(i).get_size_for_tablet_space_usage(size))) {
        STORAGE_LOG(WARN, "failed to get size for tablet space usage", K(ret), K(addrs.at(i)));
      } else {
        sst_meta_size_aligned += size;
      }
    }
    // aligned by 4K
    sst_meta_size_aligned = upper_align(sst_meta_size_aligned, DIO_READ_ALIGN_SIZE);
  }
  if (OB_SUCC(ret)) {
    sst_write_op_->reset();
  }
  return ret;
}

int ObSSTableMetaPersistHelper::do_persist_for_full_direct_load(
    ObSSTable *sstable,
    ObCOSSTableV2 *&out_co_sstable)
{
  int ret = OB_SUCCESS;
  out_co_sstable = nullptr;
  if (OB_UNLIKELY(!is_ready_for_persist_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "helper is not ready for persist", K(ret), KPC(this));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null sstable", K(ret), KP(sstable));
  } else if (OB_FAIL(inner_do_persist_single_sst_(*sstable,
                                                  /*skip_normal_sst*/true,
                                                  out_co_sstable))) {
    STORAGE_LOG(WARN, "failed to persist sstable", K(ret), K(sstable), KPC(this));
  }
  return ret;
}

bool ObSSTableMetaPersistHelper::is_ready_for_persist_() const
{
  return is_inited_
         && ctx_.is_inited()
         && persister_param_.is_valid()
         && nullptr != sst_write_op_
         && sst_write_op_->is_valid();
}

int ObSSTableMetaPersistHelper::inner_do_persist_single_sst_(
    ObSSTable &sstable,
    const bool skip_normal_sst,
    ObCOSSTableV2 *&out_co_sstable)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  out_co_sstable = nullptr;
  if (OB_FAIL(persist_sstable_linked_block_if_need_(sstable))) {
    STORAGE_LOG(WARN, "failed to persist sstable linked block if need", K(ret), K(sstable));
  } else if (sstable.is_co_sstable() && sstable.get_serialize_size(data_version_) > large_co_sstable_threshold_) {
    // persist large co sstable
    ObCOSSTableV2 &co_sstable = static_cast<ObCOSSTableV2 &>(sstable);
    if (OB_FAIL(persist_large_co_sstable_(co_sstable, out_co_sstable))) {
      STORAGE_LOG(WARN, "failed to persist large co_sstable", K(ret), K(co_sstable));
    }
  } else if (skip_normal_sst) {
    // just do nothing
    STORAGE_LOG(INFO, "not large co sstable, skip it", K(ret), K(sstable));
  } else if (OB_FAIL(persist_normal_sstable_(sstable))) {
    STORAGE_LOG(WARN, "failed to persist normal sstable", K(ret), K(sstable));
  }
  return ret;
}

int ObSSTableMetaPersistHelper::persist_sstable_linked_block_if_need_(ObSSTable &sstable)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  ObLinkedMacroInfoWriteParam macro_info_param;
  ObSharedObjectsWriteCtx sstable_linked_write_ctx;
  if (OB_FAIL(macro_info_param.build_linked_marco_info_param(persister_param_, start_macro_seq_))) {
    STORAGE_LOG(WARN, "failed to build linked macro info param", K(ret), K(persister_param_), K(start_macro_seq_));
  } else if (OB_FAIL(sstable.persist_linked_block_if_need(ctx_.allocator_,
                                                           macro_info_param,
                                                           start_macro_seq_,
                                                           sstable_linked_write_ctx))) {
    STORAGE_LOG(WARN, "failed t o try persist sstable linked block", K(ret), K(sstable), K(macro_info_param),
      K(start_macro_seq_), K(sstable_linked_write_ctx));
  } else if (sstable_linked_write_ctx.block_ids_.count() > 0
             && OB_FAIL(ctx_.sstable_meta_write_ctxs_.push_back(sstable_linked_write_ctx))) {
    STORAGE_LOG(WARN, "failed to push back write ctx", K(ret), K(sstable_linked_write_ctx),
      "sstable_meta_write_ctxs", ctx_.sstable_meta_write_ctxs_);
  }

  if (OB_FAIL(ret)) {
    // if failed, still push_back it to record block_id for active_delete in ss
    int tmp_ret = OB_SUCCESS;
    if (sstable_linked_write_ctx.block_ids_.count() > 0
        && OB_FAIL(ctx_.sstable_meta_write_ctxs_.push_back(sstable_linked_write_ctx))) {
      STORAGE_LOG(WARN, "failed to push back write ctx", K(ret), K(sstable_linked_write_ctx),
        "sstable_meta_write_ctxs", ctx_.sstable_meta_write_ctxs_);
    }
  }
  return ret;
}

int ObSSTableMetaPersistHelper::persist_large_co_sstable_(
    ObCOSSTableV2 &co_sstable,
    ObCOSSTableV2 *&out_co_sstable)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  out_co_sstable = nullptr;
  ObSSTableMetaHandle co_meta_handle;
  common::ObSEArray<ObSharedObjectWriteInfo, 16> write_infos;

  const int64_t ctx_id = share::is_reserve_mode()
                        ? ObCtxIds::MERGE_RESERVE_CTX_ID
                        : ObCtxIds::DEFAULT_CTX_ID;
  write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "SSTWriteInfos", ctx_id));

  if (co_sstable.get_serialize_size(data_version_) <= large_co_sstable_threshold_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported for normal co_sstable", K(ret), K(co_sstable),
      K(large_co_sstable_threshold_));
  } else {
    // serialize full co sstable and shell cg sstables when the serialize size of CO reached the limit.
    FLOG_INFO("A large_co_sstable, serialize_size > MAX_SIZE, should be serialized with Shell CG", K(ret), K(co_sstable));
    if (OB_FAIL(co_sstable.get_meta(co_meta_handle))) {
      STORAGE_LOG(WARN, "failed to get co meta handle", K(ret), K(co_sstable));
    } else {
      const ObSSTableArray &cg_sstables = co_meta_handle.get_sstable_meta().get_cg_sstables();
      for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_sstables.count(); ++idx) {
        if (OB_ISNULL(cg_sstables[idx])) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null cg sstable", K(ret), K(idx), KP(cg_sstables[idx]), K(co_sstable));
        } else if (cg_sstables[idx]->get_addr().is_disked()) {
          // do nothing
        } else if (OB_FAIL(persist_sstable_linked_block_if_need_(*cg_sstables[idx]))) {
          STORAGE_LOG(WARN, "fail to persist sstable linked_block if need", K(ret), K(idx), KPC(cg_sstables[idx]), K(start_macro_seq_));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fill_sstable_write_info_and_record_(*cg_sstables[idx],
                                                              false, /*check_has_padding_meta_cache*/
                                                              write_infos))) {
          STORAGE_LOG(WARN, "fail to fill sstable write_info", KR(ret), KPC(cg_sstables[idx]), K(idx), K(ctx_));
        } else {
          ctx_.cg_sstable_cnt_++;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(cg_sstables.count() != write_infos.count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "cg_write_infos' count mismatch", K(ret), K(cg_sstables.count()), K(write_infos.count()));
      } else if (write_infos.count() > 0
                 && OB_FAIL(sst_write_op_->do_write(write_infos))) {
        STORAGE_LOG(WARN, "failed to do write", K(ret), K(write_infos), KPC_(sst_write_op));
      } else {
        const common::ObIArray<ObMetaDiskAddr> &written_addrs = sst_write_op_->get_written_addrs();
        if (OB_FAIL(co_sstable.deep_copy(ctx_.allocator_,
                                         written_addrs,
                                         out_co_sstable))) {
          STORAGE_LOG(WARN, "failed to deep copy co_sstable", K(ret), K(co_sstable), K(written_addrs), KP(out_co_sstable));
        } else if (OB_ISNULL(out_co_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null out_co_sstable", K(ret), K(co_sstable), KP(out_co_sstable));
        } else {
          int64_t sstable_meta_size = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < written_addrs.count(); ++i) {
            int64_t size = 0;

            if (OB_FAIL(written_addrs.at(i).get_size_for_tablet_space_usage(size))) {
              STORAGE_LOG(WARN, "failed to get size for tablet space usage", K(ret), K(written_addrs.at(i)));
            } else {
              sstable_meta_size += size;
            }
          }
          // update total tablet meta size
          ctx_.total_tablet_meta_size_ += upper_align(sstable_meta_size, DIO_READ_ALIGN_SIZE);
          if (FAILEDx(fill_sstable_write_info_and_record_(*out_co_sstable,
                                                          /*check_has_padding_meta_cache*/false,
                                                          ctx_.write_infos_))) {
            STORAGE_LOG(WARN, "failed to fill out_co_sstable write info", K(ret), KPC(out_co_sstable), K(ctx_));
          } else if (FALSE_IT(ctx_.large_co_sstable_cnt_++)) {
          } else if (OB_FAIL(ctx_.tables_.push_back(out_co_sstable))) {
            STORAGE_LOG(WARN, "failed to add out_co_sstable into ctx", K(ret), K(ctx_));
          }
          STORAGE_LOG(INFO, "generate new co_sstable with addr succ", K(co_sstable), KPC(out_co_sstable), K(sstable_meta_size));
        }
      }
      if (OB_SUCC(ret)) {
        sst_write_op_->reset();
      }
    }
  }
  return ret;
}

int ObSSTableMetaPersistHelper::persist_normal_sstable_(ObSSTable &sstable)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  const bool is_co_sstable = sstable.is_co_sstable();
  if (is_co_sstable && sstable.get_serialize_size(data_version_) <= large_co_sstable_threshold_) {
    // normal co sstable
    const ObCOSSTableV2 &co_sstable = static_cast<const ObCOSSTableV2 &>(sstable);
    if (OB_FAIL(record_cg_sstables_macro_(co_sstable))) {
      STORAGE_LOG(WARN, "failed to record macro_info of small co sstable", K(ret), K(co_sstable));
    }
  } else if (!is_co_sstable) {
    // do nothing if is normal sstable
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not support process exclude small_co_sstable and normal_sstable", K(ret),
      K(sstable), "serialize size", sstable.get_serialize_size(data_version_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fill_sstable_write_info_and_record_(sstable,
                                                         /*check_has_padding_meta_cache*/true,
                                                         ctx_.write_infos_))) {
    STORAGE_LOG(WARN, "failed to fill sstable's write info", K(ret), K(sstable), K(ctx_));
  } else if (OB_FAIL(ctx_.tables_.push_back(&sstable))) {
    STORAGE_LOG(WARN, "failed to add table", K(ret), K(sstable), K(ctx_));
  } else if (is_co_sstable) {
    ctx_.small_co_sstable_cnt_++;
  } else {
    ctx_.normal_sstable_cnt_++;
  }

  return ret;
}

int ObSSTableMetaPersistHelper::record_cg_sstables_macro_(const ObCOSSTableV2 &co_sstable)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  // Statistics the number of macroblocks of cg sstables
  int64_t cg_sstable_meta_size = 0;
  ObSSTableMetaHandle co_meta_handle;

  if (OB_FAIL(co_sstable.get_meta(co_meta_handle))) {
    STORAGE_LOG(WARN, "failed to get co meta handle", K(ret), K(co_sstable));
  } else {
    const ObSSTableArray &cg_sstables = co_meta_handle.get_sstable_meta().get_cg_sstables();
    ctx_.cg_sstable_cnt_ += cg_sstables.count();

    ObSSTable *cg_sstable = nullptr;
    MacroBlockId block_id;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_sstables.count(); ++idx) {
      cg_sstable = cg_sstables[idx];
      block_id.reset();
      const ObMetaDiskAddr &sstable_addr = cg_sstable->get_addr();
      if (OB_FAIL(ObTabletBlockInfoSetBuilder::copy_sstable_macro_info(*cg_sstable,
                                                                       ctx_.shared_macro_map_,
                                                                       ctx_.block_info_set_))) {
        STORAGE_LOG(WARN, "fail to call sstable macro info", K(ret));
      } else if (sstable_addr.is_block()) {
        // this cg sstable has been persisted before
        int64_t size = 0;
        if (OB_FAIL(sstable_addr.get_macro_block_id(block_id))) {
          STORAGE_LOG(WARN, "fail to get block id from meta disk addr", K(ret), K(sstable_addr), K(block_id));
        } else if (OB_FAIL(sstable_addr.get_size_for_tablet_space_usage(size))) {
          STORAGE_LOG(WARN, "fail to get size for tablet space usage", K(ret), K(sstable_addr));
        } else if (OB_FAIL(ctx_.block_info_set_.shared_meta_block_info_set_.set_refactored(block_id, 0 /*whether to overwrite*/))) {
          if (OB_HASH_EXIST != ret) {
            STORAGE_LOG(WARN, "fail to push macro id into set", K(ret), K(sstable_addr));
          } else {
            ret = OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret)) {
          cg_sstable_meta_size += size;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ctx_.total_tablet_meta_size_ += upper_align(cg_sstable_meta_size, DIO_READ_ALIGN_SIZE);
    STORAGE_LOG(INFO, "record cg sstables macro succeed", K(cg_sstable_meta_size), K(ctx_));
  }
  return ret;
}

int ObSSTableMetaPersistHelper::fill_sstable_write_info_and_record_(
    const ObSSTable &sstable,
    const bool check_has_padding_meta_cache,
    /*out*/ ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  ObSharedObjectWriteInfo write_info;

  if (OB_FAIL(sst_write_op_->fill_write_info(persister_param_,
                                             sstable,
                                             ctx_.allocator_,
                                             /* out */ write_info))) {
    STORAGE_LOG(WARN, "failed to fill sstable write info", K(ret), K(persister_param_), K(sstable), K(write_info));
  } else if (OB_FAIL(write_infos.push_back(write_info))) {
    STORAGE_LOG(WARN, "failed to push back write info", K(ret), K(write_info));
  } else if (OB_FAIL(ObTabletBlockInfoSetBuilder::copy_sstable_macro_info(sstable,
                                                                          ctx_.shared_macro_map_,
                                                                          ctx_.block_info_set_))) {
    STORAGE_LOG(WARN, "failed to copy sstable macro info", K(ret), K(sstable));
  } else if (check_has_padding_meta_cache
             && OB_FAIL(sstable.has_padding_meta_cache())) {
    /*
      * The following defend log used ONLY in 4_2_x upgrade 4_3_x scenario !!!
      * We should fill invalid fields of SSTable's meta cache when deserialize tablet.
    */
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "meet unexpected padding meta cache", K(sstable));
  }
  return ret;
}

// ================================================
//            ObTabletPersistCommon
// ================================================

int ObTabletPersistCommon::build_async_write_start_opt(
    const ObTabletPersisterParam &param,
    const int64_t cur_macro_seq,
    blocksstable::ObStorageObjectOpt &start_opt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid()
                  || cur_macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(param), K(cur_macro_seq));
  }
  #ifdef OB_BUILD_SHARED_STORAGE
  else if (param.is_major_shared_object()) {
    start_opt.set_ss_share_meta_macro_object_opt(
      param.tablet_id_.id(), cur_macro_seq, 0/*cg_id*/, param.reorganization_scn_);
  } else if (param.is_inc_shared_object()) {
    start_opt.set_ss_tablet_sub_meta_opt(param.ls_id_.id(), param.tablet_id_.id(),
      param.op_handle_->get_atomic_op()->get_op_id(), cur_macro_seq,
      param.tablet_id_.is_ls_inner_tablet(), param.reorganization_scn_,
      /* is_object_storage */ true);
  }
  #endif
  else { // private
    start_opt.set_private_meta_macro_object_opt(param.tablet_id_.id(), param.private_transfer_epoch_);
  }
  return ret;
}

int ObTabletPersistCommon::sync_cur_macro_seq_from_opt(
    const ObTabletPersisterParam &param,
    const blocksstable::ObStorageObjectOpt &curr_opt,
    /*out*/ int64_t &cur_macro_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(param), K(curr_opt));
  } else if (param.is_major_shared_object()) {
    cur_macro_seq = curr_opt.ss_share_opt_.data_seq_;
  } else if (param.is_inc_shared_object()) {
    cur_macro_seq = curr_opt.ss_tablet_sub_meta_opt_.data_seq_;
  } else { // private
    // do nothing
  }
  return ret;
}

int ObTabletPersistCommon::wait_write_info_callback(const common::ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;
  if (write_infos.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "write info count should not be less than 1", K(ret), K(write_infos));
  } else {
    ObIMacroBlockFlushCallback *callback = write_infos.at(0).write_callback_;
    for (int64_t i = 0; OB_SUCC(ret) && i < write_infos.count(); i++) {
      if (write_infos.at(i).write_callback_ != callback) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unequaled callback", K(ret), KP(callback), KP(write_infos.at(i).write_callback_));
      }
    }
    if (OB_FAIL(ret) || OB_ISNULL(callback)) {
    } else if (OB_FAIL(callback->wait())) {
      STORAGE_LOG(WARN, "failed to wait callback", K(ret));
    }
  }
  return ret;
}

int ObTabletPersistCommon::sync_write_ctx_to_total_ctx_if_failed(
    common::ObIArray<ObSharedObjectsWriteCtx> &write_ctxs,
    common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < write_ctxs.count(); i++) {
    ObSharedObjectsWriteCtx &write_ctx = write_ctxs.at(i);
    if (OB_UNLIKELY(!write_ctx.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected invalid addr", K(ret), K(i), K(write_ctx));
    } else if(OB_FAIL(total_write_ctxs.push_back(write_ctxs.at(i)))) {
      STORAGE_LOG(WARN, "fail to push back write_ctx to total_write_ctx", K(ret), K(i), K(write_ctxs), K(total_write_ctxs));
    }
  }
  return ret;
}

/// NOTE: Don't summarize space usage in sslog
int ObTabletPersistCommon::calc_and_set_tablet_space_usage(
    const ObBlockInfoSet &block_info_set,
    /*out*/ common::ObIArray<MacroBlockId> &shared_meta_id_arr,
    /*out*/ ObTabletHandle &new_tablet_hdl,
    /*out*/ ObTabletSpaceUsage &space_usage)
{
  int ret = OB_SUCCESS;
  int64_t clustered_sstable_size = 0;
  int64_t backup_block_size = 0; // for sstable has backup_block and local_block;
  int64_t pure_backup_sstable_size = 0; // for sstable has no local_block

  if (OB_UNLIKELY(!new_tablet_hdl.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid new_tablet_hdl", K(ret), K(new_tablet_hdl));
  } else {
    backup::ObBackupDeviceMacroBlockId tmp_back_block_id;
    for (ObBlockInfoSet::SetIterator iter = block_info_set.backup_block_info_set_.begin();
        OB_SUCC(ret) && iter != block_info_set.backup_block_info_set_.end();
        ++iter) {
      if (OB_FAIL(tmp_back_block_id.set(iter->first))) {
        STORAGE_LOG(WARN, "failed to get backup block_id");
      } else {
        backup_block_size += tmp_back_block_id.get_length();
      }
    }
    for (ObBlockInfoSet::MapIterator iter = block_info_set.clustered_data_block_info_map_.begin();
        iter != block_info_set.clustered_data_block_info_map_.end();
        ++iter) {
      clustered_sstable_size += iter->second;
    }
    for (ObBlockInfoSet::SetIterator iter = block_info_set.shared_meta_block_info_set_.begin();
        OB_SUCC(ret) && iter != block_info_set.shared_meta_block_info_set_.end();
        ++iter) {
      if (OB_FAIL(shared_meta_id_arr.push_back(iter->first))) {
        STORAGE_LOG(WARN, "fail to push back macro id", K(ret), K(iter->first));
      }
    }
  }

  int64_t ss_public_sstable_occupy_size = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_tablet_hdl.get_obj()->calc_sstable_occupy_size(space_usage.all_sstable_data_occupy_size_,
                                                                        ss_public_sstable_occupy_size,
                                                                        pure_backup_sstable_size))) {
    STORAGE_LOG(WARN, "failed to calc tablet occupy_size", K(ret), KPC(new_tablet_hdl.get_obj()));
  } else {
    if (GCTX.is_shared_storage_mode()) {
      space_usage.all_sstable_data_required_size_ = space_usage.all_sstable_data_occupy_size_;
      // TODO @zs475329, all_sstable_meta_size_ should be the sum of all the sstable.meta_occupy_size_ (Both SS and SN)
      space_usage.all_sstable_meta_size_ = block_info_set.meta_block_info_set_.size() * 16 * 1024; // 16KB;
    } else {
      space_usage.all_sstable_data_required_size_ = block_info_set.data_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
      space_usage.all_sstable_meta_size_ = block_info_set.meta_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
    }
    space_usage.backup_bytes_ = backup_block_size + pure_backup_sstable_size;
    space_usage.tablet_clustered_sstable_data_size_ = clustered_sstable_size;
    // major_sstable_sizes are only used for shared_storage
    space_usage.ss_public_sstable_occupy_size_ = ss_public_sstable_occupy_size;
    new_tablet_hdl.get_obj()->set_space_usage_(space_usage);
  }
  return ret;
}

int ObTabletPersistCommon::calc_tablet_space_usage(
    const bool is_empty_shell,
    const ObBlockInfoSet &block_info_set,
    const ObTabletTableStore &table_store,
    /*out*/ ObTabletSpaceUsage &space_usage)
{
  int ret = OB_SUCCESS;
  int64_t clustered_sstable_size = 0;
  int64_t backup_block_size = 0; // for sstable has backup_block and local_block;
  int64_t pure_backup_sstable_size = 0; // for sstable has no local_block

  if (OB_UNLIKELY(!is_empty_shell && !table_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid table store", K(ret), K(table_store));
  } else {
    backup::ObBackupDeviceMacroBlockId tmp_back_block_id;
    for (ObBlockInfoSet::SetIterator iter = block_info_set.backup_block_info_set_.begin();
        OB_SUCC(ret) && iter != block_info_set.backup_block_info_set_.end();
        ++iter) {
      if (OB_FAIL(tmp_back_block_id.set(iter->first))) {
        STORAGE_LOG(WARN, "failed to get backup block_id");
      } else {
        backup_block_size += tmp_back_block_id.get_length();
      }
    }
    for (ObBlockInfoSet::MapIterator iter = block_info_set.clustered_data_block_info_map_.begin();
        iter != block_info_set.clustered_data_block_info_map_.end();
        ++iter) {
      clustered_sstable_size += iter->second;
    }
  }

  int64_t ss_public_sstable_occupy_size = 0;
  if (OB_FAIL(ret)) {
  }
  // calc sstable occupy size if not empty shell
  else if (!is_empty_shell
           && OB_FAIL(calc_sstable_occupy_size_by_table_store(table_store,
                                                               space_usage.all_sstable_data_occupy_size_,
                                                               ss_public_sstable_occupy_size,
                                                               pure_backup_sstable_size))) {
    STORAGE_LOG(WARN, "failed to calc tablet occupy_size", K(ret), K(table_store));
  } else {
    if (GCTX.is_shared_storage_mode()) {
      space_usage.all_sstable_data_required_size_ = space_usage.all_sstable_data_occupy_size_;
      // TODO @zs475329, all_sstable_meta_size_ should be the sum of all the sstable.meta_occupy_size_ (Both SS and SN)
      space_usage.all_sstable_meta_size_ = block_info_set.meta_block_info_set_.size() * 16 * 1024; // 16KB;
    } else {
      space_usage.all_sstable_data_required_size_ = block_info_set.data_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
      space_usage.all_sstable_meta_size_ = block_info_set.meta_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
    }
    space_usage.backup_bytes_ = backup_block_size + pure_backup_sstable_size;
    space_usage.tablet_clustered_sstable_data_size_ = clustered_sstable_size;
    // major_sstable_sizes are only used for shared_storage
    space_usage.ss_public_sstable_occupy_size_ = ss_public_sstable_occupy_size;
  }
  return ret;
}

int ObTabletPersistCommon::get_tablet_persist_size(
    const uint64_t data_version,
    const ObTabletMacroInfo *macro_info,
    const ObTablet *tablet,
    /*out*/ int64_t &size)
{
  int ret = OB_SUCCESS;
  size = -1;
  if (OB_ISNULL(tablet) || OB_ISNULL(macro_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(tablet));
  } else {
    ObInlineSecondaryMeta inline_meta(macro_info, ObSecondaryMetaType::TABLET_MACRO_INFO);
    ObSArray<ObInlineSecondaryMeta> meta_arr;
    if (OB_FAIL(meta_arr.push_back(inline_meta))) {
      LOG_WARN("fail to push back inline meta", K(ret), K(inline_meta));
    } else {
      size = tablet->get_serialize_size(data_version, meta_arr);
    }
  }
  return ret;
}

int ObTabletPersistCommon::fill_tablet_into_buf(
    const uint64_t data_version,
    const ObTabletMacroInfo *macro_info,
    const ObTablet *tablet,
    const int64_t size,
    /*out*/ char *buf,
    /*out*/ int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)
      || OB_ISNULL(buf)
      || OB_ISNULL(macro_info)
      || !macro_info->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(tablet));
  } else {
    ObInlineSecondaryMeta inline_meta(macro_info, ObSecondaryMetaType::TABLET_MACRO_INFO);
    ObSArray<ObInlineSecondaryMeta> meta_arr;
    if (OB_FAIL(meta_arr.push_back(inline_meta))) {
      LOG_WARN("fail to push back inline meta", K(ret), K(inline_meta));
    } else if (OB_FAIL(tablet->serialize(data_version, buf, size, pos, meta_arr))) {
      LOG_WARN("fail to serialize tablet", K(ret), KPC(tablet), K(inline_meta), K(size), K(pos), K(data_version));
    }
  }
  return ret;
}

int ObTabletPersistCommon::fill_tablet_write_info(
    const ObTabletPersisterParam &param,
    common::ObArenaAllocator &allocator,
    const ObTabletMacroInfo &macro_info,
    const ObTablet *tablet,
    /*out*/ ObSharedObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;

  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null tablet", K(ret), KPC(tablet));
  } else if (OB_UNLIKELY(!param.is_valid()
                         || param.for_ss_persist())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persist param", K(ret), K(param), K(param.for_ss_persist()));
  } else if (OB_FAIL(get_tablet_persist_size(param.data_version_,
                                             &macro_info,
                                             tablet,
                                             size))) {
    LOG_WARN("fail to push back inline meta", K(ret), KPC(tablet));
  } else {
    char *buf = static_cast<char *>(allocator.alloc(size));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for tablet serialize", K(ret), K(size));
    } else if (OB_FAIL(fill_tablet_into_buf(param.data_version_,
                                            &macro_info,
                                            tablet,
                                            size,
                                            buf,
                                            pos))) {
      LOG_WARN("fail to serialize tablet", K(ret), KPC(tablet), K(size), K(pos));
    } else {
      write_info.buffer_ = buf;
      write_info.offset_ = 0;
      write_info.size_ = size;
      write_info.ls_epoch_ = param.ls_epoch_;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    }
  }
  return ret;
}

/// @see: ObTablet::calc_sstable_occupy_size
int ObTabletPersistCommon::calc_sstable_occupy_size_by_table_store(
    const ObTabletTableStore &table_store,
    /*out*/ int64_t &all_sstable_occupy_size,
    /*out*/ int64_t &ss_public_sstable_occupy_size,
    /*out*/ int64_t &pure_backup_sstable_occupy_size)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator iter;

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid table store", K(ret), K(table_store));
  } else if (OB_FAIL(table_store.get_all_sstable(iter))) {
    STORAGE_LOG(WARN, "failed to get all sstable iter", K(ret), K(table_store));
  }

  while (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    ObSSTable *sstable = nullptr;
    ObSSTableMetaHandle meta_handle;
    uint64_t cur_sstable_occupy_size = 0;
    if (OB_FAIL(iter.get_next(table))) {
      if (OB_UNLIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next table from iter", K(ret), K(iter));
      }
    } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
    } else if (OB_ISNULL(sstable) || !sstable->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sstable is null or invalid", K(ret), KPC(sstable));
    } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
      LOG_WARN("fail to get sstable meta handle", K(ret), KPC(sstable));
    } else if (!meta_handle.is_valid()) {
      LOG_WARN("meta_handle is not valid", K(ret), K(meta_handle), KPC(sstable));
    } else if (!sstable->is_co_sstable()) {
      cur_sstable_occupy_size = meta_handle.get_sstable_meta().get_occupy_size();
    } else if (sstable->is_co_sstable()) {
      cur_sstable_occupy_size = static_cast<ObCOSSTableV2 *>(sstable)->get_cs_meta().occupy_size_;
    }

    if (OB_SUCC(ret)) {
      const bool is_ddl_dump_sstable = sstable->is_ddl_dump_sstable();
      const bool is_shared_sstable = meta_handle.get_sstable_meta().get_basic_meta().table_shared_flag_.is_shared_sstable();
      all_sstable_occupy_size += cur_sstable_occupy_size;
      // cacl shared_block_size
      if ((is_shared_sstable || is_ddl_dump_sstable) && GCTX.is_shared_storage_mode()) {
        // TODO gaishun.gs: for major_sstable, should add the meta_block occupy_size;
        ss_public_sstable_occupy_size += cur_sstable_occupy_size;
      }
      if (!meta_handle.get_sstable_meta().get_basic_meta().table_backup_flag_.has_local()
        && meta_handle.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup()) {
        pure_backup_sstable_occupy_size +=  cur_sstable_occupy_size;
      }
    }
  }
  if (OB_FAIL(ret)) {
    all_sstable_occupy_size = 0;
    ss_public_sstable_occupy_size = 0;
    pure_backup_sstable_occupy_size = 0;
  }
  return ret;
}

int ObTabletPersistCommon::make_tablet_macro_info(
    const ObTabletPersisterParam &param,
    const int64_t macro_seq,
    common::ObArenaAllocator &allocator,
    /* out */ ObBlockInfoSet &block_info_set,
    /* out */ ObLinkedMacroBlockItemWriter &linked_writer,
    /* out */ ObTabletMacroInfo &macro_info)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroInfoWriteParam linked_macro_info_param;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(macro_seq < 0))  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro_seq", K(ret), K(macro_seq));
  } else if (OB_FAIL(linked_macro_info_param.build_linked_marco_info_param(param, macro_seq))) {
    LOG_WARN("failed to build linked macro info param", K(ret), K(param), K(macro_seq));
  } else if (OB_FAIL(linked_writer.init_for_macro_info(linked_macro_info_param))) {
    LOG_WARN("failed to init linked writer", K(ret), K(linked_macro_info_param));
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else if (param.is_inc_shared_object()) {
    const ObSSTransferSrcTabletBlockInfo *src_tablet_block_info = param.src_tablet_block_info_;

    if (OB_ISNULL(src_tablet_block_info)) {
      // just do nothing
    } else if (OB_FAIL(src_tablet_block_info->merge_to(block_info_set))) {
      LOG_WARN("failed to merge tablet block info", K(ret), K(param));
    }
  }
#endif

  if (FAILEDx(macro_info.init(allocator, block_info_set, &linked_writer))) {
    LOG_WARN("failed to init tablet macro info", K(ret));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase