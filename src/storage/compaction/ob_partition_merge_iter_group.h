/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_COMPACTION_MERGE_ITER_GROUP_H_
#define OB_STORAGE_COMPACTION_MERGE_ITER_GROUP_H_
#include "storage/compaction/ob_partition_merge_iter.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
namespace oceanbase
{
namespace compaction
{

// ObPartitionMergeIterGroupItem holds a pointer to an extra CG (column group) merge
// iter along with its logical column index. The iter's lifetime is managed externally.
struct ObPartitionMergeIterGroupItem
{
  ObPartitionMergeIterGroupItem()
    : iter_(nullptr), col_idx_(-1), cg_read_info_handle_(nullptr)
  {}
  ~ObPartitionMergeIterGroupItem() = default;
  void destroy()
  {
    if (OB_NOT_NULL(iter_)) {
      iter_->~ObPartitionMergeIter();
      iter_ = nullptr;
    }
    col_idx_ = -1;
    if (OB_NOT_NULL(cg_read_info_handle_)) {
      cg_read_info_handle_->~ObCGReadInfoHandle();
      cg_read_info_handle_ = nullptr;
    }
  }
  bool is_valid() const { return iter_ != nullptr && col_idx_ >= 0 && cg_read_info_handle_ != nullptr; }
  TO_STRING_KV(KP_(iter), K_(col_idx));
  ObPartitionMergeIter *iter_;
  int64_t col_idx_;
  ObCGReadInfoHandle *cg_read_info_handle_;
};

// ObPartitionMergeIterGroup<T> is a template wrapper over a rowkey-CG merge iter T.
//
// In a pure column-store (CO) major compaction, each column group (CG) stores a subset
// of columns. All CGs share the same logical row ordering determined by the rowkey CG.
// This class manages a set of extra non-rowkey CG iters (iter_group_items_) alongside
// the primary rowkey-CG iter T. Whenever the primary iter advances (next / open_curr_range),
// the group items are driven forward to the same logical row position so that
// get_curr_row() always reflects the current row across all tracked CGs.
//
// Alignment strategy:
//   After the primary iter (T) moves to a new row whose current row_id is `target_row_id`,
//   each group item iter is advanced by calling next() in a loop until its last processed
//   row_id reaches target_row_id. Because different CGs may have different macro/micro
//   block distributions, we rely on row_id (logical row offset) rather than block
//   boundaries to determine alignment.
//
// T must be a subclass of ObPartitionMergeIter (enforced by static_assert).
// Supported specialisations: ObPartitionRowMergeIter, ObPartitionMacroMergeIter,
//   ObPartitionMicroMergeIter.
template <class T>
class ObPartitionMergeIterGroup final : public T
{
  static_assert(std::is_base_of<ObPartitionMergeIter, T>::value,
                "T must be a subclass of ObPartitionMergeIter");
public:
  template <typename... Args>
  ObPartitionMergeIterGroup(common::ObIAllocator &allocator, ObCompactionFilterHandle &filter_handle, Args&... args)
    : T(allocator, args...),
      filter_handle_(&filter_handle),
      iter_group_items_(),
      filter_check_row_(),
      first_touch_items_(true)
  {}
  virtual ~ObPartitionMergeIterGroup() { reset(); }
  virtual int init(
    const ObMergeParameter &merge_param,
    const int64_t sstable_idx,
    const ObITableReadInfo *read_info) override;

  // Reset the primary iter and clear all group item references (does NOT free them).
  virtual void reset() override
  {
    for (int64_t i = 0; i < iter_group_items_.count(); ++i) {
      iter_group_items_[i].destroy();
    }
    iter_group_items_.reset();
    T::reset();
  }
  virtual const blocksstable::ObDatumRow *get_filter_check_row() override;
  int64_t get_group_item_count() const { return iter_group_items_.count(); }
  INHERIT_TO_STRING_KV("ObPartitionMergeIterGroup", T, K_(iter_group_items));

private:
  void clear_filter_check_row();
  // Drive each group item iter forward until get_curr_row_id() == target_row_id,
  // i.e. the item iter is positioned at exactly the same logical row as the primary iter.
  int sync_group_items_to_rowid(const int64_t target_row_id);
  int fuse_group_item_row(const ObPartitionMergeIterGroupItem item);
private:
  ObCompactionFilterHandle *filter_handle_;
  common::ObSEArray<ObPartitionMergeIterGroupItem, 1> iter_group_items_;
  blocksstable::ObDatumRow filter_check_row_;
  bool first_touch_items_;
};

template <class T>
int ObPartitionMergeIterGroup<T>::sync_group_items_to_rowid(const int64_t target_row_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_group_items_.count(); ++i) {
    ObPartitionMergeIter *item_iter = iter_group_items_[i].iter_;
    if (OB_ISNULL(item_iter)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null group item iter", KR(ret), K(i));
    } else {
      int64_t range_end_rowid = 0;
      int64_t curr_row_id = 0;
      bool first_touch = first_touch_items_;
      while (OB_SUCC(ret)) {
        if (first_touch) {
          first_touch = false;
          if (OB_FAIL(item_iter->next())) {
            STORAGE_LOG(WARN, "failed to next group item iter", KR(ret), K(i));
          }
        } else if (nullptr == item_iter->get_curr_row()) {
          if (item_iter->is_iter_end()) {
            ret = OB_ITER_END;
            STORAGE_LOG(INFO, "group item iter is end", KR(ret), K(i));
            break;
          } else if (OB_FAIL(item_iter->get_curr_range_end_rowid(range_end_rowid))) {
            STORAGE_LOG(WARN, "failed to get group item iter range end rowid", KR(ret), K(i), KPC(item_iter));
          } else if (range_end_rowid >= target_row_id) {
            if (OB_FAIL(item_iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/))) {
              STORAGE_LOG(WARN, "failed to open group item iter range", KR(ret), K(i), K(range_end_rowid));
            }
          } else if (OB_FAIL(item_iter->next())) {
            STORAGE_LOG(WARN, "failed to next group item iter", KR(ret), K(i));
          }
        } else if (OB_FAIL(item_iter->get_curr_row_id(curr_row_id))) {
          STORAGE_LOG(WARN, "failed to get curr row id", KR(ret), K(i), KPC(item_iter));
        } else if (curr_row_id == target_row_id) {
          break;
        } else if (OB_UNLIKELY(curr_row_id > target_row_id)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "group item iter row id is greater than target row id",
                      KR(ret), K(i), K(target_row_id), K(curr_row_id), KPC(item_iter));
        } else if (OB_FAIL(item_iter->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "group item iter ended before primary iter, row_id mismatch",
                        KR(ret), K(i), K(target_row_id), K(curr_row_id), KPC(item_iter));
          } else {
            STORAGE_LOG(WARN, "group item iter next failed", KR(ret), K(i));
          }
        }
      } // while
      if (FAILEDx(fuse_group_item_row(iter_group_items_[i]))) {
        STORAGE_LOG(WARN, "failed to fuse group item row", KR(ret), K(i));
      }
    }
  }
  first_touch_items_ = false;
  return ret;
}

template <class T>
int ObPartitionMergeIterGroup<T>::fuse_group_item_row(const ObPartitionMergeIterGroupItem item)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *item_iter = item.iter_;
  const ObDatumRow *item_row = nullptr;
  if (OB_ISNULL(item_iter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null group item iter", KR(ret), K(item));
  } else if (OB_UNLIKELY(nullptr == (item_row = item_iter->get_curr_row())
      || item_row->get_column_count() != 1
      || item_row->storage_datums_[0].is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected null group item row", KR(ret), K(item), KPC(item_row));
  } else if (OB_UNLIKELY(item.col_idx_ < 0 || item.col_idx_ >= filter_check_row_.get_column_count()
      || !filter_check_row_.storage_datums_[item.col_idx_].is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected filter check row", KR(ret), K(item), K_(filter_check_row));
  } else {
    filter_check_row_.storage_datums_[item.col_idx_] = item_row->storage_datums_[0];
    STORAGE_LOG(INFO, "fuse group item row", K(item), KPC(item_row), K_(filter_check_row));
  }
  return ret;
}

template <class T>
int ObPartitionMergeIterGroup<T>::init(
  const ObMergeParameter &merge_param,
  const int64_t sstable_idx,
  const ObITableReadInfo *read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == filter_handle_ || nullptr == filter_handle_->filter_col_idxs_ || !filter_handle_->filter_col_idxs_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid filter col idxs", KR(ret), KPC_(filter_handle));
  } else if (OB_FAIL(T::init(merge_param, sstable_idx, read_info))) {
    STORAGE_LOG(WARN, "failed to init primary iter", KR(ret));
  } else if (OB_UNLIKELY(nullptr == T::table_ || !T::table_->is_co_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null table or not co sstable", KR(ret), KPC(T::table_));
  } else if (OB_FAIL(iter_group_items_.reserve(filter_handle_->filter_col_idxs_->count()))) {
    STORAGE_LOG(WARN, "failed to reserve iter group items", KR(ret));
  } else {
    const ObCOSSTableV2 *co_sstable = static_cast<const ObCOSSTableV2 *>(T::table_);
    const ObStaticMergeParam &static_param = merge_param.static_param_;
    for (int64_t i = 0; i < filter_handle_->filter_col_idxs_->count(); ++i) {
      ObPartitionMergeIterGroupItem item;
      void *buf = nullptr;
      ObSSTableWrapper cg_wrapper;
      const ObCompactionFilterColIdxs::ColIdxCgIdxPair &col_cg_pair = filter_handle_->filter_col_idxs_->at(i);
      if (OB_UNLIKELY(!col_cg_pair.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected invalid col cg pair", KR(ret), K(i), K(col_cg_pair));
      } else if (OB_FAIL(co_sstable->get_cg_sstable(col_cg_pair.cg_idx_, cg_wrapper))) {
        STORAGE_LOG(WARN, "failed to get cg sstable", KR(ret), K(i), K(col_cg_pair));
      } else if (OB_ISNULL(buf = static_cast<void *>(T::allocator_.alloc(sizeof(ObPartitionRowMergeIter) + sizeof(ObCGReadInfoHandle))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc group item iter", KR(ret));
      } else {
        item.iter_ = new (buf) ObPartitionRowMergeIter(T::allocator_, false/*iter_co_build_row_store*/);
        item.col_idx_ = col_cg_pair.col_idx_;
        item.cg_read_info_handle_ = new (static_cast<char *>(buf) + sizeof(ObPartitionRowMergeIter)) ObCGReadInfoHandle();
        if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_cg_read_info(
            static_param.multi_version_column_descs_.at(col_cg_pair.col_idx_),
            nullptr/*col_param*/,
            static_param.get_tablet_id(), *item.cg_read_info_handle_))) {
          STORAGE_LOG(WARN, "failed to get cg read info", KR(ret), K(i), K(col_cg_pair));
        } else if (OB_FAIL(item.iter_->init(merge_param, sstable_idx, cg_wrapper.get_sstable(), item.cg_read_info_handle_->get_read_info()))) {
          STORAGE_LOG(WARN, "failed to init group item iter", KR(ret));
        } else if (OB_FAIL(iter_group_items_.push_back(item))) {
          STORAGE_LOG(WARN, "failed to push iter group item", KR(ret));
        } else {
          STORAGE_LOG(INFO, "[COMPACTION TTL] successfully init group item iter", KR(ret), K(i), K(col_cg_pair), KPC(item.iter_));
        }
        if (OB_FAIL(ret)) {
          item.destroy();
        }
      }
    } // for
    if (FAILEDx(filter_check_row_.init(T::allocator_, static_param.multi_version_column_descs_.count()))) {
      STORAGE_LOG(WARN, "failed to init filter check row", KR(ret));
    } else {
      filter_check_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    }
  }
  return ret;
}

template <class T>
const blocksstable::ObDatumRow *ObPartitionMergeIterGroup<T>::get_filter_check_row()
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *ret_row = nullptr;
  clear_filter_check_row();
  // move group items to same rowid as primary iter & fuse filter check row
  if (OB_FAIL(sync_group_items_to_rowid(T::iter_row_id_))) {
    STORAGE_LOG(WARN, "failed to sync group items to rowid", KR(ret), K_(T::iter_row_id));
  } else {
    const ObDatumRow *rowkey_row = T::get_curr_row();
    if (OB_ISNULL(rowkey_row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null rowkey row", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_row->get_column_count(); ++i) {
      if (OB_UNLIKELY(rowkey_row->storage_datums_[i].is_nop() || !filter_check_row_.storage_datums_[i].is_nop())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected filter check row", KR(ret), K(i), KPC(rowkey_row), K_(filter_check_row));
      } else {
        filter_check_row_.storage_datums_[i] = rowkey_row->storage_datums_[i];
      }
    }
    if (OB_SUCC(ret)) {
      ret_row = &filter_check_row_;
      STORAGE_LOG(INFO, "get filter check row", KR(ret), K_(T::iter_row_id), KPC(ret_row), KPC(rowkey_row));
    }
  }
  return ret_row;
}

template <class T>
void ObPartitionMergeIterGroup<T>::clear_filter_check_row()
{
  for (int64_t i = 0; i < filter_check_row_.get_column_count(); ++i) {
    filter_check_row_.storage_datums_[i].set_nop();
  }
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MERGE_ITER_GROUP_H_
