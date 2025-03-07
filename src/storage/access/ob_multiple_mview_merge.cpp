/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_multiple_mview_merge.h"

namespace oceanbase
{
namespace storage
{

int ObBaseTableAccessInfo::construct_access_ctx(
    common::ObIAllocator *range_allocator,
    const ObTableAccessContext &ctx)
{
  int ret = OB_SUCCESS;
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = ctx.trans_version_range_.base_version_;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  share::SCN tmp_scn;
  if (OB_FAIL(tmp_scn.convert_for_tx(trans_version_range.snapshot_version_))) {
    LOG_WARN("Failed to convert scn", K(ret), K(trans_version_range));
  } else if (FALSE_IT(store_ctx_.reset())) {
  } else if (OB_FAIL(store_ctx_.init_for_read(ctx.ls_id_, ctx.tablet_id_, INT64_MAX, -1, tmp_scn))) {
    LOG_WARN("Failed to init store ctx", K(ret), K(ctx));
  } else if (FALSE_IT(store_ctx_.mvcc_acc_ctx_.tx_desc_ = ctx.store_ctx_->mvcc_acc_ctx_.tx_desc_)) {
  } else if (FALSE_IT(access_ctx_.reuse())) {
  } else if (OB_FAIL(access_ctx_.init_for_mview(range_allocator, ctx, store_ctx_))) {
    LOG_WARN("Failed to init access ctx", K(ret), K(ctx));
  }
  return ret;
}

ObMviewBaseMerge::ObMviewBaseMerge()
  : ObSingleMerge()
{
}

ObMviewBaseMerge::~ObMviewBaseMerge()
{
}

bool ObMviewBaseMerge::check_table_need_read(const ObITable &table, int64_t &major_version) const
{
  bool need_read = true;
  if (table.is_major_sstable()) {
    major_version = table.get_snapshot_version();
  } else if (major_version > 0) {
    need_read = major_version != access_ctx_->trans_version_range_.snapshot_version_;
  }
  return need_read;
}

int ObMviewBaseMerge::alloc_row_store(ObTableAccessContext &context, const ObTableAccessParam &param)
{
  // ObMviewBaseMerge is an internal ObSingleMerge, no need alloc
  return OB_SUCCESS;
}

ObMviewIncrMerge::ObMviewIncrMerge()
  : base_data_merge_(nullptr),
    base_access_info_(nullptr),
    is_table_store_refreshed_(false),
    scan_old_row_(false),
    col_descs_(nullptr),
    base_rowkey_(),
    rowkey_allocator_("ObMview", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObMviewIncrMerge::~ObMviewIncrMerge()
{
  reset();
}

void ObMviewIncrMerge::reset()
{
  is_table_store_refreshed_ = false;
  scan_old_row_ = false;
  common::ObIAllocator *alloc = nullptr == base_access_info_ ?
      nullptr : base_access_info_->access_ctx_.stmt_allocator_;
  if (OB_NOT_NULL(base_data_merge_)) {
    base_data_merge_->~ObMviewBaseMerge();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(base_data_merge_);
    }
    base_data_merge_ = nullptr;
  }
  if (OB_NOT_NULL(base_access_info_)) {
    base_access_info_->~ObBaseTableAccessInfo();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(base_access_info_);
    }
    base_access_info_ = nullptr;
  }
  col_descs_ = nullptr;
  base_rowkey_.reset();
  rowkey_allocator_.reset();
}

void ObMviewIncrMerge::reuse()
{
  if (nullptr != base_data_merge_) {
    base_data_merge_->reuse();
  }
  rowkey_allocator_.reuse();
}

int ObMviewIncrMerge::get_incr_row(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObMultipleMerge &merge,
    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool version_fit = false;
  bool read_row = false;
  const bool version_include_base = 0 == context.trans_version_range_.base_version_;
  const StorageScanType scan_type = context.mview_scan_info_->scan_type_;
  while (OB_SUCC(ret) && !read_row) {
    version_fit = false;
    row.count_ = 0;
    row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    if (OB_FAIL(get_storage_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get merged row", K(ret));
      }
    } else if (OB_FAIL(check_version_fit(param, context, row, version_fit))) {
      LOG_WARN("Failed to check version fit", K(ret));
    } else if (!version_fit) {
    } else if (FALSE_IT(scan_old_row_ = is_mview_scan_old_row(scan_type) || (is_mview_scan_final_row(scan_type) && row.row_flag_.is_delete()))) {
    } else if (version_include_base && scan_old_row_) {
      ret = OB_ITER_END;
    } else if (version_include_base || (!scan_old_row_ && has_no_nop_values(param, merge.get_nop_pos()))) {
      if (row.row_flag_.is_exist_without_delete()) {
        LOG_DEBUG("output insert row", K(ret), K(row), K(version_include_base), K_(scan_old_row),
                  K(merge.get_nop_pos().count()));
        read_row = true;
      }
    } else {
      if (OB_FAIL(base_rowkey_.assign(row.storage_datums_, param.iter_param_.get_schema_rowkey_count()))) {
        LOG_WARN("Failed to assign incr rowkey", K(ret));
      } else if (OB_FAIL(open_base_data_merge(param, context, get_table_param, base_rowkey_))) {
        LOG_WARN("Failed to open base data merge", K(ret));
      } else {
        blocksstable::ObDatumRow &base_row = base_data_merge_->get_unprojected_row();
        base_row.count_ = 0;
        base_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        if (base_data_merge_->is_empty()) {
        } else if (OB_FAIL(base_data_merge_->get_storage_row(base_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to get base data row", K(ret));
          } else if (OB_UNLIKELY(row.row_flag_.is_exist_without_delete() && !has_no_nop_values(param, merge.get_nop_pos()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Base row is not found", K(ret), K(row), K(merge.get_nop_pos()));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          LOG_DEBUG("[MVIEW QUERY]: get base row", K(ret), K(base_row), K(version_include_base), K_(scan_old_row));
        }
        if (FAILEDx(generate_output_row(param, merge.get_nop_pos(), base_row, row))) {
          LOG_WARN("Failed to generate row", K(ret));
        } else {
          read_row = row.row_flag_.is_exist_without_delete();
        }
      }
    }
  }
  if (FAILEDx(set_old_new_row_flag(param, context, row))) {
    LOG_WARN("Failed to set old new row flag", K(ret));
  }
  LOG_DEBUG("[MVIEW QUERY]: get next row", K(ret), K(row), K(version_include_base), K_(scan_old_row));
  return ret;
}

int ObMviewIncrMerge::check_version_fit(
    const ObTableAccessParam &param,
    const ObTableAccessContext &context,
    const blocksstable::ObDatumRow &row,
    bool &version_fit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.iter_param_.need_scn_ || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid scn or row", K(ret), K(param.iter_param_), K(row));
  } else {
    const int64_t trans_idx = param.iter_param_.get_read_info()->get_trans_col_index();
    version_fit = row.storage_datums_[trans_idx].get_int() > context.trans_version_range_.base_version_;
    LOG_DEBUG("[MVIEW QUERY]: check incr row version", K(ret), K(trans_idx), K(version_fit),
              K(row.storage_datums_[trans_idx].get_int()), K(context.trans_version_range_.base_version_));
  }
  return ret;
}

int ObMviewIncrMerge::generate_output_row(
    const ObTableAccessParam &param,
    ObNopPos &nop_pos,
    const blocksstable::ObDatumRow &base_row,
    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool should_output = false;
  if (OB_UNLIKELY(!row.is_valid() ||
                  !row.row_flag_.is_exist())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid incr row flag", K(ret), K(base_row), K(row));
  } else if (FALSE_IT(row.row_flag_.set_flag(ObDmlFlag::DF_INSERT))) {
  } else {
    // TableScan1(MVIEW_FIRST_DELETE), output DELETE old row
    // TableScan2(MVIEW_LAST_INSERT), output INSERT new row
    // if base_row.row_flag_.is_exist_without_delete() is true, means that the old base row is exist and
    //   TableScan is MVIEW_FIRST_DELETE, incr row is (DELETE/INSERT/UPDATE), should output old base row
    //   TableScan is MVIEW_LAST_INSERT, then DELETE row is ignored internal, incr row is(INSERT/UPDATE), should output new incr row
    // if base_row.row_flag_.is_exist_without_delete() is false, means that the old base row is not exist and
    //   TableScan is MVIEW_FIRST_DELETE, no need to ouput
    //   TableScan is MVIEW_LAST_INSERT, then DELETE row is ignored internal, incr row is(INSERT/UPDATE), should output new incr row
    should_output = base_row.row_flag_.is_exist_without_delete() || !scan_old_row_;
  }
  if (OB_FAIL(ret)) {
  } else if (should_output) {
    bool final_result = false;
    if (scan_old_row_) {
      const int64_t trans_idx = param.iter_param_.get_read_info()->get_trans_col_index();
      for (int64_t i = 0; i < base_row.count_; ++i) {
        if (OB_LIKELY(i != trans_idx)) {
          row.storage_datums_[i] = base_row.storage_datums_[i];
        }
      }
      final_result = true;
    } else if (has_no_nop_values(param, nop_pos)) {
    } else if (OB_FAIL(ObRowFuse::fuse_row(base_row, row, nop_pos, final_result))) {
      LOG_WARN("Failed to fuse incr row", K(ret), K(base_row), K(row));
    }
  } else {
    row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    LOG_DEBUG("[MVIEW QUERY]: no need to output row", K(ret), K(base_row), K(row), K_(scan_old_row));
  }
  LOG_DEBUG("[MVIEW QUERY]: generate row", K(ret), K(base_row), K(row), K_(scan_old_row));
  return ret;
}

int ObMviewIncrMerge::open_base_data_merge(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (nullptr == base_data_merge_) {
    is_table_store_refreshed_ = false;
    if (OB_ISNULL(base_access_info_ = OB_NEWx(ObBaseTableAccessInfo, context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc base access info", K(ret));
    } else if (OB_ISNULL(base_data_merge_ = OB_NEWx(ObMviewBaseMerge, context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc base data merge", K(ret));
    } else if (OB_FAIL(base_access_info_->construct_access_ctx(&rowkey_allocator_, context))) {
      LOG_WARN("Failed to cons version range and ctx", K(ret));
    } else if (OB_FAIL(base_data_merge_->init(param, base_access_info_->access_ctx_, get_table_param))) {
      LOG_WARN("Failed to init base data merge", K(ret));
    } else if (OB_ISNULL(col_descs_ = param.iter_param_.get_out_col_descs())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unexpected null out cols", K(ret));
    }
  } else {
    base_data_merge_->reuse();
    rowkey_allocator_.reuse();
  }
  if (OB_FAIL(ret)) {
  } else if (is_table_store_refreshed_) {
    LOG_INFO("table store refreshed", K(is_table_store_refreshed_), K(param.iter_param_.tablet_id_));
    if (OB_FAIL(base_access_info_->construct_access_ctx(&rowkey_allocator_, context))) {
      LOG_WARN("Failed to cons version range and ctx", K(ret));
    } else if (OB_FAIL(base_data_merge_->switch_param(param, base_access_info_->access_ctx_, get_table_param))) {
      LOG_WARN("Failed to switch param", K(ret));
    } else {
      is_table_store_refreshed_ = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (base_data_merge_->is_empty()) {
    LOG_TRACE("[MVIEW QUERY]: base data is empty", K(ret));
  } else if (OB_FAIL(base_rowkey_.prepare_memtable_readable(*col_descs_, rowkey_allocator_))) {
    LOG_WARN("Failed to prepare memtable rowkey", K(ret));
  } else if (OB_FAIL(base_data_merge_->open(rowkey))) {
    LOG_WARN("Failed to open base data merge", K(ret));
  }
  return ret;
}

int ObMviewIncrMerge::set_old_new_row_flag(
    const ObTableAccessParam &param,
    const ObTableAccessContext &context,
    blocksstable::ObDatumRow &row) const
{
  int ret = OB_SUCCESS;
  const int64_t old_new_idx = param.iter_param_.get_mview_old_new_col_index();
  if (OB_UNLIKELY(OB_INVALID_INDEX == old_new_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid mview old new col index", K(ret), K(old_new_idx));
  } else if (scan_old_row_) {
    row.storage_datums_[old_new_idx].set_string(ObMviewScanInfo::OLD_ROW);
  } else {
    row.storage_datums_[old_new_idx].set_string(ObMviewScanInfo::NEW_ROW);
  }
  return ret;
}

int ObMviewSingleMerge::open(ObTableScanRange &table_scan_range)
{
  return ObSingleMerge::open(table_scan_range.get_rowkeys().at(0));
}

int ObMviewScanMerge::open(ObTableScanRange &table_scan_range)
{
   return ObMultipleScanMerge::open(table_scan_range.get_ranges().at(0));
}

int ObMviewGetMerge::open(ObTableScanRange &table_scan_range)
{
  return ObMultipleGetMerge::open(table_scan_range.get_rowkeys());
}

int ObMviewMultiScanMerge::open(ObTableScanRange &table_scan_range)
{
  return ObMultipleMultiScanMerge::open(table_scan_range.get_ranges());
}

ObMviewMergeWrapper::ObMviewMergeWrapper()
{
  MEMSET(merges_, 0, sizeof(merges_));
}

ObMviewMergeWrapper::~ObMviewMergeWrapper()
{
  for (int64_t i = 0; i < T_MAX_ITER_TYPE; ++i) {
    if (nullptr != merges_[i]) {
       merges_[i]->~ObMviewMerge();
       merges_[i] = nullptr;
    }
  }
}

void ObMviewMergeWrapper::reuse()
{
  for (int64_t i = 0; i < T_MAX_ITER_TYPE; ++i) {
    if (nullptr != merges_[i]) {
       merges_[i]->reuse();
    }
  }
}

int ObMviewMergeWrapper::switch_param(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < T_MAX_ITER_TYPE; ++i) {
    if (nullptr != merges_[i] &&
        OB_FAIL(merges_[i]->switch_param(param, context, get_table_param))) {
      LOG_WARN("Failed to switch param", K(ret), K(i));
    }
  }
  return ret;
}

int ObMviewMergeWrapper::alloc_mview_merge(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObTableScanRange &table_scan_range,
    ObMviewMergeWrapper *&merge_wrapper,
    ObMviewMerge *&mview_merge)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator *alloctor = context.stmt_allocator_;
  if (nullptr == merge_wrapper &&
      OB_ISNULL(merge_wrapper = OB_NEWx(ObMviewMergeWrapper, alloctor))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc mview merge wrapper", K(ret));
  } else if (OB_FAIL(merge_wrapper->get_mview_merge(param, context, get_table_param, table_scan_range, mview_merge))) {
    LOG_WARN("Failed to get mview merge", K(ret));
  }
  return ret;
}

int ObMviewMergeWrapper::get_mview_merge(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObTableScanRange &table_scan_range,
    ObMviewMerge *&version_merge)
{
  int ret = OB_SUCCESS;
  ObQRIterType merge_type;
  ObMviewMerge *tmp_merge = nullptr;
  version_merge = nullptr;
  if (OB_FAIL(table_scan_range.get_query_iter_type(merge_type))) {
    LOG_WARN("Failed to get query iter type", K(ret));
  } else if (nullptr == (tmp_merge = merges_[merge_type])) {
    switch (merge_type) {
      case T_SINGLE_GET: {
        context.use_fuse_row_cache_ = context.mview_scan_info_->is_mv_refresh_query_;
        tmp_merge = OB_NEWx(ObMviewSingleMerge, context.stmt_allocator_);
        break;
      }
      case T_SINGLE_SCAN: {
        tmp_merge = OB_NEWx(ObMviewScanMerge, context.stmt_allocator_);
        break;
      }
      case T_MULTI_GET: {
        tmp_merge = OB_NEWx(ObMviewGetMerge, context.stmt_allocator_);
        break;
      }
      case T_MULTI_SCAN: {
        tmp_merge = OB_NEWx(ObMviewMultiScanMerge, context.stmt_allocator_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected merge type", K(ret), K(merge_type));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tmp_merge)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc merge", K(ret));
    } else if (OB_FAIL(tmp_merge->init(param, context, get_table_param))) {
      LOG_WARN("Failed to init merge", K(ret));
    }
    LOG_DEBUG("[MVIEW QUERY]: get version range merge", K(ret), K(merge_type), KP(tmp_merge));
  }
  if (FAILEDx(tmp_merge->open(table_scan_range))) {
    LOG_WARN("Failed to open mview merge", K(ret));
  } else {
    merges_[merge_type] = tmp_merge;
    version_merge = tmp_merge;
    version_merge->set_iter_del_row(is_mview_need_deleted_row(context.mview_scan_info_->scan_type_));
  }
  if (OB_FAIL(ret) && nullptr != tmp_merge) {
    tmp_merge->~ObMviewMerge();
    context.stmt_allocator_->free(tmp_merge);
  }
  return ret;
}

}
}