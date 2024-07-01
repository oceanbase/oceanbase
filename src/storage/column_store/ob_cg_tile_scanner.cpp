/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_cg_tile_scanner.h"
#include "ob_cg_scanner.h"
#include "ob_column_oriented_sstable.h"
#include "ob_co_sstable_rows_filter.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
namespace storage
{
int ObCGTileScanner::init(
    const ObIArray<ObTableIterParam*> &iter_params,
    const bool project_single_row,
    const bool project_without_filter,
    ObTableAccessContext &access_ctx,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  cg_scanners_.set_allocator(access_ctx.stmt_allocator_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCGTileScanner has been inited", K(ret));
  } else if (OB_UNLIKELY(iter_params.count() < 2 ||
                         !access_ctx.is_valid() ||
                         !table->is_co_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(iter_params.count()), K(access_ctx), KPC(table));
  } else if (OB_FAIL(cg_scanners_.reserve(iter_params.count()))) {
    LOG_WARN("Fail to reserve cg scanners", K(ret), K(iter_params.count()));
  } else if (project_single_row && OB_FAIL(datum_row_.init(iter_params.count()))) {
    LOG_WARN("Failed to init datum row", K(ret), K(iter_params.count()));
  } else {
    access_ctx_ = &access_ctx;
    sql_batch_size_ = project_single_row ? -1 : iter_params.at(0)->op_->get_batch_size();
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    ObCOSSTableV2* co_sstable = static_cast<ObCOSSTableV2 *>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_params.count(); ++i) {
      ObICGIterator* cg_scanner = nullptr;
      const ObTableIterParam* iter_param = iter_params.at(i);
      if (OB_ISNULL(iter_param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument, cg's iter param is null", K(ret), K(i));
      } else if (OB_FAIL(co_sstable->cg_scan(*iter_param, access_ctx, cg_scanner, true, project_single_row))) {
        LOG_WARN("Fail to cg scan", K(ret), K(i), KPC(iter_param));
      } else if (OB_UNLIKELY(!ObICGIterator::is_valid_cg_projector(cg_scanner->get_type(), project_single_row))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cg scanner", K(ret), K(cg_scanner->get_type()));
      } else if (OB_FAIL(cg_scanners_.push_back(cg_scanner))) {
        LOG_WARN("Fail to push back cg scanner", K(ret), K(i), KPC(iter_param));
      } else if (ObICGIterator::OB_CG_ROW_SCANNER == cg_scanner->get_type() ||
                 ObICGIterator::OB_CG_GROUP_BY_SCANNER == cg_scanner->get_type()) {
        static_cast<ObCGRowScanner *>(cg_scanner)->set_project_type(project_without_filter);
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    } else {
      reset();
    }
  }
  return ret;
}

int ObCGTileScanner::switch_context(
    const ObIArray<ObTableIterParam*> &iter_params,
    const bool project_single_row,
    const bool project_without_filter,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const bool col_cnt_changed)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObCGTileScanner is not inited");
  } else if (OB_UNLIKELY(iter_params.count() < 2 ||
                         iter_params.count() != cg_scanners_.count() ||
                         !access_ctx.is_valid() ||
                         !table->is_co_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(iter_params.count()), K(cg_scanners_.count()),
             K(access_ctx), KPC(table));
  } else {
    access_ctx_ = &access_ctx;
    sql_batch_size_ = project_single_row ? -1 : iter_params.at(0)->op_->get_batch_size();
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_scanners_.count(); i++) {
      storage::ObSSTableWrapper cg_wrapper;
      const ObTableIterParam &cg_param = *iter_params.at(i);
      ObICGIterator *&cg_scanner = cg_scanners_.at(i);
      if (OB_ISNULL(cg_scanner)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Uexpected null cg iter", K(ret), K(i));
      } else if (OB_UNLIKELY(col_cnt_changed)) {
        if (FALSE_IT(access_ctx.cg_iter_pool_->return_cg_iter(cg_scanner, cg_scanner->get_cg_idx()))) {
        } else if (OB_FAIL(co_sstable->cg_scan(cg_param, access_ctx, cg_scanner, true, project_single_row))) {
          LOG_WARN("Failed to cg scan", K(ret));
        }
      } else if (!is_virtual_cg(cg_param.cg_idx_) && OB_FAIL(co_sstable->fetch_cg_sstable(cg_param.cg_idx_, cg_wrapper))) {
        LOG_WARN("Fail to get cg sstable", K(ret));
      } else if (OB_FAIL(cg_scanner->switch_context(
          cg_param, access_ctx, cg_wrapper))) {
        LOG_WARN("Fail to switch context for cg iter", K(ret));
      } else if (ObICGIterator::OB_CG_ROW_SCANNER == cg_scanner->get_type() ||
                 ObICGIterator::OB_CG_GROUP_BY_SCANNER == cg_scanner->get_type()) {
        static_cast<ObCGRowScanner *>(cg_scanner)->set_project_type(project_without_filter);
      }
    }
  }
  return ret;
}

void ObCGTileScanner::reset()
{
  is_inited_ = false;
  is_reverse_scan_ = false;
  for (int64_t i = 0; i < cg_scanners_.count(); ++i) {
    ObICGIterator* cg_iter = cg_scanners_.at(i);
    FREE_PTR_FROM_CONTEXT(access_ctx_, cg_iter, ObICGIterator);
  }
  cg_scanners_.reset();
  datum_row_.reset();
  access_ctx_ = nullptr;
  sql_batch_size_ = 0;
}
void ObCGTileScanner::reuse()
{
  for (int64_t i = 0; i < cg_scanners_.count(); ++i) {
    ObICGIterator* cg_iter = cg_scanners_.at(i);
    if (OB_NOT_NULL(cg_iter)) {
      cg_iter->reuse();
    }
  }
}

int ObCGTileScanner::locate(
    const ObCSRange &range,
    const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_UNLIKELY(cg_scanners_.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected cg count", K(ret), K(cg_scanners_.count()));
  } else {
    ObICGIterator* cg_scanner = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_scanners_.count(); ++i) {
      if (OB_ISNULL(cg_scanner = cg_scanners_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null cg scanner", K(ret));
      } else if (OB_FAIL(cg_scanner->locate(range, bitmap))) {
        LOG_WARN("Fail to locate cg scanner", K(ret), K(i), K(range));
      }
    }
  }
  LOG_TRACE("[COLUMNSTORE] CGTileScanner locate range", K(ret), "type", get_type(), K(range), KP(bitmap));
  return ret;
}

int ObCGTileScanner::apply_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &filter_info,
    const int64_t row_count,
    const ObCGBitmap *parent_bitmap,
    ObCGBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGTileScanner is not inited", K(ret));
  } else if (OB_UNLIKELY(0 >= row_count || row_count != result_bitmap.size() || nullptr == filter_info.filter_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count), K(result_bitmap.size()), KP(filter_info.filter_));
  } else {
    uint64_t batch_row_count = 0;
    uint64_t remained_rows = row_count;
    ObCSRowId current = is_reverse_scan_ ? result_bitmap.get_start_id() + row_count : result_bitmap.get_start_id();
    while (OB_SUCC(ret) && remained_rows > 0) {
      // Filter batch by batch.
      if (OB_FAIL(get_next_rows(batch_row_count, sql_batch_size_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Failed to get next rows", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && batch_row_count > 0) {
        ObCSRange row_range(is_reverse_scan_ ? current - batch_row_count : current, batch_row_count);
        filter_info.start_ = 0;
        filter_info.count_ = batch_row_count;
        bool can_skip_subtree = false;
        if (nullptr != parent) {
          if (ObCGScanner::can_skip_filter(*parent, *parent_bitmap, row_range)) {
            can_skip_subtree = true;
          } else {
            common::ObBitmap *parent_result = nullptr;
            if (OB_FAIL(parent->init_bitmap(batch_row_count, parent_result))) {
              LOG_WARN("Failed to get parent bitmap", K(ret), K(batch_row_count));
            } else if (OB_FAIL(parent_bitmap->set_bitmap(row_range.start_row_id_, batch_row_count, is_reverse_scan_, *parent_result))) {
              LOG_WARN("Fail go get bitmap", K(ret));
            }
          }
        }
        if (can_skip_subtree) {
        } else if (OB_FAIL(filter_info.filter_->execute(parent, filter_info, nullptr, true))) {
          LOG_WARN("Failed to filter batch rows", K(ret), K(batch_row_count));
        } else if (OB_FAIL(result_bitmap.append_bitmap(*(filter_info.filter_->get_result()),
                                                       static_cast<uint32_t>(row_count - remained_rows),
                                                       is_reverse_scan_))) {
          LOG_WARN("Failed to append result bitmap", K(ret), K(remained_rows), K_(is_reverse_scan));
        }
        if (OB_SUCC(ret)) {
          remained_rows -= batch_row_count;
          current = is_reverse_scan_ ? current - batch_row_count : current + batch_row_count;
        }
      }
    }
  }
  LOG_TRACE("[COLUMNSTORE] apply filter info in cg tile", K(ret), K(row_count),
            "bitmap_size", result_bitmap.size(),  "popcnt", result_bitmap.popcnt());
  return ret;
}

int ObCGTileScanner::get_next_rows(uint64_t &count, const uint64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_UNLIKELY(cg_scanners_.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected cg count", K(ret), K(cg_scanners_.count()));
  } else {
    count = 0;
    bool first_scanner_end = false;
    ObICGIterator* cg_scanner = cg_scanners_.at(0);
    if (OB_ISNULL(cg_scanner)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null cg scanner", K(ret));
    } else if (OB_FAIL(cg_scanner->get_next_rows(count, capacity))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get next rows from first cg scanner", K(ret), KP(cg_scanner));
      } else if (count > 0) {
        first_scanner_end = true;
        ret = OB_SUCCESS;
      }
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < cg_scanners_.count(); ++i) {
      if (OB_ISNULL(cg_scanner = cg_scanners_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null cg scanner", K(ret), K(i));
      } else if (is_valid_cg_row_scanner(cg_scanner->get_type())) {
        ret = get_next_aligned_rows(static_cast<ObCGRowScanner*>(cg_scanner), count);
      } else {
        ret = cg_scanner->get_next_rows(count, capacity);
      }
      if (OB_FAIL(ret)) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to aligned get rows", K(ret), K(i), KP(cg_scanner));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret) && first_scanner_end) {
      ret = OB_ITER_END;
    }
  }
  LOG_TRACE("[COLUMNSTORE] get next rows in cg tile", K(ret), K(count), K(capacity));
  return ret;
}

int ObCGTileScanner::get_next_row(const blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_scanners_.count(); ++i) {
    const ObDatumRow *tmp_datum_row = nullptr;
    if (OB_FAIL(cg_scanners_[i]->get_next_row(tmp_datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next row", K(ret), K(i), KP(cg_scanners_[i]));
      }
    } else {
      datum_row_.storage_datums_[i] = tmp_datum_row->storage_datums_[0];
    }
  }
  if (OB_SUCC(ret)) {
    datum_row = &datum_row_;
  }
  return ret;
}

int ObCGTileScanner::get_next_aligned_rows(ObCGRowScanner *cg_scanner, const uint64_t target_row_count)
{
  int ret = OB_SUCCESS;
  uint64_t read_row_count = 0;
  uint64_t remain_row_count = target_row_count;
  int64_t datum_offset = 0;
  while(OB_SUCC(ret) && 0 < remain_row_count) {
    read_row_count = 0;
    if (OB_FAIL(cg_scanner->get_next_rows(read_row_count, remain_row_count, datum_offset))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail get next rows from cg scanner", K(ret), K(read_row_count), K(remain_row_count),
                 K(datum_offset), KP(cg_scanner));
      } else {
        remain_row_count -= read_row_count;
      }
    } else if (OB_UNLIKELY(0 == read_row_count || read_row_count > remain_row_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected output row count", K(ret), K(read_row_count), K(remain_row_count));
    } else {
      remain_row_count -= read_row_count;
      if (0 == remain_row_count) {
        // do not need deep copy the last rows
      } else {
        if (OB_FAIL(cg_scanner->deep_copy_projected_rows(datum_offset, read_row_count))) {
          LOG_WARN("Fail to deep copy projected rows", K(ret), K(datum_offset), K(read_row_count), KP(cg_scanner));
        } else {
          datum_offset += read_row_count;
        }
      }
    }
  }
  if (OB_UNLIKELY((OB_SUCCESS == ret || OB_ITER_END == ret) && 0 != remain_row_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected target row count, should be 0", K(ret),
             K(read_row_count), K(remain_row_count), K(target_row_count), KP(cg_scanner));
  }
  LOG_DEBUG("[COLUMNSTORE] ObCGTileScanner::get_next_aligned_rows", K(ret), K(target_row_count));
  return ret;
}

}
}
