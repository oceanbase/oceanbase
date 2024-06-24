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
#include "ob_virtual_cg_scanner.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_vector_store.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace storage
{
ObVirtualCGScanner::ObVirtualCGScanner()
  : is_inited_(false),
    is_reverse_scan_(false),
    iter_param_(nullptr),
    access_ctx_(nullptr),
    current_group_size_(0),
    cg_agg_cells_(nullptr)
{
}

void ObVirtualCGScanner::reset()
{
  is_inited_ = false;
  is_reverse_scan_ = false;
  iter_param_ = nullptr;
  current_group_size_ = 0;
  FREE_PTR_FROM_CONTEXT(access_ctx_, cg_agg_cells_, ObCGAggCells);
  cg_agg_cells_ = nullptr;
  access_ctx_ = nullptr;
}

void ObVirtualCGScanner::reuse()
{
  current_group_size_ = 0;
}

int ObVirtualCGScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  UNUSED(wrapper);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCGScanner has been inited", K(ret));
  } else if (iter_param.enable_pd_aggregate() &&
             OB_FAIL(init_agg_cells(iter_param, access_ctx))) {
    LOG_WARN("Fail to get agg cells", K(ret));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    set_cg_idx(iter_param.cg_idx_);
    is_inited_ = true;
  }
  return ret;
}

int ObVirtualCGScanner::switch_context(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  UNUSEDx(wrapper);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVirtualCGScanner not inited", K(ret));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
  }
  return ret;
}

int ObVirtualCGScanner::locate(
    const ObCSRange &range,
    const ObCGBitmap *bitmap)
{
  UNUSEDx(range, bitmap);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVirtualCGScanner not inited", K(ret));
  } else {
    current_group_size_ = nullptr == bitmap ? range.get_row_count() : bitmap->popcnt();
  }
  return ret;
}

int ObVirtualCGScanner::apply_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &filter_info,
    const int64_t row_count,
    const ObCGBitmap *parent_bitmap,
    ObCGBitmap &result_bitmap)
{
  UNUSEDx(parent,parent_bitmap);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVirtualCGScanner not inited", K(ret));
  } else if (OB_UNLIKELY(0 >= row_count || row_count != result_bitmap.size() || nullptr == filter_info.filter_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count), K(result_bitmap.size()), KP(filter_info.filter_));
  } else if (OB_NOT_NULL(cg_agg_cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state, not supported now", K(ret));
  } else {
    int64_t capacity = row_count;
    const common::ObBitmap *bitmap = nullptr;
    filter_info.is_pd_to_cg_ = true;
    while (OB_SUCC(ret) && capacity > 0) {
      int64_t batch_size = MIN(iter_param_->op_->get_batch_size(), capacity);
      filter_info.start_ = row_count - capacity;
      filter_info.count_ = batch_size;
      if (OB_FAIL(filter_info.filter_->execute(nullptr/*parent*/, filter_info, nullptr/*micro_scanner*/, true/*use_vectorize*/))) {
        LOG_WARN("Fail to execute filter", K(ret));
      } else if (OB_ISNULL(bitmap = filter_info.filter_->get_result()) || bitmap->size() != batch_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null filter bitmap", K(ret), KPC(filter_info.filter_), K(batch_size));
      } else if (OB_FAIL(result_bitmap.append_bitmap(*bitmap,
                                                     is_reverse_scan_ ? (capacity - bitmap->size()) : static_cast<uint32_t>(result_bitmap.size() - capacity),
                                                     false))){
        LOG_WARN("Fail to append bitmap", K(ret), K(row_count), KPC(bitmap), K(result_bitmap));
      } else {
        capacity -= batch_size;
      }
    }
  }
  return ret;
}

int ObVirtualCGScanner::get_next_rows(uint64_t &count, const uint64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVirtualCGScanner is not init", K(ret));
  } else if (OB_ISNULL(cg_agg_cells_)) {
    count = current_group_size_ > capacity ? capacity : current_group_size_;
    current_group_size_ -= count;
    if (0 == current_group_size_) {
      ret = OB_ITER_END;
    }
  } else {
    if (OB_FAIL(cg_agg_cells_->process(*iter_param_, *access_ctx_, 0/*col_idx*/, nullptr/*reader*/, nullptr/*row_ids*/,
                                       current_group_size_))) {
      LOG_WARN("Fail to process agg cells", K(ret));
    } else {
      count = current_group_size_;
      current_group_size_ = 0;
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObVirtualCGScanner::init_agg_cells(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == access_ctx.block_row_store_ ||
                  nullptr == iter_param.output_exprs_ ||
                  0 == iter_param.output_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregated expr count", K(ret), KPC(iter_param.output_exprs_));
  } else if (nullptr == cg_agg_cells_) {
    void *buf = nullptr;
    if (OB_ISNULL(cg_agg_cells_ = OB_NEWx(ObCGAggCells, access_ctx.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc cg agg cells", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_param.output_exprs_->count(); ++i) {
    ObAggCell *cell = nullptr;
    if (OB_FAIL(static_cast<ObAggregatedStore*>(access_ctx.block_row_store_)->get_agg_cell(
        iter_param.output_exprs_->at(i), cell))) {
      LOG_WARN("Fail to get agg cell", K(ret));
    } else if (OB_UNLIKELY(cell->need_access_data())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected agg cell", K(ret), KPC(cell));
    } else if (OB_FAIL(cg_agg_cells_->add_agg_cell(cell))) {
      LOG_WARN("Fail to push back", K(ret));
    }
  }
  return ret;
}

void ObDefaultCGScanner::reset()
{
  if (cg_agg_cells_ != nullptr) {
    cg_agg_cells_->~ObCGAggCells();
    if (stmt_allocator_ != nullptr ) {
      stmt_allocator_->free(cg_agg_cells_);
    }
    cg_agg_cells_ = nullptr;
  }
  total_row_count_ = 0;
  query_range_valid_row_count_ = 0;
  iter_param_ = nullptr;
  filter_ = nullptr;
  datum_infos_.reset();
  default_row_.reset();
  stmt_allocator_ = nullptr;
  filter_result_ = false;
  is_inited_ = false;
}

void ObDefaultCGScanner::reuse()
{
  query_range_valid_row_count_ = 0;
  filter_ = nullptr;
  filter_result_ = false;
}

int ObDefaultCGScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObDefaultCGScanner has been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == iter_param.read_info_ ||
                        iter_param.read_info_->get_request_count() != 1 ||
                        access_ctx.stmt_allocator_ == nullptr ||
                        !wrapper.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected argument", K(ret), K(wrapper), K(iter_param), K(access_ctx));
  } else if (OB_FAIL(init_datum_infos_and_default_row(iter_param, access_ctx))) {
    STORAGE_LOG(WARN, "Failed to init_datum_infos_and_default_row", K(ret), K(iter_param), K(access_ctx));
  } else if (OB_FAIL(init_cg_agg_cells(iter_param, access_ctx))) {
    STORAGE_LOG(WARN, "failed to init cg_add_cells", K(ret), K(iter_param), K(access_ctx));
  } else if (OB_FAIL(wrapper.get_merge_row_cnt(iter_param, total_row_count_))) {
    STORAGE_LOG(WARN, "fail to get merge row cnt", K(ret), K(iter_param), K(total_row_count_), K(wrapper));
  } else {
    query_range_valid_row_count_ = 0;
    iter_param_ = &iter_param;
    filter_ = nullptr;
    filter_result_ = false;
    stmt_allocator_ = access_ctx.stmt_allocator_;
    set_cg_idx(iter_param.cg_idx_);
    is_inited_ = true;
  }

  return ret;
}

int ObDefaultCGScanner::init_cg_agg_cells(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  cg_agg_cells_ = nullptr;

  if (iter_param.enable_pd_aggregate()) {
    ObAggregatedStore *agg_store = static_cast<ObAggregatedStore *>(access_ctx.block_row_store_);
    cg_agg_cells_ = OB_NEWx(ObCGAggCells, access_ctx.stmt_allocator_);
    if (OB_ISNULL(cg_agg_cells_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc mermory", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < iter_param.output_exprs_->count(); ++i) {
        ObAggCell *cell = nullptr;
        if (OB_FAIL(agg_store->get_agg_cell(iter_param.output_exprs_->at(i), cell))) {
          LOG_WARN("Fail to get agg cell", K(ret));
        } else if (OB_FAIL(cg_agg_cells_->add_agg_cell(cell))) {
          LOG_WARN("Fail to push back", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cg_agg_cells_)) {
    cg_agg_cells_->~ObCGAggCells();
    access_ctx.stmt_allocator_->free(cg_agg_cells_);
    cg_agg_cells_ = nullptr;
  }

  return ret;
}

int ObDefaultCGScanner::init_datum_infos_and_default_row(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  const share::schema::ObColumnParam *column_param = nullptr;
  datum_infos_.reset();
  default_row_.reset();

  if (OB_UNLIKELY(1 != iter_param.read_info_->get_request_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected request count", K(ret), KPC(iter_param.read_info_));
  } else if (OB_FAIL(default_row_.init(iter_param.read_info_->get_request_count()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(iter_param.get_cg_column_param(column_param))) {
    STORAGE_LOG(WARN, "failed to get cg column param", K(ret), K(iter_param));
  } else if (OB_ISNULL(column_param)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null column_param", K(ret), K(iter_param));
  } else if (OB_FAIL(default_row_.storage_datums_[0].from_obj_enhance(column_param->get_orig_default_value()))) {
    STORAGE_LOG(WARN, "Failed to transefer obj to datum", K(ret));
  } else if (OB_FAIL(pad_column(column_param->get_meta_type(), column_param->get_accuracy(), *access_ctx.stmt_allocator_, default_row_.storage_datums_[0]))) {
    LOG_WARN("Failed to pad default column", K(ret), KPC(column_param), K_(default_row));
  } else if (OB_FAIL(add_lob_header_if_need(*column_param, default_row_.local_allocator_, default_row_.storage_datums_[0]))) {
    STORAGE_LOG(WARN, "Failed to add lob header to default value", K(ret));
  } else if (iter_param.vectorized_enabled_ && !iter_param.enable_pd_aggregate()) {
    const int64_t expr_count = iter_param.output_exprs_->count();
    datum_infos_.set_allocator(access_ctx.stmt_allocator_);
    sql::ObEvalCtx &eval_ctx = iter_param.op_->get_eval_ctx();
    if (OB_FAIL(datum_infos_.init(expr_count))) {
      LOG_WARN("Failed to init datum infos", K(ret), K(expr_count));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; i++) {
      sql::ObExpr *expr = nullptr;
      common::ObDatum *datums = nullptr;
      if (OB_ISNULL(expr = iter_param.output_exprs_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null expr", K(ret), K(i), K(iter_param.output_exprs_));
      } else if (!iter_param.vectorized_enabled_) {
        datums = &expr->locate_datum_for_write(iter_param.op_->get_eval_ctx());
      } else if (OB_ISNULL(datums = expr->locate_batch_datums(eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null datums", K(ret), K(i), KPC(expr));
      } else if (OB_UNLIKELY(!(expr->is_variable_res_buf())
                             && datums->ptr_ != eval_ctx.frames_[expr->frame_idx_] + expr->res_buf_off_)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected sql expr datum buffer", K(ret), KP(datums->ptr_), K(eval_ctx), KPC(expr));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(datum_infos_.push_back(blocksstable::ObSqlDatumInfo(datums, iter_param.output_exprs_->at(i))))) {
        LOG_WARN("fail to push back datum", K(ret), K(datums));
      }
    }
  }

  return ret;
}

int ObDefaultCGScanner::switch_context(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDefaultCGScanner not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == iter_param.read_info_ ||
                        iter_param.read_info_->get_request_count() != 1 ||
                        access_ctx.stmt_allocator_ == nullptr ||
                        !wrapper.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected argument", K(ret), K(wrapper), K(iter_param), K(access_ctx));
  } else if (OB_FAIL(wrapper.get_merge_row_cnt(iter_param, total_row_count_))) {
    STORAGE_LOG(WARN, "fail to get ddl merge row cnt", K(ret), K(iter_param), K(total_row_count_), K(wrapper));
  } else {
    query_range_valid_row_count_ = 0;
    iter_param_ = &iter_param;
    stmt_allocator_ = access_ctx.stmt_allocator_;
  }

  return ret;
}

int ObDefaultCGScanner::locate(const ObCSRange &range, const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDefaultCGScanner not init", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid() || range.end_row_id_ >= total_row_count_ ||
              (nullptr != bitmap && bitmap->get_start_id() + bitmap->size() > total_row_count_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(range), K(total_row_count_), KPC(bitmap));
  } else if (range.start_row_id_ >= total_row_count_) {
    ret = OB_ITER_END;
  } else {
    query_range_valid_row_count_ = nullptr == bitmap ? range.get_row_count() : bitmap->popcnt();
  }
  return ret;
}

int ObDefaultCGScanner::apply_filter(
  sql::ObPushdownFilterExecutor *parent,
  sql::PushdownFilterInfo &filter_info,
  const int64_t row_count,
  const ObCGBitmap *parent_bitmap,
  ObCGBitmap &result_bitmap)
{
  UNUSEDx(parent, row_count, parent_bitmap);
  int ret = OB_SUCCESS;
  bool result = false;
  sql::ObPushdownFilterExecutor *filter = filter_info.filter_;

  if (OB_UNLIKELY(nullptr == filter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected filter node", K(ret), KPC(filter));
  } else if (filter_ != nullptr) {
    if (filter != filter_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "filter not equal", K(ret), KPC(filter), KPC(filter_));
    } else {
      result = filter_result_;
    }
  } else if (OB_FAIL(do_filter(filter, *filter_info.skip_bit_, result))) {
    STORAGE_LOG(WARN, "failed to do filter", K(ret), KPC(filter));
  }

  if (OB_FAIL(ret)) {
  } else {
    filter_result_ = result;
    filter_ = filter;
    if (result) {
      result_bitmap.set_all_true();
    } else {
      result_bitmap.set_all_false();
    }
  }

  return ret;
}

int ObDefaultCGScanner::get_next_rows(uint64_t &count, const uint64_t capacity)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDefaultCGScanner not init", K(ret));
  } else if (OB_UNLIKELY(query_range_valid_row_count_ == 0)) {
    ret = OB_ITER_END;
  } else if (cg_agg_cells_ != nullptr) {
    count = query_range_valid_row_count_;
    if(OB_FAIL(cg_agg_cells_->process(default_row_.storage_datums_[0], count))) {
      STORAGE_LOG(WARN, "failed to process cg_agg_cells", K(ret), K(default_row_), K(count));
    }
  } else {
    count = query_range_valid_row_count_ > capacity ? capacity : query_range_valid_row_count_;
    if (iter_param_->op_->enable_rich_format_ &&
        OB_FAIL(init_exprs_uniform_header(iter_param_->output_exprs_, iter_param_->op_->get_eval_ctx(), count))) {
      LOG_WARN("Failed to init exprs vector header", K(ret));
    }
    for(int64_t curr_row = 0; OB_SUCC(ret) && curr_row < count; curr_row++) {
      for (int64_t i = 0; OB_SUCC(ret) && i < iter_param_->out_cols_project_->count(); i++) {
        common::ObDatum &datum = datum_infos_.at(i).datum_ptr_[curr_row];
        int32_t col_idx = iter_param_->out_cols_project_->at(i);
        if (OB_UNLIKELY(col_idx >= default_row_.get_column_count())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected col idx", K(ret), K(col_idx), K(default_row_), KPC(iter_param_));
        } else if (OB_FAIL(datum.from_storage_datum(default_row_.storage_datums_[col_idx], datum_infos_.at(i).get_obj_datum_map()))) {
          LOG_WARN("Failed to from storage datum", K(ret), K(col_idx), K(default_row_), K(datum_infos_.at(i).get_obj_datum_map()));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ret = query_range_valid_row_count_ <= count ? OB_ITER_END : OB_SUCCESS;
    query_range_valid_row_count_ -= count;
  }

  return ret;
}

int ObDefaultCGScanner::get_next_row(const blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = &default_row_;
  return ret;
}

int ObDefaultCGScanner::do_filter(sql::ObPushdownFilterExecutor *filter, const sql::ObBitVector &skip_bit, bool &result)
{
  int ret = OB_SUCCESS;
  bool filtered = false;

  if (filter->is_filter_node()) {
    if (filter->is_filter_black_node()) {
      sql::ObPhysicalFilterExecutor *black_filter = static_cast<sql::ObPhysicalFilterExecutor *>(filter);
      sql::ObPushdownOperator &pushdown_op = black_filter->get_op();
      if (pushdown_op.enable_rich_format_ &&
          OB_FAIL(storage::init_exprs_uniform_header(black_filter->get_cg_col_exprs(), pushdown_op.get_eval_ctx(), 1))) {
        LOG_WARN("Failed to init exprs vector header", K(ret));
      } else if (OB_FAIL(black_filter->filter(default_row_.storage_datums_, filter->get_col_count(), skip_bit, filtered))) {
        LOG_WARN("Failed to filter row with black filter", K(ret), K(default_row_), KPC(black_filter));
      }
    } else {
      sql::ObWhiteFilterExecutor *white_filter = static_cast<sql::ObWhiteFilterExecutor *>(filter);
      if (OB_FAIL(blocksstable::ObIMicroBlockReader::filter_white_filter(*white_filter, default_row_.storage_datums_[0], filtered))) {
        LOG_WARN("Failed to filter row with white filter", K(ret), KPC(white_filter), K(default_row_));
      }
    }
    if (OB_SUCC(ret)) {
      result = !filtered;
    }
  } else {
    if (OB_UNLIKELY(filter->get_child_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected child count of filter executor", K(ret),
               K(filter->get_child_count()), KP(filter));
    } else {
      sql::ObPushdownFilterExecutor **children = filter->get_childs();
      for (int64_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); ++i) {
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(do_filter(children[i], skip_bit, result))) {
          STORAGE_LOG(WARN, "failed to do filter", K(ret), KPC(children[i]));
        } else if ((result && filter->is_logic_or_node()) || (!result && filter->is_logic_and_node())) {
          break;
        }
      }
    }
  }

  return ret;
}

int ObDefaultCGScanner::add_lob_header_if_need(
    const share::schema::ObColumnParam &column_param,
    ObIAllocator &allocator,
    blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  const ObObjMeta &meta = column_param.get_meta_type();
  if (! meta.is_lob_storage() || datum.is_null()) {
  } else {
    // lob value must have lob header if not null
    ObString data = datum.get_string();
    ObString out;
    if (OB_FAIL(ObLobManager::fill_lob_header(allocator, data, out))) {
      STORAGE_LOG(WARN, "fill lob header fail", K(ret), K(meta), K(datum), K(data));
    } else {
      datum.set_string(out);
    }
  }
  return ret;
}

ObDefaultCGGroupByScanner::ObDefaultCGGroupByScanner()
  : ObDefaultCGScanner(),
    group_by_agg_idxs_(),
    group_by_cell_(nullptr)
{}

ObDefaultCGGroupByScanner::~ObDefaultCGGroupByScanner()
{
  reset();
}

void ObDefaultCGGroupByScanner::reset()
{
  ObDefaultCGScanner::reset();
  output_exprs_ = nullptr;
  group_by_agg_idxs_.reset();
  group_by_cell_ = nullptr;
}

int ObDefaultCGGroupByScanner::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDefaultCGScanner::init(iter_param, access_ctx, wrapper))) {
    LOG_WARN("Failed to init ObCGRowScanner", K(ret));
  } else if (OB_UNLIKELY(nullptr == iter_param.output_exprs_ ||
                         0 == iter_param.output_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected group by expr count", K(ret), KPC(iter_param.output_exprs_));
  } else {
    output_exprs_ = iter_param.output_exprs_;
    group_by_cell_ = (static_cast<ObVectorStore*>(access_ctx.block_row_store_))->get_group_by_cell();
    set_cg_idx(iter_param.cg_idx_);
  }
  return ret;
}

int ObDefaultCGGroupByScanner::init_group_by_info()
{
  int ret = OB_SUCCESS;
  ObGroupByAggIdxArray agg_idxs;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_->count(); ++i) {
    agg_idxs.reuse();
    if (OB_ISNULL(output_exprs_->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null output expr", K(ret));
    } else if (OB_FAIL(group_by_cell_->assign_agg_cells(output_exprs_->at(i), agg_idxs))) {
      LOG_WARN("Failed to extact agg cells", K(ret));
    } else if (OB_FAIL(group_by_agg_idxs_.push_back(agg_idxs))) {
      LOG_WARN("Failed to push back", K(ret));
    }
  }
  return ret;
}

int ObDefaultCGGroupByScanner::decide_group_size(int64_t &group_size)
{
  int ret = OB_SUCCESS;
  group_size = query_range_valid_row_count_;
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(group_size));
  return ret;
}

int ObDefaultCGGroupByScanner::decide_can_group_by(
    const int32_t group_by_col,
    bool &can_group_by)
{
  UNUSEDx(group_by_col);
  int ret = OB_SUCCESS;
  can_group_by = true;
  const ObCGBitmap *bitmap = nullptr;
  if (OB_FAIL(group_by_cell_->decide_use_group_by(
    query_range_valid_row_count_, query_range_valid_row_count_, 1, bitmap, can_group_by))) {
    LOG_WARN("Failed to decide use group by", K(ret));
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(query_range_valid_row_count_), K(can_group_by));
  return ret;
}

int ObDefaultCGGroupByScanner::read_distinct(const int32_t group_by_col)
{
  UNUSED(group_by_col);
  int ret = OB_SUCCESS;
  common::ObDatum *datums = group_by_cell_->get_group_by_col_datums_to_fill();
  if (OB_FAIL(datums[0].from_storage_datum(default_row_.storage_datums_[0], datum_infos_.at(0).get_obj_datum_map()))) {
    LOG_WARN("Failed to from storage datum", K(ret));
  } else {
    group_by_cell_->set_distinct_cnt(1);
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), KPC(group_by_cell_));
  return ret;
}

int ObDefaultCGGroupByScanner::read_reference(const int32_t group_by_col)
{
  UNUSED(group_by_col);
  int ret = OB_SUCCESS;
  if (0 == query_range_valid_row_count_) {
    ret = OB_ITER_END;
  } else {
    const int64_t read_cnt = MIN(group_by_cell_->get_batch_size(), query_range_valid_row_count_);
    uint32_t *ref_buf = group_by_cell_->get_refs_buf();
    MEMSET(ref_buf, 0, sizeof(uint32_t) * read_cnt);
    group_by_cell_->set_ref_cnt(read_cnt);
    query_range_valid_row_count_ -= read_cnt;
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), KPC(group_by_cell_));
  return ret;
}

int ObDefaultCGGroupByScanner::calc_aggregate(const bool is_group_by_col)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < datum_infos_.count(); ++i) {
    ObGroupByAggIdxArray &agg_idxs = group_by_agg_idxs_.at(i);
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_idxs.count(); ++i) {
      const int32_t agg_idx = agg_idxs.at(i);
      if (OB_FAIL(group_by_cell_->eval_batch(default_row_.storage_datums_, group_by_cell_->get_ref_cnt(), agg_idx, is_group_by_col, true))) {
        LOG_WARN("Failed to eval batch", K(ret));
      }
    }
  }
  return ret;
}

}
}
