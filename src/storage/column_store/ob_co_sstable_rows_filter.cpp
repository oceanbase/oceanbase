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
#include "ob_cg_scanner.h"
#include "ob_cg_tile_scanner.h"
#include "ob_co_sstable_rows_filter.h"
#include "ob_column_oriented_sstable.h"
#include "ob_co_where_optimizer.h"
#include "storage/access/ob_block_row_store.h"
#include "storage/access/ob_table_access_context.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
namespace storage
{

ObCOSSTableRowsFilter::ObCOSSTableRowsFilter()
  : is_inited_(false),
    prepared_(false),
    subtree_filter_iter_to_locate_(0),
    subtree_filter_iter_to_filter_(0),
    batch_size_(1),
    iter_param_(nullptr),
    access_ctx_(nullptr),
    co_sstable_(nullptr),
    allocator_(nullptr),
    filter_(nullptr),
    filter_iters_(),
    iter_filter_node_(),
    bitmap_buffer_(),
    pd_filter_info_(),
    can_continuous_filter_(true)
{
}

ObCOSSTableRowsFilter::~ObCOSSTableRowsFilter()
{
  reset();
}

int ObCOSSTableRowsFilter::init(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    ObITable *table)
{
  int ret = OB_SUCCESS;
  access_ctx_ = &context;
  ObBlockRowStore *block_row_store = context.block_row_store_;
  uint32_t depth = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCOSSTableRowsFilter init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == block_row_store || !block_row_store->is_valid() ||
                         nullptr == context.stmt_allocator_ || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(block_row_store), KP(context.stmt_allocator_), K(param));
  } else if (OB_FAIL(init_co_sstable(table))) {
    LOG_WARN("Failed to init co_sstable", K(ret), KP(table));
  } else if (OB_FAIL(pd_filter_info_.init(param, *context.stmt_allocator_))) {
    LOG_WARN("Fail to init pd filter info", K(ret));
  } else {
    iter_param_ = &param;
    allocator_ = context.stmt_allocator_;
    batch_size_ = param.get_storage_rowsets_size();
    if (nullptr != param.pushdown_filter_ && OB_FAIL(rewrite_filter(depth))) {
      LOG_WARN("Failed rewriter filter", K(ret), KPC(block_row_store), KPC_(filter));
    } else if (nullptr != context.sample_filter_
                && OB_FAIL(context.sample_filter_->combine_to_filter_tree(filter_))) {
      LOG_WARN("Failed to combine sample filter to filter tree", K(ret), KP_(context.sample_filter), KP_(filter));
    } else if (FALSE_IT(depth = nullptr == context.sample_filter_ ? depth : depth + 1)) {
    } else if (OB_FAIL(init_bitmap_buffer(depth))) {
      LOG_WARN("Failed to init bitmap buffer", K(ret), K(depth));
    } else if (OB_FAIL(filter_tree_can_continuous_filter(filter_, can_continuous_filter_))) {
      LOG_WARN("failed to filter_tree_can_continuous_filter", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::rewrite_filter(uint32_t &depth)
{
  int ret = OB_SUCCESS;
  // only rewrite filter tree without sample filter
  if (OB_ISNULL(filter_ = pd_filter_info_.filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null pd filter", K(ret), K_(pd_filter_info));
  } else if (!filter_->is_filter_rewrited()) {
    // There is no need to rewrite filter again when refresh table in ObMultipleMerge
    // or retry scanning in DAS.
    // TODO: reorder pushdown filter by filter ratio, io cost, runtime filter(runtime filter
    // should keep last), etc.
    ObCOWhereOptimizer where_optimizer(*co_sstable_, *filter_);
    if (iter_param_->enable_pd_filter_reorder() && OB_FAIL(where_optimizer.analyze())) {
      LOG_WARN("Failed to analyze in where optimzier", K(ret));
    } else if (OB_FAIL(judge_whether_use_common_cg_iter(filter_))) {
      LOG_WARN("Failed to judge where use common column group iterator", K(ret), KPC_(filter));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rewrite_filter_tree(filter_, depth))) {
      LOG_WARN("Failed to rewrite filter", K(ret), KPC_(filter));
    } else if (OB_UNLIKELY(depth < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected depth", K(ret), K(depth), KPC_(filter));
    } else {
      filter_->set_filter_rewrited();
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::switch_context(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    ObITable *table,
    const bool col_cnt_changed)
{
  int ret = OB_SUCCESS;
  ObBlockRowStore *block_row_store = context.block_row_store_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObCOSSTableRowsFilter is not inited");
  } else if (OB_UNLIKELY(nullptr == block_row_store || !block_row_store->is_valid() ||
                         nullptr == context.stmt_allocator_ || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(block_row_store), KP(context.stmt_allocator_), K(param));
  } else if (OB_FAIL(init_co_sstable(table))) {
    LOG_WARN("Failed to init co_sstable", K(ret), KP(table));
  } else if (iter_filter_node_.count() != filter_iters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(iter_filter_node_.count()), K(filter_iters_.count()));
  } else {
    iter_param_ = &param;
    access_ctx_ = &context;
    batch_size_ = param.get_storage_rowsets_size();
    common::ObSEArray<ObTableIterParam*, 8> iter_params;
    for (int64_t i = 0; i < iter_filter_node_.count(); i++) {
      sql::ObPushdownFilterExecutor *filter = iter_filter_node_.at(i);
      ObICGIterator *&cg_iter = filter_iters_.at(i);
      iter_params.reuse();
      if (OB_FAIL(construct_cg_iter_params(filter, iter_params))) {
        LOG_WARN("Failed to construct cg scan param", K(ret));
      } else if (OB_FAIL(switch_context_for_cg_iter(false, false, true, co_sstable_, context, iter_params, col_cnt_changed, cg_iter))) {
        LOG_WARN("Fail to switch context for cg iter", K(ret));
      } else if (ObICGIterator::OB_CG_SCANNER == cg_iter->get_type() &&
                 param.enable_skip_index() &&
                 OB_FAIL(reinterpret_cast<ObCGScanner *>(cg_iter)->build_index_filter(*filter))) {
        LOG_WARN("Failed to construct skip filter", K(ret), KPC(filter));
      }
    }
  }
  return ret;
}

void ObCOSSTableRowsFilter::reset()
{
  is_inited_ = false;
  prepared_ = false;
  subtree_filter_iter_to_locate_ = 0;
  subtree_filter_iter_to_filter_ = 0;
  batch_size_ = 1;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  co_sstable_ = nullptr;
  clear_filter_state(filter_);
  filter_ = nullptr;
  for (int64_t i = 0; i < filter_iters_.count(); ++i) {
    ObICGIterator* cg_iter = filter_iters_[i];
    cg_iter->~ObICGIterator();
    allocator_->free(cg_iter);
  }
  filter_iters_.reset();
  iter_filter_node_.reset();
  for (int64_t i = 0; i < bitmap_buffer_.count(); ++i) {
    ObCGBitmap* bitmap = bitmap_buffer_[i];
    bitmap->~ObCGBitmap();
    allocator_->free(bitmap);
  }
  bitmap_buffer_.reset();
  pd_filter_info_.reset();
  allocator_ = nullptr;
}

void ObCOSSTableRowsFilter::reuse()
{
  for (int64_t i = 0; i < filter_iters_.count(); ++i) {
    filter_iters_[i]->reuse();
  }
  prepared_ = false;
  subtree_filter_iter_to_locate_ = 0;
  subtree_filter_iter_to_filter_ = 0;
  co_sstable_ = nullptr;
}

int ObCOSSTableRowsFilter::apply(const ObCSRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOSSTableRowsFilter is not inited", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(range));
  } else if (!prepared_ && OB_FAIL(prepare_apply(range))) {
    LOG_WARN("Failed to prepare apply filter", K(range), KPC(this));
  } else if (OB_FAIL(apply_filter(nullptr, filter_, range, 0))) {
    LOG_WARN("Failed to apply filter", K(ret), K(range), KPC_(filter));
  } else {
    adjust_batch_size();
    prepared_ = false;
    LOG_DEBUG("[COLUMNSTORE] apply filter info", K(range),
              K(bitmap_buffer_[0]->size()), K(bitmap_buffer_[0]->popcnt()));
  }
  return ret;
}

int ObCOSSTableRowsFilter::prepare_apply(const ObCSRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOSSTableRowsFilter is not inited", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(range));
  } else if (FALSE_IT(subtree_filter_iter_to_locate_ = 0)) {
  } else if (FALSE_IT(subtree_filter_iter_to_filter_ = 0)) {
  } else if (OB_FAIL(try_locating_cg_iter(0, range))) {
    LOG_WARN("Failed to try locating cg iter", K(ret), K(range));
  } else {
    prepared_ = true;
  }
  return ret;
}

const ObCGBitmap* ObCOSSTableRowsFilter::get_result_bitmap()
{
  const ObCGBitmap* result_bitmap = nullptr;
  if (OB_LIKELY(is_inited_ && nullptr != filter_
                 && !bitmap_buffer_.empty())) {
    result_bitmap = bitmap_buffer_[0];
  }
  return result_bitmap;
}

int ObCOSSTableRowsFilter::apply_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter,
    const ObCSRange &range,
    const uint32_t depth)
{
  int ret = OB_SUCCESS;
  ObCGBitmap *result = nullptr;
  const int64_t iter_idx = filter->get_cg_iter_idx();
  if (OB_FAIL(prepare_bitmap_buffer(range, depth, parent, *filter, result))) {
    LOG_WARN("Failed to prepare bitmap buffer", K(ret), K(range), K(depth), KP(filter));
    // Parent prepare_skip_filter can not be called here.
  } else if (filter->is_sample_node()) {
    ObSampleFilterExecutor *sample_executor = static_cast<ObSampleFilterExecutor *>(filter);
    if (OB_FAIL(sample_executor->apply_sample_filter(range, *result->get_inner_bitmap()))) {
      LOG_WARN("Failed to apply sample filter", K(ret), K(range), KP(result), KPC(sample_executor));
    }
  } else if (sql::ObPushdownFilterExecutor::INVALID_CG_ITER_IDX != iter_idx) {
    ObICGIterator *cg_iter = filter_iters_.at(iter_idx);
    // Call try_locating_cg_iter before pushdown_filter
    // to avoid skip filter because of pruning when apply_filter.
    pd_filter_info_.reuse();
    pd_filter_info_.filter_ = filter;
    if (OB_FAIL(try_locating_cg_iter(iter_idx, range))) {
      LOG_WARN("Failed to locate", K(ret), K(range), K(iter_idx));
    } else if (OB_UNLIKELY(iter_idx >= subtree_filter_iter_to_locate_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Should locate befor pushdown filter", K(ret), K(iter_idx),
               K_(subtree_filter_iter_to_locate));
    } else if (OB_FAIL(cg_iter->apply_filter(
                parent,
                pd_filter_info_,
                range.get_row_count(),
                depth == 0 ? nullptr : bitmap_buffer_[depth - 1],
                *result))) {
      LOG_WARN("Failed to pushdown filter", K(ret), KP(parent), KP(filter));
    } else {
      subtree_filter_iter_to_filter_ = iter_idx + 1;
    }
  } else if (filter->is_logic_op_node()) {
    if (OB_UNLIKELY(filter->get_child_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected child count of filter executor", K(ret),
               K(filter->get_child_count()), KP(filter));
    } else {
      sql::ObPushdownFilterExecutor **children = filter->get_childs();
      bool is_skip = false;
      for (uint32_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); ++i) {
        const ObCGBitmap *child_result = nullptr;
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(apply_filter(filter, children[i], range, depth + 1))) {
          LOG_WARN("Failed to apply filter", K(ret), K(i), KP(children[i]));
        } else if (OB_ISNULL(child_result = get_child_bitmap(depth))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected get null filter bitmap", K(ret));
        } else if (OB_FAIL(post_apply_filter(*filter, *result, *child_result,
                                             is_skip))) {
          LOG_WARN("Failed to post apply filter", K(ret), KP(result),
                   KP(child_result));
        } else if (!is_skip) {
          if (OB_FAIL(try_locating_cg_iter(subtree_filter_iter_to_filter_, range))) {
            LOG_WARN("Failed to locate", K(ret), K(range), K_(subtree_filter_iter_to_filter));
          }
        } else {
          break;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected physical filter node whose iter_idx is invalid", K(ret), KPC(filter));
  }
  return ret;
}

int ObCOSSTableRowsFilter::post_apply_filter(
    sql::ObPushdownFilterExecutor &filter,
    ObCGBitmap &result,
    const ObCGBitmap &child_result,
    bool &is_skip)
{
  int ret = OB_SUCCESS;
  is_skip = false;
  if (filter.is_logic_and_node()) {
    if (OB_FAIL(result.bit_and(child_result))) {
      LOG_WARN("Failed to merge result bitmap", K(ret));
    } else if (result.is_all_false()) {
      is_skip = true;
    }
  } else  {
    if (OB_FAIL(result.bit_or(child_result))) {
      LOG_WARN("Failed to merge result bitmap", K(ret));
    } else if (result.is_all_true()) {
      is_skip = true;
    }
  }
  LOG_DEBUG("[COLUMNSTORE] post apply filter info", "is_and", filter.is_logic_and_node(), K(result.size()),
            K(result.popcnt()), K(child_result.size()), K(child_result.popcnt()));
  return ret;
}

int ObCOSSTableRowsFilter::rewrite_filter_tree(
    sql::ObPushdownFilterExecutor *filter,
    uint32_t &depth)
{
  int ret = OB_SUCCESS;
  depth = 1;
  if (filter->is_filter_node()) {
    if (OB_FAIL(push_cg_iter(filter))) {
      LOG_WARN("Failed to produce new coulmn group iterator",
                K(ret), KPC(filter));
    }
  } else if (filter->is_logic_op_node()) {
    if (OB_UNLIKELY(filter->get_child_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected number of child in filter executor", K(ret),
                K(filter->get_child_count()), KP(filter));
    } else if (!filter->get_cg_idxs().empty()) {
      if (OB_FAIL(push_cg_iter(filter))) {
        LOG_WARN("Failed to produce new coulmn group iterator",
                  K(ret), KPC(filter));
      }
    } else {
      sql::ObPushdownFilterExecutor **children = filter->get_childs();
      uint32_t max_sub_tree_depth = 0;
      uint32_t sub_tree_depth = 0;
      for (uint32_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); ++i) {
        if (OB_FAIL(rewrite_filter_tree(children[i], sub_tree_depth))) {
          LOG_WARN("Failed to rewrite filter", K(ret), K(i), KPC(children[i]));
        } else {
          max_sub_tree_depth = MAX(max_sub_tree_depth, sub_tree_depth);
        }
      }
      if (OB_SUCC(ret)) {
        depth = max_sub_tree_depth + 1;
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unspported executor type", K(ret), K(filter->get_type()));
  }
  return ret;
}

int ObCOSSTableRowsFilter::push_cg_iter(
    sql::ObPushdownFilterExecutor *filter)
{
  int ret = OB_SUCCESS;
  ObICGIterator *cg_iter = nullptr;
  if (OB_UNLIKELY(nullptr == co_sstable_ || nullptr == filter || !filter->is_cg_param_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid co_sstable", K(ret), KP_(co_sstable), KPC(filter));
  } else {
    common::ObSEArray<ObTableIterParam*, 8> iter_params;
    if (OB_FAIL(construct_cg_iter_params(filter, iter_params))) {
      LOG_WARN("Failed to construct cg scan param", K(ret));
    } else if (1 == iter_params.count()) {
      ObICGIterator *cg_scanner = nullptr;
      if (OB_FAIL(co_sstable_->cg_scan(*iter_params.at(0), *access_ctx_, cg_scanner, false, false))) {
        LOG_WARN("Failed to cg scan", K(ret));
      } else if (ObICGIterator::OB_CG_SCANNER == cg_scanner->get_type() &&
                 iter_params.at(0)->enable_skip_index() &&
                 OB_FAIL(reinterpret_cast<ObCGScanner *>(cg_scanner)->build_index_filter(*filter))) {
        LOG_WARN("Failed to construct skip filter", K(ret), KPC(filter));
      } else {
        cg_iter = cg_scanner;
      }
    } else if (OB_ISNULL(cg_iter = OB_NEWx(ObCGTileScanner, access_ctx_->stmt_allocator_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc cg tile scanner", K(ret));
    } else if (OB_FAIL(static_cast<ObCGTileScanner*>(cg_iter)->init(iter_params, false, true, *access_ctx_, co_sstable_))) {
      LOG_WARN("Fail to init cg tile scanner", K(ret), K(iter_params));
    }
    LOG_DEBUG("[COLUMNSTORE] init one cg iter", K(ret), KPC(cg_iter), K(iter_params));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(filter_iters_.push_back(cg_iter))) {
    LOG_WARN("Failed to push cg iter", K(ret), K(filter_iters_), KP(cg_iter));
  } else if (OB_FAIL(iter_filter_node_.push_back(filter))) {
    LOG_WARN("Failed to push filter node", K(ret));
  } else {
    filter->set_cg_iter_idx(filter_iters_.count() - 1);
  }
  if (OB_FAIL(ret) && nullptr != cg_iter) {
    cg_iter->~ObICGIterator();
    allocator_->free(cg_iter);
    cg_iter = nullptr;
  }
  return ret;
}

int ObCOSSTableRowsFilter::construct_cg_iter_params(
    const sql::ObPushdownFilterExecutor *filter,
    common::ObIArray<ObTableIterParam*> &iter_params)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<uint32_t> &col_group_idxs = filter->get_cg_idxs();
  const common::ObIArray<sql::ObExpr *> *cg_col_exprs = filter->get_cg_col_exprs();
  ObTableIterParam* cg_param = nullptr;
  int64_t col_expr_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_group_idxs.count(); i++) {
    const uint32_t cg_idx = col_group_idxs.at(i);
    if (OB_UNLIKELY(!is_virtual_cg(cg_idx) && nullptr == cg_col_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected cg col expr", K(ret), K(cg_idx));
    } else if (OB_FAIL(access_ctx_->cg_param_pool_->get_iter_param(cg_idx, *iter_param_,
        is_virtual_cg(cg_idx) ? nullptr : cg_col_exprs->at(col_expr_idx++), cg_param))) {
      LOG_WARN("Fail to get iter param", K(ret), K(cg_idx), KPC(iter_param_));
    } else if (OB_FAIL(iter_params.push_back(cg_param))) {
      LOG_WARN("Fail to push back iter param", K(ret), K(cg_param));
    }
    LOG_DEBUG("[COLUMNSTORE] cons one cg param", K(ret), K(cg_idx), K(cg_param));
  }
  return ret;
}

int ObCOSSTableRowsFilter::judge_whether_use_common_cg_iter(
    sql::ObPushdownFilterExecutor *filter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == filter
                  || sql::ObPushdownFilterExecutor::INVALID_CG_ITER_IDX != filter->get_cg_iter_idx())) {
    ret = OB_ERR_UNEXPECTED;
    const int64_t cg_iter_idx = filter ? filter->get_cg_iter_idx() : sql::ObPushdownFilterExecutor::INVALID_CG_ITER_IDX;
    LOG_WARN("Unexpected filter", K(ret), KP(filter), K(cg_iter_idx));
  } else if (filter->is_filter_node()) {
    if (OB_UNLIKELY(!filter->is_cg_param_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected physical filter node", K(ret), KPC(filter));
    } else {
      set_status_of_filter_tree(filter);
    }
  } else if (filter->is_logic_op_node()) {
    sql::ObCommonFilterTreeStatus status = sql::ObCommonFilterTreeStatus::NONE_FILTER;
    if (!filter->get_cg_idxs().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected not empty col_group_idxs", K(ret), KPC(filter));
    } else if (OB_UNLIKELY(filter->get_child_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected number of child in filter executor",
                K(ret), K(filter->get_child_count()), KPC(filter));
    } else {
      sql::ObPushdownFilterExecutor **children = filter->get_childs();
      for (uint32_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); ++i) {
        if (OB_FAIL(judge_whether_use_common_cg_iter(children[i]))) {
          LOG_WARN("Failed to judge where use common column group iter",
                   K(ret), KPC(filter));
        } else if (0 == i) {
          status = children[i]->get_status();
        } else {
          status = (sql::ObCommonFilterTreeStatus)(filter_tree_merge_status[status][children[i]->get_status()]);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(transform_filter_tree(*filter))) {
          LOG_WARN("Failed to transform filter tree", K(ret), KPC(filter));
        } else if (filter->is_cg_param_valid()) {
          filter->set_status(status);
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unspported executor type", K(ret), K(filter->get_type()));
  }
  return ret;
}

int ObCOSSTableRowsFilter::transform_filter_tree(
    sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint32_t, 4> tmp_filter_indexes;
  common::ObSEArray<uint32_t, 4> common_cg_ids;
  common::ObSEArray<sql::ObExpr*, 4> common_cg_exprs;
  int64_t base_filter_idx = 0;
  while (OB_SUCC(ret)) {
    sql::ObPushdownFilterExecutor *common_filter_executor = nullptr;
    if (OB_FAIL(find_common_sub_filter_tree(filter,
                                            tmp_filter_indexes,
                                            common_cg_ids,
                                            common_cg_exprs,
                                            base_filter_idx))) {
      LOG_WARN("Failed to find common sub filter tree", K(ret), K(base_filter_idx));
    } else if (1 < tmp_filter_indexes.count()) {
      if (filter.get_child_count() == tmp_filter_indexes.count()) {
        common_filter_executor = &filter;
      } else if (OB_FAIL(filter.pull_up_common_node(tmp_filter_indexes, common_filter_executor))) {
        LOG_WARN("Failed to pull up common node", K(ret), K(tmp_filter_indexes));
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(common_filter_executor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null common_filter_executor", K(ret));
        } else if (OB_FAIL(common_filter_executor->set_cg_param(common_cg_ids, common_cg_exprs))) {
          LOG_WARN("Failed to set cg param to filter", K(ret), KPC(common_filter_executor),
                   K(common_cg_ids), K(common_cg_exprs));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++base_filter_idx;
      if (common_filter_executor == &filter || base_filter_idx >= filter.get_child_count()) {
        break;
      }
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::find_common_sub_filter_tree(
    sql::ObPushdownFilterExecutor &filter,
    ObIArray<uint32_t> &filter_indexes,
    common::ObIArray<uint32_t> &common_cg_ids,
    common::ObIArray<sql::ObExpr *> &common_cg_exprs,
    const int64_t base_filter_idx)
{
  int ret = OB_SUCCESS;
  filter_indexes.reuse();
  common_cg_ids.reuse();
  common_cg_exprs.reuse();
  sql::ObPushdownFilterExecutor **children_filters = filter.get_childs();
  sql::ObPushdownFilterExecutor *base_filter = children_filters[base_filter_idx];
  sql::ObCommonFilterTreeStatus prev_status = base_filter->get_status();
  const common::ObIArray<uint32_t> &base_cg_ids = base_filter->get_cg_idxs();
  const common::ObIArray<sql::ObExpr *> *base_cg_exprs = base_filter->get_cg_col_exprs();
  const uint32_t child_count = filter.get_child_count();
  if (OB_FAIL(common_cg_ids.assign(base_cg_ids))) {
    LOG_WARN("Failed to assign common cg ids", K(ret));
  } else if (nullptr != base_cg_exprs && OB_FAIL(common_cg_exprs.assign(*base_cg_exprs))) {
    LOG_WARN("Failed to assign common cg exprs", K(ret));
  } else if (OB_FAIL(filter_indexes.push_back(base_filter_idx))) {
    LOG_WARN("Failed to push back filters", K(ret), K(base_filter_idx), K(filter_indexes));
  } else {
    bool is_common = false;
    for (uint32_t i = base_filter_idx + 1; OB_SUCC(ret) && i < child_count; ++i) {
      if (is_common_filter_tree_status(prev_status, children_filters[i]->get_status())) {
        if (OB_FAIL(assign_common_col_groups(
                    children_filters[i],
                    prev_status,
                    common_cg_ids,
                    common_cg_exprs,
                    is_common))) {
          LOG_WARN("Failed to assign common col groups", K(ret), KPC(base_filter), KPC(children_filters[i]));
        } else if (is_common) {
          prev_status = merge_common_filter_tree_status(prev_status, children_filters[i]->get_status());
          if (OB_FAIL(filter_indexes.push_back(i))) {
            LOG_WARN("Failed to push back filter executor", K(ret), K(filter_indexes));
          }
        }
      }
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::init_co_sstable(ObITable *table)
{
  int ret = OB_SUCCESS;
  if (nullptr == table || !table->is_co_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invaild ObITable arguments", K(ret), KP(table));
  } else {
    co_sstable_ = static_cast<ObCOSSTableV2*>(table);
  }
  return ret;
}

int ObCOSSTableRowsFilter::try_locating_cg_iter(
    const int64_t iter_idx_to_filter_next,
    const ObCSRange &range)
{
  int ret = OB_SUCCESS;
  if (subtree_filter_iter_to_locate_ < iter_idx_to_filter_next) {
    subtree_filter_iter_to_locate_ = iter_idx_to_filter_next;
  }
  int64_t max_cg_iter_to_locate = MIN(static_cast<int64_t>(iter_idx_to_filter_next
                                                           + MAX_NUM_OF_CG_ITER_TO_LOCATE_IN_ADVANCE),
                                      filter_iters_.count());
  for (; OB_SUCC(ret) && subtree_filter_iter_to_locate_ < max_cg_iter_to_locate; ++subtree_filter_iter_to_locate_) {
    if (OB_FAIL(filter_iters_[subtree_filter_iter_to_locate_]->locate(range))) {
      LOG_WARN("Failed to locate cg iter", K(ret), K(range), K_(subtree_filter_iter_to_locate),
               KP(filter_iters_[subtree_filter_iter_to_locate_]));
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::init_bitmap_buffer(uint32_t bitmap_buffer_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(bitmap_buffer_.reserve(bitmap_buffer_count))) {
    LOG_WARN("Failed to reserve", K(ret), K(bitmap_buffer_count));
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < bitmap_buffer_count; ++i) {
      ObCGBitmap* filter_bitmap = nullptr;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObCGBitmap)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for filter bitmap", K(ret));
      } else if (FALSE_IT(filter_bitmap = new (buf) ObCGBitmap(*allocator_))) {
      } else if (OB_FAIL(filter_bitmap->init(batch_size_))) {
        LOG_WARN("Failed to init bitmap", K(ret), K_(batch_size));
      } else if (OB_FAIL(bitmap_buffer_.push_back(filter_bitmap))) {
        LOG_WARN("Failed to push_back filter bitmap", K(ret), KP(filter_bitmap));
      }
      if (OB_FAIL(ret) && nullptr != filter_bitmap) {
        filter_bitmap->~ObCGBitmap();
        allocator_->free(filter_bitmap);
      }
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::prepare_bitmap_buffer(
    const ObCSRange &range,
    const uint32_t depth,
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor &filter,
    ObCGBitmap *&result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(depth >= bitmap_buffer_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected depth", K(ret), K(depth), K(bitmap_buffer_.count()));
  } else {
    result = bitmap_buffer_[depth];
    if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected nullptr filter_bitmap", K(ret), K(depth));
    } else if (OB_FAIL(result->reserve(range.get_row_count()))) {
      LOG_WARN("Failed to expand size for filter bitmap", K(ret), K(range));
    } else if (0 == depth) {
      result->reuse(range.start_row_id_, filter.is_logic_and_node());
    } else {
      const ObCGBitmap *parent_bitmap = bitmap_buffer_[depth - 1];
      if (OB_UNLIKELY(nullptr == parent || nullptr == parent_bitmap)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected filter node or bitmap", K(ret), KP(parent), KP(parent_bitmap));
      } else if (filter.get_type() == parent->get_type()) {
        if (OB_FAIL(result->copy_from(*parent_bitmap))) {
          LOG_WARN("Fail to copy bitmap", K(ret), KPC(parent_bitmap));
        }
      } else {
        result->reuse(range.start_row_id_, filter.is_logic_and_node());
      }
    }
  }
  return ret;
}

int ObCOSSTableRowsFilter::assign_common_col_groups(
    const sql::ObPushdownFilterExecutor *filter,
    const sql::ObCommonFilterTreeStatus prev_status,
    common::ObIArray<uint32_t> &common_cg_ids,
    common::ObIArray<sql::ObExpr *> &common_cg_exprs,
    bool &is_common)
{
  // Asssert that there is no duplication in col_group_idxs.
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == filter || !filter->is_cg_param_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(filter));
  } else {
    is_common = false;
    const common::ObIArray<uint32_t> &cur_cg_ids = filter->get_cg_idxs();
    const common::ObIArray<sql::ObExpr *> *cur_cg_exprs = filter->get_cg_col_exprs();
    ObSEArray<uint32_t, 4> tmp_array;
    const common::ObIArray<uint32_t> &longer_array = cur_cg_ids.count() > common_cg_ids.count()
        ? cur_cg_ids : common_cg_ids;
    const common::ObIArray<uint32_t> &shorter_array = cur_cg_ids.count() > common_cg_ids.count()
        ? common_cg_ids : cur_cg_ids;
    if (OB_FAIL(common::get_difference(shorter_array, longer_array, tmp_array))) {
      LOG_WARN("Failed to get difference", K(ret), K(common_cg_ids), K(cur_cg_ids));
    } else if (tmp_array.empty()) {
      is_common = true;
      if (cur_cg_ids.count() > common_cg_ids.count()) {
        if (OB_FAIL(common_cg_ids.assign(cur_cg_ids))) {
          LOG_WARN("Fail to assign cg ids", K(ret));
        } else if (nullptr != cur_cg_exprs && OB_FAIL(common_cg_exprs.assign(*cur_cg_exprs))) {
          LOG_WARN("Fail to assign cg exprs", K(ret));
        }
      }
    } else if (prev_status > sql::ObCommonFilterTreeStatus::WHITE &&
               filter->get_status() > sql::ObCommonFilterTreeStatus::WHITE &&
               tmp_array.count() < shorter_array.count()) {
      is_common = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_cg_ids.count(); i++) {
        if (!is_contain(common_cg_ids, cur_cg_ids.at(i))) {
          if (OB_FAIL(common_cg_ids.push_back(cur_cg_ids.at(i)))) {
            LOG_WARN("Fail to push back cg idx", K(ret));
          } else if (nullptr != cur_cg_exprs && OB_FAIL(common_cg_exprs.push_back(cur_cg_exprs->at(i)))) {
            LOG_WARN("Fail to push back cg expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

void ObCOSSTableRowsFilter::adjust_batch_size()
{
  // TODO(hanling): Optimize in the future.
}

OB_INLINE ObCGBitmap* ObCOSSTableRowsFilter::get_child_bitmap(uint32_t depth)
{
  ObCGBitmap *child_bitmap = nullptr;
  if (OB_LIKELY(depth + 1 < bitmap_buffer_.count())) {
    child_bitmap = bitmap_buffer_[depth + 1];
  }
  return child_bitmap;
}

sql::ObCommonFilterTreeStatus ObCOSSTableRowsFilter::merge_common_filter_tree_status(
    sql::ObCommonFilterTreeStatus status_one,
    sql::ObCommonFilterTreeStatus status_two)
{
  sql::ObCommonFilterTreeStatus ret = sql::ObCommonFilterTreeStatus::NONE_FILTER;
  if (is_common_filter_tree_status(status_one, status_two)) {
    ret = (sql::ObCommonFilterTreeStatus)(filter_tree_merge_status[status_one][status_two]);
  }
  return ret;
}

/*
 *   filter1        filter2       is_common  status
 *     NOP            NOP            O       NONE_FILTER
 *    WHITE          WHITE           X       WHITE
 *    WHITE        SINGLE_BLACK      O       NONE_FILTER
 *    WHITE        MULTI_BLACK       O      NONE_FILTER
 *  SINGLE_BLACK   SINGLE_BLACK      X      SINGLE_BLACK
 *  SINGLE_BLACK   MULTI_BLACK       X      MULTI_BLACK
 *  MULTI_BLACK    MULTI_BLACK       X      MULTI_BLACK
 *
 *  merge condition: cg idxs is contained by the other
 */
bool ObCOSSTableRowsFilter::is_common_filter_tree_status(
    const sql::ObCommonFilterTreeStatus status_one,
    const sql::ObCommonFilterTreeStatus status_two)
{
  return (sql::ObCommonFilterTreeStatus)(filter_tree_merge_status[status_one][status_two]) > 0;
}

void ObCOSSTableRowsFilter::set_status_of_filter_tree(
    sql::ObPushdownFilterExecutor *filter)
{
  // Caller ensure that filter is not unexpected.
  if (filter->is_filter_white_node()) {
    filter->set_status(sql::ObCommonFilterTreeStatus::WHITE);
  } else if (filter->get_cg_idxs().count() == 1) {
    filter->set_status(sql::ObCommonFilterTreeStatus::SINGLE_BLACK);
  } else {
    filter->set_status(sql::ObCommonFilterTreeStatus::MULTI_BLACK);
  }
}

void ObCOSSTableRowsFilter::clear_filter_state(sql::ObPushdownFilterExecutor *filter)
{
  if (filter) {
    filter->set_cg_iter_idx(sql::ObPushdownFilterExecutor::INVALID_CG_ITER_IDX);
    if (filter->is_logic_op_node()) {
      sql::ObPushdownFilterExecutor **children = filter->get_childs();
      for (uint32_t i = 0; i < filter->get_child_count(); i++) {
        clear_filter_state(children[i]);
      }
   }
  }
}

int ObCOSSTableRowsFilter::switch_context_for_cg_iter(
    const bool is_projector,
    const bool project_single_row,
    const bool without_filter,
    ObCOSSTableV2 *co_sstable,
    ObTableAccessContext &context,
    common::ObIArray<ObTableIterParam*> &cg_params,
    const bool col_cnt_changed,
    ObICGIterator *&cg_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == co_sstable || cg_params.empty() || nullptr == cg_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(co_sstable), K(cg_params.count()), KP(cg_iter));
  } else if (1 == cg_params.count()) {
    storage::ObSSTableWrapper cg_wrapper;
    const ObTableIterParam &cg_param = *cg_params.at(0);
    if (OB_UNLIKELY(!ObICGIterator::is_valid_cg_scanner(cg_iter->get_type()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected iter type", K(ret), "iter_type", cg_iter->get_type(), KPC(cg_iter));
    } else if (OB_UNLIKELY(col_cnt_changed)) {
      if (FALSE_IT(context.cg_iter_pool_->return_cg_iter(cg_iter, cg_iter->get_cg_idx()))) {
      } else if (OB_FAIL(co_sstable->cg_scan(cg_param, context, cg_iter, is_projector, project_single_row))) {
        LOG_WARN("Failed to cg scan", K(ret));
      }
    } else if (!is_virtual_cg(cg_param.cg_idx_) && OB_FAIL(co_sstable->fetch_cg_sstable(cg_param.cg_idx_, cg_wrapper))) {
      LOG_WARN("Fail to get cg sstable wrapper", K(ret));
    } else if (OB_FAIL(cg_iter->switch_context(cg_param, context, cg_wrapper))) {
      LOG_WARN("Failed to switch context for project iter", K(ret));
    } else if (ObICGIterator::OB_CG_ROW_SCANNER == cg_iter->get_type()) {
      static_cast<ObCGRowScanner *>(cg_iter)->set_project_type(is_projector && without_filter);
    }
  } else if (cg_iter->get_type() != ObICGIterator::OB_CG_TILE_SCANNER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected iter type", K(ret), KPC(cg_iter));
  } else if (OB_FAIL(static_cast<ObCGTileScanner*>(cg_iter)->switch_context(
      cg_params, project_single_row, is_projector && without_filter, context, co_sstable, col_cnt_changed))) {
    LOG_WARN("Failed to switch context for project iter", K(ret));
  }
  return ret;
}

int ObCOSSTableRowsFilter::filter_tree_can_continuous_filter(sql::ObPushdownFilterExecutor *filter,
                                                             bool &can_continuous_filter) const
{
  int ret = OB_SUCCESS;
  if (nullptr == filter) {
    can_continuous_filter =  true;
  } else if (!filter->filter_can_continuous_filter()) {
    can_continuous_filter = false;
  } else {
    for (int64_t i = 0; i < filter->get_child_count(); ++i) {
      sql::ObPushdownFilterExecutor *child = nullptr;
      (void)filter->get_child(i, child);
      if (OB_FAIL(SMART_CALL(filter_tree_can_continuous_filter(child, can_continuous_filter)))) {
        LOG_WARN("failed to filter_tree_can_continuous_filter");
      } else if (!can_continuous_filter) {
        break;
      }
    }
  }
  return ret;
}

}
}
