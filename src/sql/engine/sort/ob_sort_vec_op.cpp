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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_sort_vec_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/expr/ob_expr_topn_filter.h"

namespace oceanbase
{
namespace sql
{
ObSortVecSpec::ObSortVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type) :
  ObOpSpec(alloc, type), topn_expr_(nullptr), topk_limit_expr_(nullptr), topk_offset_expr_(nullptr),
  sk_exprs_(alloc), addon_exprs_(alloc), sk_collations_(alloc), addon_collations_(alloc),
  minimum_row_count_(0), topk_precision_(0), prefix_pos_(0), is_local_merge_sort_(false),
  is_fetch_with_ties_(false), prescan_enabled_(false), enable_encode_sortkey_opt_(false),
  has_addon_(false), part_cnt_(0), pd_topn_filter_info_(alloc)
{}

OB_SERIALIZE_MEMBER((ObSortVecSpec, ObOpSpec), topn_expr_, topk_limit_expr_, topk_offset_expr_,
                    sk_exprs_, addon_exprs_, sk_collations_, addon_collations_, minimum_row_count_,
                    topk_precision_, prefix_pos_, is_local_merge_sort_, is_fetch_with_ties_,
                    prescan_enabled_, enable_encode_sortkey_opt_, has_addon_, part_cnt_, compress_type_,
                    pd_topn_filter_info_);

ObSortVecOp::ObSortVecOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input) :
  ObOperator(ctx_, spec, input), sort_op_provider_(op_monitor_info_), sort_row_count_(0),
  is_first_(true), ret_row_count_(0), sk_row_store_(), addon_row_store_(), sk_row_iter_(),
  addon_row_iter_()
{}

int ObSortVecOp::inner_rescan()
{
  if (MY_SPEC.enable_pd_topn_filter() && !MY_SPEC.pd_topn_filter_info_.is_shuffle_) {
    // for local topn runtime filter, rescan topn filter expression context
    reset_pd_topn_filter_expr_ctx();
  }
  reset();
  return ObOperator::inner_rescan();
}

void ObSortVecOp::reset_pd_topn_filter_expr_ctx()
{
  uint32_t expr_ctx_id = MY_SPEC.pd_topn_filter_info_.expr_ctx_id_;
  ObExprTopNFilterContext *topn_filter_ctx =
      static_cast<ObExprTopNFilterContext *>(ctx_.get_expr_op_ctx(expr_ctx_id));
  if (nullptr != topn_filter_ctx) {
    topn_filter_ctx->reset_for_rescan();
  }
}

void ObSortVecOp::reset()
{
  sort_row_count_ = 0;
  ret_row_count_ = 0;
  is_first_ = true;
  sk_row_iter_.reset();
  addon_row_iter_.reset();
  sk_row_store_.reset();
  addon_row_store_.reset();
  sort_op_provider_.reset();
}

void ObSortVecOp::destroy()
{
  reset();
  sk_row_store_.~ObTempRowStore();
  addon_row_store_.~ObTempRowStore();
  sort_op_provider_.unregister_profile_if_necessary();
  sort_op_provider_.~ObSortVecOpProvider();
  ObOperator::destroy();
}

int ObSortVecOp::inner_close()
{
  sort_op_provider_.collect_memory_dump_info(op_monitor_info_);
  sort_op_provider_.unregister_profile();
  if (MY_SPEC.enable_pd_topn_filter()) {
    // TODO XUNSI: update plan monitor info of pushdown topn filter
    // but all the others_values of the plan_monitor_node is used,
    // add an extra string in json format is a considered way
  }
  return OB_SUCCESS;
}

int ObSortVecOp::get_int_value(const ObExpr *in_val, int64_t &out_val)
{
  int ret = OB_SUCCESS;
  if (NULL != in_val) {
    int64_t skip_mem = 0;
    const ObBitVector *my_skip = to_bit_vector(reinterpret_cast<void *>(&skip_mem));
    if (OB_FAIL(in_val->eval_vector(eval_ctx_, *my_skip, 1, true))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (in_val->get_vector(eval_ctx_)->is_null(0)) {
      out_val = 0;
    } else {
      out_val = in_val->get_vector(eval_ctx_)->get_int(0);
    }
  }
  return ret;
}

int ObSortVecOp::get_topn_count(int64_t &topn_cnt)
{
  int ret = OB_SUCCESS;
  topn_cnt = INT64_MAX;
  if ((OB_ISNULL(MY_SPEC.topn_expr_) && OB_ISNULL(MY_SPEC.topk_limit_expr_))) {
    // do nothing
  } else if (((NULL != MY_SPEC.topn_expr_) && (NULL != MY_SPEC.topk_limit_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid topn_expr or topk_limit_expr", K(MY_SPEC.topn_expr_),
             K(MY_SPEC.topk_limit_expr_), K(ret));
  } else if (NULL != MY_SPEC.topn_expr_) {
    if (OB_FAIL(get_int_value(MY_SPEC.topn_expr_, topn_cnt))) {
      LOG_WARN("failed to get int value", K(ret), K(MY_SPEC.topn_expr_));
    } else {
      topn_cnt = std::max(MY_SPEC.minimum_row_count_, topn_cnt);
    }
  } else if (NULL != MY_SPEC.topk_limit_expr_) {
    int64_t limit = -1;
    int64_t offset = 0;
    if ((OB_FAIL(get_int_value(MY_SPEC.topk_limit_expr_, limit))
         || OB_FAIL(get_int_value(MY_SPEC.topk_offset_expr_, offset)))) {
      LOG_WARN("Get limit/offset value failed", K(ret));
    } else if (OB_UNLIKELY(limit < 0 || offset < 0)) {
      ret = OB_ERR_ILLEGAL_VALUE;
      LOG_WARN("Invalid limit/offset value", K(limit), K(offset), K(ret));
    } else {
      topn_cnt = std::max(MY_SPEC.minimum_row_count_, limit + offset);
      int64_t row_count = 0;
      ObPhyOperatorType op_type = child_->get_spec().type_;
      if (PHY_HASH_GROUP_BY != op_type && PHY_VEC_HASH_GROUP_BY != op_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid child_op_", K(op_type), K(ret));
      } else if (PHY_HASH_GROUP_BY == op_type) {
        ObHashGroupByOp *hash_groupby_op = static_cast<ObHashGroupByOp *>(child_);
        row_count = hash_groupby_op->get_hash_groupby_row_count();
      } else if (PHY_VEC_HASH_GROUP_BY == op_type) {
        ObHashGroupByVecOp *hash_groupby_op = static_cast<ObHashGroupByVecOp *>(child_);
        row_count = hash_groupby_op->get_hash_groupby_row_count();
      }
      if (OB_SUCC(ret)) {
        topn_cnt =
          std::max(topn_cnt, static_cast<int64_t>(row_count * MY_SPEC.topk_precision_ / 100));
        if (topn_cnt >= row_count) {
          ctx_.get_physical_plan_ctx()->set_is_result_accurate(true);
        } else {
          ctx_.get_physical_plan_ctx()->set_is_result_accurate(false);
        }
      }
    }
  }
  return ret;
}

int ObSortVecOp::process_sort_batch()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.prefix_pos_ > 0) {
    // prefix sort get child row in it's own wrap, do nothing here
  } else {
    bool need_dump = false;
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      const ObBatchRows *input_brs = NULL;
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(child_->get_next_batch(MY_SPEC.max_batch_size_, input_brs))) {
        LOG_WARN("get next batch failed", K(ret));
      } else {
        if (input_brs->size_ > 0) {
          sort_row_count_ +=
            input_brs->size_ - input_brs->skip_->accumulate_bit_cnt(input_brs->size_);
          OZ(sort_op_provider_.add_batch(*input_brs, need_dump));
          sort_op_provider_.collect_memory_dump_info(op_monitor_info_);
        }
        if (input_brs->end_ || (need_dump && MY_SPEC.prescan_enabled_)) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && need_dump && MY_SPEC.prescan_enabled_
        && OB_FAIL(scan_all_then_sort_batch())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to scan all rows before inmem sort", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    OZ(sort_op_provider_.sort());
    sort_op_provider_.collect_memory_dump_info(op_monitor_info_);
  }
  return ret;
}

int ObSortVecOp::init_temp_row_store(const common::ObIArray<ObExpr *> &exprs,
                                     const int64_t batch_size, const ObMemAttr &mem_attr,
                                     const bool is_sort_key, ObCompressorType compress_type,
                                     ObTempRowStore &row_store)
{
  int ret = OB_SUCCESS;
  const bool enable_trunc = true;
  const bool reorder_fixed_expr = true;
  if (row_store.is_inited()) {
    // do nothing
  } else if (OB_FAIL(row_store.init(exprs, batch_size, mem_attr, 2 * 1024 * 1024, true,
                             sort_op_provider_.get_extra_size(is_sort_key) /* row_extra_size */,
                             compress_type, reorder_fixed_expr, enable_trunc))) {
    LOG_WARN("init row store failed", K(ret));
  } else if (OB_FAIL(row_store.alloc_dir_id())) {
    LOG_WARN("failed to alloc dir id", K(ret));
  }
  return ret;
}

int ObSortVecOp::init_prescan_row_store()
{
  int ret = OB_SUCCESS;
  sk_row_store_.reset();
  addon_row_store_.reset();
  ObMemAttr mem_attr(ctx_.get_my_session()->get_effective_tenant_id(), "SORT_VEC_CTX",
                     ObCtxIds::WORK_AREA);
  if (OB_FAIL(init_temp_row_store(MY_SPEC.sk_exprs_, MY_SPEC.max_batch_size_, mem_attr, true,
                                  MY_SPEC.compress_type_, sk_row_store_))) {
    LOG_WARN("failed to init temp row store", K(ret));
  } else if (MY_SPEC.has_addon_
             && OB_FAIL(init_temp_row_store(MY_SPEC.addon_exprs_, MY_SPEC.max_batch_size_, mem_attr,
                                            false, MY_SPEC.compress_type_, addon_row_store_))) {
    LOG_WARN("failed to init temp row store", K(ret));
  }
  return ret;
}

int ObSortVecOp::add_batch_prescan_store(const ObBatchRows &input_brs)
{
  int ret = OB_SUCCESS;
  int64_t sk_row_count = -1;
  int64_t addon_row_count = -1;
  if (OB_FAIL(sk_row_store_.add_batch(MY_SPEC.sk_exprs_, eval_ctx_, input_brs, sk_row_count))) {
    LOG_WARN("failed to add row to cache store", K(ret));
  } else if (MY_SPEC.has_addon_) {
    if (OB_FAIL(addon_row_store_.add_batch(MY_SPEC.addon_exprs_, eval_ctx_, input_brs,
                                           addon_row_count))) {
      LOG_WARN("failed to add row to cache store", K(ret));
    } else if (sk_row_count != addon_row_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort key row count and addon row count not match", K(sk_row_count),
               K(addon_row_count));
    }
  }
  return ret;
}

int ObSortVecOp::finish_add_prescan_store()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sk_row_store_.finish_add_row(false))) {
    LOG_WARN("fail to finish add row", K(ret));
  } else if (OB_FAIL(sk_row_store_.begin(sk_row_iter_))) {
    LOG_WARN("fail to get cache_store iter", K(ret));
  } else if (MY_SPEC.has_addon_) {
    if (OB_FAIL(addon_row_store_.finish_add_row(false))) {
      LOG_WARN("fail to finish add row", K(ret));
    } else if (OB_FAIL(addon_row_store_.begin(addon_row_iter_))) {
      LOG_WARN("fail to get cache_store iter", K(ret));
    }
  }
  return ret;
}

int ObSortVecOp::get_next_batch_prescan_store(const int64_t max_rows, int64_t &read_rows,
                                              const ObCompactRow **sk_stored_rows,
                                              const ObCompactRow **addon_stored_rows)
{
  int ret = OB_SUCCESS;
  int64_t sk_read_rows = -1;
  if (OB_FAIL(sk_row_iter_.get_next_batch(max_rows, sk_read_rows, sk_stored_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get batch row");
    }
  } else if (MY_SPEC.has_addon_) {
    int64_t addon_max_read_rows = sk_read_rows;
    int64_t addon_read_rows = 0;
    while (OB_SUCC(ret) && addon_max_read_rows > 0) {
      addon_read_rows = 0;
      if (OB_FAIL(addon_row_iter_.get_next_batch(addon_max_read_rows, addon_read_rows,
                  addon_stored_rows + (sk_read_rows - addon_max_read_rows)))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get batch row");
        }
      } else {
        addon_max_read_rows -= addon_read_rows;
      }
    }
  }
  if (OB_SUCC(ret)) {
    read_rows = sk_read_rows;
  }
  return ret;
}

int ObSortVecOp::scan_all_then_sort_batch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_prescan_row_store())) {
    LOG_WARN("failed to init prescan row store", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      const ObBatchRows *input_brs = NULL;
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(child_->get_next_batch(MY_SPEC.max_batch_size_, input_brs))) {
        LOG_WARN("get next batch failed", K(ret));
      } else {
        if (input_brs->size_ > 0) {
          sort_row_count_ +=
            input_brs->size_ - input_brs->skip_->accumulate_bit_cnt(input_brs->size_);
          if (OB_FAIL(add_batch_prescan_store(*input_brs))) {
            LOG_WARN("failed to add batch prescan store", K(ret));
          }
        }
        if (input_brs->end_) {
          break;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_add_prescan_store())) {
        LOG_WARN("failed to finish add prescan store", K(ret));
      } else {
        const ObCompactRow *sk_rows[MY_SPEC.max_batch_size_];
        const ObCompactRow *addon_rows[MY_SPEC.max_batch_size_];
        int64_t read_rows = -1;
        if (MY_SPEC.has_addon_) {
          addon_row_iter_.set_blk_holder(&blk_holder_);
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(
                get_next_batch_prescan_store(MY_SPEC.max_batch_size_, read_rows, &sk_rows[0],
                                             MY_SPEC.has_addon_ ? &addon_rows[0] : nullptr))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next batch", K(ret));
            }
          } else if (OB_FAIL(sort_op_provider_.add_batch_stored_row(
                       read_rows, &sk_rows[0], MY_SPEC.has_addon_ ? &addon_rows[0] : nullptr))) {
            LOG_WARN("failed to add batch stored row", K(ret));
          }
        }
        if (MY_SPEC.has_addon_) {
          blk_holder_.release();
          addon_row_iter_.reset();
          addon_row_store_.reset();
        }
        sk_row_iter_.reset();
        sk_row_store_.reset();
      }
    }
  }
  return ret;
}

int ObSortVecOp::sort_component_next_batch(const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  ret = sort_op_provider_.get_next_batch(max_cnt, read_rows);
  brs_.size_ = read_rows;
  if (common::OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.size_ = 0;
    brs_.end_ = true;
  }
  return ret;
}

int ObSortVecOp::init_sort(int64_t tenant_id, int64_t row_count, int64_t topn_cnt)
{
  int ret = OB_SUCCESS;
  ObSortVecOpContext context;
  context.tenant_id_ = tenant_id;
  context.sk_exprs_ = &MY_SPEC.sk_exprs_;
  context.addon_exprs_ = &MY_SPEC.addon_exprs_;
  context.sk_collations_ = &MY_SPEC.sk_collations_;
  context.addon_collations_ = &MY_SPEC.addon_collations_;
  context.eval_ctx_ = &eval_ctx_;
  context.exec_ctx_ = &ctx_;
  context.enable_encode_sortkey_ = MY_SPEC.enable_encode_sortkey_opt_;
  context.topn_cnt_ = topn_cnt;
  context.is_fetch_with_ties_ = MY_SPEC.is_fetch_with_ties_;
  context.has_addon_ = MY_SPEC.has_addon_;
  context.compress_type_ = MY_SPEC.compress_type_;
  context.enable_pd_topn_filter_ = MY_SPEC.enable_pd_topn_filter();
  context.pd_topn_filter_info_ = &MY_SPEC.pd_topn_filter_info_;
  context.op_ = this;
  if (MY_SPEC.prefix_pos_ > 0) {
    context.prefix_pos_ = MY_SPEC.prefix_pos_;
    context.sort_row_cnt_ = &sort_row_count_;
  } else {
    context.in_local_order_ = MY_SPEC.is_local_merge_sort_;
    context.part_cnt_ = MY_SPEC.part_cnt_;
  }
  int aqs_head =
    MY_SPEC.enable_encode_sortkey_opt_ ? sizeof(oceanbase::sql::ObSortOpImpl::AQSItem) : 0;
  if (OB_FAIL(sort_op_provider_.init(context))) {
    LOG_WARN("failed to init sort operator provider", K(ret));
  } else {
    sk_row_store_.set_allocator(*sort_op_provider_.get_malloc_allocator());
    addon_row_store_.set_allocator(*sort_op_provider_.get_malloc_allocator());
    sort_op_provider_.set_input_rows(row_count);
    sort_op_provider_.set_input_width(MY_SPEC.width_ + aqs_head);
    sort_op_provider_.set_operator_type(MY_SPEC.type_);
    sort_op_provider_.set_operator_id(MY_SPEC.id_);
    sort_op_provider_.set_io_event_observer(&io_event_observer_);
  }
  return ret;
}

int ObSortVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (is_first_) {
    is_first_ = false;
    int64_t topn_cnt = INT64_MAX;
    int64_t row_count = MY_SPEC.rows_;
    const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, MY_SPEC.rows_,
                                                  row_count))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(get_topn_count(topn_cnt))) {
      LOG_WARN("failed to get topn count", K(ret));
    } else if (topn_cnt <= 0) {
      brs_.end_ = true;
      brs_.size_ = 0;
    } else if (OB_FAIL(init_sort(tenant_id, row_count, topn_cnt))) {
      LOG_WARN("failed to init batch sort", K(ret));
    } else if (OB_FAIL(process_sort_batch())) {
      LOG_WARN("process sort failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !brs_.end_) {
    clear_evaluated_flag();
    if (OB_FAIL(sort_component_next_batch(std::min(max_row_cnt, MY_SPEC.max_batch_size_)))) {
      LOG_WARN("get next row failed");
    } else {
      ret_row_count_ += brs_.size_;
      if (brs_.end_) {
        LOG_DEBUG("finish ObSortVecOp::inner_get_next_batch", K(MY_SPEC.output_), K(brs_),
                  K(ret_row_count_));
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
