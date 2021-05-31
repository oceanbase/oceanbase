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

#include "sql/engine/sort/ob_sort.h"

#include "lib/utility/utility.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/time/ob_time_utility.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace sql {

ObPrefixSort::ObPrefixSort()
    : prefix_pos_(0), full_sort_columns_(NULL), prev_row_(NULL), cur_row_(NULL), op_(NULL), sort_row_count_(NULL)
{}

void ObPrefixSort::reuse()
{
  ObSortImpl::reuse();
  prev_row_ = NULL;
  cur_row_ = NULL;
}

void ObPrefixSort::reset()
{
  ObSortImpl::reset();
  prev_row_ = NULL;
  cur_row_ = NULL;
}

int ObPrefixSort::init(const int64_t tenant_id, const int64_t prefix_pos, const SortColumns& sort_columns,
    const SortExtraInfos& extra_infos, const ObPhyOperator& op, ObExecContext& exec_ctx, int64_t& sort_row_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || prefix_pos <= 0 || prefix_pos > sort_columns.count() ||
             sort_columns.count() != extra_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(prefix_pos), K(sort_columns), K(extra_infos));
  } else {
    prefix_pos_ = prefix_pos;
    full_sort_columns_ = &sort_columns;
    // NOTE: %cnt may be zero, some plan is wrong generated with prefix sort:
    // %prefix_pos == %sort_columns.count(), the sort operator should be eliminated but not.
    //
    // To be compatible with this plan, we keep this behavior.
    const int64_t cnt = sort_columns.count() - prefix_pos;
    base_sort_columns_.init(cnt, const_cast<ObSortColumn*>(&sort_columns.at(0) + prefix_pos), cnt);
    base_extra_infos_.init(cnt, const_cast<ObOpSchemaObj*>(&extra_infos.at(0) + prefix_pos), cnt);
    prev_row_ = NULL;
    cur_row_ = NULL;
    op_ = &op;
    exec_ctx_ = &exec_ctx;
    sort_row_count_ = &sort_row_cnt;
    if (OB_FAIL(ObSortImpl::init(tenant_id, base_sort_columns_, &base_extra_infos_))) {
      LOG_WARN("sort impl init failed", K(ret));
    } else if (OB_FAIL(fetch_rows())) {
      LOG_WARN("fetch rows failed");
    }
  }
  return ret;
}

int ObPrefixSort::fetch_rows()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSortImpl::reuse();
    int64_t row_count = 0;
    prev_row_ = NULL;
    if (NULL != cur_row_) {
      row_count += 1;
      if (OB_FAIL(add_row(*cur_row_, prev_row_))) {
        LOG_WARN("add row to sort impl failed", K(ret));
      } else if (OB_ISNULL(prev_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add stored row is NULL", K(ret));
      }
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(op_->get_next_row(*exec_ctx_, cur_row_))) {
        if (OB_ITER_END == ret) {
          // Set %cur_row_ to NULL to indicate that all rows are fetched.
          cur_row_ = NULL;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
        break;
      } else if (NULL == cur_row_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL row returned", K(ret));
      } else {
        *sort_row_count_ += 1;
        row_count += 1;
        // check the prefix is the same with previous row
        bool same_prefix = true;
        if (NULL != prev_row_) {
          const ObObj* cells = prev_row_->cells();
          if (cur_row_->projector_size_ > 0) {
            for (int64_t i = 0; same_prefix && i < prefix_pos_; i++) {
              const ObSortColumn& sc = full_sort_columns_->at(i);
              same_prefix =
                  0 == cur_row_->get_cell(sc.index_).compare(cells[cur_row_->projector_[sc.index_]], sc.cs_type_);
            }
          } else {
            for (int64_t i = 0; same_prefix && i < prefix_pos_; i++) {
              const ObSortColumn& sc = full_sort_columns_->at(i);
              same_prefix = 0 == cur_row_->get_cell(sc.index_).compare(cells[sc.index_], sc.cs_type_);
            }
          }
        }
        if (!same_prefix) {
          // row are saved in %cur_row_, will be added in the next call
          break;
        }
        if (OB_FAIL(add_row(*cur_row_, prev_row_))) {
          LOG_WARN("add row to sort impl failed", K(ret));
        } else if (OB_ISNULL(prev_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add stored row is NULL", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && row_count > 0) {
      if (OB_FAIL(ObSortImpl::sort())) {
        LOG_WARN("sort rows failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPrefixSort::get_next_row(const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(ObSortImpl::get_next_row(row))) {
      if (OB_ITER_END == ret) {
        if (NULL != cur_row_) {
          if (OB_FAIL(fetch_rows())) {
            LOG_WARN("fetch rows failed", K(ret));
          } else if (OB_FAIL(ObSortImpl::get_next_row(row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("sort impl get next row failed", K(ret));
            }
          }
        }
      } else {
        LOG_WARN("sort impl get next row failed", K(ret));
      }
    }
  }
  return ret;
}

ObSort::ObSortCtx::ObSortCtx(ObExecContext& ctx)
    : ObPhyOperatorCtx(ctx),
      sort_impl_(),
      prefix_sort_impl_(),
      topn_sort_(),
      read_func_(&ObSort::sort_impl_next),
      sort_row_count_(0),
      is_first_(true),
      ret_row_count_(0)
{}

void ObSort::ObSortCtx::destroy()
{
  sort_impl_.~ObSortImpl();
  prefix_sort_impl_.~ObPrefixSort();
  topn_sort_.~ObInMemoryTopnSort();
  read_func_ = NULL;
  sort_row_count_ = 0;
  is_first_ = true;
  ret_row_count_ = 0;
  ObPhyOperatorCtx::destroy_base();
}

ObSort::ObSort(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      ObSortableTrait(alloc),
      mem_limit_(MEM_LIMIT_SIZE),
      topn_expr_(NULL),
      minimum_row_count_(0),
      topk_precision_(0),
      topk_limit_expr_(NULL),
      topk_offset_expr_(NULL),
      prefix_pos_(0),
      is_local_merge_sort_(false),
      is_fetch_with_ties_(false)
{}

void ObSort::reset()
{
  mem_limit_ = MEM_LIMIT_SIZE;
  topn_expr_ = NULL;
  topk_limit_expr_ = NULL;
  topk_offset_expr_ = NULL;
  minimum_row_count_ = 0;
  topk_precision_ = 0;
  prefix_pos_ = 0;
  is_local_merge_sort_ = false;
  is_fetch_with_ties_ = false;
  ObSingleChildPhyOperator::reset();
  ObSortableTrait::reset();
}

void ObSort::reuse()
{
  mem_limit_ = MEM_LIMIT_SIZE;
  ObSingleChildPhyOperator::reuse();
  ObSortableTrait::reuse();
}

void ObSort::set_mem_limit(const int64_t limit)
{
  LOG_TRACE("sort mem", K(limit));
  mem_limit_ = limit;
}

int ObSort::set_topk_params(
    ObSqlExpression* limit, ObSqlExpression* offset, int64_t minimum_row_count, int64_t topk_precision)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(limit) || minimum_row_count < 0 || topk_precision < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(limit), K(minimum_row_count), K(topk_precision), K(ret));
  } else {
    topk_limit_expr_ = limit;
    topk_offset_expr_ = offset;
    minimum_row_count_ = minimum_row_count;
    topk_precision_ = topk_precision;
  }
  return ret;
}

int ObSort::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObSortCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc op ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("create current row failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObSort::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t3) init_op_ctx(exec_ctx))) {
    LOG_WARN("failed to init sort ctx", K(ret));
  }
  return ret;
}

int ObSort::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObSortCtx* sort_ctx = NULL;
  if (OB_ISNULL(sort_ctx = GET_PHY_OPERATOR_CTX(ObSortCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get sort ctx", K(ret));
  } else {
    sort_ctx->sort_impl_.reset();
    sort_ctx->prefix_sort_impl_.reset();
    sort_ctx->topn_sort_.reset();
    sort_ctx->read_func_ = &ObSort::sort_impl_next;
    sort_ctx->sort_row_count_ = 0;
    sort_ctx->ret_row_count_ = 0;
    sort_ctx->is_first_ = true;
    if (OB_FAIL(ObSingleChildPhyOperator::rescan(exec_ctx))) {
      LOG_WARN("rescan single child operator failed", K(ret));
    }
  }
  return ret;
}

int ObSort::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObSortCtx* sort_ctx = NULL;
  if (OB_ISNULL(sort_ctx = GET_PHY_OPERATOR_CTX(ObSortCtx, exec_ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    sort_ctx->sort_impl_.unregister_profile();
    sort_ctx->prefix_sort_impl_.unregister_profile();
  }
  return ret;
}

int ObSort::process_sort(ObExecContext& exec_ctx, ObSortCtx& sort_ctx) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("child op is null", K(ret));
  } else {
    if (sort_ctx.read_func_ == &ObSort::prefix_sort_impl_next) {
      // prefix sort get child row in it's own wrap, do nothing here
    } else if (sort_ctx.read_func_ == &ObSort::topn_sort_next) {
      bool need_sort = false;
      while (OB_SUCC(ret) && !need_sort && OB_SUCC(child_op_->get_next_row(sort_ctx.exec_ctx_, input_row))) {
        if (OB_FAIL(try_check_status(exec_ctx))) {
          LOG_WARN("failed to check status", K(ret));
        } else if (NULL == input_row) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null input row", K(ret));
        } else {
          if (0 == sort_ctx.sort_row_count_) {
            // first row, we need to calc topn when all data of child_op is loaded into memory
            int64_t topn_cnt = INT64_MAX;
            if (OB_FAIL(get_topn_count(exec_ctx, topn_cnt))) {
              LOG_WARN("failed to get topn count", K(ret));
            } else {
              sort_ctx.topn_sort_.set_topn(topn_cnt);
            }
          }

          sort_ctx.sort_row_count_++;
          OZ(sort_ctx.topn_sort_.add_row(*input_row, need_sort));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        sort_ctx.topn_sort_.set_iter_end();
      }
      OZ(sort_ctx.topn_sort_.sort_rows());
    } else if (sort_ctx.read_func_ == &ObSort::sort_impl_next) {
      while (OB_SUCC(ret) && OB_SUCC(child_op_->get_next_row(exec_ctx, input_row))) {
        if (OB_FAIL(try_check_status(exec_ctx))) {
          LOG_WARN("failed to check status", K(ret));
        } else if (NULL == input_row) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null input row", K(ret));
        }
        sort_ctx.sort_row_count_++;
        OZ(sort_ctx.sort_impl_.add_row(*input_row));
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      OZ(sort_ctx.sort_impl_.sort());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid read function pointer", K(ret), K(*reinterpret_cast<int64_t*>(&sort_ctx.read_func_)));
    }
  }
  return ret;
}

int ObSort::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObSortCtx* sort_ctx = NULL;
  int64_t row_count = get_rows();
  if ((OB_ISNULL(sort_ctx = GET_PHY_OPERATOR_CTX(ObSortCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get sort ctx", K(ret));
  } else if (OB_FAIL(try_check_status(exec_ctx))) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&exec_ctx, px_est_size_factor_, get_rows(), row_count))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (sort_ctx->is_first_) {
    // The name 'get_effective_tenant_id()' is really confusing. Here what we want is to account
    // the resource usage(memory usage in this case) to a 'real' tenant rather than billing
    // the innocent DEFAULT tenant. We should think about changing the name of this function.
    const int64_t tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();
    sort_ctx->is_first_ = false;
    int64_t row_count = get_rows();
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&exec_ctx, px_est_size_factor_, get_rows(), row_count))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (NULL != topn_expr_ || NULL != topk_limit_expr_) {  // topn sort
      OZ(sort_ctx->topn_sort_.init_tenant_id(tenant_id));
      OZ(sort_ctx->topn_sort_.set_sort_columns(sort_columns_, prefix_pos_));
      sort_ctx->read_func_ = &ObSort::topn_sort_next;
      sort_ctx->topn_sort_.set_fetch_with_ties(is_fetch_with_ties_);
    } else if (prefix_pos_ > 0) {
      OZ(sort_ctx->prefix_sort_impl_.init(tenant_id,
          prefix_pos_,
          sort_columns_,
          get_op_schema_objs(),
          *child_op_,
          exec_ctx,
          sort_ctx->sort_row_count_));
      sort_ctx->read_func_ = &ObSort::prefix_sort_impl_next;
      sort_ctx->prefix_sort_impl_.set_input_rows(row_count);
      sort_ctx->prefix_sort_impl_.set_input_width(get_width());
      sort_ctx->prefix_sort_impl_.set_operator_type(get_type());
      sort_ctx->prefix_sort_impl_.set_operator_id(get_id());
      sort_ctx->prefix_sort_impl_.set_exec_ctx(&exec_ctx);
    } else {
      OZ(sort_ctx->sort_impl_.init(tenant_id, sort_columns_, &get_op_schema_objs(), is_local_merge_sort_));
      sort_ctx->read_func_ = &ObSort::sort_impl_next;
      sort_ctx->sort_impl_.set_input_rows(row_count);
      sort_ctx->sort_impl_.set_input_width(get_width());
      sort_ctx->sort_impl_.set_operator_type(get_type());
      sort_ctx->sort_impl_.set_operator_id(get_id());
      sort_ctx->sort_impl_.set_exec_ctx(&exec_ctx);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(process_sort(exec_ctx, *sort_ctx))) {  // process sort
        LOG_WARN("process sort failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL((this->*sort_ctx->read_func_)(exec_ctx, *sort_ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      }
    } else {
      ++sort_ctx->ret_row_count_;
    }
  }
  return ret;
}

int ObSort::topn_sort_next(ObExecContext& exec_ctx, ObSortCtx& sort_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (!is_fetch_with_ties_ && sort_ctx.ret_row_count_ >= sort_ctx.topn_sort_.get_topn_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(sort_ctx.topn_sort_.get_next_row(sort_ctx.cur_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("topn sort get next row failed", K(ret));
    } else if (prefix_pos_ > 0 && !sort_ctx.topn_sort_.is_iter_end()) {
      // topn prefix sort, iterate one prefix at a time, need to process_sort() again.
      if (OB_FAIL(process_sort(exec_ctx, sort_ctx))) {
        LOG_WARN("process sort failed", K(ret));
      } else if (OB_FAIL(sort_ctx.topn_sort_.get_next_row(sort_ctx.cur_row_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("topn sort get next row failed", K(ret));
        }
      } else {
        row = &sort_ctx.cur_row_;
      }
    }
  } else {
    row = &sort_ctx.cur_row_;
  }
  return ret;
}

int ObSort::get_sort_row_count(ObExecContext& exec_ctx, int64_t& sort_row_count) const
{
  int ret = OB_SUCCESS;
  ObSortCtx* sort_ctx = NULL;
  if (OB_I(t1)(OB_ISNULL(sort_ctx = GET_PHY_OPERATOR_CTX(ObSortCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sort ctx failed", K(ret));
  } else {
    sort_row_count = sort_ctx->sort_row_count_;
  }
  return ret;
}

int ObSort::get_int_value(ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val) const
{
  int ret = OB_SUCCESS;
  ObNewRow input_row;
  ObObj result;
  ObExprCtx expr_ctx;
  if (NULL != in_val && !in_val->is_empty()) {
    if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
      LOG_WARN("wrap expr context failed", K(ret));
    } else if (OB_FAIL(in_val->calc(expr_ctx, input_row, result))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (!result.is_int()) {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_INT64_V2(result, out_val);
      if (OB_FAIL(ret)) {
        LOG_WARN("Cast limit/offset value failed", K(ret), K(result));
      }
    } else {
      ret = result.get_int(out_val);
    }
  }
  return ret;
}

int ObSort::get_topn_count(ObExecContext& exec_ctx, int64_t& topn_cnt) const
{
  int ret = OB_SUCCESS;
  topn_cnt = INT64_MAX;
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  if ((OB_ISNULL(topn_expr_) && OB_ISNULL(topk_limit_expr_)) || ((NULL != topn_expr_) && (NULL != topk_limit_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid topn_expr or topk_limit_expr", K(topn_expr_), K(topk_limit_expr_), K(ret));
  } else if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan ctx is NULL", K(ret));
  } else if (NULL != topn_expr_) {
    if (OB_FAIL(get_int_value(exec_ctx, topn_expr_, topn_cnt))) {
      LOG_WARN("failed to get int value", K(ret), K(topn_expr_));
    } else {
      topn_cnt = std::max(minimum_row_count_, topn_cnt);
      LOG_TRACE("get topn", K(ret), K(topn_cnt));
    }
  } else if (NULL != topk_limit_expr_) {
    int64_t limit = -1;
    int64_t offset = 0;
    if (OB_ISNULL(child_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid child_op_ is NULL", K(ret));
    } else if ((OB_FAIL(get_int_value(exec_ctx, topk_limit_expr_, limit)) ||
                   OB_FAIL(get_int_value(exec_ctx, topk_offset_expr_, offset)))) {
      LOG_WARN("Get limit/offset value failed", K(ret));
    } else if (OB_UNLIKELY(limit < 0 || offset < 0)) {
      ret = OB_ERR_ILLEGAL_VALUE;
      LOG_WARN("Invalid limit/offset value", K(limit), K(offset), K(ret));
    } else {
      topn_cnt = std::max(minimum_row_count_, limit + offset);
      int64_t row_count = 0;
      ObPhyOperatorType op_type = child_op_->get_type();
      if (PHY_HASH_GROUP_BY != op_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid child_op_", K(op_type), K(ret));
      } else {
        ObHashGroupBy* hash_groupby_op = static_cast<ObHashGroupBy*>(child_op_);
        if (OB_ISNULL(hash_groupby_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("casted hash_groupby is NULL", K(ret));
        } else if (OB_FAIL(hash_groupby_op->get_hash_groupby_row_count(exec_ctx, row_count))) {
          LOG_WARN("failed to get hash_groupy row count", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret)) {
        topn_cnt = std::max(topn_cnt, static_cast<int64_t>(row_count * topk_precision_ / 100));
        if (topn_cnt >= row_count) {
          plan_ctx->set_is_result_accurate(true);
        } else {
          plan_ctx->set_is_result_accurate(false);
        }
      }
    }
  } else { /*will not go here*/
  }
  return ret;
}

OB_DEF_SERIALIZE(ObSort)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize child operator failed", K(ret));
  } else {
    OB_UNIS_ENCODE(sort_columns_);
    OB_UNIS_ENCODE(mem_limit_);
    bool has_topn = (NULL != topn_expr_ && !topn_expr_->is_empty());
    OB_UNIS_ENCODE(has_topn);
    if (has_topn) {
      OB_UNIS_ENCODE(*topn_expr_);
    }

    // for topk
    OB_UNIS_ENCODE(minimum_row_count_);
    OB_UNIS_ENCODE(topk_precision_);
    bool has_topk_limit = (topk_limit_expr_ != NULL && !topk_limit_expr_->is_empty());
    bool has_topk_offset = (topk_offset_expr_ != NULL && !topk_offset_expr_->is_empty());
    OB_UNIS_ENCODE(has_topk_limit);
    OB_UNIS_ENCODE(has_topk_offset);
    if (has_topk_limit) {
      OB_UNIS_ENCODE(*topk_limit_expr_);
    }
    if (has_topk_offset) {
      OB_UNIS_ENCODE(*topk_offset_expr_);
    }
    OB_UNIS_ENCODE(prefix_pos_);
    OB_UNIS_ENCODE(is_local_merge_sort_);
    OB_UNIS_ENCODE(is_fetch_with_ties_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSort)
{
  int64_t len = 0;
  len += ObSingleChildPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(sort_columns_);
  OB_UNIS_ADD_LEN(mem_limit_);
  bool has_topn = (NULL != topn_expr_ && !topn_expr_->is_empty());
  OB_UNIS_ADD_LEN(has_topn);
  if (has_topn) {
    OB_UNIS_ADD_LEN(*topn_expr_);
  }

  // for topk
  OB_UNIS_ADD_LEN(minimum_row_count_);
  OB_UNIS_ADD_LEN(topk_precision_);
  bool has_topk_limit = (topk_limit_expr_ != NULL && !topk_limit_expr_->is_empty());
  bool has_topk_offset = (topk_offset_expr_ != NULL && !topk_offset_expr_->is_empty());
  OB_UNIS_ADD_LEN(has_topk_limit);
  OB_UNIS_ADD_LEN(has_topk_offset);
  if (has_topk_limit) {
    OB_UNIS_ADD_LEN(*topk_limit_expr_);
  }
  if (has_topk_offset) {
    OB_UNIS_ADD_LEN(*topk_offset_expr_);
  }
  OB_UNIS_ADD_LEN(prefix_pos_);
  OB_UNIS_ADD_LEN(is_local_merge_sort_);
  OB_UNIS_ADD_LEN(is_fetch_with_ties_);
  return len;
}

OB_DEF_DESERIALIZE(ObSort)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSingleChildPhyOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize ObSingleChildPhyOperator failed", K(ret));
  } else {
    OB_UNIS_DECODE(sort_columns_);
    OB_UNIS_DECODE(mem_limit_);
    bool has_topn = false;
    OB_UNIS_DECODE(has_topn);
    if (has_topn) {
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, topn_expr_))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_LIKELY(NULL != topn_expr_)) {
        OB_UNIS_DECODE(*topn_expr_);
      }
    }

    // for topk
    bool has_topk_limit = false;
    bool has_topk_offset = false;
    OB_UNIS_DECODE(minimum_row_count_);
    OB_UNIS_DECODE(topk_precision_);
    OB_UNIS_DECODE(has_topk_limit);
    OB_UNIS_DECODE(has_topk_offset);
    if (OB_SUCC(ret) && has_topk_limit) {
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, topk_limit_expr_))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_LIKELY(topk_limit_expr_ != NULL)) {
        OB_UNIS_DECODE(*topk_limit_expr_);
      }
    }
    if (OB_SUCC(ret) && has_topk_offset) {
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, topk_offset_expr_))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_LIKELY(topk_offset_expr_ != NULL)) {
        OB_UNIS_DECODE(*topk_offset_expr_);
      }
    }
    if (OB_SUCC(ret)) {
      OB_UNIS_DECODE(prefix_pos_);
    }
    if (OB_SUCC(ret)) {
      OB_UNIS_DECODE(is_local_merge_sort_);
      OB_UNIS_DECODE(is_fetch_with_ties_);
    }
  }
  return ret;
}
// OB_SERIALIZE_MEMBER((ObSort, ObSingleChildPhyOperator), sort_columns_, mem_limit_);

}  // namespace sql
}  // namespace oceanbase
