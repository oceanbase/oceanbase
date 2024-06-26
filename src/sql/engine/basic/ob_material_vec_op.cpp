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

#include "sql/engine/basic/ob_material_vec_op.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObMaterialVecSpec, ObOpSpec));
OB_SERIALIZE_MEMBER(ObMaterialVecOpInput, bypass_);

int ObMaterialVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t row_count = MY_SPEC.rows_;
  if (OB_ISNULL(child_) || OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(child_), K(ret));
  } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count))) {
    LOG_WARN("failed to get px size", K(ret));
  } else {
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    ObMemAttr mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed");
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned");
    } else if (OB_FAIL(store_.init(child_->get_spec().output_,
                                   MY_SPEC.max_batch_size_,
                                   mem_attr,
                                   0 /*mem_limit*/,
                                   true /*enable_dump*/,
                                   true /*reuse_vector_array*/,
                                   MY_SPEC.compress_type_))) {
      LOG_WARN("init row store failed");
    } else {
      const int64_t size = OB_INVALID_ID == row_count ? 0 : row_count * MY_SPEC.width_;
      if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                          tenant_id, size, MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else {
        store_.set_allocator(mem_context_->get_malloc_allocator());
        store_.set_callback(&sql_mem_processor_);
        store_.set_io_event_observer(&io_event_observer_);
        store_.set_dir_id(sql_mem_processor_.get_dir_id());
        LOG_TRACE("trace init sql mem mgr for material", K(size), K(profile_.get_cache_size()),
           K(profile_.get_expect_size()));
      }
    }
    is_first_ = true;
  }

  return ret;
}

void ObMaterialVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  reset();
  store_.~ObTempColumnStore();
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  ObOperator::destroy();
}

int ObMaterialVecOp::get_all_batch_from_child()
{
  int ret = OB_SUCCESS;

  const ObBatchRows *input_brs = nullptr;
  bool iter_end = false;
  while (OB_SUCCESS == ret && !iter_end) {
    int64_t add_row_size = -1;
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(MY_SPEC.max_batch_size_, input_brs))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (OB_FAIL(process_dump())) {
      LOG_WARN("failed to process dump", K(ret));
    } else if (OB_FAIL(store_.add_batch(child_->get_spec().output_,
                                        eval_ctx_,
                                        *input_brs,
                                        add_row_size))) {
      LOG_WARN("failed to add row to row store", K(ret));
    } else {
      iter_end = input_brs->end_;
    }
  }

  // normally child_ should not return OB_ITER_END. We add a defence check here to prevent
  // some one return OB_ITER_END by mistake.
  if (OB_UNLIKELY(OB_ITER_END == ret)) {
    brs_.size_ = 0;
    brs_.end_ = true;
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(store_.finish_add_row(false))) {
      LOG_WARN("failed to finish add row to row store", K(ret));
    } else if (OB_FAIL(store_.begin(store_it_))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      is_first_ = false;
    }
  }
  return ret;
}

int ObMaterialVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset();
  // restart material op
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("operator rescan failed", K(ret));
  }
  return ret;
}

int ObMaterialVecOp::rewind()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {//do not cascade rescan
    LOG_WARN("failed to do inner rescan", K(ret));
  } else {
    store_it_.reset();
  }
  return ret;
}

int ObMaterialVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  sql_mem_processor_.unregister_profile();
  reset();
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }

  return ret;
}

void ObMaterialVecOp::reset()
{
  store_it_.reset();
  store_.reset();
  is_first_ = true;
}

int ObMaterialVecOp::inner_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = -1;
  int64_t max_batch_size = std::min(MY_SPEC.max_batch_size_, max_row_cnt);
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (static_cast<ObMaterialVecOpInput *>(input_)->is_bypass()) {
    clear_evaluated_flag();
    const ObBatchRows *input_brs = nullptr;
    if (OB_FAIL(child_->get_next_batch(max_batch_size, input_brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      }
    } else {
      brs_.copy(input_brs);
    }
  } else {
    clear_evaluated_flag();
    if (is_first_ && OB_FAIL(get_all_batch_from_child())) {
      LOG_WARN("failed to get all batch from child", K(child_), K(ret));
    } else if (OB_FAIL(store_it_.get_next_batch(child_->get_spec().output_,
                                                eval_ctx_,
                                                max_batch_size,
                                                read_rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch from datum store", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      brs_.size_ = read_rows;
    } else if (OB_ITER_END == ret) {
      brs_.size_ = 0;
      brs_.end_ = true;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMaterialVecOp::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  UNUSED(updated);
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return store_.get_row_cnt_in_memory() > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              return sql_mem_processor_.get_data_size() > max_memory_size;
            },
            dumped, sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(store_.dump(false))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      sql_mem_processor_.reset();
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump", K(sql_mem_processor_.get_data_size()),
        K(store_.get_row_cnt_in_memory()),
        K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
