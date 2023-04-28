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

#include "sql/engine/basic/ob_material_op.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObMaterialSpec, ObOpSpec));
OB_SERIALIZE_MEMBER(ObMaterialOpInput, bypass_);

int ObMaterialOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else {
    is_first_ = true;
  }
  return ret;
}

void ObMaterialOp::destroy()
{
  material_impl_.unregister_profile_if_necessary();
  material_impl_.~ObMaterialOpImpl();
  ObOperator::destroy();
}

int ObMaterialOp::init_material_impl(int64_t tenant_id, int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(material_impl_.init(tenant_id, &eval_ctx_, &ctx_, &io_event_observer_))) {
    LOG_WARN("failed to init material impl", K(tenant_id));
  } else {
    material_impl_.set_input_rows(row_count);
    material_impl_.set_input_width(MY_SPEC.width_);
    material_impl_.set_operator_type(MY_SPEC.type_);
    material_impl_.set_operator_id(MY_SPEC.id_);
  }
  return ret;
}

int ObMaterialOp::get_all_row_from_child(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = session.get_effective_tenant_id();
  int64_t row_count = MY_SPEC.rows_;
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(init_material_impl(tenant_id, row_count))) {
    LOG_WARN("failed to init material impl");
  }

  while (OB_SUCCESS == ret) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      // do nothing
    } else if (OB_FAIL(material_impl_.add_row(child_->get_spec().output_))) {
      LOG_WARN("failed to add row to row store", K(ret));
    }
  }
  if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
    // 最后一批数据retain到内存中
    if (OB_FAIL(material_impl_.finish_add_row())) {
      LOG_WARN("failed to finish add row to row store", K(ret));
    } else {
      is_first_ = false;
    }
  }
  return ret;
}

int ObMaterialOp::get_all_batch_from_child(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = session.get_effective_tenant_id();
  int64_t row_count = MY_SPEC.rows_;
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(init_material_impl(tenant_id, row_count))) {
    LOG_WARN("failed to init material impl");
  }

  const ObBatchRows *input_brs = nullptr;
  bool iter_end = false;
  while (OB_SUCCESS == ret && !iter_end) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(MY_SPEC.max_batch_size_, input_brs))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (OB_FAIL(material_impl_.add_batch(child_->get_spec().output_,
                                                *input_brs->skip_, input_brs->size_))) {
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
    // 最后一批数据retain到内存中
    if (OB_FAIL(material_impl_.finish_add_row())) {
      LOG_WARN("failed to finish add row to row store", K(ret));
    } else {
      is_first_ = false;
    }
  }
  return ret;
}

int ObMaterialOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  material_impl_.rescan();
  // restart material op
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("operator rescan failed", K(ret));
  }
  is_first_ = true;
  return ret;
}

int ObMaterialOp::rewind()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {//do not cascade rescan
    LOG_WARN("failed to do inner rescan", K(ret));
  } else {
    material_impl_.rewind();
  }
  return ret;
}

int ObMaterialOp::inner_close()
{
  int ret = OB_SUCCESS;
  material_impl_.reset();
  return ret;
}

int ObMaterialOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (static_cast<ObMaterialOpInput *>(input_)->is_bypass()) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      LOG_WARN("fail get next child row", K(ret));
    }
  } else {
    clear_evaluated_flag();
    if (is_first_ && OB_FAIL(get_all_row_from_child(*ctx_.get_my_session()))) {
      LOG_WARN("failed to get all row from child", K(child_), K(ret));
    } else if (OB_FAIL(material_impl_.get_next_row(child_->get_spec().output_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get row from row store failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMaterialOp::inner_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = -1;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (static_cast<ObMaterialOpInput *>(input_)->is_bypass()) {
    clear_evaluated_flag();
    const ObBatchRows *input_brs = nullptr;
    if (OB_FAIL(child_->get_next_batch(std::min(MY_SPEC.max_batch_size_, max_row_cnt), input_brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      }
    } else {
      brs_.copy(input_brs);
    }
  } else {
    clear_evaluated_flag();
    if (is_first_ && OB_FAIL(get_all_batch_from_child(*ctx_.get_my_session()))) {
      LOG_WARN("failed to get all batch from child", K(child_), K(ret));
    } else if (OB_FAIL(material_impl_.get_next_batch(child_->get_spec().output_,
                                                     std::min(MY_SPEC.max_batch_size_, max_row_cnt),
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

} // end namespace sql
} // end namespace oceanbase
