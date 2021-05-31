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

#include "sql/engine/basic/ob_material.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(ObMaterialInput, bypass_);

int ObMaterialInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  UNUSED(ctx);
  UNUSED(op);
  UNUSED(task_info);
  return OB_SUCCESS;
}

ObPhyOperatorType ObMaterialInput::get_phy_op_type() const
{
  return PHY_MATERIAL;
}

////////////////////////////////////////////////////////////////////////

OB_SERIALIZE_MEMBER((ObMaterial, ObSingleChildPhyOperator));

void ObMaterial::reset()
{
  ObSingleChildPhyOperator::reset();
}

void ObMaterial::reuse()
{
  ObSingleChildPhyOperator::reuse();
}

int ObMaterial::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMaterialCtx, ctx, get_id(), get_type(), op_ctx))) {
    SQL_ENG_LOG(WARN, "create physical operator context failed", K(ret));
  } else if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get my session", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    SQL_ENG_LOG(WARN, "create current row failed", K(ret));
  }
  return ret;
}

int ObMaterial::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObMaterialCtx* mat_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_FAIL(init_op_ctx(exec_ctx))) {
    SQL_ENG_LOG(WARN, "failed to init mat ctx", K(ret));
  } else if (OB_ISNULL(mat_ctx = GET_PHY_OPERATOR_CTX(ObMaterialCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to get mat ctx", K(ret));
  } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to get my session", K(ret));
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "child operator is NULL", K(child_op_), K(ret));
  } else {
    mat_ctx->is_first_ = true;
  }
  return ret;
}

int ObMaterial::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObMaterialCtx* mat_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(mat_ctx = GET_PHY_OPERATOR_CTX(ObMaterialCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to get mat ctx", K(ret));
  } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get my session", K(ret));
  } else if (exec_ctx.is_restart_plan()) {
    mat_ctx->row_store_it_.reset();
    mat_ctx->row_store_.reset();
    // restart material op
    if (OB_FAIL(ObPhyOperator::rescan(exec_ctx))) {
      LOG_WARN("operator rescan failed", K(ret));
    }
    mat_ctx->row_id_ = 0;
    mat_ctx->is_first_ = true;
  } else {
    mat_ctx->row_store_it_.reset();
    mat_ctx->row_id_ = 0;
  }
  return ret;
}

int ObMaterial::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObMaterialCtx* mat_ctx = NULL;
  if (OB_ISNULL(mat_ctx = GET_PHY_OPERATOR_CTX(ObMaterialCtx, exec_ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    mat_ctx->row_store_it_.reset();
    mat_ctx->row_store_.reset();
    mat_ctx->row_id_ = 0;
    mat_ctx->sql_mem_processor_.unregister_profile();
  }
  return ret;
}

int ObMaterial::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMaterialCtx* mat_ctx = NULL;
  if (OB_ISNULL(mat_ctx = GET_PHY_OPERATOR_CTX(ObMaterialCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get physical operator context", K(ret));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    SQL_ENG_LOG(WARN, "check physical plan status failed", K(ret));
  } else if (mat_ctx->is_first_ && OB_FAIL(get_all_row_from_child(*mat_ctx, *(exec_ctx.get_my_session())))) {
    SQL_ENG_LOG(WARN, "failed toget all row from child", K(child_op_), K(ret));
  } else if (OB_FAIL(mat_ctx->row_store_it_.get_next_row(mat_ctx->get_cur_row()))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from row store failed", K(ret), K(mat_ctx->row_id_));
    } else if (exec_ctx.is_restart_plan()) {
      mat_ctx->row_store_it_.reset();
      mat_ctx->row_store_.reset();
    }
  } else {
    mat_ctx->row_id_++;
    row = &mat_ctx->get_cur_row();
  }
  return ret;
}

int ObMaterial::get_material_row_count(ObExecContext& exec_ctx, int64_t& material_row_count) const
{
  int ret = OB_SUCCESS;
  ObMaterialCtx* material_ctx = NULL;
  if (OB_ISNULL(material_ctx = GET_PHY_OPERATOR_CTX(ObMaterialCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "get material ctx failed", K(ret));
  } else {
    material_row_count = material_ctx->row_store_.get_row_cnt();
  }
  return ret;
}

int ObMaterial::process_dump(ObMaterialCtx& mat_ctx) const
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  UNUSED(updated);
  // as it needs to preserve ordering, it needs to dump all of them except the last piece of memory.
  // currently it does this from front to end
  if (OB_FAIL(mat_ctx.sql_mem_processor_.update_max_available_mem_size_periodically(
          &mat_ctx.mem_context_->get_malloc_allocator(),
          [&](int64_t cur_cnt) { return mat_ctx.row_store_.get_row_cnt_in_memory() > cur_cnt; },
          updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (mat_ctx.need_dump() && GCONF.is_sql_operator_dump_enabled() &&
             OB_FAIL(mat_ctx.sql_mem_processor_.extend_max_memory_size(
                 &mat_ctx.mem_context_->get_malloc_allocator(),
                 [&](int64_t max_memory_size) { return mat_ctx.sql_mem_processor_.get_data_size() > max_memory_size; },
                 dumped,
                 mat_ctx.sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(mat_ctx.row_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      mat_ctx.sql_mem_processor_.reset();
      mat_ctx.sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump",
          K(mat_ctx.sql_mem_processor_.get_data_size()),
          K(mat_ctx.row_store_.get_row_cnt_in_memory()),
          K(mat_ctx.sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int ObMaterial::get_all_row_from_child(ObMaterialCtx& mat_ctx, ObSQLSessionInfo& session) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  bool first_row = true;
  int64_t tenant_id = session.get_effective_tenant_id();
  if (OB_ISNULL(mat_ctx.mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mat_ctx.mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mat_ctx.mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mat_ctx.row_store_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
    LOG_WARN("init row store failed", K(ret));
  } else {
    mat_ctx.row_store_.set_allocator(mat_ctx.mem_context_->get_malloc_allocator());
    mat_ctx.row_store_.set_callback(&mat_ctx.sql_mem_processor_);
  }
  while (OB_SUCCESS == ret && OB_SUCC(child_op_->get_next_row(mat_ctx.exec_ctx_, input_row))) {
    if (first_row) {
      int64_t row_count = get_rows();
      if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&mat_ctx.exec_ctx_, px_est_size_factor_, row_count, row_count))) {
        LOG_WARN("failed to get px size", K(ret));
      } else if (OB_FAIL(mat_ctx.sql_mem_processor_.init(&mat_ctx.mem_context_->get_malloc_allocator(),
                     tenant_id,
                     row_count * get_width(),
                     get_type(),
                     get_id(),
                     &mat_ctx.exec_ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else {
        mat_ctx.row_store_.set_dir_id(mat_ctx.sql_mem_processor_.get_dir_id());
        LOG_TRACE("trace init sql mem mgr for material",
            K(row_count),
            K(get_width()),
            K(mat_ctx.profile_.get_cache_size()),
            K(mat_ctx.profile_.get_expect_size()));
      }
      first_row = false;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_dump(mat_ctx))) {
      LOG_WARN("failed to process dump", K(ret));
    } else if (OB_FAIL(mat_ctx.row_store_.add_row(*input_row))) {
      SQL_ENG_LOG(WARN, "failed to add row to row store", K(ret));
    }
  }
  if (OB_UNLIKELY(OB_ITER_END != ret)) {
    SQL_ENG_LOG(WARN, "fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
    // last batch of data got retained in memory
    if (OB_FAIL(mat_ctx.row_store_.finish_add_row(false))) {
      SQL_ENG_LOG(WARN, "failed to finish add row to row store", K(ret));
    } else if (OB_FAIL(mat_ctx.row_store_.begin(mat_ctx.row_store_it_))) {
      SQL_ENG_LOG(WARN, "failed to begin iterator for chunk row store", K(ret));
    } else {
      mat_ctx.is_first_ = false;
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
