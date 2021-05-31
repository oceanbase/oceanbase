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

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER((ObMaterialSpec, ObOpSpec));

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
  datum_store_it_.reset();
  datum_store_.reset();
  destroy_mem_context();
  ObOperator::destroy();
}

int ObMaterialOp::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  UNUSED(updated);
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
          &mem_context_->get_malloc_allocator(),
          [&](int64_t cur_cnt) { return datum_store_.get_row_cnt_in_memory() > cur_cnt; },
          updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() && GCONF.is_sql_operator_dump_enabled() &&
             OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                 &mem_context_->get_malloc_allocator(),
                 [&](int64_t max_memory_size) { return sql_mem_processor_.get_data_size() > max_memory_size; },
                 dumped,
                 sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(datum_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      sql_mem_processor_.reset();
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump",
          K(sql_mem_processor_.get_data_size()),
          K(datum_store_.get_row_cnt_in_memory()),
          K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int ObMaterialOp::get_all_row_from_child(ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  bool first_row = true;
  int64_t tenant_id = session.get_effective_tenant_id();
  if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(datum_store_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
    LOG_WARN("init row store failed", K(ret));
  } else {
    datum_store_.set_allocator(mem_context_->get_malloc_allocator());
    datum_store_.set_callback(&sql_mem_processor_);
  }
  while (OB_SUCCESS == ret) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
    } else if (first_row) {
      int64_t row_count = MY_SPEC.rows_;
      if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count))) {
        LOG_WARN("failed to get px size", K(ret));
      } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                     tenant_id,
                     row_count * MY_SPEC.width_,
                     MY_SPEC.type_,
                     MY_SPEC.id_,
                     &ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else {
        datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        LOG_TRACE("trace init sql mem mgr for material",
            K(row_count),
            K(MY_SPEC.width_),
            K(profile_.get_cache_size()),
            K(profile_.get_expect_size()));
      }
      first_row = false;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_dump())) {
      LOG_WARN("failed to process dump", K(ret));
    } else if (OB_FAIL(datum_store_.add_row(child_->get_spec().output_, &eval_ctx_))) {
      LOG_WARN("failed to add row to row store", K(ret));
    }
  }
  if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
    if (OB_FAIL(datum_store_.finish_add_row(false))) {
      LOG_WARN("failed to finish add row to row store", K(ret));
    } else if (OB_FAIL(datum_store_.begin(datum_store_it_))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      is_first_ = false;
    }
  }
  return ret;
}

int ObMaterialOp::rescan()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = ctx_.get_my_session();
  if (ctx_.is_restart_plan()) {
    datum_store_it_.reset();
    datum_store_.reset();
    // restart material op
    if (OB_FAIL(ObOperator::rescan())) {
      LOG_WARN("operator rescan failed", K(ret));
    }
    is_first_ = true;
  } else {
    datum_store_it_.reset();
  }
  return ret;
}

int ObMaterialOp::inner_close()
{
  int ret = OB_SUCCESS;
  datum_store_it_.reset();
  datum_store_.reset();
  sql_mem_processor_.unregister_profile();
  return ret;
}

int ObMaterialOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else {
    clear_evaluated_flag();
    if (is_first_ && OB_FAIL(get_all_row_from_child(*ctx_.get_my_session()))) {
      LOG_WARN("failed toget all row from child", K(child_), K(ret));
    } else if (OB_FAIL(datum_store_it_.get_next_row(child_->get_spec().output_, eval_ctx_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get row from row store failed", K(ret));
      } else {
        if (ctx_.is_restart_plan()) {
          datum_store_it_.reset();
          datum_store_.reset();
        }
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
