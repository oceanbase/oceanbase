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
#include "ob_udf_ctx_mgr.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"

namespace oceanbase
{
namespace sql
{

using namespace common;
using ObUdfCtx = ObUdfFunction::ObUdfCtx;

ObUdfCtxMgr::~ObUdfCtxMgr() {
  if (ctxs_.created()) {
    common::hash::ObHashMap<uint64_t, ObNormalUdfExeUnit *,
        common::hash::NoPthreadDefendMode>::iterator iter = ctxs_.begin();
    for (; iter != ctxs_.end(); iter++) {
      ObNormalUdfExeUnit *normal_unit = iter->second;
      if (OB_NOT_NULL(normal_unit->normal_func_) && OB_NOT_NULL(normal_unit->udf_ctx_)) {
        IGNORE_RETURN normal_unit->normal_func_->process_deinit_func(*normal_unit->udf_ctx_);
      }
    }
  }
  allocator_.reset();
  ctxs_.destroy();
}

int ObUdfCtxMgr::register_udf_expr(const ObExprDllUdf *expr, const ObNormalUdfFunction *func, ObNormalUdfExeUnit *&udf_exec_unit)
{
  int ret = OB_SUCCESS;
  uint64_t expr_id = common::OB_INVALID_ID;
  ObUdfCtx *new_udf_ctx = nullptr;
  ObNormalUdfExeUnit *tmp_udf_exec_unit = nullptr;
  if (OB_FAIL(try_init_map())) {
    LOG_WARN("failed to init udf ctx map", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the expr is null", K(ret));
  } else if (FALSE_IT(expr_id = expr->get_id())) {
  } else if (OB_ISNULL(new_udf_ctx = (ObUdfCtx *)allocator_.alloc(sizeof(ObUdfCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(tmp_udf_exec_unit = (ObNormalUdfExeUnit *)allocator_.alloc(sizeof(ObNormalUdfExeUnit)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObNormalUdfExeUnit)));
  } else {
    new_udf_ctx->state_ = ObUdfFunction::UDF_UNINITIALIZED;
    IGNORE_RETURN ObUdfUtil::construct_udf_args(new_udf_ctx->udf_args_);
    IGNORE_RETURN ObUdfUtil::construct_udf_init(new_udf_ctx->udf_init_);
    tmp_udf_exec_unit->udf_ctx_ = new_udf_ctx;
    tmp_udf_exec_unit->normal_func_ = func;
    if (OB_FAIL(ctxs_.set_refactored(expr_id, tmp_udf_exec_unit))) {
      LOG_WARN("failed to set new udf ctx", K(ret));
    } else {
      udf_exec_unit = tmp_udf_exec_unit;
    }
  }
  return ret;
}

int ObUdfCtxMgr::get_udf_ctx(uint64_t expr_id, ObNormalUdfExeUnit *&udf_exec_unit)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_init_map())) {
    LOG_WARN("failed to init udf ctx map", K(ret));
  } else if (OB_FAIL(ctxs_.get_refactored(expr_id, udf_exec_unit))) {
    LOG_WARN("failed to get udf_ctx", K(ret));
  }
  return ret;
}

int ObUdfCtxMgr::try_init_map()
{
  int ret = OB_SUCCESS;
  if (!ctxs_.created()) {
    if (OB_FAIL(ctxs_.create(common::hash::cal_next_prime(BUKET_NUM),
                             common::ObModIds::OB_SQL_UDF,
                             common::ObModIds::OB_SQL_UDF))) {
      LOG_WARN("create hash failed", K(ret));
    }
  }
  return ret;
}

int ObUdfCtxMgr::reset()
{
  int ret = OB_SUCCESS;
  if (ctxs_.created()) {
    common::hash::ObHashMap<uint64_t, ObNormalUdfExeUnit *, common::hash::NoPthreadDefendMode>::iterator iter = ctxs_.begin();
    for (; iter != ctxs_.end(); iter++) {
      ObNormalUdfExeUnit *normal_unit = iter->second;
      if (OB_NOT_NULL(normal_unit->normal_func_) && OB_NOT_NULL(normal_unit->udf_ctx_)) {
        IGNORE_RETURN normal_unit->normal_func_->process_deinit_func(*normal_unit->udf_ctx_);
      }
    }
    ctxs_.clear();
  }
  allocator_.reset();
  return ret;
}



}
}

