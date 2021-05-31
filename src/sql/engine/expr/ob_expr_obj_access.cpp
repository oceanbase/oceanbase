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
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER(
    (ObExprObjAccess, ObExprOperator), get_attr_func_, param_idxs_, access_idx_cnt_, for_write_, coll_idx_);

ObExprObjAccess::ObExprObjAccess(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_OBJ_ACCESS_REF, N_OBJ_ACCESS, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION),
      get_attr_func_(0),
      param_idxs_(alloc),
      access_idx_cnt_(0),
      for_write_(false),
      coll_idx_(OB_INVALID_INDEX)
{}

ObExprObjAccess::~ObExprObjAccess()
{}

void ObExprObjAccess::reset()
{
  get_attr_func_ = 0;
  param_idxs_.reset();
  access_idx_cnt_ = 0;
  for_write_ = false;
  coll_idx_ = OB_INVALID_INDEX;
  ObExprOperator::reset();
}

int ObExprObjAccess::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OBJ_ACCESS_REF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprObjAccess& other_expr = static_cast<const ObExprObjAccess&>(other);
    get_attr_func_ = other_expr.get_attr_func_;
    access_idx_cnt_ = other_expr.access_idx_cnt_;
    for_write_ = other_expr.for_write_;
    coll_idx_ = other_expr.coll_idx_;
  }
  return ret;
}

#define GET_VALID_INT64_PARAM(obj)                                       \
  do {                                                                   \
    if (OB_SUCC(ret)) {                                                  \
      int64_t param_value = 0;                                           \
      if (obj.is_integer_type() || obj.is_ext()) {                       \
        param_value = obj.get_int();                                     \
      } else if (obj.is_number()) {                                      \
        if (!obj.get_number().is_valid_int64(param_value)) {             \
          number::ObNumber number = obj.get_number();                    \
          if (OB_FAIL(number.round(0))) {                                \
            LOG_WARN("failed to round number", K(ret), K(number));       \
          } else if (!number.is_valid_int64(param_value)) {              \
            ret = OB_ARRAY_OUT_OF_RANGE;                                 \
            LOG_WARN("array index is out of range", K(ret), K(number));  \
          }                                                              \
        }                                                                \
      } else {                                                           \
        ret = OB_ERR_UNEXPECTED;                                         \
        LOG_WARN("obj param is invalid type", K(obj), K(i));             \
      }                                                                  \
      if (OB_SUCC(ret) && OB_FAIL(param_array.push_back(param_value))) { \
        LOG_WARN("store param array failed", K(ret), K(i));              \
      }                                                                  \
    }                                                                    \
  } while (0)

int ObExprObjAccess::init_param_array(
    const ParamStore& param_store, const ObObj* objs_stack, int64_t param_num, ParamArray& param_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_idxs_.count(); ++i) {
    CK(param_idxs_.at(i) >= 0 && param_idxs_.at(i) < param_store.count());
    if (OB_SUCC(ret)) {
      const ObObjParam& obj_param = param_store.at(param_idxs_.at(i));
      GET_VALID_INT64_PARAM(obj_param);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    const ObObj& obj = objs_stack[i];
    GET_VALID_INT64_PARAM(obj);
  }
  return ret;
}

#define SET_RESULT(res, is_null) \
  do {                           \
    if (OB_SUCC(ret)) {          \
      if (is_null) {             \
        result.set_null();       \
      } else {                   \
        result.set_int(res);     \
      }                          \
    }                            \
  } while (0)

int ObExprObjAccess::calc_result(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, const ParamStore& param_store) const
{
  int ret = OB_SUCCESS;
  typedef int32_t (*GetAttr)(int64_t, int64_t[], int64_t*);
  GetAttr get_attr = reinterpret_cast<GetAttr>(get_attr_func_);
  ParamArray param_array;
  CK(OB_NOT_NULL(get_attr));
  OZ(init_param_array(param_store, objs_stack, param_num, param_array));
  if (OB_SUCC(ret)) {
    int64_t* param_ptr = const_cast<int64_t*>(param_array.head());
    int64_t attr_addr = 0;
    if (OB_FAIL(get_attr(param_array.count(), param_ptr, &attr_addr))) {
      LOG_WARN("failed to get attr", K(ret));
    } else if (OB_UNLIKELY(0 == attr_addr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get attribute failed", K(ret), K(attr_addr));
    } else if (for_write_ || get_result_type().is_ext()) {
      result.set_extend(attr_addr, get_result_type().get_extend_type());
    } else {
      ObObj* datum = reinterpret_cast<ObObj*>(attr_addr);
      if (OB_FAIL(result.apply(*datum))) {
        LOG_WARN("apply failed", K(ret), KPC(datum), K(result), K(get_result_type().get_obj_meta()));
      }

      if (OB_SUCC(ret)) {
        if ((ObLongTextType == result.get_meta().get_type() && get_result_type().get_obj_meta().is_lob_locator()) ||
            (result.get_meta().is_lob_locator() && ObLongTextType == get_result_type().get_type())) {
        } else if (!result.is_null() && result.get_meta().get_type() != get_result_type().get_obj_meta().get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("obj access result meta not equel to expr type",
              K(ret),
              KPC(datum),
              K(result),
              K(get_result_type().get_obj_meta()));
        }
      }
    }
  }
  return ret;
}

int ObExprObjAccess::calc_resultN(ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr_ctx.phy_plan_ctx_));
  if (OB_SUCC(ret)) {
    const ParamStore& param_store = expr_ctx.phy_plan_ctx_->get_param_store();
    if (OB_FAIL(calc_result(result, objs_stack, param_num, param_store))) {
      LOG_WARN("failed to calc result inner", K(ret));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
