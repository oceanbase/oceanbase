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
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObExprCollPred, ObExprOperator), ms_type_, ms_modifier_);

typedef hash::ObHashMap<ObObj, int64_t, common::hash::NoPthreadDefendMode> LocalNTSHashMap;

ObExprCollPred::ObExprCollPred(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_OP_COLL_PRED, N_COLL_PRED, 2, NOT_ROW_DIMENSION)
{}

ObExprCollPred::~ObExprCollPred()
{}

void ObExprCollPred::reset()
{
  ms_type_ = ObMultiSetType::MULTISET_TYPE_INVALID;
  ms_modifier_ = ObMultiSetModifier::MULTISET_MODIFIER_INVALID;
  ObExprOperator::reset();
}

int ObExprCollPred::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_COLL_PRED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprCollPred& other_expr = static_cast<const ObExprCollPred&>(other);
    ms_type_ = other_expr.ms_type_;
    ms_modifier_ = other_expr.ms_modifier_;
  }
  return ret;
}

int ObExprCollPred::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    type.set_type(ObTinyIntType);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("multiset operator not support non udt type", K(type1), K(type2), K(ret));
  }

  return ret;
}

#define FILL_HASH_MAP(map_name, coll, map_cnt)                                           \
  do {                                                                                   \
    if (OB_FAIL(map_name.create(coll->get_actual_count(), ObModIds::OB_SQL_HASH_SET))) { \
      LOG_WARN("fail create hash map", K(coll->get_actual_count()), K(ret));             \
    } else {                                                                             \
      const ObObj* elem = NULL;                                                          \
      bool is_del = false;                                                               \
      ObObj* data_arr = static_cast<ObObj*>(coll->get_data());                           \
      for (int64_t i = 0; OB_SUCC(ret) && i < coll->get_count(); ++i) {                  \
        if (OB_FAIL(coll->is_elem_deleted(i, is_del))) {                                 \
          LOG_WARN("failed to check is collection elem deleted", K(ret));                \
        } else if (!is_del) {                                                            \
          elem = data_arr + i;                                                           \
          ret = map_name.set_refactored(*elem, i);                                       \
          if (OB_HASH_EXIST == ret) {                                                    \
            ret = OB_SUCCESS;                                                            \
            continue;                                                                    \
          } else if (OB_SUCC(ret)) {                                                     \
            map_cnt++;                                                                   \
          } else {                                                                       \
            LOG_WARN("insert elem into hashmap failed.", K(ret));                        \
          }                                                                              \
        }                                                                                \
      }                                                                                  \
    }                                                                                    \
  } while (0)

int ObExprCollPred::compare_obj(const ObObj& obj1, const ObObj& obj2, ObCompareCtx& cmp_ctx)
{
  UNUSED(cmp_ctx);
  return obj1 == obj2 ? 0 : 1;
}

int ObExprCollPred::calc_is_submultiset(const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx,
    const ObExprCalcType calc_type, CollectionPredRes& result)
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  result = COLL_PRED_FALSE;
  if (obj1.get_meta().is_ext() && obj2.get_meta().is_ext()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(calc_type), K(result));
  }
  return ret;
}

int ObExprCollPred::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    switch (ms_type_) {
      case MULTISET_TYPE_SUBMULTISET: {
        CollectionPredRes res = COLL_PRED_INVALID;
        if (OB_FAIL(calc_is_submultiset(obj1, obj2, expr_ctx, result_type_.get_calc_meta(), res))) {
          LOG_WARN("failed to call submultiset", K(ret));
        } else {
          if (COLL_PRED_NULL == res) {
            result.set_null();
          } else {
            if (res ^ (MULTISET_MODIFIER_NOT == ms_modifier_)) {
              result.set_tinyint(1);
            } else {
              result.set_tinyint(0);
            }
          }
        }
      } break;
      case MULTISET_TYPE_MEMBER_OF: {
        if (obj2.get_meta().is_ext()) {
          ret = OB_NOT_SUPPORTED;
        }
      } break;
      case MULTISET_TYPE_IS_SET: {
        if (obj1.get_meta().is_ext()) {
          ret = OB_NOT_SUPPORTED;
        }
      } break;
      case MULTISET_TYPE_EMPTY: {
        if (obj1.get_meta().is_ext()) {
          ret = OB_NOT_SUPPORTED;
        }
      } break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown collection predicate condition", K(ret));
        break;
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
