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

#include "lib/charset/ob_charset.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_oracle_nullif.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace sql {

ObExprOracleNullif::ObExprOracleNullif(ObIAllocator& alloc) : ObExprNullif(alloc)
{}

int ObExprOracleNullif::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  CK(OB_NOT_NULL(session));
  CK(2 == param_num);
  if (OB_SUCC(ret)) {
    ObExprResType& type1 = types[0];
    ObExprResType& type2 = types[1];
    if (type1.is_null() || type1.is_lob()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", ob_obj_type_str(type2.get_type()));
      LOG_WARN("invalid type of parameter", K(ret), K(type1));
    } else if (is_same_type(type1, type2)) {
      if (is_numberic_type(type1.get_oracle_type(), type2.get_oracle_type())) {
        calc_numberic_type(type, type1, type2);
      } else if (ob_is_raw(type1.get_type())) {
        type.set_raw();
      } else {
        ObExprResType cmp_type;
        if (OB_FAIL(calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()))) {
          LOG_WARN("failed to calc cmp type", K(ret), K(type1), K(type2));
        } else {
          type.set_type(type1.get_type());
          type.set_accuracy(type1.get_accuracy());
          if (ob_is_string_type(type.get_type())) {
            const ObSQLSessionInfo* session = type_ctx.get_session();
            ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
            CK(OB_NOT_NULL(session));
            OZ(params.push_back(&type1));
            OZ(params.push_back(&type2));
            OZ(aggregate_string_type_and_charset_oracle(*session, params, type));
            OZ(deduce_string_param_calc_type_and_charset(*session, type, params));
            if (OB_SUCC(ret)) {
              // deduce length
              if (type.get_collation_type() == CS_TYPE_BINARY) {
                ObLength len_in_byte = -1;
                if (OB_FAIL(type1.get_length_for_meta_in_bytes(len_in_byte))) {
                  LOG_WARN("get length in bytes failed", K(ret), K(type1));
                } else {
                  type.set_length(len_in_byte);
                }
              } else {
                type.set_length(type1.get_length());
              }
            }
          }
          type.set_calc_type(cmp_type.get_calc_type());
          type.set_calc_collation(cmp_type);
        }
      }
    } else {
      if (type2.is_null()) {
        if (type1.is_number()) {
          type.set_number();
          type.set_precision(PRECISION_UNKNOWN_YET);
          type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        } else {
          type.set_meta(type1.get_obj_meta());
          type.set_accuracy(type1.get_accuracy());
        }
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(
            OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()), ob_obj_type_str(type2.get_type()));
        LOG_WARN("diff type of parameters", K(ret), K(type1), K(type2));
      }
    }
    if (OB_SUCC(ret) && session->use_static_typing_engine()) {
      type1.set_calc_meta(type.get_obj_meta());
      type2.set_calc_meta(type.get_obj_meta());
    }
  }
  return ret;
}

bool ObExprOracleNullif::is_same_type(const ObExprResType& type1, const ObExprResType& type2) const
{
  bool ret = false;
  ObObjOType oracleType1 = type1.get_oracle_type();
  ObObjOType oracleType2 = type2.get_oracle_type();
  if (oracleType1 == oracleType2 || is_numberic_type(oracleType1, oracleType2) ||
      is_string_type(oracleType1, oracleType2) || is_time_type(oracleType1, oracleType2)) {
    ret = true;
  }

  return ret;
}

bool ObExprOracleNullif::is_numberic_type(const ObObjOType& otype1, const ObObjOType& otype2) const
{
  bool ret = false;
  if ((otype1 >= ObOSmallIntType && otype1 <= ObONumberType) &&
      (otype2 >= ObOSmallIntType && otype2 <= ObONumberType)) {
    ret = true;
  }
  return ret;
}

void ObExprOracleNullif::calc_numberic_type(ObExprResType& type, ObExprResType& type1, ObExprResType& type2) const
{
  int ret = OB_SUCCESS;
  ObObjOType oracleType1 = type1.get_oracle_type();
  ObObjOType oracleType2 = type2.get_oracle_type();
  ObObjOType result = ObONumberType;
  if (oracleType1 == ObOBinFloatType || oracleType1 == ObOBinDoubleType) {
    result = oracleType1;
  }
  // enum id: binary float(5), binary double(6), number(7)
  if (oracleType2 == ObOBinFloatType || oracleType2 == ObOBinDoubleType) {
    if (result == ObONumberType) {
      result = oracleType2;
    } else {
      result = (result > oracleType2) ? result : oracleType2;
    }
  }
  if (result == ObONumberType) {
    type.set_number();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  } else if (result == ObOBinFloatType) {
    type.set_float();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  } else {
    type.set_double();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  }
  if (OB_SUCC(ret)) {
    type1.set_calc_type(type.get_type());
    type2.set_calc_type(type.get_type());
  }
}

bool ObExprOracleNullif::is_string_type(const ObObjOType& otype1, const ObObjOType& otype2) const
{
  bool ret = false;
  if (((otype1 >= ObOCharType && otype1 <= ObOVarcharType) && (otype2 >= ObOCharType && otype2 <= ObOVarcharType)) ||
      ((otype1 >= ObONVarchar2Type && otype1 <= ObONCharType) &&
          (otype2 >= ObONVarchar2Type && otype2 <= ObONCharType))) {
    ret = true;
  }
  return ret;
}

bool ObExprOracleNullif::is_time_type(const ObObjOType& otype1, const ObObjOType& otype2) const
{
  bool ret = false;
  if ((otype1 >= ObODateType && otype1 <= ObOTimestampType) && (otype2 >= ObODateType && otype2 <= ObOTimestampType)) {
    ret = true;
  }
  return ret;
}

int ObExprOracleNullif::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  CK(2 == rt_expr.arg_cnt_);

  OV(OB_NOT_NULL(
         rt_expr.inner_functions_ = reinterpret_cast<void**>(expr_cg_ctx.allocator_->alloc(sizeof(DatumCmpFunc) * 1))),
      OB_ALLOCATE_MEMORY_FAILED);

  if (OB_SUCC(ret)) {
    DatumCmpFunc cmp_func = NULL;
    const ObDatumMeta& left_meta = rt_expr.args_[0]->datum_meta_;
    const ObDatumMeta& right_meta = rt_expr.args_[1]->datum_meta_;
    const ObCollationType cmp_cs_type = left_meta.cs_type_;
    CK(left_meta.cs_type_ == right_meta.cs_type_);
    CK(OB_NOT_NULL(cmp_func = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                       left_meta.type_, right_meta.type_, lib::is_oracle_mode(), cmp_cs_type)));
    OX(rt_expr.inner_func_cnt_ = 1);
    OX(rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func));
    OX(rt_expr.eval_func_ = first_param_can_be_null_ ? eval_nullif : eval_nullif_not_null);
  }
  return ret;
}

int ObExprOracleNullif::eval_nullif(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* l = NULL;
  ObDatum* r = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, l, r))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (l->is_null()) {
    res.set_null();
  } else if (r->is_null()) {
    // left is not null, right is null, not equal
    res.set_datum(*l);
  } else {
    bool equal = (0 == reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[0])(*l, *r));
    if (equal) {
      res.set_null();
    } else {
      res.set_datum(*l);
    }
  }
  return ret;
}

int ObExprOracleNullif::eval_nullif_not_null(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* l = NULL;
  ObDatum* r = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, l, r))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (l->is_null()) {
    ObObjType type2 = expr.args_[1]->obj_meta_.get_type();
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", ob_obj_type_str(type2));
  } else if (r->is_null()) {
    // left is not null, right is null, not equal
    res.set_datum(*l);
  } else {
    bool equal = (0 == reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[0])(*l, *r));
    if (equal) {
      res.set_null();
    } else {
      res.set_datum(*l);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
