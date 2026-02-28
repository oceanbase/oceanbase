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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_TIME_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_TIME_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/vector/ob_vector_define.h"

namespace oceanbase
{
namespace sql
{

#define DISPATCH_NEED_CALC_DATE(FUNC, SRCVEC, DSTVEC, calc_type, need_calc_date)\
if (need_calc_date) {\
  ret = FUNC<SRCVEC, DSTVEC, true>(expr, ctx, calc_type, skip, bound);\
} else {\
  ret = FUNC<SRCVEC, DSTVEC, false>(expr, ctx, calc_type, skip, bound);\
}

#define DISPATCH_STRING_CALC_EXPR_VECTOR(FUNC, calc_type, need_calc_date)\
if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, DiscVec), CONCAT(Integer, FixedVec), calc_type, need_calc_date)\
} else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, DiscVec), CONCAT(Integer, UniVec), calc_type, need_calc_date)\
} else if (VEC_DISCRETE == arg_format && VEC_UNIFORM_CONST == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, DiscVec), CONCAT(Integer,UniCVec), calc_type, need_calc_date)\
} else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, UniVec), CONCAT(Integer, FixedVec), calc_type, need_calc_date)\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, UniVec), CONCAT(Integer, UniVec), calc_type, need_calc_date)\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, UniVec), CONCAT(Integer, UniCVec), calc_type, need_calc_date)\
} else if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, ContVec), CONCAT(Integer, FixedVec), calc_type, need_calc_date)\
} else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, ContVec), CONCAT(Integer, UniVec), calc_type, need_calc_date)\
} else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM_CONST == res_format) {\
  DISPATCH_NEED_CALC_DATE(FUNC, CONCAT(Str, ContVec), CONCAT(Integer, UniCVec), calc_type, need_calc_date)\
} else { \
  DISPATCH_NEED_CALC_DATE(FUNC, ObVectorBase, ObVectorBase, calc_type, need_calc_date)\
}

#define DISPATCH_INTEGER_CALC_EXPR_VECTOR(FUNC, calc_type)\
if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
  ret = FUNC<CONCAT(Integer, FixedVec), CONCAT(Integer, FixedVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Integer, FixedVec), CONCAT(Integer, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = FUNC<CONCAT(Integer, FixedVec), CONCAT(Integer,UniCVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
  ret = FUNC<CONCAT(Integer, UniVec), CONCAT(Integer, FixedVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Integer, UniVec), CONCAT(Integer, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = FUNC<CONCAT(Integer, UniVec), CONCAT(Integer, UniCVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
  ret = FUNC<CONCAT(Integer, UniCVec), CONCAT(Integer, FixedVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Integer, UniCVec), CONCAT(Integer, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = FUNC<CONCAT(Integer, UniCVec), CONCAT(Integer, UniCVec)>(expr, ctx, calc_type, skip, bound);\
} else { \
  ret = FUNC<ObVectorBase, ObVectorBase>(expr, ctx, calc_type, skip, bound);\
}

#define DISPATCH_STRING_NAME_EXPR_VECTOR(FUNC, calc_type)\
if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {\
  ret = FUNC<CONCAT(Str, DiscVec), CONCAT(Str, DiscVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Str, DiscVec), CONCAT(Str, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_DISCRETE == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = FUNC<CONCAT(Str, DiscVec), CONCAT(Str, ContVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {\
  ret = FUNC<CONCAT(Str, UniVec),  CONCAT(Str, DiscVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Str, UniVec), CONCAT(Str, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = FUNC<CONCAT(Str, UniVec), CONCAT(Str, ContVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {\
  ret = FUNC<CONCAT(Str, ContVec),  CONCAT(Str, DiscVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Str, ContVec), CONCAT(Str, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_CONTINUOUS == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = FUNC<CONCAT(Str, ContVec), CONCAT(Str, ContVec)>(expr, ctx, calc_type, skip, bound);\
} else { \
  ret = FUNC<ObVectorBase, ObVectorBase>(expr, ctx, calc_type, skip, bound);\
}

#define DISPATCH_INTEGER_NAME_EXPR_VECTOR(FUNC, calc_type)\
if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {\
  ret = FUNC<CONCAT(Integer, FixedVec), CONCAT(Str, DiscVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Integer, FixedVec), CONCAT(Str, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = FUNC<CONCAT(Integer, FixedVec), CONCAT(Str, ContVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {\
  ret = FUNC<CONCAT(Integer, UniVec),  CONCAT(Str, DiscVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Integer, UniVec), CONCAT(Str, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = FUNC<CONCAT(Integer, UniVec), CONCAT(Str, ContVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_DISCRETE == res_format) {\
  ret = FUNC<CONCAT(Integer, UniCVec),  CONCAT(Str, DiscVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = FUNC<CONCAT(Integer, UniCVec), CONCAT(Str, UniVec)>(expr, ctx, calc_type, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = FUNC<CONCAT(Integer, UniCVec), CONCAT(Str, ContVec)>(expr, ctx, calc_type, skip, bound);\
} else { \
  ret = FUNC<ObVectorBase, ObVectorBase>(expr, ctx, calc_type, skip, bound);\
}

#define DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(ArgVec, ResVec, NeedCalcDate) \
  template int ObExprTimeBase::calc_for_string_vector< \
      ArgVec, ResVec, NeedCalcDate>(const ObExpr &expr, ObEvalCtx &ctx, int calc_type, \
                                   const ObBitVectorImpl<uint64_t> &skip, const EvalBound &bound);

#define DEFINE_ALL_DAYOF_STRING_EXPR_VECTORS() \
  /* 使用宏来生成所有需要的实例化 */ \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrDiscVec, IntegerFixedVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrDiscVec, IntegerUniVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrDiscVec, IntegerUniCVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrUniVec, IntegerFixedVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrUniVec, IntegerUniVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrUniVec, IntegerUniCVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrContVec, IntegerFixedVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrContVec, IntegerUniVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(StrContVec, IntegerUniCVec, true) \
  DEFINE_DAYOF_STRING_EXPR_VECTOR_INSTANTIATION(ObVectorBase, ObVectorBase, true)

class ObExprTime : public ObFuncExprOperator
{
public:
  explicit ObExprTime(common::ObIAllocator &alloc);
  virtual ~ObExprTime();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN; }
  static int calc_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTime);
};

class ObExprTimeBase : public ObFuncExprOperator
{
public:
  explicit ObExprTimeBase(common::ObIAllocator &alloc, int32_t date_type, ObExprOperatorType type,
                          const char *name, ObValidForGeneratedColFlag valid_for_generated_col);
  virtual ~ObExprTimeBase();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                  int32_t type, bool with_date, bool is_allow_incomplete_dates = false);
  template <typename ArgVec, typename ResVec, bool NeedCalcDate = false>
  static int calc_for_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound);
  template <typename ArgVec, typename ResVec>
  static int calc_for_integer_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound);
  template <typename ArgVec, typename ResVec>
  static int calc_name_for_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound);
  template <typename ArgVec, typename ResVec>
  static int calc_name_for_integer_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound);
  template <typename ArgVec, typename ResVec, typename IN_TYPE, int DT_PART>
  static int calc_for_date_vector(const ObExpr &expr, ObEvalCtx &ctx,
    const ObBitVector &skip, const EvalBound &bound);
  DECLARE_SET_LOCAL_SESSION_VARS;
private :
  int32_t dt_type_;
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeBase);
};
inline int ObExprTimeBase::calc_result_type1(ObExprResType &type,
                                           ObExprResType &type1,
                                           common::ObExprTypeCtx &type_ctx) const
{
  type.set_int32();
  type.set_precision(4);
  type.set_scale(0);
  common::ObObjTypeClass tc1 = ob_obj_type_class(type1.get_type());
  if (ob_is_enum_or_set_type(type1.get_type()) || ob_is_string_type(type1.get_type())) {
    type1.set_calc_type_default_varchar();
  } else if ((common::ObFloatTC == tc1) || (common::ObDoubleTC == tc1)) {
    type1.set_calc_type(common::ObNumberType);
  }
  return common::OB_SUCCESS;
}

class ObExprHour : public ObExprTimeBase
{
public:
  explicit ObExprHour(common::ObIAllocator &alloc);
  virtual ~ObExprHour();
  static int calc_hour(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_hour_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprHour);
};

class ObExprMinute: public ObExprTimeBase
{
public:
  ObExprMinute();
  explicit ObExprMinute(common::ObIAllocator &alloc);
  virtual ~ObExprMinute();
  static int calc_minute(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_minute_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMinute);
};

class ObExprSecond: public ObExprTimeBase
{
public:
  ObExprSecond();
  explicit ObExprSecond(common::ObIAllocator &alloc);
  virtual ~ObExprSecond();
  static int calc_second(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_second_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSecond);
};

class ObExprMicrosecond: public ObExprTimeBase
{
public:
  ObExprMicrosecond();
  explicit ObExprMicrosecond(common::ObIAllocator &alloc);
  virtual ~ObExprMicrosecond();
  static int calc_microsecond(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_microsecond_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMicrosecond);
};

class ObExprYear: public ObExprTimeBase
{
public:
  ObExprYear();
  explicit ObExprYear(common::ObIAllocator &alloc);
  virtual ~ObExprYear();
  static int calc_year(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_year_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprYear);
};

class ObExprMonth: public ObExprTimeBase
{
public:
  ObExprMonth();
  explicit ObExprMonth(common::ObIAllocator &alloc);
  virtual ~ObExprMonth();
  static int calc_month(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_month_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMonth);
};

class ObExprMonthName: public ObExprTimeBase
{
public:
  ObExprMonthName();
  explicit ObExprMonthName(common::ObIAllocator &alloc);
  virtual ~ObExprMonthName();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_month_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_month_name_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static const char* get_month_name(int month);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMonthName);
};
}
}


#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_TIME_H_ */
