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

#define USING_LOG_PREFIX COMMON

#include "lib/utility/utility.h"
#include "common/object/ob_obj_compare.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "common/object/ob_object.h"
#include "lib/worker.h"
#include "lib/json_type/ob_json_base.h" // for ObIJsonBase
#include "lib/json_type/ob_json_bin.h" // for ObJsonBin
#include "lib/wide_integer/ob_wide_integer_cmp_funcs.h"

namespace oceanbase
{
namespace common
{

bool is_calc_with_end_space(ObObjType type1, ObObjType type2,
                            bool is_oracle_mode,
                            ObCollationType cs_type1,
                            ObCollationType cs_type2)
{
  return is_oracle_mode && ( (ObVarcharType == type1 && CS_TYPE_BINARY != cs_type1)
                             || (ObVarcharType == type2 && CS_TYPE_BINARY != cs_type2)
                             || (ObNVarchar2Type == type1)
                             || (ObNVarchar2Type == type2) );
}
#define OBJ_TYPE_CLASS_CHECK(obj, tc)\
  if (OB_UNLIKELY(obj.get_type_class() != tc)) { \
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "unexpected error. mismatch function for comparison", K(obj), K(tc));\
    right_to_die_or_duty_to_live();\
  }

#define CALC_WITH_END_SPACE(ob1, ob2, cmctx)                 \
  is_calc_with_end_space(ob1.get_type(),                     \
                          ob2.get_type(),                    \
                          lib::is_oracle_mode(),             \
                          ob1.get_collation_type(),          \
                          ob2.get_collation_type()           \
                          )

#define DEFINE_CMP_OP_FUNC(tc, type, op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<tc, tc, op>(const ObObj &obj1, \
                                             const ObObj &obj2, \
                                             const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, tc);\
    return obj1.get_##type() op_str obj2.get_##type(); \
  }

#define DEFINE_CMP_FUNC(tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<tc, tc>(const ObObj &obj1, \
                                      const ObObj &obj2, \
                                      const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, tc);\
    return obj1.get_##type() < obj2.get_##type() \
           ? CR_LT \
           : obj1.get_##type() > obj2.get_##type() \
             ? CR_GT \
             : CR_EQ; \
  }

#define DEFINE_CMP_OP_FUNC_NULL_NULL(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNullTC, ObNullTC, op>(const ObObj &/*obj1*/, \
                                                         const ObObj &/*obj2*/, \
                                                         const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? static_cast<int>(0 op_str 0) : CR_NULL; \
  }

#define DEFINE_CMP_FUNC_NULL_NULL() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNullTC, ObNullTC>(const ObObj &/*obj1*/, \
                                                  const ObObj &/*obj2*/, \
                                                  const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? CR_EQ : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_EXT_EXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObExtendTC, ObExtendTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return (obj1.is_min_value() && obj2.is_min_value()) || (obj1.is_max_value() && obj2.is_max_value()) \
           ? static_cast<int>(0 op_str 0) \
           : obj1.is_min_value() || obj2.is_max_value() \
             ? static_cast<int>(-1 op_str 1) \
             : obj1.is_max_value() || obj2.is_min_value() \
               ? static_cast<int>(1 op_str -1) \
               : CR_OB_ERROR; \
  }

#define DEFINE_CMP_FUNC_EXT_EXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObExtendTC, ObExtendTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return (obj1.is_min_value() && obj2.is_min_value()) || (obj1.is_max_value() && obj2.is_max_value()) \
           ? CR_EQ \
           : obj1.is_min_value() || obj2.is_max_value() \
             ? CR_LT \
             : obj1.is_max_value() || obj2.is_min_value() \
               ? CR_GT \
               : CR_OB_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_NULL_EXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNullTC, ObExtendTC, op>(const ObObj &/*obj1*/, \
                                                           const ObObj &obj2, \
                                                           const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? obj2.is_min_value() \
             ? static_cast<int>(0 op_str -1) \
             : obj2.is_max_value() \
               ? static_cast<int>(0 op_str 1) \
               : CR_OB_ERROR \
           : CR_NULL; \
  }

#define DEFINE_CMP_FUNC_NULL_EXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNullTC, ObExtendTC>(const ObObj &/*obj1*/, \
                                                    const ObObj &obj2, \
                                                    const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? obj2.is_min_value() \
             ? CR_GT \
             : obj2.is_max_value() \
               ? CR_LT \
               : CR_OB_ERROR \
           : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_EXT_NULL(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObExtendTC, ObNullTC, op>(const ObObj &obj1, \
                                                           const ObObj &obj2, \
                                                           const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObNullTC, ObExtendTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_EXT_NULL() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObExtendTC, ObNullTC>(const ObObj &obj1, \
                                                    const ObObj &/*obj2*/, \
                                                    const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? obj1.is_min_value() \
             ? CR_LT \
             : obj1.is_max_value() \
               ? CR_GT \
               : CR_OB_ERROR \
           : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_NULL_XXX(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNullTC, ObMaxTC, op>(const ObObj &/*obj1*/, \
                                                        const ObObj &/*obj2*/, \
                                                        const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? NULL_LAST == cmp_ctx.null_pos_ \
             ? static_cast<int>(1 op_str 0) \
             : static_cast<int>(0 op_str 1) \
           : CR_NULL; \
  }

#define DEFINE_CMP_FUNC_NULL_XXX() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNullTC, ObMaxTC>(const ObObj &/*obj1*/, \
                                                 const ObObj &/*obj2*/, \
                                                 const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? NULL_LAST == cmp_ctx.null_pos_ \
             ? CR_GT \
             : CR_LT \
           : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_XXX_NULL(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObMaxTC, ObNullTC, op>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObNullTC, ObMaxTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_XXX_NULL(tc) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObMaxTC, ObNullTC>(const ObObj &/*obj1*/, \
                                                 const ObObj &/*obj2*/, \
                                                 const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? NULL_LAST == cmp_ctx.null_pos_ \
             ? CR_LT \
             : CR_GT \
           : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_XXX_EXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObMaxTC, ObExtendTC, op>(const ObObj &/*obj1*/, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj2.is_min_value() \
           ? static_cast<int>(0 op_str -1) \
           : obj2.is_max_value() \
             ? static_cast<int>(0 op_str 1) \
             : CR_OB_ERROR; \
  }

#define DEFINE_CMP_FUNC_XXX_EXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObMaxTC, ObExtendTC>(const ObObj &/*obj1*/, \
                                                   const ObObj &obj2, \
                                                   const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj2.is_min_value() \
           ? CR_GT \
           : obj2.is_max_value() \
             ? CR_LT \
             : CR_OB_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_EXT_XXX(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObExtendTC, ObMaxTC, op>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObMaxTC, ObExtendTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_EXT_XXX() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObExtendTC, ObMaxTC>(const ObObj &obj1, \
                                                   const ObObj &/*obj2*/, \
                                                   const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj1.is_min_value() \
           ? CR_LT \
           : obj1.is_max_value() \
             ? CR_GT \
             : CR_OB_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_INT_UINT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObIntTC, ObUIntTC, op>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObIntTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObUIntTC);\
    return obj1.get_int() < 0 \
           ? obj1.get_int() op_str 0 \
           : obj1.get_uint64() op_str obj2.get_uint64(); \
  }

#define DEFINE_CMP_FUNC_INT_UINT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObIntTC, ObUIntTC>(const ObObj &obj1, \
                                                 const ObObj &obj2, \
                                                 const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObIntTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObUIntTC);\
    return (obj1.get_int() < 0 || obj1.get_uint64() < obj2.get_uint64()) \
           ? CR_LT \
           : obj1.get_uint64() > obj2.get_uint64() \
             ? CR_GT \
             : CR_EQ; \
  }

#define IS_FIXED_DOUBLE_CMP obj1.is_fixed_double() && obj2.is_fixed_double() && \
  lib::is_mysql_mode()

#define DEFINE_CMP_OP_FUNC_REAL_REAL_EQ(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real1_tc, real2_tc, CO_EQ>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_FALSE; \
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real2_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_TRUE;\
      } else if (isnan(l_num)) {\
        ret = CR_FALSE;\
      } else {\
        ret = CR_FALSE;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      ret = fixed_double_cmp(obj1, obj2) == 0 ? CR_TRUE : CR_FALSE;\
    } else {\
      ret = (l_num == r_num ? CR_TRUE : CR_FALSE);\
    }\
    return ret;\
  }

#define DEFINE_CMP_OP_FUNC_REAL_REAL_LE(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real1_tc, real2_tc, CO_LE>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_FALSE;\
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real2_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_TRUE;\
      } else if (isnan(l_num)) {\
        ret = CR_FALSE;\
      } else {\
        ret = CR_TRUE;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      ret = fixed_double_cmp(obj1, obj2) <= 0 ? CR_TRUE : CR_FALSE;\
    } else {\
      ret = (l_num <= r_num ? CR_TRUE : CR_FALSE);\
    }\
    return ret;\
  }

  #define DEFINE_CMP_OP_FUNC_REAL_REAL_LT(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real1_tc, real2_tc, CO_LT>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_FALSE; \
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real1_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_FALSE;\
      } else if (isnan(l_num)) {\
        ret = CR_FALSE;\
      } else {\
        ret = CR_TRUE;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      ret = fixed_double_cmp(obj1, obj2) < 0 ? CR_TRUE : CR_FALSE;\
    } else {\
      ret = (l_num < r_num ? CR_TRUE : CR_FALSE);\
    }\
    return ret;\
  }

  #define DEFINE_CMP_OP_FUNC_REAL_REAL_GE(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real1_tc, real2_tc, CO_GE>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_FALSE; \
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real2_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_TRUE;\
      } else if (isnan(l_num)) {\
        ret = CR_TRUE;\
      } else {\
        ret = CR_FALSE;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      ret = fixed_double_cmp(obj1, obj2) >= 0 ? CR_TRUE : CR_FALSE;\
    } else {\
      ret = (l_num >= r_num ? CR_TRUE : CR_FALSE);\
    }\
    return ret;\
  }

  #define DEFINE_CMP_OP_FUNC_REAL_REAL_GT(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real1_tc, real2_tc, CO_GT>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_FALSE; \
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real2_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_FALSE;\
      } else if (isnan(l_num)) {\
        ret = CR_TRUE;\
      } else {\
        ret = CR_FALSE;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      ret = fixed_double_cmp(obj1, obj2) > 0 ? CR_TRUE : CR_FALSE;\
    } else {\
      ret = (l_num > r_num ? CR_TRUE : CR_FALSE);\
    }\
    return ret;\
  }

  #define DEFINE_CMP_OP_FUNC_REAL_REAL_NE(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real1_tc, real2_tc, CO_NE>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_FALSE; \
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real2_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_FALSE;\
      } else {\
        ret = CR_TRUE;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      ret = fixed_double_cmp(obj1, obj2) != 0 ? CR_TRUE : CR_FALSE;\
    } else {\
      ret = (l_num != r_num ? CR_TRUE : CR_FALSE);\
    }\
    return ret;\
  }

#define DEFINE_CMP_FUNC_REAL_REAL(real1_tc, real1_type, real2_tc, real2_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<real1_tc, real2_tc>(const ObObj &obj1, \
                                                 const ObObj &obj2, \
                                                 const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, real1_tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real2_tc);\
    ObCmpRes ret = CR_EQ; \
    real1_type l_num = obj1.get_##real1_type();\
    real2_type r_num = obj2.get_##real2_type();\
    if (isnan(l_num) || isnan(r_num)) {\
      if (isnan(l_num) && isnan(r_num)) {\
        ret = CR_EQ;\
      } else if (isnan(l_num)) {\
        ret = CR_GT;\
      } else {\
        ret = CR_LT;\
      }\
    } else if (IS_FIXED_DOUBLE_CMP) {\
      int cmp_res = fixed_double_cmp(obj1, obj2);\
      ret = cmp_res == 0 ? CR_EQ : (cmp_res < 0 ? CR_LT : CR_GT);\
    } else {\
      ret = l_num == r_num ? CR_EQ : (l_num < r_num ? CR_LT : CR_GT);\
    }\
    return ret;\
  }


#define DEFINE_CMP_OP_FUNC_INT_ENUMSET(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObIntTC, ObEnumSetTC, op>(const ObObj &obj1, \
                                                           const ObObj &obj2, \
                                                           const ObCompareCtx &/*cmp_ctx*/) \
{ \
    OBJ_TYPE_CLASS_CHECK(obj1, ObIntTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObEnumSetTC);\
  int int_ret = obj1.get_int() < 0 \
           ? obj1.get_int() op_str 0 \
           : obj1.get_uint64() op_str obj2.get_uint64(); \
  return int_ret;\
  }

#define DEFINE_CMP_FUNC_INT_ENUMSET() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObIntTC, ObEnumSetTC>(const ObObj &obj1, \
                                                  const ObObj &obj2, \
                                                  const ObCompareCtx &/*cmp_ctx*/) \
{ \
    OBJ_TYPE_CLASS_CHECK(obj1, ObIntTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObEnumSetTC);\
    return (obj1.get_int() < 0 || obj1.get_uint64() < obj2.get_uint64()) \
           ? CR_LT \
           : obj1.get_uint64() > obj2.get_uint64() \
             ? CR_GT \
             : CR_EQ; \
  }


// obj1 LE obj2 is equal to obj2 GE obj1, we say that LE and GE is symmetric.
// so sym_op is short for symmetric operator, which is used for reuse other functions.
#define DEFINE_CMP_OP_FUNC_UINT_INT(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObUIntTC, ObIntTC, op>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObIntTC, ObUIntTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_UINT_INT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObUIntTC, ObIntTC>(const ObObj &obj1, \
                                                 const ObObj &obj2, \
                                                 const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<ObIntTC, ObUIntTC>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_OP_FUNC_UINT_ENUMSET(op, op_str) \
    template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObUIntTC, ObEnumSetTC, op>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObUIntTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObEnumSetTC);\
  return  obj1.get_uint64() op_str obj2.get_uint64(); \
}

#define DEFINE_CMP_FUNC_UINT_ENUMSET() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObUIntTC, ObEnumSetTC>(const ObObj &obj1, \
                                                  const ObObj &obj2, \
                                                  const ObCompareCtx &/*cmp_ctx*/) \
{ \
    OBJ_TYPE_CLASS_CHECK(obj1, ObUIntTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObEnumSetTC);\
    return (obj1.get_uint64() < obj2.get_uint64() ? CR_LT \
           : obj1.get_uint64() > obj2.get_uint64() \
             ? CR_GT \
             : CR_EQ);\
}

#define DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<tc, real_tc, op>(const ObObj &obj1, \
                                                  const ObObj &obj2, \
                                                  const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real_tc);\
    return static_cast<double>(obj1.get_##type()) op_str static_cast<double>(obj2.get_##real_type()); \
  }

#define DEFINE_CMP_FUNC_XXX_REAL(tc, type, real_tc, real_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<tc, real_tc>(const ObObj &obj1, \
                                           const ObObj &obj2, \
                                           const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, real_tc);\
    return static_cast<double>(obj1.get_##type()) < static_cast<double>(obj2.get_##real_type()) \
           ? CR_LT \
           : static_cast<double>(obj1.get_##type()) > static_cast<double>(obj2.get_##real_type()) \
             ? CR_GT \
             : CR_EQ; \
  }

#define DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real_tc, tc, op>(const ObObj &obj1, \
                                               const ObObj &obj2, \
                                               const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<tc, real_tc, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_REAL_XXX(real_tc, real_type, tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<real_tc, tc>(const ObObj &obj1, \
                                           const ObObj &obj2, \
                                           const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<tc, real_tc>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<tc, ObNumberTC, op>(const ObObj &obj1, \
                                                     const ObObj &obj2, \
                                                     const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);\
    int val = 0 ; \
    if (tc == ObNumberTC) {\
      if (CO_EQ == op) { \
        val = number::ObNumber::is_equal(obj2.nmb_desc_, obj2.v_.nmb_digits_, obj1.nmb_desc_, obj1.v_.nmb_digits_);\
      } else { \
        val = 0 op_str number::ObNumber::compare(obj2.nmb_desc_, obj2.v_.nmb_digits_, obj1.nmb_desc_, obj1.v_.nmb_digits_); \
      } \
    } else {\
      if (CO_EQ == op) { \
        val = obj2.get_number().is_equal(obj1.get_##type()); \
      } else { \
        val = 0 op_str obj2.get_number().compare(obj1.get_##type()); \
      } \
    }\
    return val; \
  }

#define DEFINE_CMP_FUNC_XXX_NUMBER(tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<tc, ObNumberTC>(const ObObj &obj1, \
                                              const ObObj &obj2, \
                                              const ObCompareCtx &/*cmp_ctx*/) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, tc);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);\
    int val = 0 ; \
    if (tc == ObNumberTC) {\
      val = number::ObNumber::compare(obj2.nmb_desc_, obj2.v_.nmb_digits_, obj1.nmb_desc_, obj1.v_.nmb_digits_); \
    } else {\
      val = obj2.get_number().compare(obj1.get_##type()); \
    }\
    return -INT_TO_CR(val); \
  }

#define DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, op, sys_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNumberTC, tc, op>(const ObObj &obj1, \
                                                     const ObObj &obj2, \
                                                     const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<tc, ObNumberTC, sys_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_NUMBER_XXX(tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNumberTC, tc>(const ObObj &obj1, \
                                              const ObObj &obj2, \
                                              const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<tc, ObNumberTC>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_OP_FUNC_ENUMSET_INT(op, sys_op) \
  template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObEnumSetTC, ObIntTC, op>(const ObObj &obj1, \
                                                    const ObObj &obj2, \
                                                    const ObCompareCtx &cmp_ctx) \
{ \
  return ObObjCmpFuncs::cmp_op_func<ObIntTC, ObEnumSetTC, sys_op>(obj2, obj1, cmp_ctx); \
}

#define DEFINE_CMP_FUNC_ENUMSET_INT() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObEnumSetTC, ObIntTC>(const ObObj &obj1, \
                                             const ObObj &obj2, \
                                             const ObCompareCtx &cmp_ctx) \
{ \
  return -ObObjCmpFuncs::cmp_func<ObIntTC, ObEnumSetTC>(obj2, obj1, cmp_ctx); \
}

#define DEFINE_CMP_OP_FUNC_ENUMSET_UINT(op, sys_op) \
  template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObEnumSetTC, ObUIntTC, op>(const ObObj &obj1, \
                                                    const ObObj &obj2, \
                                                    const ObCompareCtx &cmp_ctx) \
{ \
  return ObObjCmpFuncs::cmp_op_func<ObUIntTC, ObEnumSetTC, sys_op>(obj2, obj1, cmp_ctx); \
}

#define DEFINE_CMP_FUNC_ENUMSET_UINT() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObEnumSetTC, ObUIntTC>(const ObObj &obj1, \
                                             const ObObj &obj2, \
                                             const ObCompareCtx &cmp_ctx) \
{ \
  return -ObObjCmpFuncs::cmp_func<ObUIntTC, ObEnumSetTC>(obj2, obj1, cmp_ctx); \
}

#define DEFINE_CMP_OP_FUNC_STRING_STRING(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObStringTC, ObStringTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObStringTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObStringTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
	  return CS_TYPE_INVALID != cs_type \
           ? static_cast<int>(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                                  obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0) \
           : CR_OB_ERROR; \
  }

#define DEFINE_CMP_FUNC_STRING_STRING() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObStringTC, ObStringTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObStringTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObStringTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    return CS_TYPE_INVALID != cs_type \
           ? INT_TO_CR(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                           obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))) \
           : CR_OB_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_RAW_RAW(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObRawTC, ObRawTC, op>(const ObObj &obj1, \
                                                       const ObObj &obj2, \
                                                       const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObRawTC); \
    OBJ_TYPE_CLASS_CHECK(obj2, ObRawTC); \
    if (CS_TYPE_BINARY != obj1.get_collation_type() \
        || CS_TYPE_BINARY != obj2.get_collation_type()) { \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(cmp_ctx.cmp_cs_type_)); \
    } else { \
      ret = static_cast<int>(ObCharset::strcmpsp(CS_TYPE_BINARY, obj1.v_.string_, obj1.val_len_, \
                                                 obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0); \
    } \
    return ret; \
  }

#define DEFINE_CMP_FUNC_RAW_RAW() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObRawTC, ObRawTC>(const ObObj &obj1, \
                                                const ObObj &obj2, \
                                                const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObRawTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObRawTC);\
    if (CS_TYPE_BINARY != obj1.get_collation_type() \
        || CS_TYPE_BINARY != obj2.get_collation_type()) { \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(cmp_ctx.cmp_cs_type_)); \
    } else { \
      ret = INT_TO_CR(ObCharset::strcmpsp(CS_TYPE_BINARY, obj1.v_.string_, obj1.val_len_, \
                                          obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))); \
    } \
    return ret; \
  }


// stringtc vs texttc temporarily
// Notice:
// 1. ObObj compare only support inrow lobs, because lob manager cannot be reached from here.
//    Need to convert outrow lobs inrow before comparation
// 2. ObObj::get_string(Obstring &) only gets inrow lob data
#define DEFINE_CMP_OP_FUNC_STRING_TEXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObStringTC, ObTextTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObStringTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObTextTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    ObString data_str; \
    if (obj2.is_outrow_lob()) { \
      LOG_ERROR("not support outrow lobs", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (OB_FAIL(obj2.get_string(data_str))) { \
      LOG_ERROR("invalid text object", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else { \
      ret = CS_TYPE_INVALID != cs_type \
            ? static_cast<int>(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                                   data_str.ptr(), data_str.length(), \
                                                   CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0) \
            : CR_OB_ERROR; \
    } \
    return ret; \
  }

#define DEFINE_CMP_FUNC_STRING_TEXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObStringTC, ObTextTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObStringTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObTextTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    ObString data_str; \
    if (obj2.is_outrow_lob()) { \
      LOG_ERROR("not support outrow lobs", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (OB_FAIL(obj2.get_string(data_str))) { \
      LOG_ERROR("invalid text object", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else { \
      ret = CS_TYPE_INVALID != cs_type \
            ? INT_TO_CR(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                            data_str.ptr(), data_str.length(), \
                                            CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))) \
            : CR_OB_ERROR; \
    } \
    return ret; \
  }

// texttc vs stringtc temporarily
#define DEFINE_CMP_OP_FUNC_TEXT_STRING(op, sys_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObTextTC, ObStringTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObStringTC, ObTextTC, sys_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_TEXT_STRING() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObTextTC, ObStringTC>(const ObObj &obj1, \
                                           const ObObj &obj2, \
                                           const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<ObStringTC, ObTextTC>(obj2, obj1, cmp_ctx); \
  }


// texttc vs texttc temporarily
#define DEFINE_CMP_OP_FUNC_TEXT_TEXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObTextTC, ObTextTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObTextTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObTextTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    ObString data_str1; \
    ObString data_str2; \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) { \
      LOG_ERROR("not support outrow lobs", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (OB_FAIL(obj1.get_string(data_str1))) { \
      LOG_ERROR("invalid text object1", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (OB_FAIL(obj2.get_string(data_str2))) { \
      LOG_ERROR("invalid text object2", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else { \
      ret = CS_TYPE_INVALID != cs_type \
            ? static_cast<int>(ObCharset::strcmpsp(cs_type, data_str1.ptr(), data_str1.length(), \
                                                   data_str2.ptr(), data_str2.length(), \
                                                   CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0) \
            : CR_OB_ERROR; \
    } \
    return ret; \
  }

#define DEFINE_CMP_FUNC_TEXT_TEXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObTextTC, ObTextTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObTextTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObTextTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    ObString data_str1; \
    ObString data_str2; \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) { \
      LOG_ERROR("not support outrow lobs", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (OB_FAIL(obj1.get_string(data_str1))) { \
      LOG_ERROR("invalid text object1", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (OB_FAIL(obj2.get_string(data_str2))) { \
      LOG_ERROR("invalid text object2", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else { \
      ret = CS_TYPE_INVALID != cs_type \
            ? INT_TO_CR(ObCharset::strcmpsp(cs_type, data_str1.ptr(), data_str1.length(), \
                                                   data_str2.ptr(), data_str2.length(), \
                                                   CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))) \
            : CR_OB_ERROR; \
    } \
    return ret; \
  }


//datetimetc VS datetimetc
#define DEFINE_CMP_OP_FUNC_DT_DT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObDateTimeTC, ObDateTimeTC, op>(const ObObj &obj1, \
                                                                 const ObObj &obj2, \
                                                                 const ObCompareCtx &cmp_ctx) \
  {                                                                     \
    UNUSED(cmp_ctx);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);                           \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);                           \
    ObCmpRes ret = CR_FALSE;                                            \
    int64_t v1 = obj1.get_datetime();                                   \
    int64_t v2 = obj2.get_datetime();                                   \
    if (obj1.get_type() != obj2.get_type()) {                           \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {             \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2));         \
        ret = CR_OB_ERROR;                                              \
      } else {                                                          \
        /*same tc while not same type*/                                 \
        if (ObDateTimeType == obj1.get_type()) {                        \
          v1 -= cmp_ctx.tz_off_;                                        \
        } else {                                                        \
          v2 -= cmp_ctx.tz_off_;                                        \
        }                                                               \
      }                                                                 \
      LOG_INFO("come here when old server send task to new server",     \
               K(obj1), K(obj2), KCSTRING(lbt()));			        \
    } else {                                                            \
      /*same tc and same type. do nothing*/                             \
    }                                                                   \
    return CR_OB_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_OB_ERROR; \
  }

//datetimetc VS datetimetc
#define DEFINE_CMP_FUNC_DT_DT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObDateTimeTC, ObDateTimeTC>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);\
    ObCmpRes ret = CR_FALSE; \
    int64_t v1 = obj1.get_datetime();\
    int64_t v2 = obj2.get_datetime();\
    if (obj1.get_type() != obj2.get_type()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else { \
        /*same tc while not same type*/ \
        if (ObDateTimeType == obj1.get_type()) { \
          v1 -= cmp_ctx.tz_off_; \
        } else { \
          v2 -= cmp_ctx.tz_off_; \
        } \
      } \
    } else { \
      /*same tc and same type. do nothing*/ \
    }\
    return CR_OB_ERROR != ret \
           ? v1 < v2 \
             ? CR_LT \
             : v1 > v2 \
               ? CR_GT \
               : CR_EQ \
           : CR_OB_ERROR; \
  }


// type            storedtime
// data            local
// timestamp nano  local
// timestamptz     utc + tzid
// timestampltz    utc + tzid
//datetimetc VS otimestamptc
#define DEFINE_CMP_OP_FUNC_DT_OT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObDateTimeTC, ObOTimestampTC, op>(const ObObj &obj1, \
                                                                   const ObObj &obj2, \
                                                                   const ObCompareCtx &cmp_ctx) \
  {                                                                     \
    UNUSED(cmp_ctx); \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC); \
    ObCmpRes ret = CR_FALSE; \
    ObOTimestampData v1; \
    v1.time_us_ = obj1.get_datetime();\
    ObOTimestampData v2 = obj2.get_otimestamp_value();\
    if (!obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else {\
        v1.time_us_ -= cmp_ctx.tz_off_;\
      }\
    }\
    return (CR_OB_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_OB_ERROR); \
  }

//datetimetc VS otimestamptc
#define DEFINE_CMP_FUNC_DT_OT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObDateTimeTC, ObOTimestampTC>(const ObObj &obj1, \
                                                              const ObObj &obj2, \
                                                              const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);\
    ObCmpRes ret = CR_FALSE;\
    ObOTimestampData v1; \
    v1.time_us_ = obj1.get_datetime();\
    ObOTimestampData v2 = obj2.get_otimestamp_value(); \
    if (!obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else {\
        v1.time_us_ -= cmp_ctx.tz_off_;\
      }\
    }\
    return (CR_OB_ERROR != ret \
            ? (v1 < v2 \
                ? CR_LT \
                : (v1 > v2 \
                   ? CR_GT \
                   : CR_EQ))\
            : CR_OB_ERROR);\
  }


//otimestamptc VS datetimetc
#define DEFINE_CMP_OP_FUNC_OT_DT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObOTimestampTC, ObDateTimeTC, op>(const ObObj &obj1, \
                                                                   const ObObj &obj2, \
                                                                   const ObCompareCtx &cmp_ctx) \
  { \
    UNUSED(cmp_ctx); \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC); \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC); \
    ObCmpRes ret = CR_FALSE;\
    ObOTimestampData v1 = obj1.get_otimestamp_value(); \
    ObOTimestampData v2; \
    v2.time_us_ = obj2.get_datetime();\
    if (!obj1.is_timestamp_nano()) {\
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else {\
        v2.time_us_ -= cmp_ctx.tz_off_;\
      }\
    } \
    return (CR_OB_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_OB_ERROR); \
  }

//otimestamptc VS datetimetc
#define DEFINE_CMP_FUNC_OT_DT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObOTimestampTC, ObDateTimeTC>(const ObObj &obj1, \
                                                              const ObObj &obj2, \
                                                              const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);\
    ObCmpRes ret = CR_FALSE;\
    ObOTimestampData v1 = obj1.get_otimestamp_value();\
    ObOTimestampData v2;\
    v2.time_us_ = obj2.get_datetime();\
    if (!obj1.is_timestamp_nano()) {\
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else {\
        v2.time_us_ -= cmp_ctx.tz_off_;\
      }\
    }\
    return (CR_OB_ERROR != ret \
            ? (v1 < v2 \
              ? CR_LT \
              : (v1 > v2 \
                 ? CR_GT \
                 : CR_EQ)) \
            : CR_OB_ERROR);\
  }
//otimestamptc VS otimestamptc
#define DEFINE_CMP_OP_FUNC_OT_OT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObOTimestampTC, ObOTimestampTC, op>(const ObObj &obj1, \
                                                                     const ObObj &obj2, \
                                                                     const ObCompareCtx &cmp_ctx) \
  {                                                                     \
    UNUSED(cmp_ctx); \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC); \
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);\
    ObCmpRes ret = CR_FALSE;\
    ObOTimestampData v1 = obj1.get_otimestamp_value(); \
    ObOTimestampData v2 = obj2.get_otimestamp_value(); \
    if (obj1.is_timestamp_nano() != obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else {\
        if (obj1.is_timestamp_nano()) {\
          v1.time_us_ -= cmp_ctx.tz_off_;\
        } else {\
          v2.time_us_ -= cmp_ctx.tz_off_;\
        }\
      } \
    } \
    return (CR_OB_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_OB_ERROR); \
  }

//otimestamptc VS otimestamptc
#define DEFINE_CMP_FUNC_OT_OT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObOTimestampTC, ObOTimestampTC>(const ObObj &obj1, \
                                                              const ObObj &obj2, \
                                                              const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);\
    ObCmpRes ret = CR_FALSE;\
    ObOTimestampData v1 = obj1.get_otimestamp_value();\
    ObOTimestampData v2 = obj2.get_otimestamp_value();\
    if (obj1.is_timestamp_nano() != obj2.is_timestamp_nano()) {\
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_OB_ERROR; \
      } else if (obj1.is_timestamp_nano()) {\
        v1.time_us_ -= cmp_ctx.tz_off_;\
      } else {\
        v2.time_us_ -= cmp_ctx.tz_off_;\
      }\
    } \
    return (CR_OB_ERROR != ret \
            ? (v1 < v2 \
              ? CR_LT \
              : (v1 > v2 \
                 ? CR_GT \
                 : CR_EQ)) \
            : CR_OB_ERROR);\
  }


//intervaltc VS intervaltc
#define DEFINE_CMP_OP_FUNC_IT_IT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObIntervalTC, ObIntervalTC, op>(const ObObj &obj1, \
                                                                 const ObObj &obj2, \
                                                                 const ObCompareCtx &cmp_ctx) \
  { \
    UNUSED(cmp_ctx); \
    OBJ_TYPE_CLASS_CHECK(obj1, ObIntervalTC); \
    OBJ_TYPE_CLASS_CHECK(obj2, ObIntervalTC); \
    ObCmpRes ret = CR_FALSE;\
    if (obj1.get_type() != obj2.get_type()) { \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "different interval type can not compare", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (obj1.is_interval_ym()) { \
      ret = obj1.get_interval_ym() op_str obj2.get_interval_ym() ? CR_TRUE : CR_FALSE; \
    } else { \
      ret = obj1.get_interval_ds() op_str obj2.get_interval_ds() ? CR_TRUE : CR_FALSE; \
    } \
    return ret; \
  }

//intervaltc VS intervaltc
#define DEFINE_CMP_FUNC_IT_IT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObIntervalTC, ObIntervalTC>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &cmp_ctx) \
  { \
    UNUSED(cmp_ctx); \
    OBJ_TYPE_CLASS_CHECK(obj1, ObIntervalTC); \
    OBJ_TYPE_CLASS_CHECK(obj2, ObIntervalTC); \
    ObCmpRes ret = CR_FALSE; \
    if (obj1.get_type() != obj2.get_type()) { \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "different interval type can not compare", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else if (obj1.is_interval_ym()) { \
      ObIntervalYMValue v1 = obj1.get_interval_ym(); \
      ObIntervalYMValue v2 = obj2.get_interval_ym(); \
      if (v1 == v2) { \
        ret = CR_EQ; \
      } else { \
        ret = (v1 > v2) ? CR_GT : CR_LT; \
      } \
    } else { \
      ObIntervalDSValue v1 = obj1.get_interval_ds(); \
      ObIntervalDSValue v2 = obj2.get_interval_ds(); \
      if (v1 == v2) { \
        ret = CR_EQ; \
      } else { \
        ret = (v1 > v2) ? CR_GT : CR_LT; \
      } \
    } \
    return ret; \
  }

// lobtc vs lobtc temporarily
#define DEFINE_CMP_OP_FUNC_LOB_LOB(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObLobTC, ObLobTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObLobTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObLobTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    int cmp_ret = CR_OB_ERROR;\
    int ret = OB_SUCCESS;     \
    ObLobLocator *lob_locator1 = NULL;  \
    ObLobLocator *lob_locator2 = NULL;  \
    ObString str1;  \
    ObString str2;  \
    if (OB_FAIL(obj1.get_lob_locator(lob_locator1)) || OB_ISNULL(lob_locator1)) { \
      LOG_ERROR("get lob locator failed", K(ret), K(lob_locator1));               \
    } else if (OB_FAIL(obj2.get_lob_locator(lob_locator2)) || OB_ISNULL(lob_locator2)) {  \
      LOG_ERROR("get lob locator failed", K(ret), K(lob_locator2));   \
    } else if (OB_FAIL(lob_locator1->get_payload(str1))) {            \
      LOG_ERROR("get lob payload failed", K(ret));  \
    } else if (OB_FAIL(lob_locator2->get_payload(str2))) {  \
      LOG_ERROR("get lob payload failed", K(ret));  \
    } else {  \
      cmp_ret = CS_TYPE_INVALID != cs_type \
           ? static_cast<int>(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
              obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0) \
           : CR_OB_ERROR; \
    } \
    return cmp_ret; \
  }

// jsontc vs jsontc
#define DEFINE_CMP_OP_FUNC_JSON_JSON(op, op_str)                                                \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_op_func<ObJsonTC, ObJsonTC, op>(const ObObj &obj1,                     \
                                                         const ObObj &obj2,                     \
                                                         const ObCompareCtx &cmp_ctx)           \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObJsonTC);                                                       \
    OBJ_TYPE_CLASS_CHECK(obj2, ObJsonTC);                                                       \
    UNUSED(cmp_ctx);                                                                            \
    int cmp_ret = CR_OB_ERROR;                                                                  \
    int ret = OB_SUCCESS;                                                                       \
    int result = 0;                                                                             \
    ObString data_str1;                                                                         \
    ObString data_str2;                                                                         \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) {                                         \
      ret = OB_NOT_SUPPORTED;                                                                   \
      LOG_ERROR("not support outrow json lobs", K(ret), K(obj1), K(obj2));                      \
    } else if (OB_FAIL(obj1.get_string(data_str1))) {                                           \
      ret = OB_ERR_UNEXPECTED;                                                                  \
      LOG_ERROR("invalid json lob object1", K(ret),                                             \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
    } else if (OB_FAIL(obj2.get_string(data_str2))) {                                           \
      ret = OB_ERR_UNEXPECTED;                                                                  \
      LOG_ERROR("invalid json lob object2", K(ret),                                             \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
    } else {                                                                                    \
      ObJsonBinCtx ctx1;                                                                        \
      ObJsonBinCtx ctx2;                                                                        \
      ObJsonBin j_bin1(data_str1.ptr(), data_str1.length(), &ctx1);                             \
      ObJsonBin j_bin2(data_str2.ptr(), data_str2.length(), &ctx2);                             \
      ObIJsonBase *j_base1 = &j_bin1;                                                           \
      ObIJsonBase *j_base2 = &j_bin2;                                                           \
      if (OB_FAIL(j_bin1.reset_iter())) {                                                       \
        LOG_WARN("fail to reset json bin1 iter", K(ret), K(data_str1.length()));                \
      } else if (OB_FAIL(j_bin2.reset_iter())) {                                                \
        LOG_WARN("fail to reset json bin2 iter", K(ret), K(data_str2.length()));                \
      } else if (OB_FAIL(j_base1->compare(*j_base2, result))) {                                 \
        LOG_WARN("fail to compare json", K(ret), K(data_str1.length()), K(data_str2.length())); \
      } else {                                                                                  \
        cmp_ret = result op_str 0;                                                              \
      }                                                                                         \
    }                                                                                           \
    return cmp_ret;                                                                             \
  }

#define DEFINE_CMP_FUNC_JSON_JSON()                                                             \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_func<ObJsonTC, ObJsonTC>(const ObObj &obj1,                            \
                                                const ObObj &obj2,                              \
                                                const ObCompareCtx &cmp_ctx)                    \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObJsonTC);                                                       \
    OBJ_TYPE_CLASS_CHECK(obj2, ObJsonTC);                                                       \
    UNUSED(cmp_ctx);                                                                            \
    int ret = OB_SUCCESS;                                                                       \
    int result = CR_OB_ERROR;                                                                   \
    ObString data_str1;                                                                         \
    ObString data_str2;                                                                         \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) {                                         \
      LOG_WARN("not support outrow json lobs", K(obj1), K(obj2));                               \
      ret = CR_OB_ERROR;                                                                        \
    } else if (OB_FAIL(obj1.get_string(data_str1))) {                                           \
      LOG_WARN("invalid json lob object1",                                                      \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
      ret = CR_OB_ERROR;                                                                        \
    } else if (OB_FAIL(obj2.get_string(data_str2))) {                                           \
      LOG_WARN("invalid json lob object2",                                                      \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
      ret = CR_OB_ERROR;                                                                        \
    } else {                                                                                    \
      ObJsonBinCtx ctx1;                                                                        \
      ObJsonBinCtx ctx2;                                                                        \
      ObJsonBin j_bin1(data_str1.ptr(), data_str1.length(), &ctx1);                             \
      ObJsonBin j_bin2(data_str2.ptr(), data_str2.length(), &ctx2);                             \
      ObIJsonBase *j_base1 = &j_bin1;                                                           \
      ObIJsonBase *j_base2 = &j_bin2;                                                           \
      if (OB_FAIL(j_bin1.reset_iter())) {                                                       \
        LOG_WARN("fail to reset json bin1 iter", K(ret), K(data_str1.length()));                \
      } else if (OB_FAIL(j_bin2.reset_iter())) {                                                \
        LOG_WARN("fail to reset json bin2 iter", K(ret), K(data_str2.length()));                \
      } else if (OB_FAIL(j_base1->compare(*j_base2, result))) {                                 \
        LOG_WARN("fail to compare json", K(ret), K(data_str1.length()), K(data_str2.length())); \
      } else {                                                                                  \
        result = INT_TO_CR(result);                                                             \
      }                                                                                         \
    }                                                                                           \
                                                                                                \
    return result;                                                                              \
  }


#define DEFINE_CMP_FUNC_JSON_EXTEND()                                                           \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_func<ObJsonTC, ObExtendTC>(const ObObj &obj1,                          \
                                                   const ObObj &/*obj2*/,                       \
                                                   const ObCompareCtx &/*cmp_ctx*/)             \
  {                                                                                             \
    int result = INT_TO_CR(-1);                                                                 \
                                                                                                \
    return result;                                                                              \
  }

#define DEFINE_CMP_OP_FUNC_JSON_EXTEND(op, op_str)                                              \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_op_func<ObJsonTC, ObExtendTC, op>(const ObObj &obj1,                     \
                                                         const ObObj &obj2,                     \
                                                         const ObCompareCtx &cmp_ctx)           \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObJsonTC);                                                       \
    OBJ_TYPE_CLASS_CHECK(obj2, ObExtendTC);                                                     \
    UNUSED(cmp_ctx);                                                                            \
    int result = INT_TO_CR(-1);                                                                 \
                                                                                                \
    return result;                                                                              \
  }

// geometrytc vs geometrytc
#define DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(op, op_str)                                        \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_op_func<ObGeometryTC, ObGeometryTC, op>(const ObObj &obj1,             \
                                                                 const ObObj &obj2,             \
                                                                 const ObCompareCtx &cmp_ctx)   \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObGeometryTC);                                                   \
    OBJ_TYPE_CLASS_CHECK(obj2, ObGeometryTC);                                                   \
    UNUSED(cmp_ctx);                                                                            \
    int ret = OB_SUCCESS;                                                                       \
    int cmp_ret = CR_OB_ERROR;                                                                  \
    if (op == CO_EQ || op == CO_NE) {                                                           \
      ObString wkb1 = obj1.get_string();                                                        \
      ObString wkb2 = obj2.get_string();                                                        \
      ObLobLocatorV2 lob1(wkb1, obj1.has_lob_header());                                         \
      ObLobLocatorV2 lob2(wkb2, obj2.has_lob_header());                                         \
      if (lob1.has_inrow_data() && lob2.has_inrow_data()) {                                     \
        if (OB_FAIL(lob1.get_inrow_data(wkb1))) {                                               \
          LOG_WARN("get obj1 inrow data failed", K(ret), K(lob1));                              \
        } else if (OB_FAIL(lob2.get_inrow_data(wkb2))) {                                        \
          LOG_WARN("get obj2 inrow data failed", K(ret), K(lob2));                              \
        } else {                                                                                \
          cmp_ret = static_cast<int>(ObCharset::strcmpsp(CS_TYPE_BINARY,                        \
                                                      wkb1.ptr(), wkb1.length(),                \
                                                      wkb2.ptr(), wkb2.length(),                \
                                                      false)                                    \
                                                      op_str 0);                                \
        }                                                                                       \
      } else {                                                                                  \
        cmp_ret = static_cast<int>(ObCharset::strcmpsp(CS_TYPE_BINARY,                          \
                                                     obj1.v_.string_, obj1.val_len_,            \
                                                     obj2.v_.string_, obj2.val_len_,            \
                                                     false)                                     \
                                                     op_str 0);                                 \
      }                                                                                         \
    }                                                                                           \
    return cmp_ret;                                                                             \
  }

// There is no greater than and less than in geometry, if not equal then error
#define DEFINE_CMP_FUNC_GEOMETRY_GEOMETRY()                                                     \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_func<ObGeometryTC, ObGeometryTC>(const ObObj &obj1,                    \
                                                          const ObObj &obj2,                    \
                                                          const ObCompareCtx &cmp_ctx)          \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObGeometryTC);                                                   \
    OBJ_TYPE_CLASS_CHECK(obj2, ObGeometryTC);                                                   \
    UNUSED(cmp_ctx);                                                                            \
    ObString wkb1 = obj1.get_string();                                                          \
    ObString wkb2 = obj2.get_string();                                                          \
    ObLobLocatorV2 lob1(wkb1, obj1.has_lob_header());                                           \
    ObLobLocatorV2 lob2(wkb2, obj2.has_lob_header());                                           \
    if (lob1.has_inrow_data() && lob2.has_inrow_data()) {                                       \
      (void)lob1.get_inrow_data(wkb1);                                                          \
      (void)lob2.get_inrow_data(wkb2);                                                          \
    }                                                                                           \
    int result = ObCharset::strcmpsp(CS_TYPE_BINARY, wkb1.ptr(), wkb1.length(),                 \
                                     wkb2.ptr(), wkb2.length(), false);                         \
    if (0 == result) {                                                                          \
      result = CR_EQ;                                                                           \
    } else if (0 > result) {                                                                    \
      result = CR_LT;                                                                           \
    } else {                                                                                    \
      result = CR_GT;                                                                           \
    }                                                                                           \
    return result;                                                                              \
  }

#define DEFINE_CMP_FUNC_ROWT_ROWT()                                            \
  template <>                                                                  \
  inline int ObObjCmpFuncs::cmp_func<ObRowIDTC, ObRowIDTC>(                    \
      const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &cmp_ctx) {     \
    UNUSED(cmp_ctx);                                                           \
    OBJ_TYPE_CLASS_CHECK(obj1, ObRowIDTC);                                     \
    OBJ_TYPE_CLASS_CHECK(obj2, ObRowIDTC);                                     \
    ObCmpRes ret = CR_FALSE;                                                   \
    if (OB_UNLIKELY(obj1.get_type() != obj2.get_type()) ||                     \
        OB_UNLIKELY(!obj1.is_urowid())) {                                      \
      ret = CR_OB_ERROR;                                                       \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "only support urowid for now", K(ret));                        \
    } else {                                                                   \
      ret =                                                                    \
          static_cast<ObCmpRes>(obj1.get_urowid().compare(obj2.get_urowid())); \
    }                                                                          \
    return ret;                                                                \
  }

#define DEFINE_CMP_OP_FUNC_ROWT_ROWT(op, op_str)                             \
  template <>                                                                \
  inline int ObObjCmpFuncs::cmp_op_func<ObRowIDTC, ObRowIDTC, op>(           \
      const ObObj &obj1, const ObObj &obj2,                                  \
      const ObCompareCtx & /*cmp_ctx*/) {                                    \
    OBJ_TYPE_CLASS_CHECK(obj1, ObRowIDTC);                                   \
    OBJ_TYPE_CLASS_CHECK(obj2, ObRowIDTC);                                   \
    ObCmpRes ret = CR_FALSE;                                                 \
    if (OB_UNLIKELY(obj1.get_type() != obj2.get_type()) ||                   \
        OB_UNLIKELY(!obj1.is_urowid())) {                                    \
      ret = CR_OB_ERROR;                                                     \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "only support urowid for now", K(ret));                      \
    } else {                                                                 \
      ret = obj1.get_urowid() op_str obj2.get_urowid() ? CR_TRUE : CR_FALSE; \
    }                                                                        \
    return ret;                                                              \
  }

#define DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(op, op_str) \
  template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObEnumSetInnerTC, ObIntTC, op>(const ObObj &obj1, \
                                                              const ObObj &obj2, \
                                                              const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObIntTC);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = obj2.get_int() < 0 \
           ? 0 op_str obj2.get_int()  \
           : obj1_value op_str obj2.get_uint64(); \
  } \
  return cmp_ret;\
}

#define DEFINE_CMP_FUNC_ENUMSETINNER_INT() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObEnumSetInnerTC, ObIntTC>(const ObObj &obj1, \
                                                       const ObObj &obj2, \
                                                       const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObIntTC);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = (obj2.get_int() < 0 || obj1_value > obj2.get_uint64()) \
           ? CR_GT \
           : obj1_value < obj2.get_uint64() \
             ? CR_LT \
             : CR_EQ; \
  }\
  return cmp_ret;\
}


#define DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(op, op_str) \
  template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObEnumSetInnerTC, ObUIntTC, op>(const ObObj &obj1, \
                                                               const ObObj &obj2, \
                                                               const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObUIntTC);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = obj1_value op_str obj2.get_uint64(); \
  } \
  return cmp_ret;\
}

#define DEFINE_CMP_FUNC_ENUMSETINNER_UINT() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObEnumSetInnerTC, ObUIntTC>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObUIntTC);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = (obj1_value < obj2.get_uint64()  ? CR_LT \
               : obj1_value > obj2.get_uint64() \
               ? CR_GT \
               : CR_EQ); \
  }\
  return cmp_ret;\
}

#define DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(op, op_str) \
  template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObEnumSetInnerTC, ObNumberTC, op>(const ObObj &obj1, \
                                                              const ObObj &obj2, \
                                                              const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    if (CO_EQ == op) { \
      cmp_ret = obj2.get_number().is_equal(obj1_value); \
    } else { \
      cmp_ret = 0 op_str obj2.get_number().compare(obj1_value); \
    } \
  } \
  return cmp_ret;\
}

#define DEFINE_CMP_FUNC_ENUMSETINNER_NUMBER() \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObEnumSetInnerTC, ObNumberTC>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = -INT_TO_CR(obj2.get_number().compare(obj1_value)); \
  }\
  return cmp_ret;\
}

#define DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, op, op_str) \
  template <> inline \
int ObObjCmpFuncs::cmp_op_func<ObEnumSetInnerTC, real_tc, op>(const ObObj &obj1, \
                                                              const ObObj &obj2, \
                                                              const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, real_tc);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = static_cast<double>(obj1_value) op_str static_cast<double>(obj2.get_##real_type()); \
  } \
  return cmp_ret;\
}

#define DEFINE_CMP_FUNC_ENUMSETINNER_REAL(real_tc, real_type) \
  template <> inline \
int ObObjCmpFuncs::cmp_func<ObEnumSetInnerTC, real_tc>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
{ \
  OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);\
  OBJ_TYPE_CLASS_CHECK(obj2, real_tc);\
  ObEnumSetInnerValue inner_value;\
  int cmp_ret = CR_OB_ERROR;\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) { \
    cmp_ret = CR_OB_ERROR;\
  } else {\
    uint64_t obj1_value = inner_value.numberic_value_; \
    cmp_ret = static_cast<double>(obj1_value) < static_cast<double>(obj2.get_##real_type()) \
           ? CR_LT \
           : static_cast<double>(obj1_value) > static_cast<double>(obj2.get_##real_type()) \
             ? CR_GT \
             : CR_EQ; \
  }\
  return cmp_ret;\
}

#define DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(op, op_str)                                     \
  template <>                                                                                      \
  int ObObjCmpFuncs::cmp_op_func<ObEnumSetInnerTC, ObDecimalIntTC, op>(                            \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);                                                  \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDecimalIntTC);                                                    \
    ObEnumSetInnerValue inner_value;                                                               \
    int cmp_res = 0;                                                                               \
    int ret_val = 0;                                                                               \
    int ret = OB_SUCCESS;                                                                          \
    ObScale lh_scale = 0, rh_scale = obj2.get_scale();                                             \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalInt *decint = nullptr;                                                                \
    uint64_t obj1_value = 0;                                                                       \
    int32_t int_bytes = 0;                                                                         \
    if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) {                                      \
      ret_val = CR_OB_ERROR;                                                                       \
    } else if (FALSE_IT(obj1_value = inner_value.numberic_value_)) {                               \
    } else if (OB_FAIL(wide::from_integer(obj1_value, tmp_alloc, decint, int_bytes))) {            \
      LOG_ERROR("cast integer to decimal int failed", K(ret));                                     \
      ret_val = CR_OB_ERROR;                                                                       \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj2.get_decimal_int(), obj2.get_int_bytes(),      \
                                                rh_scale, lh_scale, rh_val))) {                    \
        LOG_ERROR("scale decimal int failed", K(ret));                                             \
        ret_val = CR_OB_ERROR;                                                                     \
      } else {                                                                                     \
        rh_val.from(decint, int_bytes);                                                            \
      }                                                                                            \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, lh_scale, rh_scale, lh_val))) { \
        LOG_ERROR("scale decimal int failed", K(ret));                                             \
      } else {                                                                                     \
        rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(decint, int_bytes);                                                              \
      rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                   \
    }                                                                                              \
    if (OB_SUCC(ret)) {                                                                            \
      if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                       \
        LOG_ERROR("compare failed", K(ret));                                                       \
        ret_val = CR_OB_ERROR;                                                                     \
      } else {                                                                                     \
        ret_val = (cmp_res op_str 0);                                                              \
      }                                                                                            \
    }                                                                                              \
    return ret_val;                                                                                \
  }

#define DEFINE_CMP_FUNC_ENUMSETINNER_DECIMALINT()                                                  \
  template <>                                                                                      \
  int ObObjCmpFuncs::cmp_func<ObEnumSetInnerTC, ObDecimalIntTC>(                                   \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);                                                  \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDecimalIntTC);                                                    \
    ObEnumSetInnerValue inner_value;                                                               \
    int cmp_res = 0;                                                                               \
    int ret = OB_SUCCESS;                                                                          \
    ObScale lh_scale = 0, rh_scale = obj2.get_scale();                                             \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalInt *decint = nullptr;                                                                \
    uint64_t obj1_value = 0;                                                                       \
    int32_t int_bytes = 0;                                                                         \
    if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) {                                      \
      LOG_ERROR("get enumset inner value failed", K(ret));                                         \
    } else if (FALSE_IT(obj1_value = inner_value.numberic_value_)) {                               \
    } else if (OB_FAIL(wide::from_integer(obj1_value, tmp_alloc, decint, int_bytes))) {            \
      LOG_ERROR("cast integer to decimal int failed", K(ret));                                     \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj2.get_decimal_int(), obj2.get_int_bytes(),      \
                                                rh_scale, lh_scale, rh_val))) {                    \
        LOG_ERROR("scale decimal int failed", K(ret));                                             \
      } else {                                                                                     \
        rh_val.from(decint, int_bytes);                                                            \
      }                                                                                            \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, lh_scale, rh_scale, lh_val))) { \
        LOG_ERROR("scale decimal int failed", K(ret));                                             \
      } else {                                                                                     \
        rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(decint, int_bytes);                                                              \
      rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                   \
    }                                                                                              \
    if (OB_SUCC(ret)) {                                                                            \
      if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                       \
        LOG_ERROR("compare failed", K(ret));                                                       \
      }                                                                                            \
    }                                                                                              \
    return cmp_res;                                                                                \
  }

#define DEFINE_CMP_FUNC_LOB_LOB() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObLobTC, ObLobTC>(const ObObj &obj1, \
                                                const ObObj &obj2, \
                                                const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObLobTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObLobTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    int cmp_ret = CR_OB_ERROR;\
    int ret = OB_SUCCESS;     \
    ObLobLocator *lob_locator1 = NULL;  \
    ObLobLocator *lob_locator2 = NULL;  \
    ObString str1;  \
    ObString str2;  \
    if (OB_FAIL(obj1.get_lob_locator(lob_locator1)) || OB_ISNULL(lob_locator1)) { \
      LOG_ERROR("get lob locator failed", K(ret), K(lob_locator1));               \
    } else if (OB_FAIL(obj2.get_lob_locator(lob_locator2)) || OB_ISNULL(lob_locator2)) {  \
      LOG_ERROR("get lob locator failed", K(ret), K(lob_locator2));   \
    } else if (OB_FAIL(lob_locator1->get_payload(str1))) {            \
      LOG_ERROR("get lob payload failed", K(ret));  \
    } else if (OB_FAIL(lob_locator2->get_payload(str2))) {  \
      LOG_ERROR("get lob payload failed", K(ret));  \
    } else {  \
      cmp_ret = CS_TYPE_INVALID != cs_type \
           ? INT_TO_CR(ObCharset::strcmpsp(cs_type, str1.ptr(), str1.length(), \
                            str2.ptr(), str2.length(), CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))) \
           : CR_OB_ERROR; \
    }   \
	  return cmp_ret; \
  }

#define DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(op, op_str)                                       \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObDecimalIntTC, op>(                       \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDecimalIntTC);                                                    \
    int ret = OB_SUCCESS;                                                                          \
    int val = 0;                                                                                   \
    int cmp_res = 0;                                                                               \
    int16_t lh_scale = obj1.get_scale(), rh_scale = obj2.get_scale();                              \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    if (lh_scale < rh_scale) {                                                                     \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_WARN("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                 \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj2.get_decimal_int(), obj2.get_int_bytes(),      \
                                                rh_scale, lh_scale, rh_val))) {                    \
        LOG_WARN("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                   \
    }                                                                                              \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                  \
      LOG_ERROR("compare error", K(ret));                                                          \
    } else if (op == CO_CMP) {                                                                     \
      val = cmp_res;                                                                               \
    } else {                                                                                       \
      val = (cmp_res op_str 0);                                                                    \
    }                                                                                              \
    return val;                                                                                    \
  }

#define DEFINE_CMP_FUNC_DECIMALINT_DECIMALINT()                                                    \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObDecimalIntTC, ObDecimalIntTC>(                              \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    int cmp_res =                                                                                  \
      ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObDecimalIntTC, CO_CMP>(obj1, obj2, ctx);         \
    return INT_TO_CR(cmp_res);                                                                     \
  }

#define DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(op, op_str, tc, val_type, val_func)                  \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, tc, op>(                                   \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, tc);                                                                \
    int ret = OB_SUCCESS;                                                                          \
    int cmp_res = 0;                                                                               \
    int ret_val = 0;                                                                               \
    val_type val = obj2.get_##val_func();                                                          \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalInt *rh_decint = nullptr;                                                             \
    int32_t rh_int_bytes = 0;                                                                      \
    int16_t lh_scale = obj1.get_scale(), rh_scale = 0;                                             \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    if (OB_FAIL(wide::from_integer(val, tmp_alloc, rh_decint, rh_int_bytes))) {                    \
      LOG_ERROR("from_integer failed", K(ret));                                                    \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_WARN("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        rh_val.from(rh_decint, rh_int_bytes);                                                      \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(                                                                                 \
            wide::common_scale_decimalint(rh_decint, rh_int_bytes, rh_scale, lh_scale, rh_val))) { \
        LOG_WARN("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(rh_decint, rh_int_bytes);                                                        \
    }                                                                                              \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                  \
      LOG_WARN("compare failed", K(ret));                                                          \
    } else if (op == CO_CMP) {                                                                     \
      ret_val = cmp_res;                                                                           \
    } else {                                                                                       \
      ret_val = (cmp_res op_str 0);                                                                \
    }                                                                                              \
    return ret_val;                                                                                \
  }

#define DEFINE_CMP_FUNC_DECIMALINT_INTEGER(tc, val_type, val_func)                                 \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObDecimalIntTC, tc>(const ObObj &obj1, const ObObj &obj2,     \
                                                         const ObCompareCtx &ctx)                  \
  {                                                                                                \
    int cmp_ret = ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, tc, CO_CMP>(obj1, obj2, ctx);         \
    return cmp_ret;                                                                                \
  }

#define DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(op, op_str, tc)                                      \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<tc, ObDecimalIntTC, op>(                                   \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    int ret = ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, tc, op>(obj2, obj1, ctx);                 \
    return -ret;                                                                                   \
  }

#define DEFINE_CMP_FUNC_INTEGER_DECIMALINT(tc)                                                     \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<tc, ObDecimalIntTC>(const ObObj &obj1, const ObObj &obj2,     \
                                                         const ObCompareCtx &ctx)                  \
  {                                                                                                \
    return -ObObjCmpFuncs::cmp_func<ObDecimalIntTC, tc>(obj2, obj1, ctx);                          \
  }

#define DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(op, op_str)                                           \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObNumberTC, op>(                           \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);                                                        \
    int ret = OB_SUCCESS;                                                                          \
    int cmp_res = 0;                                                                               \
    int ret_val = 0;                                                                               \
    ObDecimalInt *decint = nullptr;                                                                \
    int32_t int_bytes = 0;                                                                         \
    number::ObNumber nmb = obj2.get_number();                                                      \
    int16_t lh_scale = obj1.get_scale(), rh_scale = nmb.get_scale();                               \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalIntBuilder lh_val;                                                                    \
    ObDecimalIntBuilder rh_val;                                                                    \
    if (OB_FAIL(wide::from_number(nmb, tmp_alloc, rh_scale, decint, int_bytes))) {                 \
      LOG_ERROR("cast number to decimal int failed", K(ret));                                      \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_ERROR("scale decimal int failed", K(ret), K(lh_scale), K(rh_scale));                   \
      } else if (FALSE_IT(rh_val.from(decint, int_bytes))) {                                       \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, rh_scale, lh_scale, rh_val))) { \
        LOG_ERROR("scale decimal int failed", K(ret), K(rh_scale), K(lh_scale));                   \
      } else if (FALSE_IT(lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes()))) {            \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(decint, int_bytes);                                                              \
    }                                                                                              \
    if (OB_SUCC(ret)) {                                                                            \
      if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                       \
        LOG_ERROR("compare failed", K(ret));                                                       \
      } else {                                                                                     \
        ret_val = (cmp_res op_str 0);                                                              \
      }                                                                                            \
    }                                                                                              \
    return ret_val;                                                                                \
  }

#define DEFINE_CMP_FUNC_DECIMALINT_NUMBER()                                                        \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObDecimalIntTC, ObNumberTC>(                                  \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);                                                        \
    int ret = OB_SUCCESS;                                                                          \
    int cmp_res = 0;                                                                               \
    ObDecimalInt *decint = nullptr;                                                                \
    int32_t int_bytes = 0;                                                                         \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    number::ObNumber nmb = obj2.get_number();                                                      \
    ObScale lh_scale = obj1.get_scale(), rh_scale = nmb.get_scale();                               \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    if (OB_FAIL(wide::from_number(nmb, tmp_alloc, rh_scale, decint, int_bytes))) {                 \
      LOG_ERROR("scale decimal int failed", K(ret));                                               \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_ERROR("scale decimal int failed", K(ret), K(lh_scale), K(rh_scale));                   \
      } else if (FALSE_IT(rh_val.from(decint, int_bytes))) {                                       \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, rh_scale, lh_scale, rh_val))) { \
        LOG_ERROR("scale decimal int failed", K(ret), K(rh_scale), K(lh_scale));                   \
      } else if (FALSE_IT(lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes()))) {            \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(decint, int_bytes);                                                              \
    }                                                                                              \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                  \
      LOG_ERROR("compare failed", K(ret));                                                         \
    }                                                                                              \
    return cmp_res;                                                                                \
  }

#define DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(op, op_str)                                           \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObNumberTC, ObDecimalIntTC, op>(                           \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    int ret = ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObNumberTC, op>(obj2, obj1, ctx);         \
    return -ret;                                                                                   \
  }

#define DEFINE_CMP_FUNC_NUMBER_DECIMALINT()                                                        \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObNumberTC, ObDecimalIntTC>(                                  \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    return -ObObjCmpFuncs::cmp_func<ObDecimalIntTC, ObNumberTC>(obj2, obj1, ctx);                  \
  }

// ObUserDefinedSQLTC vs ObUserDefinedSQLTC
// for udt nested null bitmap
#define DEFINE_CMP_OP_FUNC_UDT_UDT(op, op_str)                                                \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_op_func<ObUserDefinedSQLTC, ObUserDefinedSQLTC, op>(const ObObj &obj1, \
                                                         const ObObj &obj2,                     \
                                                         const ObCompareCtx &cmp_ctx)           \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObUserDefinedSQLTC);                                                       \
    OBJ_TYPE_CLASS_CHECK(obj2, ObUserDefinedSQLTC);                                                       \
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
	  return CS_TYPE_INVALID != cs_type \
           ? static_cast<int>(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                                  obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0) \
           : CR_OB_ERROR; \
  }

#define DEFINE_CMP_FUNC_UDT_UDT()                                                             \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_func<ObUserDefinedSQLTC, ObUserDefinedSQLTC>(const ObObj &obj1,        \
                                                const ObObj &obj2,                              \
                                                const ObCompareCtx &cmp_ctx)                    \
  {                                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObUserDefinedSQLTC);                                             \
    OBJ_TYPE_CLASS_CHECK(obj2, ObUserDefinedSQLTC);                                             \
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    return CS_TYPE_INVALID != cs_type \
           ? INT_TO_CR(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                           obj2.v_.string_, obj2.val_len_, CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))) \
           : CR_OB_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObCollectionSQLTC, ObCollectionSQLTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObCollectionSQLTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObCollectionSQLTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) { \
      LOG_ERROR("not support outrow lobs", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else { \
      ObString data_str1 = obj1.get_string();\
      ObString data_str2 = obj2.get_string();\
      ret = CS_TYPE_INVALID != cs_type \
            ? static_cast<int>(ObCharset::strcmpsp(cs_type, data_str1.ptr(), data_str1.length(), \
                                                   data_str2.ptr(), data_str2.length(), \
                                                   CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx)) op_str 0) \
            : CR_OB_ERROR; \
    } \
    return ret; \
  }

#define DEFINE_CMP_FUNC_COLLECTION_COLLECTION() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObCollectionSQLTC, ObCollectionSQLTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &cmp_ctx) \
  { \
    int ret = CR_OB_ERROR; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObCollectionSQLTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObCollectionSQLTC);\
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid collation", K(obj1.get_collation_type()), K(obj2.get_collation_type()), K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) { \
      LOG_ERROR("not support outrow lobs", K(obj1), K(obj2)); \
      ret = CR_OB_ERROR; \
    } else { \
      ObString data_str1 = obj1.get_string();\
      ObString data_str2 = obj2.get_string();\
      ret = CS_TYPE_INVALID != cs_type \
            ? INT_TO_CR(ObCharset::strcmpsp(cs_type, data_str1.ptr(), data_str1.length(), \
                                                   data_str2.ptr(), data_str2.length(), \
                                                   CALC_WITH_END_SPACE(obj1, obj2, cmp_ctx))) \
            : CR_OB_ERROR; \
    } \
    return ret; \
  }
//==============================

#define DEFINE_CMP_FUNCS(tc, type) \
  DEFINE_CMP_OP_FUNC(tc, type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_NE, !=); \
  DEFINE_CMP_FUNC(tc, type)

#define DEFINE_CMP_FUNCS_XXX_REAL(tc, type, real_tc, real_type) \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_NE, !=); \
  DEFINE_CMP_FUNC_XXX_REAL(tc, type, real_tc, real_type); \

#define DEFINE_CMP_FUNCS_REAL_XXX(real_tc, real_type, tc, type) \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_REAL_XXX(real_tc, real_type, tc, type); \

#define DEFINE_CMP_FUNCS_XXX_NUMBER(tc, type) \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_NE, !=); \
  DEFINE_CMP_FUNC_XXX_NUMBER(tc, type); \

#define DEFINE_CMP_FUNCS_NUMBER_XXX(tc, type) \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_NUMBER_XXX(tc, type); \

#define DEFINE_CMP_FUNCS_REAL_REAL(real1_tc, real1_type, real2_tc, real2_type) \
  DEFINE_CMP_OP_FUNC_REAL_REAL_EQ(real1_tc, real1_type, real2_tc, real2_type); \
  DEFINE_CMP_OP_FUNC_REAL_REAL_LE(real1_tc, real1_type, real2_tc, real2_type); \
  DEFINE_CMP_OP_FUNC_REAL_REAL_LT(real1_tc, real1_type, real2_tc, real2_type); \
  DEFINE_CMP_OP_FUNC_REAL_REAL_GE(real1_tc, real1_type, real2_tc, real2_type); \
  DEFINE_CMP_OP_FUNC_REAL_REAL_GT(real1_tc, real1_type, real2_tc, real2_type); \
  DEFINE_CMP_OP_FUNC_REAL_REAL_NE(real1_tc, real1_type, real2_tc, real2_type); \
  DEFINE_CMP_FUNC_REAL_REAL(real1_tc, real1_type, real2_tc, real2_type)

#define DEFINE_CMP_FUNCS_DECIMALINT_DECIMALINT()                                                   \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_EQ, ==);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_LE, <=);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_LT, <);                                              \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_GE, >=);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_GT, >);                                              \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_NE, !=);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_CMP, =);                                             \
  DEFINE_CMP_FUNC_DECIMALINT_DECIMALINT()

#define DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(int_tc, val_type, val_func)                            \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_EQ, ==, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_LE, <=, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_LT, <, int_tc, val_type, val_func);                     \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_GE, >=, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_GT, >, int_tc, val_type, val_func);                     \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_NE, !=, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_CMP, =, int_tc, val_type, val_func);                    \
  DEFINE_CMP_FUNC_DECIMALINT_INTEGER(int_tc, val_type, val_func)

#define DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(int_tc)                                                \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_EQ, ==, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_LE, <=, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_LT, <, int_tc);                                         \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_GE, >=, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_GT, >, int_tc);                                         \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_NE, !=, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_CMP, =, int_tc);                                        \
  DEFINE_CMP_FUNC_INTEGER_DECIMALINT(int_tc)

#define DEFINE_CMP_FUNCS_DECIMALINT_NUMBER()                                                       \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_EQ, ==);                                                 \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_LE, <=);                                                 \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_LT, <);                                                  \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_GE, >=);                                                 \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_GT, >);                                                  \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_NE, !=);                                                 \
  DEFINE_CMP_FUNC_DECIMALINT_NUMBER()

#define DEFINE_CMP_FUNCS_NUMBER_DECIMALINT()                                                       \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_EQ, ==);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_LE, <=);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_LT, <);                                                  \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_GE, >=);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_GT, >);                                                  \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_NE, !=);                                                 \
  DEFINE_CMP_FUNC_NUMBER_DECIMALINT()

//==============================

#define DEFINE_CMP_FUNCS_NULL_NULL() \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_NE, !=); \
  DEFINE_CMP_FUNC_NULL_NULL()

#define DEFINE_CMP_FUNCS_NULL_EXT() \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_NE, !=); \
  DEFINE_CMP_FUNC_NULL_EXT()

#define DEFINE_CMP_FUNCS_INT_INT() \
  DEFINE_CMP_FUNCS(ObIntTC, int);

#define DEFINE_CMP_FUNCS_BIT_BIT() \
  DEFINE_CMP_FUNCS(ObBitTC, bit);

#define DEFINE_CMP_FUNCS_ENUMSET_ENUMSET() \
  DEFINE_CMP_FUNCS(ObEnumSetTC, uint64);

#define DEFINE_CMP_FUNCS_BIT_BIT() \
  DEFINE_CMP_FUNCS(ObBitTC, bit);

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_INT_UINT() \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_NE, !=); \
  DEFINE_CMP_FUNC_INT_UINT()

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_INT_FLOAT() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObIntTC, int, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_INT_DOUBLE() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObIntTC, int, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_INT_NUMBER() \
  DEFINE_CMP_FUNCS_XXX_NUMBER(ObIntTC, int);

#define DEFINE_CMP_FUNCS_INT_ENUMSET() \
  DEFINE_CMP_OP_FUNC_INT_ENUMSET(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_INT_ENUMSET(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_INT_ENUMSET(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_INT_ENUMSET(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_INT_ENUMSET(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_INT_ENUMSET(CO_NE, !=); \
  DEFINE_CMP_FUNC_INT_ENUMSET()

#define DEFINE_CMP_FUNCS_UINT_INT() \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_UINT_INT()

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_UINT_UINT() \
  DEFINE_CMP_FUNCS(ObUIntTC, uint64);

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_UINT_FLOAT() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObUIntTC, uint64, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_UINT_DOUBLE() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObUIntTC, uint64, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_UINT_NUMBER() \
  DEFINE_CMP_FUNCS_XXX_NUMBER(ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_UINT_ENUMSET() \
  DEFINE_CMP_OP_FUNC_UINT_ENUMSET(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_UINT_ENUMSET(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_UINT_ENUMSET(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_UINT_ENUMSET(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_UINT_ENUMSET(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_UINT_ENUMSET(CO_NE, !=); \
  DEFINE_CMP_FUNC_UINT_ENUMSET()

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_ENUMSET_FLOAT();\
  DEFINE_CMP_FUNCS_XXX_REAL(ObEnumSetTC, uint64, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_ENUMSET_DOUBLE();\
  DEFINE_CMP_FUNCS_XXX_REAL(ObEnumSetTC, uint64, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_ENUMSET_NUMBER();\
  DEFINE_CMP_FUNCS_XXX_NUMBER(ObEnumSetTC, uint64);

#define DEFINE_CMP_FUNCS_ENUMSET_ENUMSET() \
  DEFINE_CMP_FUNCS(ObEnumSetTC, uint64);

#define DEFINE_CMP_FUNCS_FLOAT_INT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObFloatTC, float, ObIntTC, int);

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_FLOAT_UINT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObFloatTC, float, ObUIntTC, uint64);

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_FLOAT_FLOAT() \
  DEFINE_CMP_FUNCS_REAL_REAL(ObFloatTC, float, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_FLOAT_DOUBLE() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObFloatTC, float, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_FLOAT_ENUMSET() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObFloatTC, float, ObEnumSetTC, uint64);

#define DEFINE_CMP_FUNCS_DOUBLE_INT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObIntTC, int);

#define DEFINE_CMP_FUNCS_DOUBLE_UINT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObUIntTC, uint64);

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_DOUBLE_FLOAT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_DOUBLE_DOUBLE() \
  DEFINE_CMP_FUNCS_REAL_REAL(ObDoubleTC, double, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_DOUBLE_ENUMSET() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObEnumSetTC, uint64);

#define DEFINE_CMP_FUNCS_NUMBER_INT() \
  DEFINE_CMP_FUNCS_NUMBER_XXX(ObIntTC, int);

#define DEFINE_CMP_FUNCS_NUMBER_UINT() \
  DEFINE_CMP_FUNCS_NUMBER_XXX(ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_NUMBER_NUMBER() \
  DEFINE_CMP_FUNCS(ObNumberTC, number);

#define DEFINE_CMP_FUNCS_NUMBER_ENUMSET() \
  DEFINE_CMP_FUNCS_NUMBER_XXX(ObEnumSetTC, uint64);


#define DEFINE_CMP_FUNCS_DATETIME_DATETIME() \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_NE, !=); \
    DEFINE_CMP_FUNC_DT_DT(); \

#define DEFINE_CMP_FUNCS_DATE_DATE() \
  DEFINE_CMP_FUNCS(ObDateTC, date);

#define DEFINE_CMP_FUNCS_TIME_TIME() \
  DEFINE_CMP_FUNCS(ObTimeTC, time);

#define DEFINE_CMP_FUNCS_YEAR_YEAR() \
  DEFINE_CMP_FUNCS(ObYearTC, year);

#define DEFINE_CMP_FUNCS_DATETIME_OTIMESTAMP() \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_NE, !=); \
    DEFINE_CMP_FUNC_DT_OT(); \

#define DEFINE_CMP_FUNCS_OTIMESTAMP_DATETIME() \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_NE, !=); \
    DEFINE_CMP_FUNC_OT_DT(); \

#define DEFINE_CMP_FUNCS_OTIMESTAMP_OTIMESTAMP() \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_NE, !=); \
    DEFINE_CMP_FUNC_OT_OT(); \

#define DEFINE_CMP_FUNCS_INTERVAL_INTERVAL() \
    DEFINE_CMP_OP_FUNC_IT_IT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_IT_IT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_IT_IT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_IT_IT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_IT_IT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_IT_IT(CO_NE, !=); \
    DEFINE_CMP_FUNC_IT_IT();

#define DEFINE_CMP_FUNCS_ROWID_ROWID()     \
  DEFINE_CMP_OP_FUNC_ROWT_ROWT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_ROWT_ROWT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_ROWT_ROWT(CO_LT, <);  \
  DEFINE_CMP_OP_FUNC_ROWT_ROWT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_ROWT_ROWT(CO_GT, >);  \
  DEFINE_CMP_OP_FUNC_ROWT_ROWT(CO_NE, !=); \
  DEFINE_CMP_FUNC_ROWT_ROWT();

#define DEFINE_CMP_FUNCS_STRING_STRING() \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_NE, !=); \
  DEFINE_CMP_FUNC_STRING_STRING()

#define DEFINE_CMP_FUNCS_RAW_RAW() \
  DEFINE_CMP_OP_FUNC_RAW_RAW(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_RAW_RAW(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_RAW_RAW(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_RAW_RAW(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_RAW_RAW(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_RAW_RAW(CO_NE, !=); \
  DEFINE_CMP_FUNC_RAW_RAW()

#define DEFINE_CMP_FUNCS_ENUMSETINNER_REAL(real_tc, real_type) \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_REAL(real_tc, real_type, CO_NE, !=); \
  DEFINE_CMP_FUNC_ENUMSETINNER_REAL(real_tc, real_type);

#define DEFINE_CMP_FUNCS_ENUMSET_INT();\
  DEFINE_CMP_OP_FUNC_ENUMSET_INT(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_ENUMSET_INT(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_ENUMSET_INT(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_ENUMSET_INT(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_ENUMSET_INT(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_ENUMSET_INT(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_ENUMSET_INT();

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_ENUMSET_UINT();\
  DEFINE_CMP_OP_FUNC_ENUMSET_UINT(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_ENUMSET_UINT(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_ENUMSET_UINT(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_ENUMSET_UINT(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_ENUMSET_UINT(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_ENUMSET_UINT(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_ENUMSET_UINT();


#define DEFINE_CMP_FUNCS_ENUMSETINNER_INT()\
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_INT(CO_NE, !=); \
  DEFINE_CMP_FUNC_ENUMSETINNER_INT();

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_ENUMSETINNER_UINT() \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_UINT(CO_NE, !=); \
  DEFINE_CMP_FUNC_ENUMSETINNER_UINT();

// float/double comparison using "==" or "!=" matches MySQL
// and Oracle doesn't support raw float/double
#define DEFINE_CMP_FUNCS_ENUMSETINNER_FLOAT()\
  DEFINE_CMP_FUNCS_ENUMSETINNER_REAL(ObFloatTC, float);

#define DEFINE_CMP_FUNCS_ENUMSETINNER_DOUBLE()\
  DEFINE_CMP_FUNCS_ENUMSETINNER_REAL(ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_ENUMSETINNER_NUMBER() \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_NUMBER(CO_NE, !=); \
  DEFINE_CMP_FUNC_ENUMSETINNER_NUMBER()

#define DEFINE_CMP_FUNCS_ENUMSETINNER_DECIMALINT()                                                 \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(CO_EQ, ==);                                           \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(CO_LE, <=);                                           \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(CO_LT, <);                                            \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(CO_GE, >=);                                           \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(CO_GT, >);                                            \
  DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(CO_NE, !=);                                           \
  DEFINE_CMP_FUNC_ENUMSETINNER_DECIMALINT();

#define DEFINE_CMP_FUNCS_TEXT_TEXT() \
  DEFINE_CMP_OP_FUNC_TEXT_TEXT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_TEXT_TEXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_TEXT_TEXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_TEXT_TEXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_TEXT_TEXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_TEXT_TEXT(CO_NE, !=); \
  DEFINE_CMP_FUNC_TEXT_TEXT()

#define DEFINE_CMP_FUNCS_LOB_LOB() \
  DEFINE_CMP_OP_FUNC_LOB_LOB(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_LOB_LOB(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_LOB_LOB(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_LOB_LOB(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_LOB_LOB(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_LOB_LOB(CO_NE, !=); \
  DEFINE_CMP_FUNC_LOB_LOB()

#define DEFINE_CMP_FUNCS_JSON_JSON() \
  DEFINE_CMP_OP_FUNC_JSON_JSON(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_JSON_JSON(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_JSON_JSON(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_JSON_JSON(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_JSON_JSON(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_JSON_JSON(CO_NE, !=); \
  DEFINE_CMP_FUNC_JSON_JSON()

#define DEFINE_CMP_FUNCS_GEOMETRY_GEOMETRY() \
  DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_GEOMETRY_GEOMETRY(CO_NE, !=); \
  DEFINE_CMP_FUNC_GEOMETRY_GEOMETRY()

#define DEFINE_CMP_FUNCS_UDT_UDT() \
  DEFINE_CMP_OP_FUNC_UDT_UDT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_UDT_UDT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_UDT_UDT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_UDT_UDT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_UDT_UDT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_UDT_UDT(CO_NE, !=); \
  DEFINE_CMP_FUNC_UDT_UDT()

#define DEFINE_CMP_FUNCS_COLLECTION_COLLECTION() \
  DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_COLLECTION_COLLECTION(CO_NE, !=); \
  DEFINE_CMP_FUNC_COLLECTION_COLLECTION()

#define DEFINE_CMP_FUNCS_STRING_TEXT() \
  DEFINE_CMP_OP_FUNC_STRING_TEXT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_STRING_TEXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_STRING_TEXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_STRING_TEXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_STRING_TEXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_STRING_TEXT(CO_NE, !=); \
  DEFINE_CMP_FUNC_STRING_TEXT()

#define DEFINE_CMP_FUNCS_TEXT_STRING() \
  DEFINE_CMP_OP_FUNC_TEXT_STRING(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_TEXT_STRING(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_TEXT_STRING(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_TEXT_STRING(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_TEXT_STRING(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_TEXT_STRING(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_TEXT_STRING()

//==============================

#define DEFINE_CMP_FUNCS_EXT_NULL() \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_EXT_NULL()

#define DEFINE_CMP_FUNCS_EXT_EXT() \
  DEFINE_CMP_OP_FUNC(ObExtendTC, ext, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC(ObExtendTC, ext, CO_NE, !=); \
  DEFINE_CMP_FUNC_EXT_EXT()

#define DEFINE_CMP_FUNCS_NULL_XXX() \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_NE, !=); \
  DEFINE_CMP_FUNC_NULL_XXX()

#define DEFINE_CMP_FUNCS_XXX_NULL() \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_XXX_NULL()

#define DEFINE_CMP_FUNCS_XXX_EXT() \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_NE, !=); \
  DEFINE_CMP_FUNC_XXX_EXT()

#define DEFINE_CMP_FUNCS_EXT_XXX() \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_EXT_XXX()

#define DEFINE_CMP_FUNCS_JSON_EXTEND() \
  DEFINE_CMP_OP_FUNC_JSON_EXTEND(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_JSON_EXTEND(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_JSON_EXTEND(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_JSON_EXTEND(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_JSON_EXTEND(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_JSON_EXTEND(CO_NE, !=); \
  DEFINE_CMP_FUNC_JSON_EXTEND()

#define DEFINE_CMP_FUNCS_UNKNOWN_UNKNOWN() \
  DEFINE_CMP_FUNCS(ObUnknownTC, unknown);

//==============================

DEFINE_CMP_FUNCS_NULL_NULL();
DEFINE_CMP_FUNCS_NULL_EXT();

DEFINE_CMP_FUNCS_INT_INT();
DEFINE_CMP_FUNCS_BIT_BIT();
DEFINE_CMP_FUNCS_INT_UINT();
DEFINE_CMP_FUNCS_INT_FLOAT();
DEFINE_CMP_FUNCS_INT_DOUBLE();
DEFINE_CMP_FUNCS_INT_NUMBER();
DEFINE_CMP_FUNCS_INT_ENUMSET();

DEFINE_CMP_FUNCS_UINT_INT();
DEFINE_CMP_FUNCS_UINT_UINT();
DEFINE_CMP_FUNCS_UINT_FLOAT();
DEFINE_CMP_FUNCS_UINT_DOUBLE();
DEFINE_CMP_FUNCS_UINT_NUMBER();
DEFINE_CMP_FUNCS_UINT_ENUMSET();

DEFINE_CMP_FUNCS_ENUMSET_INT();
DEFINE_CMP_FUNCS_ENUMSET_UINT();
DEFINE_CMP_FUNCS_ENUMSET_FLOAT();
DEFINE_CMP_FUNCS_ENUMSET_DOUBLE();
DEFINE_CMP_FUNCS_ENUMSET_NUMBER();
DEFINE_CMP_FUNCS_ENUMSET_ENUMSET();//for sort

DEFINE_CMP_FUNCS_FLOAT_INT();
DEFINE_CMP_FUNCS_FLOAT_UINT();
DEFINE_CMP_FUNCS_FLOAT_FLOAT();
DEFINE_CMP_FUNCS_FLOAT_DOUBLE();
DEFINE_CMP_FUNCS_FLOAT_ENUMSET();

DEFINE_CMP_FUNCS_DOUBLE_INT();
DEFINE_CMP_FUNCS_DOUBLE_UINT();
DEFINE_CMP_FUNCS_DOUBLE_FLOAT();
DEFINE_CMP_FUNCS_DOUBLE_DOUBLE();
DEFINE_CMP_FUNCS_DOUBLE_ENUMSET();

DEFINE_CMP_FUNCS_NUMBER_INT();
DEFINE_CMP_FUNCS_NUMBER_UINT();
DEFINE_CMP_FUNCS_NUMBER_NUMBER();
DEFINE_CMP_FUNCS_NUMBER_ENUMSET();


DEFINE_CMP_FUNCS_DECIMALINT_DECIMALINT();
DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(ObIntTC, int64_t, int);
DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(ObUIntTC, uint64_t, uint64);
DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(ObIntTC);
DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(ObUIntTC);
DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(ObEnumSetTC, uint64_t, uint64);
DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(ObEnumSetTC);
DEFINE_CMP_FUNCS_DECIMALINT_NUMBER();
DEFINE_CMP_FUNCS_NUMBER_DECIMALINT();

DEFINE_CMP_FUNCS_DATETIME_DATETIME();

DEFINE_CMP_FUNCS_DATETIME_OTIMESTAMP();
DEFINE_CMP_FUNCS_OTIMESTAMP_DATETIME();
DEFINE_CMP_FUNCS_OTIMESTAMP_OTIMESTAMP();

DEFINE_CMP_FUNCS_INTERVAL_INTERVAL();

DEFINE_CMP_FUNCS_ROWID_ROWID();

DEFINE_CMP_FUNCS_DATE_DATE();
DEFINE_CMP_FUNCS_TIME_TIME();
DEFINE_CMP_FUNCS_YEAR_YEAR();
DEFINE_CMP_FUNCS_STRING_STRING();
DEFINE_CMP_FUNCS_RAW_RAW();
DEFINE_CMP_FUNCS_TEXT_TEXT();
DEFINE_CMP_FUNCS_STRING_TEXT();
DEFINE_CMP_FUNCS_TEXT_STRING();
DEFINE_CMP_FUNCS_LOB_LOB();
DEFINE_CMP_FUNCS_JSON_JSON();
DEFINE_CMP_FUNCS_GEOMETRY_GEOMETRY();
DEFINE_CMP_FUNCS_UDT_UDT();
DEFINE_CMP_FUNCS_COLLECTION_COLLECTION();

DEFINE_CMP_FUNCS_ENUMSETINNER_INT();
DEFINE_CMP_FUNCS_ENUMSETINNER_UINT();
DEFINE_CMP_FUNCS_ENUMSETINNER_FLOAT();
DEFINE_CMP_FUNCS_ENUMSETINNER_DOUBLE();
DEFINE_CMP_FUNCS_ENUMSETINNER_NUMBER();
DEFINE_CMP_FUNCS_ENUMSETINNER_DECIMALINT();

DEFINE_CMP_FUNCS_EXT_NULL();
DEFINE_CMP_FUNCS_EXT_EXT();

DEFINE_CMP_FUNCS_UNKNOWN_UNKNOWN();

DEFINE_CMP_FUNCS_NULL_XXX();
DEFINE_CMP_FUNCS_XXX_NULL();
DEFINE_CMP_FUNCS_XXX_EXT();
DEFINE_CMP_FUNCS_EXT_XXX();
DEFINE_CMP_FUNCS_JSON_EXTEND();

#define DEFINE_CMP_FUNCS_ENTRY(tc1, tc2) \
{ \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_EQ>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_LE>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_LT>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_GE>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_GT>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_NE>, \
  ObObjCmpFuncs::cmp_func<tc1, tc2> \
}

#define DEFINE_CMP_FUNCS_ENTRY_NULL   {NULL, NULL, NULL, NULL, NULL, NULL, NULL}

#define DECLARE_CMP_FUNCS_NULLSAFE_NULL_NULL \
  ObObjStrongCompare::compare_nullsafe_null_null

#define DEFINE_CMP_FUNCS_NULLSAFE_NULL_NULL                             \
  static int compare_nullsafe_null_null(                                \
                                        const ObObj &obj1,              \
                                        const ObObj &obj2,              \
                                        ObCollationType cs_type,        \
                                        ObCmpNullPos null_pos)          \
  {                                                                     \
    int cmp = ObObjCmpFuncs::CR_EQ;                                     \
    ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, null_pos); \
    cmp = ObObjCmpFuncs::cmp_func<ObNullTC, ObNullTC>(obj1, obj2, cmp_ctx); \
    return cmp;                                                         \
  }

#define DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(tc)  \
  ObObjStrongCompare::compare_nullsafe_##tc##_null

#define DEFINE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(tc)         \
  static int compare_nullsafe_##tc##_null(                              \
                                          const ObObj &obj1,            \
                                          const ObObj &obj2,            \
                                          ObCollationType cs_type,      \
                                          ObCmpNullPos null_pos)        \
  {                                                                     \
    int cmp = ObObjCmpFuncs::CR_EQ;                                     \
    ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, null_pos); \
    if (!obj1.is_null()) {                                              \
      cmp = ObObjCmpFuncs::cmp_func<tc, ObNullTC>(obj1, obj2, cmp_ctx); \
    } else {                                                            \
      cmp = ObObjCmpFuncs::cmp_func<ObNullTC, ObNullTC>(obj1, obj2, cmp_ctx); \
    }                                                                   \
    return cmp;                                                         \
  }

#define DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(tc)   \
  ObObjStrongCompare::compare_nullsafe_null_##tc

#define DEFINE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(tc)                    \
  static int compare_nullsafe_null_##tc(                                \
                                        const ObObj &obj1,              \
                                        const ObObj &obj2,              \
                                        ObCollationType cs_type,        \
                                        ObCmpNullPos null_pos)          \
  {                                                                     \
    int cmp = ObObjCmpFuncs::CR_EQ;                                     \
    ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, null_pos); \
    if (!obj2.is_null()) {                                              \
      cmp = ObObjCmpFuncs::cmp_func<ObNullTC, tc>(obj1, obj2, cmp_ctx); \
    } else {                                                            \
      cmp = ObObjCmpFuncs::cmp_func<ObNullTC, ObNullTC>(obj1, obj2, cmp_ctx); \
    }                                                                   \
    return cmp;                                                         \
  }

#define DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(tc1, tc2, tc3, tc4)        \
  ObObjStrongCompare::compare_nullsafe_##tc1##_##tc2

#define DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(tc1, tc2, tc3, tc4)             \
  static int compare_nullsafe_##tc1##_##tc2(                            \
    const ObObj &obj1,                                                  \
    const ObObj &obj2,                                                  \
    ObCollationType cs_type,                                            \
    ObCmpNullPos null_pos)                                              \
  {                                                                     \
    int cmp = ObObjCmpFuncs::CR_EQ;                                     \
                                                                        \
    ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, null_pos); \
    if (!obj1.is_null() && !obj2.is_null()) {                           \
      cmp = ObObjCmpFuncs::cmp_func<tc1, tc2>(obj1, obj2, cmp_ctx);     \
    } else if (!obj1.is_null() && obj2.is_null()) {                     \
      cmp = ObObjCmpFuncs::cmp_func<tc3, ObNullTC>(obj1, obj2, cmp_ctx); \
    } else if (obj1.is_null() && !obj2.is_null()) {                     \
      cmp = ObObjCmpFuncs::cmp_func<ObNullTC, tc4>(obj1, obj2, cmp_ctx); \
    } else if (obj1.is_null() && obj2.is_null()) {                      \
      cmp = ObObjCmpFuncs::cmp_func<ObNullTC, ObNullTC>(obj1, obj2, cmp_ctx); \
    }                                                                   \
    return cmp;                                                         \
  }

namespace ObObjStrongCompare
{
DEFINE_CMP_FUNCS_NULLSAFE_NULL_NULL
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObBitTC, ObBitTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDateTC, ObDateTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDateTimeTC, ObDateTimeTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObDoubleTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObEnumSetTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObFloatTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObDoubleTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObFloatTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObNumberTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObDecimalIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObDoubleTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObEnumSetTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObFloatTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObNumberTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObDecimalIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObExtendTC, ObExtendTC, ObExtendTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObDoubleTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObEnumSetTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObFloatTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObDoubleTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObEnumSetTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObFloatTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObNumberTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObDecimalIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObEnumSetTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObNumberTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObDecimalIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObStringTC, ObStringTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObStringTC, ObTextTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObTextTC, ObStringTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObTextTC, ObTextTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObTimeTC, ObTimeTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObDoubleTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObEnumSetTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObFloatTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObDecimalIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObNumberTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUnknownTC, ObUnknownTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObYearTC, ObYearTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObOTimestampTC, ObOTimestampTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObOTimestampTC, ObDateTimeTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObRawTC, ObRawTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntervalTC, ObIntervalTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObRowIDTC, ObRowIDTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObLobTC, ObLobTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObJsonTC, ObJsonTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObGeometryTC, ObGeometryTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObDecimalIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObUIntTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObNumberTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObEnumSetTC, ObMaxTC, ObMaxTC)

DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObUserDefinedSQLTC, ObUserDefinedSQLTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_ENTRY(ObCollectionSQLTC, ObCollectionSQLTC, ObMaxTC, ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObExtendTC)
DEFINE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC)
DEFINE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObExtendTC)
DEFINE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC)
}

const obj_cmp_func_nullsafe ObObjCmpFuncs::cmp_funcs_nullsafe[ObMaxTC][ObMaxTC] =
{
  { // null
    DECLARE_CMP_FUNCS_NULLSAFE_NULL_NULL,
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObExtendTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC), //bit
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),//setenun
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),//setenuninner
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),// otimestamp
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),// raw
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),//interval
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),//rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_LEFTNULL_ENTRY(ObMaxTC),// decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // int
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObUIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObFloatTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObDoubleTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObNumberTC, ObMaxTC, ObMaxTC),
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObEnumSetTC, ObMaxTC, ObMaxTC),  // enumset
    NULL,  //enumsetInner will not go here,
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntTC, ObDecimalIntTC, ObMaxTC, ObMaxTC), // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // uint
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObUIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObFloatTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObDoubleTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObNumberTC, ObMaxTC, ObMaxTC),
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObEnumSetTC, ObMaxTC, ObMaxTC),//enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUIntTC, ObDecimalIntTC, ObMaxTC, ObMaxTC), // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // float
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObUIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObFloatTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObDoubleTC, ObMaxTC, ObMaxTC),
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObFloatTC, ObEnumSetTC, ObMaxTC, ObMaxTC),//enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // double
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObUIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObFloatTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObDoubleTC, ObMaxTC, ObMaxTC),
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDoubleTC, ObEnumSetTC, ObMaxTC, ObMaxTC),//enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimstamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // number
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObUIntTC, ObMaxTC, ObMaxTC),
    NULL,  // float
    NULL,  // double
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObNumberTC, ObMaxTC, ObMaxTC),
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObEnumSetTC, ObMaxTC, ObMaxTC),//enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObNumberTC, ObDecimalIntTC, ObMaxTC, ObMaxTC), // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // datetime
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDateTimeTC, ObDateTimeTC, ObMaxTC, ObMaxTC),
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    NULL,  //enumset
    NULL,  //enumsetInner will not go here
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDateTimeTC, ObDateTimeTC, ObMaxTC, ObMaxTC), // otimestamp
    NULL, //raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // date
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDateTC, ObDateTC, ObMaxTC, ObMaxTC),
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL,  //text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL,  // otimestamp
    NULL,  // raw
    NULL,  // interval
    NULL,  // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // time
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObTimeTC, ObTimeTC, ObMaxTC, ObMaxTC),
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // year
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObYearTC, ObYearTC, ObMaxTC, ObMaxTC),
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // string
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObStringTC, ObStringTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObStringTC, ObTextTC, ObMaxTC, ObMaxTC), //text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // extend
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObExtendTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObExtendTC, ObExtendTC, ObExtendTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), //text
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), //enumset
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), //enumsetInner
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), // otimestamp
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), // raw
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), // interval
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObExtendTC, ObMaxTC, ObExtendTC, ObMaxTC), // decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // unknown
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUnknownTC, ObUnknownTC, ObMaxTC, ObMaxTC),
    NULL, //text
    NULL,  // bit
    NULL,  //enumset
    NULL,  //enumsetInner
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // text
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObTextTC, ObStringTC, ObMaxTC, ObMaxTC),  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),  // extend
    NULL,  // unknown
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObTextTC, ObTextTC, ObMaxTC, ObMaxTC), //  text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // bit
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),  // extend
    NULL,  // unknown
    NULL, //  text
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObBitTC, ObBitTC, ObMaxTC, ObMaxTC),// bit
    NULL,  //enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { //enumset
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObUIntTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObFloatTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObDoubleTC, ObMaxTC, ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObNumberTC, ObMaxTC, ObMaxTC),
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC),
    NULL,  // unknown
    NULL, //text
    NULL,  //bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObEnumSetTC, ObMaxTC, ObMaxTC),//enumset just for sort
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetTC, ObDecimalIntTC, ObMaxTC, ObMaxTC),
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { //enumsetInner
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObIntTC, ObMaxTC, ObMaxTC), //int
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObUIntTC, ObMaxTC, ObMaxTC),//uint
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObFloatTC, ObMaxTC, ObMaxTC), //float
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObDoubleTC, ObMaxTC, ObMaxTC),//double
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObNumberTC, ObMaxTC, ObMaxTC), //number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC), //extended
    NULL,  // unknown
    NULL,  //text
    NULL,  //bit
    NULL,  //enumset will not go here
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObEnumSetInnerTC, ObDecimalIntTC, ObMaxTC, ObMaxTC), // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // otimestamp
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL, // int
    NULL, // uint
    NULL, // float
    NULL, // double
    NULL, // number
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObOTimestampTC, ObDateTimeTC, ObMaxTC, ObMaxTC),
    NULL, // date
    NULL, // time
    NULL, // year
    NULL, // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObMaxTC), // extended
    NULL, // unknown
    NULL, // text
    NULL, // bit
    NULL, // enumset
    NULL, // enumsetInner will not go here
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObOTimestampTC, ObOTimestampTC, ObMaxTC, ObMaxTC), // otimestamp
    NULL, //raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // raw
   DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
   NULL, // int
   NULL, // uint
   NULL, // float
   NULL, // double
   NULL, // number
   NULL, // datetime
   NULL, // date
   NULL, // time
   NULL, // year
   NULL, // string
   DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObMaxTC),
   NULL, // unknown
   NULL, // text
   NULL, // bit
   NULL, // enumset
   NULL, // enumsetInner will not go here
   NULL, // otimestamp
   DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObRawTC, ObRawTC, ObMaxTC, ObMaxTC), // raw
   NULL, // interval
   NULL, // rowid
   NULL, // lob
   NULL, // json
   NULL, // geometry
   NULL, // udt
   NULL, // decimal int
   NULL, // collection
   NULL, // mysql date
   NULL, // mysql datetime
   NULL, // roaringbitmap
  },
  { // interval
   DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
   NULL, // int
   NULL, // uint
   NULL, // float
   NULL, // double
   NULL, // number
   NULL, // datetime
   NULL, // date
   NULL, // time
   NULL, // year
   NULL, // string
   DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObMaxTC),
   NULL, // unknown
   NULL, // text
   NULL, // bit
   NULL, // enumset
   NULL, // enumsetInner will not go here
   NULL, // otimestamp
   NULL, // raw
   DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObIntervalTC, ObIntervalTC, ObMaxTC, ObMaxTC), // interval
   NULL, // rowid
   NULL, // lob
   NULL, // json
   NULL, // geometry
   NULL, // udt
   NULL, // decimal int
   NULL, // collection
   NULL, // mysql date
   NULL, // mysql datetime
   NULL, // roaringbitmap
  },
  { // rowid
   DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
   NULL, // int
   NULL, // uint
   NULL, // float
   NULL, // double
   NULL, // number
   NULL, // datetime
   NULL, // date
   NULL, // time
   NULL, // year
   NULL, // string
   DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObMaxTC),
   NULL, // unknown
   NULL, // text
   NULL, // bit
   NULL, // enumset
   NULL, // enumsetInner will not go here
   NULL, // otimestamp
   NULL, // raw
   NULL, // interval
   DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObRowIDTC, ObRowIDTC, ObMaxTC, ObMaxTC), // rowid
   NULL, // lob
   NULL, // json
   NULL, // geometry
   NULL, // udt
   NULL, // decimal int
   NULL, // collection
   NULL, // mysql date
   NULL, // mysql datetime
   NULL, // roaringbitmap
  },
  { // lob
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObLobTC, ObLobTC, ObMaxTC, ObMaxTC), // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // json
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObJsonTC, ObJsonTC, ObMaxTC, ObMaxTC), // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // geometry
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObGeometryTC, ObGeometryTC, ObMaxTC, ObMaxTC), // geometry
    NULL, // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // udt
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObUserDefinedSQLTC, ObUserDefinedSQLTC, ObMaxTC, ObMaxTC), // udt
    NULL, // decimal int
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // decimal int
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC), // null
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObIntTC, ObMaxTC, ObMaxTC), // int
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObUIntTC, ObMaxTC, ObMaxTC), // uint
    NULL, // float
    NULL, // double
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObNumberTC, ObMaxTC, ObMaxTC), // number
    NULL, // datetime
    NULL, // date
    NULL, // time
    NULL, // year
    NULL, // string
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObMaxTC, ObExtendTC, ObMaxTC, ObExtendTC), // extend
    NULL, // unknown
    NULL, // text
    NULL, // bit
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObEnumSetTC, ObMaxTC, ObMaxTC), // enumset
    NULL, // enumset inner
    NULL, // otimestamp
    NULL, // raw.
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObDecimalIntTC, ObDecimalIntTC, ObMaxTC, ObMaxTC), // decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // collection
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimalint
    DECLARE_CMP_FUNCS_NULLSAFE_ENTRY(ObCollectionSQLTC, ObCollectionSQLTC, ObMaxTC, ObMaxTC), // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // mysql date
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC), // null
    NULL, // int
    NULL, // uint
    NULL, // float
    NULL, // double
    NULL, // number
    NULL, // datetime
    NULL, // date
    NULL, // time
    NULL, // year
    NULL, // string
    NULL, // extend
    NULL, // unknown
    NULL, // text
    NULL, // bit
    NULL, // enumset
    NULL, // enumset inner
    NULL, // otimestamp
    NULL, // raw.
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // mysql datetime
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC), // null
    NULL, // int
    NULL, // uint
    NULL, // float
    NULL, // double
    NULL, // number
    NULL, // datetime
    NULL, // date
    NULL, // time
    NULL, // year
    NULL, // string
    NULL, // extend
    NULL, // unknown
    NULL, // text
    NULL, // bit
    NULL, // enumset
    NULL, // enumset inner
    NULL, // otimestamp
    NULL, // raw.
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
  { // roaringbitmap
    DECLARE_CMP_FUNCS_NULLSAFE_RIGHTNULL_ENTRY(ObMaxTC),
    NULL,  // int
    NULL,  // uint
    NULL,  // float
    NULL,  // double
    NULL,  // number
    NULL,  // datetime
    NULL,  // date
    NULL,  // time
    NULL,  // year
    NULL,  // string
    NULL,  // extend
    NULL,  // unknown
    NULL,  // text
    NULL,  // bit
    NULL,  // enumset
    NULL,  //enumsetInner will not go here
    NULL, // otimestamp
    NULL, // raw
    NULL, // interval
    NULL, // rowid
    NULL, // lob
    NULL, // json
    NULL, // geometry
    NULL, // udt
    NULL, // decimalint
    NULL, // collection
    NULL, // mysql date
    NULL, // mysql datetime
    NULL, // roaringbitmap
  },
};

const obj_cmp_func ObObjCmpFuncs::cmp_funcs[ObMaxTC][ObMaxTC][CO_MAX] =
{
  { // null
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//text
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//bit
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//setenun
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//setenuninner
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//raw
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//interval
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//rowid
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//lob
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//json
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//geometry
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//udt
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC), // roaringbitmap
  },
  { // int
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObEnumSetTC),  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObDecimalIntTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // uint
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObEnumSetTC),//enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObDecimalIntTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // float
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObEnumSetTC),//enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL, // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // double
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObEnumSetTC),//enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // number
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObEnumSetTC),//enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObDecimalIntTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // datetime
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY(ObDateTimeTC, ObDateTimeTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY(ObDateTimeTC, ObOTimestampTC),//otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // date
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY(ObDateTC, ObDateTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // time
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY(ObTimeTC, ObTimeTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // year
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY(ObYearTC, ObYearTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY(ObStringTC, ObStringTC),
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY(ObStringTC, ObTextTC), //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // extend
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC), //text
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//enumset
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//enumsetInner
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//raw
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//interval
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // unknown
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUnknownTC, ObUnknownTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // text
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY(ObTextTC, ObStringTC),  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),  // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY(ObTextTC, ObTextTC), //  text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // bit
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),  // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //  text
    DEFINE_CMP_FUNCS_ENTRY(ObBitTC, ObBitTC),// bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { //enumset
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //bit
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObEnumSetTC),//enumset just for sort
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObDecimalIntTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { //enumsetInner
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetInnerTC, ObIntTC), //int
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetInnerTC, ObUIntTC),//uint
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetInnerTC, ObFloatTC), //float
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetInnerTC, ObDoubleTC),//double
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetInnerTC, ObNumberTC), //number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC), //extended
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetInnerTC, ObDecimalIntTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY(ObOTimestampTC, ObDateTimeTC), // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY(ObOTimestampTC, ObOTimestampTC),  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // raw
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObRawTC, ObRawTC),  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // interval
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC), //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY(ObIntervalTC, ObIntervalTC),  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // rowid
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC), //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY(ObRowIDTC, ObRowIDTC),  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // lob
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // json
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObJsonTC, ObExtendTC), //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY(ObJsonTC, ObJsonTC),  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL, // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // geometry
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY(ObGeometryTC, ObGeometryTC),  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // udt
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY(ObUserDefinedSQLTC, ObUserDefinedSQLTC), //  compare for udt nested null bitmap
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // decimalint
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObIntTC), // int
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObUIntTC), // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL, // float
    DEFINE_CMP_FUNCS_ENTRY_NULL, // double
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObNumberTC), // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC), // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObEnumSetTC), // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL, // json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // udt
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObDecimalIntTC), // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // collection
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY(ObCollectionSQLTC, ObCollectionSQLTC), // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // mysql date
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL, // int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL, // float
    DEFINE_CMP_FUNCS_ENTRY_NULL, // double
    DEFINE_CMP_FUNCS_ENTRY_NULL, // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL, // int
    DEFINE_CMP_FUNCS_ENTRY_NULL, // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL, // float
    DEFINE_CMP_FUNCS_ENTRY_NULL, // double
    DEFINE_CMP_FUNCS_ENTRY_NULL, // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL, // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL, // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL, // json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
  { // roaringbitmap
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // collection
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql date
    DEFINE_CMP_FUNCS_ENTRY_NULL, // mysql datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL, // roaringbitmap
  },
};

const ObObj ObObjCmpFuncs::cmp_res_objs_bool[CR_BOOL_CNT] =
{
  ObObj(static_cast<int32_t>(false)),
  ObObj(static_cast<int32_t>(true)),
  ObObj(ObNullType),
};

const ObObj ObObjCmpFuncs::cmp_res_objs_int[CR_INT_CNT] =
{
  ObObj(static_cast<int32_t>(-1)),
  ObObj(static_cast<int32_t>(0)),
  ObObj(static_cast<int32_t>(1)),
  ObObj(ObNullType)
};

int ObObjCmpFuncs::compare_oper(const ObObj &obj1,
                                const ObObj &obj2,
                                ObCollationType cs_type,
                                ObCmpOp cmp_op,
                                bool &bret)
{
  int ret = OB_SUCCESS;
  int cmp = CR_FALSE;
  ObObjType type1 = obj1.get_type();
  ObObjType type2 = obj2.get_type();
  // maybe we should not check tc1, tc2 and cmp_op,
  // because this function is so fundamental and performance related.
  if (OB_UNLIKELY(ob_is_invalid_obj_type(type1)
                  || ob_is_invalid_obj_type(type2)
                  || ob_is_invalid_cmp_op_bool(cmp_op))) {
    LOG_ERROR("invalid obj1 or obj2 or cmp_op", K(obj1), K(obj2), K(cmp_op));
    ret = OB_ERR_UNEXPECTED;
  } else {
    obj_cmp_func cmp_op_func = NULL;
    if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                  obj2.get_meta(),
                                                  cmp_op,
                                                  cmp_op_func))) {
      LOG_ERROR("obj1 and obj2 can't compare", K(obj1), K(obj2), K(cmp_op));
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, default_null_pos());
      if (OB_UNLIKELY(CR_OB_ERROR == (cmp = cmp_op_func(obj1, obj2, cmp_ctx)))) {
        LOG_ERROR("failed to compare obj1 and obj2", K(obj1), K(obj2), K(cmp_op));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  bret = static_cast<bool>(cmp);
  return ret;
}

// TODO by fengshuo.fs: remove this function
bool ObObjCmpFuncs::compare_oper_nullsafe(const ObObj &obj1,
                                          const ObObj &obj2,
                                          ObCollationType cs_type,
                                          ObCmpOp cmp_op)
{
  int cmp = CR_FALSE;
  ObObjType type1 = obj1.get_type();
  ObObjType type2 = obj2.get_type();
  // maybe we should not check tc1, tc2 and cmp_op,
  // because this function is so fundamental and performance related.
  if (OB_UNLIKELY(ob_is_invalid_obj_type(type1)
                  || ob_is_invalid_obj_type(type2)
                  || ob_is_invalid_cmp_op_bool(cmp_op))) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid obj1 or obj2 or cmp_op", K(obj1), K(obj2), K(cmp_op));
    right_to_die_or_duty_to_live();
  } else {
    obj_cmp_func cmp_op_func = NULL;
    if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                  obj2.get_meta(),
                                                  cmp_op,
                                                  cmp_op_func))) {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "obj1 and obj2 can't compare", K(obj1), K(obj2), K(cmp_op));
      right_to_die_or_duty_to_live();
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, default_null_pos());
      if (OB_UNLIKELY(CR_OB_ERROR == (cmp = cmp_op_func(obj1, obj2, cmp_ctx)))) {
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "failed to compare obj1 and obj2", K(obj1), K(obj2), K(cmp_op));
        right_to_die_or_duty_to_live();
      }
    }
  }
  return static_cast<bool>(cmp);
}

int ObObjCmpFuncs::compare(const ObObj &obj1,
                           const ObObj &obj2,
                           ObCollationType cs_type,
                           int &cmp)
{
  int ret = OB_SUCCESS;
  obj_cmp_func cmp_func = NULL;
  cmp = CR_EQ;
  if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                obj2.get_meta(),
                                                CO_CMP,
                                                cmp_func))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj1 and obj2 can't compare", K(obj1), K(obj2), K(obj1.get_meta()), K(obj2.get_meta()));
  } else {
    ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, lib::is_oracle_mode() ? NULL_LAST : NULL_FIRST);
    if (OB_UNLIKELY(CR_OB_ERROR == (cmp = cmp_func(obj1, obj2, cmp_ctx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to compare obj1 and obj2", K(obj1), K(obj2), K(obj1.get_meta()), K(obj2.get_meta()));
    }
  }
  return ret;
}

// TODO by fengshuo.fs: remove this function
int ObObjCmpFuncs::compare_nullsafe(const ObObj &obj1,
                                    const ObObj &obj2,
                                    ObCollationType cs_type)
{
  int cmp = CR_EQ;
  obj_cmp_func cmp_func = NULL;
  if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                obj2.get_meta(),
                                                CO_CMP,
                                                cmp_func))) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "obj1 and obj2 can't compare", K(obj1), K(obj2), K(obj1.get_meta()), K(obj2.get_meta()));
    right_to_die_or_duty_to_live();
  } else {
    ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, lib::is_oracle_mode() ? NULL_LAST : NULL_FIRST);
    if (OB_UNLIKELY(CR_OB_ERROR == (cmp = cmp_func(obj1, obj2, cmp_ctx)))) {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "failed to compare obj1 and obj2", K(obj1), K(obj2), K(obj1.get_meta()), K(obj2.get_meta()));
      right_to_die_or_duty_to_live();
    }
  }
  return cmp;
}

int ObObjCmpFuncs::compare(const ObObj &obj1,
                           const ObObj &obj2,
                           ObCompareCtx &cmp_ctx,
                           int &cmp)
{
  int ret = OB_SUCCESS;
  ObObjType type1 = obj1.get_type();
  ObObjType type2 = obj2.get_type();
  cmp = CR_EQ;
  // maybe we should not check tc1 and tc2,
  // because this function is so fundamental and performance related.
  if (ob_is_invalid_obj_type(type1)
                  || ob_is_invalid_obj_type(type2)) {
    LOG_ERROR("invalid obj1 or obj2", K(obj1), K(obj2));
    ret = OB_ERR_UNEXPECTED;
  } else {
    obj_cmp_func cmp_func = NULL;
    if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                  obj2.get_meta(),
                                                  CO_CMP,
                                                  cmp_func))) {
      LOG_ERROR("obj1 and obj2 can't compare", K(obj1), K(obj2));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(CR_OB_ERROR == (cmp = cmp_func(obj1, obj2, cmp_ctx)))) {
      LOG_ERROR("failed to compare obj1 and obj2", K(obj1), K(obj2));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // do nothing
    }
  }
  return ret;
}

// TODO by fengshuo.fs: remove this function
int ObObjCmpFuncs::compare_nullsafe(const ObObj &obj1,
                                    const ObObj &obj2,
                                    ObCompareCtx &cmp_ctx)
{
  int cmp = CR_EQ;
  ObObjType type1 = obj1.get_type();
  ObObjType type2 = obj2.get_type();
  // maybe we should not check tc1 and tc2,
  // because this function is so fundamental and performance related.
  if (ob_is_invalid_obj_type(type1)
                  || ob_is_invalid_obj_type(type2)) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid obj1 or obj2", K(obj1), K(obj2));
    right_to_die_or_duty_to_live();
  } else {
    obj_cmp_func cmp_func = NULL;
    if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                  obj2.get_meta(),
                                                  CO_CMP,
                                                  cmp_func))) {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "obj1 and obj2 can't compare", K(obj1), K(obj2));
      right_to_die_or_duty_to_live();
    } else if (OB_UNLIKELY(CR_OB_ERROR == (cmp = cmp_func(obj1, obj2, cmp_ctx)))) {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "failed to compare obj1 and obj2", K(obj1), K(obj2));
      right_to_die_or_duty_to_live();
    } else {
      // do nothing
    }
  }
  return cmp;
}

int ObObjCmpFuncs::compare(ObObj &result,
                           const ObObj &obj1,
                           const ObObj &obj2,
                           const ObCompareCtx &cmp_ctx,
                           ObCmpOp cmp_op,
                           bool &need_cast)
{
  int ret = OB_SUCCESS;
  ObObjType type1 = obj1.get_type();
  ObObjType type2 = obj2.get_type();
  obj_cmp_func cmp_op_func = NULL;
  need_cast = false;
  if (OB_UNLIKELY(ob_is_invalid_obj_type(type1)
                  || ob_is_invalid_obj_type(type2)
                  || ob_is_invalid_cmp_op(cmp_op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument", K(ret), K(obj1), K(obj2), K(cmp_op));
  } else if (OB_UNLIKELY(false == can_cmp_without_cast(obj1.get_meta(),
                                                       obj2.get_meta(),
                                                       cmp_op,
                                                       cmp_op_func))) {
    need_cast = true;
  } else {
    int cmp = cmp_op_func(obj1, obj2, cmp_ctx);
    if (OB_UNLIKELY(CR_OB_ERROR == cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to compare obj1 and obj2", K(ret), K(obj1), K(obj2), K(cmp_op));
    } else {
      // CR_LT is -1, CR_EQ is 0, so we add 1 to cmp_res_objs_int.
      result = (CO_CMP == cmp_op) ? (cmp_res_objs_int + 1)[cmp] : cmp_res_objs_bool[cmp];
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
