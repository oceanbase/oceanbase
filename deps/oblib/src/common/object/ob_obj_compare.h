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

#ifndef OCEANBASE_COMMON_OB_OBJ_COMPARE_
#define OCEANBASE_COMMON_OB_OBJ_COMPARE_

#include "lib/timezone/ob_timezone_info.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace common
{

bool is_calc_with_end_space(ObObjType type1, ObObjType type2,
                            bool is_oracle_mode,
                            ObCollationType cs_type1,
                            ObCollationType cs_type2);

enum ObCmpOp
{
  CO_EQ = 0,
  CO_LE,
  CO_LT,
  CO_GE,
  CO_GT,
  CO_NE,
  CO_CMP,
  CO_MAX
};

OB_INLINE bool ob_is_valid_cmp_op(ObCmpOp cmp_op) { return 0 <= cmp_op && cmp_op < CO_MAX; }
OB_INLINE bool ob_is_invalid_cmp_op(ObCmpOp cmp_op) { return !ob_is_valid_cmp_op(cmp_op); }
OB_INLINE bool ob_is_valid_cmp_op_bool(ObCmpOp cmp_op) { return 0 <= cmp_op && cmp_op < CO_CMP; }
OB_INLINE bool ob_is_invalid_cmp_op_bool(ObCmpOp cmp_op) { return !ob_is_valid_cmp_op_bool(cmp_op); }

class ObCompareCtx final
{
public:
  //member functions
  ObCompareCtx(const ObObjType cmp_type,
               const ObCollationType cmp_cs_type,
               const bool is_null_safe,
               const int64_t tz_off,
               const ObCmpNullPos null_pos)
    : cmp_type_(cmp_type),
      cmp_cs_type_(cmp_cs_type),
      is_null_safe_(is_null_safe),
      tz_off_(tz_off),
      null_pos_(null_pos)
  {}
  ObCompareCtx()
    : cmp_type_(ObMaxType),
      cmp_cs_type_(CS_TYPE_INVALID),
      is_null_safe_(false),
      tz_off_(INVALID_TZ_OFF),
      null_pos_(default_null_pos())
  {}
  TO_STRING_KV(K_(cmp_type), K_(cmp_cs_type), K_(is_null_safe), K_(tz_off));

public:
  //data members
  ObObjType cmp_type_;    // used by upper functions, not in these compare functions.
  ObCollationType cmp_cs_type_;
  bool is_null_safe_;
  int64_t tz_off_;
  // used for null comparison, NULL_FIRST means nulls is the least value, NULL_LAST means greatest
  ObCmpNullPos null_pos_;
};

typedef int (*obj_cmp_func)(const ObObj &obj1,
                            const ObObj &obj2,
                            const ObCompareCtx &cmp_ctx);

typedef int (*obj_cmp_func_nullsafe)(const ObObj &obj1,
                                     const ObObj &obj2,
                                     ObCollationType cs_type,
                                     ObCmpNullPos null_pos);

/**
 * at the very beginning, all compare functions in this class should be used to compare objects
 * with same type ONLY, because we can't do any cast operation here.
 * but after some performance problems, we realized that some cast operations can be skipped even
 * if objects have different types, such as int VS number, int VS float / double.
 * so actually we can do any compare operations which NEED NOT cast.
 */
class ObObjCmpFuncs
{
public:
  static inline bool is_datetime_timestamp_cmp(ObObjType type1, ObObjType type2)
  {
    return (ObTimestampType == type1 && ObDateTimeType == type2)
            || (ObDateTimeType == type1 && ObTimestampType == type2);
  }
  static inline bool is_otimestamp_cmp(ObObjType type1, ObObjType type2)
  {
    return ((ObTimestampTZType == type1 || ObTimestampLTZType == type1) && (ObDateTimeType == type2 || ObTimestampNanoType == type2))
            || ((ObDateTimeType == type1 || ObTimestampNanoType == type1) && (ObTimestampTZType == type2 || ObTimestampLTZType == type2));
  }
  /*
   * obj1 and obj2 can be compared without any casts
   */
  OB_INLINE static bool can_cmp_without_cast(const ObObjMeta& meta1,
                                             const ObObjMeta& meta2,
                                             ObCmpOp cmp_op,
                                             obj_cmp_func &cmp_func);
  /*
   * obj1 and obj2 can be compared without any casts, nullsafe
   */
  OB_INLINE static bool can_cmp_without_cast(ObObjType type1,
                                             ObObjType type2,
                                             ObCmpOp cmp_op,
                                             obj_cmp_func_nullsafe &cmp_func_nullsafe);
  static int compare_oper(const ObObj &obj1,
                          const ObObj &obj2,
                          ObCollationType cs_type,
                          ObCmpOp cmp_op,
                          bool &bret);
  /**
   * null safe compare.
   * will HANG (right_to_die_or_duty_to_live) if error, such as can't compare without cast.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cs_type: compare collation.
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE. CO_CMP is not allowed.
   * @return bool.
   */
  static bool compare_oper_nullsafe(const ObObj &obj1,
                                    const ObObj &obj2,
                                    ObCollationType cs_type,
                                    ObCmpOp cmp_op);
  
  static int compare(const ObObj &obj1,
                     const ObObj &obj2,
                     ObCollationType cs_type,
                     int &cmp);
  /**
   * null safe compare.
   * will HANG (right_to_die_or_duty_to_live) if error, such as can't compare without cast.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cs_type: compare collation.
   * @return -1: obj1 < obj2.
   *          0: obj1 = obj2.
   *          1: obj1 > obj2.
   */
  static int compare_nullsafe(const ObObj &obj1,
                              const ObObj &obj2,
                              ObCollationType cs_type);

  static int compare(const ObObj &obj1,
                     const ObObj &obj2,
                     ObCompareCtx &cmp_ctx,
                     int &cmp);
  static int compare_nullsafe(const ObObj &obj1,
                              const ObObj &obj2,
                              ObCompareCtx &cmp_ctx);
  /**
   * compare.
   * @param[out] result: true / false / -1 / 0 / 1 / null.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cmp_ctx
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE / CO_CMP.
   * @param[out] need_cast: set to true if can't compare without cast, otherwise false.
   * @return ob error code.
   */
  static int compare(ObObj &result,
                     const ObObj &obj1,
                     const ObObj &obj2,
                     const ObCompareCtx &cmp_ctx,
                     const ObCmpOp cmp_op,
                     bool &need_cast);
  /**
   * fast path
   * @param[out] result: true / false / -1 / 0 / 1 / null.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cmp_ctx
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE / CO_CMP.
   * @param[in] cmp_op_func: can NOT be NULL
   * @return ob error code.
   * */

   /* This inline func will be called outside of this compilation unit
      so, we have to implement it within this .h file
   */
  OB_INLINE static int compare(ObObj &result,
                               const ObObj &obj1,
                               const ObObj &obj2,
                               const ObCompareCtx &cmp_ctx,
                               const ObCmpOp cmp_op,
                               const obj_cmp_func cmp_op_func)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(cmp_op_func)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "unexpected error. null cmp func pointer", K(ret), K(obj1), K(obj2), K(cmp_op), K(cmp_op_func));
    } else {
      int cmp = cmp_op_func(obj1, obj2, cmp_ctx);
      if (OB_UNLIKELY(CR_OB_ERROR == cmp)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "failed to compare obj1 and obj2", K(ret), K(obj1), K(obj2), K(cmp_op));
      } else {
        // CR_LT is -1, CR_EQ is 0, so we add 1 to cmp_res_objs_int.
        result = (CO_CMP == cmp_op) ? (cmp_res_objs_int + 1)[cmp] : cmp_res_objs_bool[cmp];
      }
    }
    return ret;
  }

  /*
   * if comparison between obj1 and obj2 which of typeclass tc1 and tc2 need no cast
   *
   * then func will be not NULL if ret is OB_SUCCESS
   *
   * Otherwise, func will be NULL if any cast is necessary
   *
   */

  OB_INLINE static int get_cmp_func(const ObObjTypeClass tc1,
                                    const ObObjTypeClass tc2,
                                    ObCmpOp cmp_op,
                                    obj_cmp_func &func);

  OB_INLINE static int get_cmp_func(const ObObjTypeClass tc1,
                                    const ObObjTypeClass tc2,
                                    ObCmpOp cmp_op,
                                    obj_cmp_func_nullsafe &func);

  OB_INLINE static int fixed_double_cmp(const ObObj &obj1, const ObObj &obj2)
  {
    int ret = 0;
    const double P[] =
    {
      5/1e000, 5/1e001, 5/1e002, 5/1e003, 5/1e004, 5/1e005, 5/1e006, 5/1e007,
      5/1e008, 5/1e009, 5/1e010, 5/1e011, 5/1e012, 5/1e013, 5/1e014, 5/1e015,
      5/1e016, 5/1e017, 5/1e018, 5/1e019, 5/1e020, 5/1e021, 5/1e022, 5/1e023,
      5/1e024, 5/1e025, 5/1e026, 5/1e027, 5/1e028, 5/1e029, 5/1e030, 5/1e031
    };
    // Compatible with mysql, the condition for judging whether two fixed double are equal
    // is that their fabs is less than 5 divided by the log10 of the maximum scale plus 1.
    const int cmp_scale = MAX(obj1.get_scale(), obj2.get_scale()) + 1;
    double p = 0;
    if (cmp_scale <= 0 || cmp_scale > OB_NOT_FIXED_SCALE) {
      p = P[OB_NOT_FIXED_SCALE];
      COMMON_LOG(ERROR, "not fixed obj", K(obj1), K(obj2), K(lbt()));
    } else {
      p = P[cmp_scale];
    }
    const double l = obj1.get_double();
    const double r = obj2.get_double();
    if (isnan(l) || isnan(r)) {
      if (isnan(l) && isnan(r)) {
        ret = 0;
      } else if (isnan(l)) {
        ret = 1;
      } else {
        ret = -1;
      }
    } else if (l == r || fabs(l - r) < p) {
      ret = 0;
    } else {
      ret = (l < r ? -1 : 1);
    }
    return ret;
  }

  enum ObCmpRes
  {
    // for bool.
    CR_FALSE = 0,
    CR_TRUE = 1,
    // for int.
    CR_LT = -1,
    CR_EQ = 0,
    CR_GT = 1,
    // other.
    CR_NULL = 2,
    CR_OB_ERROR = 3,
    // count.
    CR_BOOL_CNT = 3,
    CR_INT_CNT = 4
  };
  // return CR_FALSE / CR_TRUE / CR_NULL / CR_OB_ERROR.
  template <ObObjTypeClass tc1, ObObjTypeClass tc2, ObCmpOp op>
  static int cmp_op_func(const ObObj &Obj1, const ObObj &obj2,
                         const ObCompareCtx &cmp_ctx);
  // return CR_LT / CR_EQ / CR_GT / CR_NULL / CR_OB_ERROR.
  template <ObObjTypeClass tc1, ObObjTypeClass tc2>
  static int cmp_func(const ObObj &Obj1, const ObObj &obj2, const ObCompareCtx &cmp_ctx);
private:
  OB_INLINE static int INT_TO_CR(int val) { return val < 0 ? CR_LT : val > 0 ? CR_GT : CR_EQ; }
private:
  static const obj_cmp_func cmp_funcs[ObMaxTC][ObMaxTC][CO_MAX];
  static const obj_cmp_func_nullsafe cmp_funcs_nullsafe[ObMaxTC][ObMaxTC];
  static const ObObj cmp_res_objs_bool[CR_BOOL_CNT];
  static const ObObj cmp_res_objs_int[CR_INT_CNT];
};

OB_INLINE int ObObjCmpFuncs::get_cmp_func(const ObObjTypeClass tc1,
                                          const ObObjTypeClass tc2,
                                          ObCmpOp cmp_op,
                                          obj_cmp_func &func)
{
  int ret = OB_SUCCESS;
  func = NULL;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op))) {
    func = NULL;
  } else if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1)
             || OB_UNLIKELY(ob_is_invalid_obj_tc(tc2)))) {
    func = NULL;
  } else {
    func = cmp_funcs[tc1][tc2][cmp_op];
  }
  return ret;
}

OB_INLINE int ObObjCmpFuncs::get_cmp_func(const ObObjTypeClass tc1,
                                          const ObObjTypeClass tc2,
                                          ObCmpOp cmp_op,
                                          obj_cmp_func_nullsafe &func)
{
  int ret = OB_SUCCESS;
  func = nullptr;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op))) {
    func = nullptr;
  } else if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) ||
                         ob_is_invalid_obj_tc(tc2))) {
    func = nullptr;
  } else {
    func = cmp_funcs_nullsafe[tc1][tc2];
  }
  return ret;
}

OB_INLINE bool ObObjCmpFuncs::can_cmp_without_cast(const ObObjMeta& meta1,
                                                   const ObObjMeta& meta2,
                                                   ObCmpOp cmp_op,
                                                   obj_cmp_func &cmp_func)
{
  int ret = OB_SUCCESS;
  const ObObjType type1 = meta1.get_type();
  const ObObjType type2 = meta2.get_type();
  const ObObjTypeClass tc1 = meta1.get_type_class();
  const ObObjTypeClass tc2 = meta2.get_type_class();
  cmp_func = NULL;
  bool need_no_cast = false;
  if (OB_UNLIKELY(is_datetime_timestamp_cmp(type1, type2))
      || OB_UNLIKELY(is_otimestamp_cmp(type1, type2))) {
    need_no_cast = false;
  } else if ((ObIntervalYMType == type1 && ObIntervalDSType == type2)
              || (ObIntervalYMType == type2 && ObIntervalDSType == type1)) {
    need_no_cast = false;
  } else if (OB_UNLIKELY(ob_is_string_type(type1)
                         && ob_is_string_type(type2)
                         && ((meta1.get_collation_type() != meta2.get_collation_type())))) {
    need_no_cast = false;
  } else if (OB_FAIL(get_cmp_func(tc1, tc2, cmp_op, cmp_func))) {
    COMMON_LOG(ERROR, "get cmp func failed", K(type1), K(type2), K(tc1), K(tc2), K(cmp_op));
  } else {
    need_no_cast = (cmp_func != NULL);
  }
  return need_no_cast;
}

OB_INLINE bool ObObjCmpFuncs::can_cmp_without_cast(ObObjType type1,
                                                   ObObjType type2,
                                                   ObCmpOp cmp_op,
                                                   obj_cmp_func_nullsafe &cmp_func)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  cmp_func = nullptr;

  bool need_no_cast = true;
  if (is_datetime_timestamp_cmp(type1, type2)) {
    need_no_cast = false;
  } else if ((ObIntervalYMType == type1 && ObIntervalDSType == type2)
              || (ObIntervalYMType == type2 && ObIntervalDSType == type1)) {
    need_no_cast = false;
  } else if (OB_FAIL(get_cmp_func(tc1, tc2, cmp_op, cmp_func))) {
    COMMON_LOG(ERROR, "get cmp func failed", K(type1), K(type2), K(tc1), K(tc2), K(cmp_op));
  } else {
    need_no_cast = (cmp_func != nullptr);
  }
  return need_no_cast;
}

} // namespace common
} // namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_OBJ_COMPARE_ */
