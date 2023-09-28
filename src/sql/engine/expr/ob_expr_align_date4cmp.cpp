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
#include "sql/engine/expr/ob_expr_align_date4cmp.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "common/object/ob_obj_compare.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

static const int8_t DAYS_OF_MON[2][12 + 1] = {
  {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
  {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

#define IS_LEAP_YEAR(y) ((((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0)) ? 1 : 0)

ObExprAlignDate4Cmp::ObExprAlignDate4Cmp(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ALIGN_DATE4CMP, N_ALIGN_DATE4CMP, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprAlignDate4Cmp::calc_result_type3(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           ObExprResType &type3,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *raw_expr = get_raw_expr();
  ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *>(raw_expr);
  if (OB_ISNULL(op_expr) || OB_UNLIKELY(op_expr->get_param_count() != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op raw expr is null or param_count error", K(ret), K(op_expr));
  } else {
    ObRawExpr *param3 = op_expr->get_param_expr(2);
    if (!param3->is_const_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw expr is not const expr", K(ret), KPC(param3));
    } else {
      ObConstRawExpr *const_param = static_cast<ObConstRawExpr *>(param3);
      if (OB_ISNULL(const_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("third child of the expr is null", K(ret), K(const_param));
      } else {
        if (const_param->get_value().is_null()) {
          type.set_null();
        } else {
          ObObjType res_type = ObObjType(const_param->get_value().get_int());
          switch(res_type) {
            case ObNullType: {
              type.set_null();
              break;
            }
            case ObDateTimeType: {
              type.set_datetime();
              break;
            }
            case ObDateType: {
              type.set_date();
              break;
            }
            default: {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("unexpected res type.", K(ret), K(res_type));
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAlignDate4Cmp::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (rt_expr.arg_cnt_ != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the arg_cnt of expr_align_date4cmp error.", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_[0]) ||
             OB_ISNULL(rt_expr.args_[1]) ||
             OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the arg of expr_align_date4cmp is null.", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = eval_align_date4cmp;
    rt_expr.extra_ = raw_expr.get_extra();
  }
  return ret;
}

int ObExprAlignDate4Cmp::eval_align_date4cmp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum* date_datum = NULL;
  ObDatum* cmp_type_datum = NULL;
  ObDatum* res_type_datum = NULL;
  DateArgType date_arg_type = NON_DATE;
  ObObjType res_type;
  ObTime ob_time;
  ObObjType date_arg_obj_type = ObMaxType;

  if (OB_FAIL(expr.args_[0]->eval(ctx, date_datum))) {
    LOG_WARN("expr_align_date4cmp eval arg[0] date fail.", K(ret), K(expr));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, cmp_type_datum))) {
    LOG_WARN("expr_align_date4cmp eval arg[1] cmp_type fail.", K(ret), K(expr));
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, res_type_datum))) {
    LOG_WARN("expr_align_date4cmp eval arg[2] res_type fail.", K(ret), K(expr));
  } else {
    date_arg_obj_type = expr.args_[0]->datum_meta_.type_;
    res_type = ObObjType(res_type_datum->get_int());
    if(OB_FAIL(datum_to_ob_time(expr, date_datum, date_arg_obj_type, date_arg_type, ob_time))) {
      LOG_WARN("datum_to_ob_time fail.", K(ret), K(date_datum), K(date_arg_obj_type));
    }
  }

  bool offset = false;
  const bool is_zero_on_warn = CM_IS_ZERO_ON_WARN(expr.extra_);
  const bool is_no_zero_date = CM_IS_NO_ZERO_DATE(expr.extra_);
  const bool is_warn_on_fail = CM_IS_WARN_ON_FAIL(expr.extra_);
  const bool is_allow_invalid_dates = CM_IS_ALLOW_INVALID_DATES(expr.extra_);
  if (OB_SUCC(ret)) {
    switch(date_arg_type) {
      case VALID_DATE: {
        const bool is_valid_time = true;
        if (OB_FAIL(set_res(res, ob_time, res_type, is_valid_time, offset,
                            is_zero_on_warn, is_no_zero_date, is_warn_on_fail))) {
          LOG_WARN("set_res fail.", K(ret), K(ob_time), K(res_type),
                                    K(date_arg_type), K(is_valid_time));
        }
        break;
      }
      case INVALID_DATE: {
        bool is_valid_time = false;
        int cmp_type = cmp_type_datum->get_int();
        if (cmp_type == T_OP_EQ || cmp_type == T_OP_NSEQ || cmp_type == T_OP_NE) {
          // Compatible with previous versions of ob, not with mysql
          if (is_allow_invalid_dates) {
            int32_t offset = get_day_over_limit(ob_time);
            if (offset > 0) {
              push_back_n_days(ob_time, offset);
              is_valid_time = true;
            }
          }
        } else {  // cmp_type is: >, >=, <, <=
          // Compatible with mysql, not with previous versions of ob
          if (is_day_over_limit(ob_time)) {
            offset = (cmp_type == T_OP_GT || cmp_type == T_OP_LE) ? false : true;
            set_valid_time_floor(ob_time);
            is_valid_time = true;
          }
        }
        if (OB_FAIL(set_res(res, ob_time, res_type, is_valid_time, offset,
                            is_zero_on_warn, is_no_zero_date, is_warn_on_fail))) {
          LOG_WARN("set_res fail.", K(ret), K(ob_time), K(res_type),
                                    K(date_arg_type), K(is_valid_time));
        }
        break;
      }
      case NON_DATE: {
        const bool is_valid_time = false;
        if (OB_FAIL(set_res(res, ob_time, res_type, is_valid_time, offset,
                            is_zero_on_warn, is_no_zero_date, is_warn_on_fail))) {
          LOG_WARN("set_res fail.", K(ret), K(ob_time), K(res_type),
                                        K(date_arg_type), K(is_valid_time));
        }
        break;
      }
      case NULL_DATE: {
        res.set_null();
        break;
      }
      case ZERO_DATE: {
        if (OB_FAIL(set_zero_res(res, ob_time, res_type, is_no_zero_date))) {
          LOG_WARN("set_zero_res fail.", K(ret), K(ob_time), K(res_type));
        }
      }
    }
  }

  return ret;
}

// Because the date greater than 31 will be considered as NON_DATE,
// the maximum number of days to be pushed back in this case is 31-28=3.
// For year and month, the maximum pushback is limited to 1.
void ObExprAlignDate4Cmp::push_back_n_days(ObTime &ob_time, int32_t offset)
{
  ob_time.parts_[DT_YEAR] += ob_time.parts_[DT_MON] / MONTHS_PER_YEAR;
  ob_time.parts_[DT_MON] = (ob_time.parts_[DT_MON] + 1) % MONTHS_PER_YEAR;
  ob_time.parts_[DT_MDAY] = offset;
  ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
}

void ObExprAlignDate4Cmp::set_valid_time_floor(ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if(IS_LEAP_YEAR(ob_time.parts_[DT_YEAR])) {
    ob_time.parts_[DT_MDAY] = DAYS_OF_MON[1][ob_time.parts_[DT_MON]];
  } else {
    ob_time.parts_[DT_MDAY] = DAYS_OF_MON[0][ob_time.parts_[DT_MON]];
  }

  ob_time.parts_[DT_HOUR] = HOURS_PER_DAY - 1;
  ob_time.parts_[DT_MIN] = MINS_PER_HOUR - 1;
  ob_time.parts_[DT_SEC] = SECS_PER_MIN - 1;
  ob_time.parts_[DT_USEC] = USECS_PER_SEC - 1;

  ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
}

bool ObExprAlignDate4Cmp::is_day_over_limit(const ObTime &ob_time)
{
  bool res = true;
  if (ob_time.parts_[DT_MON] < 1 || ob_time.parts_[DT_MON] > 12) {
    res = false;
  } else if(IS_LEAP_YEAR(ob_time.parts_[DT_YEAR])) {
    res = ob_time.parts_[DT_MDAY] > DAYS_OF_MON[1][ob_time.parts_[DT_MON]];
  } else {
    res = ob_time.parts_[DT_MDAY] > DAYS_OF_MON[0][ob_time.parts_[DT_MON]];
  }
  return res;
}

int32_t ObExprAlignDate4Cmp::get_day_over_limit(const ObTime &ob_time)
{
  int32_t res = 0;
  if (ob_time.parts_[DT_MON] < 1 || ob_time.parts_[DT_MON] > 12) {
  } else if(IS_LEAP_YEAR(ob_time.parts_[DT_YEAR])) {
    res = ob_time.parts_[DT_MDAY] - DAYS_OF_MON[1][ob_time.parts_[DT_MON]];
  } else {
    res = ob_time.parts_[DT_MDAY] - DAYS_OF_MON[0][ob_time.parts_[DT_MON]];
  }
  return res > 0 ? res : 0;
}

ObExprAlignDate4Cmp::DateArgType ObExprAlignDate4Cmp::validate_time(ObTime &ob_time)
{
  DateArgType date_arg_type = VALID_DATE;
  const int32_t *parts = ob_time.parts_;
  if (!HAS_TYPE_ORACLE(ob_time.mode_)
      && 0 == parts[DT_YEAR] && 0 == parts[DT_MON] && 0 == parts[DT_MDAY] && 0 == parts[DT_HOUR]
      && 0 == parts[DT_MIN] && 0 == parts[DT_SEC] && 0 == parts[DT_USEC]) {
    date_arg_type = ZERO_DATE;
  } else {
    const int64_t *part_min = (HAS_TYPE_ORACLE(ob_time.mode_) ? TZ_PART_MIN : DT_PART_MIN);
    const int64_t *part_max = (HAS_TYPE_ORACLE(ob_time.mode_) ? TZ_PART_MAX : DT_PART_MAX);
    for (int i = 0; i < DATETIME_PART_CNT; ++i) {
      if (!(part_min[i] <= parts[i] && parts[i] <= part_max[i])) {
        date_arg_type = INVALID_DATE;
      }
    }
    int is_leap = IS_LEAP_YEAR(parts[DT_YEAR]);
    if (parts[DT_MDAY] > 31 ||
        parts[DT_MDAY] > DAYS_OF_MON[is_leap][parts[DT_MON]]) {
      date_arg_type = INVALID_DATE;
    }
  }
  return date_arg_type;
}

int ObExprAlignDate4Cmp::integer_to_ob_time(const int64_t &date,
                                            DateArgType &date_arg_type,
                                            ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if (date == 0) {
    date_arg_type = ZERO_DATE;
  } else {
    ObDateSqlMode date_sql_mode;
    // First try to perform a lenient type conversion.
    // If it fails, it means the value is not convertible to time.
    date_sql_mode.allow_invalid_dates_ = true;
    date_sql_mode.no_zero_date_ = false;
    if (OB_FAIL(ObTimeConverter::int_to_ob_time_with_date(date, ob_time, true, date_sql_mode))) {
      date_arg_type = NON_DATE;
      ret = OB_SUCCESS;
    } else {
      // Then perform a strict type conversion check.
      date_arg_type = validate_time(ob_time);
    }
  }
  return ret;
}

static const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE = 512;

template <typename IN_TYPE>
static int common_floating_number(const IN_TYPE in_val,
                                  const ob_gcvt_arg_type arg_type,
                                  ObIAllocator &alloc,
                                  number::ObNumber &number)
{
  int ret = OB_SUCCESS;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE];
  MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE);
  int64_t length = 0;
  if (OB_GCVT_ARG_DOUBLE == arg_type) {
    length = ob_gcvt_opt(in_val, arg_type, static_cast<int32_t>(sizeof(buf) - 1),
                         buf, NULL, lib::is_oracle_mode(), TRUE);
  } else {
    length = ob_gcvt(in_val, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
  }
  ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
  ObScale res_scale; // useless
  ObPrecision res_precision;
  ret = number.from_sci_opt(str.ptr(), str.length(), alloc,
                                  &res_precision, &res_scale);
  return ret;
}

int ObExprAlignDate4Cmp::double_to_ob_time(const double &date, DateArgType &date_arg_type, ObTime &ob_time) {
  int ret = OB_SUCCESS;
  ObNumStackOnceAlloc tmp_alloc;
  number::ObNumber number;
  if (OB_FAIL(common_floating_number(date, OB_GCVT_ARG_DOUBLE, tmp_alloc, number))) {
    ret = OB_SUCCESS;
    date_arg_type = NON_DATE;
  } else if (OB_FAIL(number_to_ob_time(number, date_arg_type, ob_time))) {
    LOG_WARN("number to ob_time failed", K(ret), K(number));
  }
  return ret;
}

int ObExprAlignDate4Cmp::number_to_ob_time(const number::ObNumber &date, DateArgType &date_arg_type, ObTime &ob_time) {
  int ret = OB_SUCCESS;
  int64_t int_part = 0;
  int64_t dec_part = 0;
  if (!date.is_int_parts_valid_int64(int_part, dec_part)) {
    date_arg_type = NON_DATE;
  } else if (OB_FAIL(integer_to_ob_time(int_part, date_arg_type, ob_time))) {
    LOG_WARN("cast integer to ob_time fail.", K(ret), K(int_part));
  } else {
    ob_time.parts_[DT_USEC] = (dec_part + 500) / 1000;
  }
  return ret;
}

int ObExprAlignDate4Cmp::str_to_ob_time(const ObString &date, DateArgType &date_arg_type, ObTime &ob_time) {
  int ret = OB_SUCCESS;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.allow_invalid_dates_ = true;
  date_sql_mode.no_zero_date_ = false;
  if (OB_FAIL(ObTimeConverter::str_to_ob_time_with_date(date, ob_time, NULL, true, date_sql_mode))) {
    date_arg_type = NON_DATE;
    ret = OB_SUCCESS;
  } else {
    date_arg_type = validate_time(ob_time);
  }
  return ret;
}

int ObExprAlignDate4Cmp::datum_to_ob_time(const ObExpr &expr,
                                          const ObDatum* date_datum,
                                          const ObObjType& date_arg_obj_type,
                                          DateArgType &date_arg_type,
                                          ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if (date_datum->is_null()) {
    date_arg_type = NULL_DATE;
  } else if (ob_is_string_type(date_arg_obj_type)) {
    ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString str = date_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&lob_allocator, date_arg_obj_type,
                              CS_TYPE_BINARY, expr.args_[0]->obj_meta_.has_lob_header(), str))) {
      LOG_WARN("fail to get real string data", K(ret), K(date_datum));
    } else if (OB_FAIL(str_to_ob_time(str, date_arg_type, ob_time))) {
      LOG_WARN("str_to_ob_time fail.", K(ret), K(date_datum));
    }
  } else {  // numeric
    switch(date_arg_obj_type) {
      case ObNullType: {
        date_arg_type = NULL_DATE;
        break;
      }
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        if (OB_FAIL(integer_to_ob_time(date_datum->get_int(), date_arg_type, ob_time))) {
          LOG_WARN("integer_to_ob_time fail.", K(ret), K(date_datum));
        }
        break;
      }
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        if (OB_FAIL(integer_to_ob_time(static_cast<int64_t>(date_datum->get_uint()),
                                       date_arg_type, ob_time))) {
          LOG_WARN("integer_to_ob_time fail.", K(ret), K(date_datum));
        }
        break;
      }
      case ObFloatType:
      case ObUFloatType: {
        if (OB_FAIL(double_to_ob_time(static_cast<double>(date_datum->get_float()),
                                      date_arg_type, ob_time))) {
          LOG_WARN("integer_to_ob_time fail.", K(ret), K(date_datum));
        }
        break;
      }
      case ObDoubleType:
      case ObUDoubleType: {
        if (OB_FAIL(double_to_ob_time(date_datum->get_double(), date_arg_type, ob_time))) {
          LOG_WARN("double_to_ob_time fail.", K(ret), K(date_datum));
        }
        break;
      }
      case ObNumberType:
      case ObUNumberType: {
        if (OB_FAIL(number_to_ob_time(date_datum->get_number(), date_arg_type, ob_time))) {
          LOG_WARN("number_to_ob_time fail.", K(ret), K(date_datum));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("date_arg_obj_type error: ", K(ret), K(date_arg_obj_type));
        break;
      }
    }
  }
  return ret;
}

int ObExprAlignDate4Cmp::set_res(ObDatum &res, ObTime &ob_time,
                                 const ObObjType &res_type,
                                 const bool is_valid_time,
                                 const bool offset,
                                 const bool is_zero_on_warn,
                                 const bool is_no_zero_date,
                                 const bool is_warn_on_fail)
{
  int ret = OB_SUCCESS;
  switch(res_type) {
    case ObNullType: {
      res.set_null();
      break;
    }
    case ObDateTimeType: {
      if (!is_valid_time) {
        if (!is_warn_on_fail) {
          ret = OB_INVALID_DATE_FORMAT;
        } else if (is_zero_on_warn && !is_no_zero_date) {
          res.set_datetime(ObTimeConverter::ZERO_DATETIME);
        } else {
          res.set_null();
        }
      } else {
        int64_t datetime_value = 0;
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, datetime_value))) {
          LOG_WARN("ob_time convert to datetime fail.", K(ret), K(ob_time));
        } else {
          res.set_datetime(datetime_value + offset);
        }
      }
      break;
    }
    case ObDateType: {
      if (!is_valid_time) {
        if (!is_warn_on_fail) {
          ret = OB_INVALID_DATE_FORMAT;
        } else if (is_zero_on_warn && !is_no_zero_date) {
          res.set_date(ObTimeConverter::ZERO_DATE);
        } else {
          res.set_null();
        }
      } else {
        int32_t date_value = ObTimeConverter::ob_time_to_date(ob_time);
        res.set_date(date_value + offset);
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected res type.", K(ret), K(res_type));
      break;
    }
  }
  return ret;
}

int ObExprAlignDate4Cmp::set_zero_res(ObDatum &res, ObTime &ob_time,
                                       const ObObjType &res_type,
                                       bool is_no_zero_date)
{
  int ret = OB_SUCCESS;
  switch(res_type) {
    case ObNullType: {
      res.set_null();
      break;
    }
    case ObDateTimeType: {
      if (is_no_zero_date) {
        res.set_null();
      } else {
        res.set_datetime(ObTimeConverter::ZERO_DATETIME + ob_time.parts_[DT_USEC]);
      }
      break;
    }
    case ObDateType: {
      if (is_no_zero_date) {
        res.set_null();
      } else {
        res.set_date(ObTimeConverter::ZERO_DATE);
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected res type.", K(ret), K(res_type));
      break;
    }
  }
  return ret;
}

bool ObExprAlignDate4Cmp::is_align_date4cmp_support_obj_type(const ObObjType &obj_type) {
  bool res = false;
  if (ObNullType <= obj_type && obj_type <= ObUNumberType) {
    res = true;
  } else if (ob_is_string_type(obj_type)) {
    res = true;
  }
  return res;
}

}
}