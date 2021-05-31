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

#include "sql/engine/expr/ob_expr_operator.h"
#include <math.h>
#include "lib/oblog/ob_log.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_less_than.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/expr/ob_expr_null_safe_equal.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_infix_expression.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase {
using namespace common;
using namespace common::number;
using namespace oceanbase::lib;
namespace sql {
static const int32_t DAYS_PER_YEAR[2] = {365, 366};
static const int8_t DAYS_PER_MON[2][12 + 1] = {
    {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}, {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

#define IS_LEAP_YEAR(y) ((((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0)) ? 1 : 0)

const char* ObExprTRDateFormat::FORMATS_TEXT[FORMAT_MAX_TYPE] = {"SYYYY",
    "YYYY",
    "YEAR",
    "SYEAR",
    "YYY",
    "YY",
    "Y",
    "IYYY",
    "IY",
    "I",
    "Q",
    "MONTH",
    "MON",
    "MM",
    "RM",
    "WW",
    "IW",
    "W",
    "DDD",
    "DD",
    "J",
    "DAY",
    "DY",
    "D",
    "HH",
    "HH12",
    "HH24",
    "MI",
    "CC",
    "SCC"};

uint64_t ObExprTRDateFormat::FORMATS_HASH[FORMAT_MAX_TYPE] = {0};

OB_SERIALIZE_MEMBER(ObFuncInputType, calc_meta_, max_length_, flag_);
OB_SERIALIZE_MEMBER_INHERIT(
    ObExprResType, ObObjMeta, accuracy_, calc_accuracy_, calc_type_, res_flags_, row_calc_cmp_types_);

const ObTimeZoneInfo* get_timezone_info(const ObSQLSessionInfo* session)
{
  return TZ_INFO(session);
}
const common::ObString* get_nls_formats(const ObSQLSessionInfo* session)
{
  return GET_NLS_FORMATS(session);
}
const common::ObObjPrintParams get_obj_print_params(const ObSQLSessionInfo* session)
{
  return CREATE_OBJ_PRINT_PARAM(session);
}

int64_t get_cur_time(ObPhysicalPlanCtx* phy_plan_ctx)
{
  return NULL != phy_plan_ctx ? phy_plan_ctx->get_cur_time().get_datetime() : 0;
}

int get_tz_offset(const ObTimeZoneInfo* tz_info, int64_t& offset)
{
  int ret = OB_SUCCESS;
  offset = 0;
  int32_t tmp_offset = 0;
  if (NULL != tz_info && OB_SUCC(tz_info->get_timezone_offset(0, tmp_offset))) {
    offset = SEC_TO_USEC(tmp_offset);
  }
  return ret;
}

int ObExprOperator::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_FAIL(result_type_.assign(other.result_type_))) {
      LOG_WARN("copy result_type failed", K(ret));
    } else if (OB_FAIL(input_types_.assign(other.input_types_))) {
      LOG_WARN("copy input_types failed", K(ret));
    } else {
      this->magic_ = other.magic_;
      this->id_ = other.id_;
      this->row_dimension_ = other.row_dimension_;
      this->real_param_num_ = other.real_param_num_;
      this->is_called_in_sql_ = other.is_called_in_sql_;
      this->extra_serialize_ = other.extra_serialize_;
    }
  }
  return ret;
}

int ObExprOperator::cg_expr(ObExprCGCtx&, const ObRawExpr& raw_expr, ObExpr&) const
{
  int ret = STATIC_ENG_NOT_IMPLEMENT;
  LOG_INFO("not implemented in sql static typing engine, "
           "will retry the old engine automatically",
      K(ret),
      K(raw_expr));
  return ret;
}

// check function pointer in vtable to detect cg_expr() is overwrite or not.
bool ObExprOperator::is_default_expr_cg() const
{
  static ObArenaAllocator alloc;
  static ObExprOperator base(alloc, T_NULL, "fake_null_operator", 0);
  typedef int (ObExprOperator::*CGFunc)(ObExprCGCtx & op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  union {
    CGFunc func_;
    int64_t val_;
    int64_t values_[2];
  } func_val;
  static_assert(sizeof(int64_t) * 2 == sizeof(CGFunc), "size mismatch");
  func_val.func_ = &ObExprOperator::cg_expr;
  // virtual member function pointer is vtable absolute offset + 1 (to avoid null)
  const int64_t func_idx = (func_val.val_ - 1) / sizeof(void*);
  return (*(void***)(&base))[func_idx] == (*(void***)(this))[func_idx];
}

int ObExprOperator::set_input_types(const ObIExprResTypes& expr_types)
{
  int ret = OB_SUCCESS;
  input_types_.reset();
  if (OB_FAIL(input_types_.reserve(expr_types.count()))) {
    LOG_WARN("fail to init input types", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_types.count(); ++i) {
    const ObExprResType& t = expr_types.at(i);
    ObFuncInputType type(t.get_calc_meta(), t.get_length(), t.get_result_flag());
    ret = input_types_.push_back(type);
  }

  return ret;
}

ObObjType ObExprOperator::get_calc_cast_type(ObObjType param_type, ObObjType calc_type)
{
  ObObjType cast_type = param_type;
  if (ObNullType == param_type || ObNullType == calc_type) {
    cast_type = param_type;
  } else if (ObMaxType == param_type || ObMaxType == calc_type) {
    cast_type = param_type;
  } else if (ob_is_integer_type(param_type) && ob_is_integer_type(calc_type)) {
    cast_type = param_type;
    //  } else if (ob_is_enumset_tc(param_type) && ob_is_enumset_numeric_type(calc_type)) {
    //    cast_type = param_type;
  } else if (param_type != calc_type) {
    cast_type = calc_type;
  }
  return cast_type;
}

int OB_INLINE ObExprOperator::cast_operand_type(
    common::ObObj& res_obj, const ObFuncInputType& res_type, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  // check if need to cast
  const ObObjType param_type = res_obj.get_type();
  const ObObjType calc_type = res_type.get_calc_type();
  const ObCollationType param_collation_type = res_obj.get_collation_type();
  const ObCollationType calc_collation_type = res_type.get_calc_meta().get_collation_type();
  if (OB_LIKELY((calc_type != param_type && ObNullType != param_type) ||
                (ob_is_string_or_lob_type(param_type) && ob_is_string_or_lob_type(calc_type) &&
                    (lib::is_oracle_mode() || param_collation_type != calc_collation_type)))) {
    LOG_DEBUG(
        "need cast operand", K(res_type), K(res_type.get_calc_meta().get_scale()), K(res_obj), K(res_obj.get_scale()));
    ObCastMode cast_mode = get_cast_mode();

    if (ob_is_string_or_lob_type(res_type.get_calc_type()) && res_type.is_zerofill()) {
      // For zerofilled string
      ObZerofillInfo zf_info(true, res_type.get_length());
      EXPR_DEFINE_CAST_CTX_ZF(expr_ctx, cast_mode, &zf_info);
      if (OB_FAIL(ObObjCaster::to_type(res_type.get_calc_type(), cast_ctx, res_obj, res_obj))) {
        LOG_WARN("fail to convert type", K(ret), K(res_type.get_calc_type()), K(res_obj));
      }
    } else {
      /* type collatino convertion rules:
       * 1.set target collation in cast_ctx.utf8mb4 is the default value
       * 2.if both src and dest type are ObStirngTC, rule:
       *   a.if calc_collation is set, use it
       *   b.if calc_collation not set, use src collation
       */
      EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
      if (ob_is_string_or_lob_type(calc_type)) {
        if (ob_is_string_or_lob_type(param_type)) {
          if (CS_TYPE_INVALID != calc_collation_type) {
            cast_ctx.dest_collation_ = calc_collation_type;
          } else if (share::is_mysql_mode() && CS_TYPE_INVALID != param_collation_type) {
            cast_ctx.dest_collation_ = param_collation_type;
          }
        } else {
          if (CS_TYPE_INVALID != calc_collation_type) {
            cast_ctx.dest_collation_ = calc_collation_type;
          } else {
            cast_ctx.dest_collation_ = ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4);
          }
        }
      }
      ObObj res_obj_copy = res_obj;
      cast_ctx.expect_obj_collation_ = cast_ctx.dest_collation_;
      if (OB_FAIL(ObObjCaster::to_type(res_type.get_calc_type(), cast_ctx, res_obj_copy, res_obj))) {
        LOG_WARN("fail to convert type", K(ret), K(res_type.get_calc_type()), K(res_obj));
      }
      if (OB_SUCC(ret) && ob_is_string_or_lob_type(param_type) && ob_is_string_or_lob_type(calc_type) &&
          param_collation_type != calc_collation_type && calc_collation_type != CS_TYPE_INVALID) {
        res_obj.set_collation_type(calc_collation_type);
      }
    }
  }
  return ret;
}

int ObExprOperator::cast_operand_type(ObObj* params, const int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const bool fallback_old_mode = false;
  if (OB_ISNULL(params) || OB_UNLIKELY(param_num < 0) || OB_UNLIKELY(param_num != real_param_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL param or invalid number", K(params), K(param_num), K(real_param_num_), K(ret));
  } else if (fallback_old_mode) {
    // fallback
  }  else if (OB_UNLIKELY(real_param_num_ <= 0 ||
              0 == input_types_.count() ||
              NOT_ROW_DIMENSION != row_dimension_ ||
              T_FUN_SYS_TIMESTAMP_NVL == get_type() /*||
              T_FUN_SYS_CAST == get_type() ||
              T_OP_CASE == get_type() ||
              T_OP_ARG_CASE == get_type() ||
              T_FUN_COLUMN_CONV == get_type()*/)) {
    // skip
  } else if (OB_UNLIKELY(input_types_.count() != real_param_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("real_param_num_ is wrong", K(input_types_.count()), K_(real_param_num));
  } else {
    // convert param types only if src and dest are different types
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_num_; ++i) {
      if (OB_FAIL(cast_operand_type(params[i], input_types_.at(i), expr_ctx))) {
        LOG_WARN("cast failed", K(ret), K(params[i]), K(input_types_.at(i)));
      }
    }
  }

  return ret;
}

int ObExprOperator::call(ObObj* stack, int64_t& stack_size, ObExprCtx& expr_ctx) const

{
  ObObj result;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stack) || OB_ISNULL(this) || OB_UNLIKELY(real_param_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stack or this is null or stack_size is wrong", K(stack), K(this), K(real_param_num_), K(ret));
  } else if (OB_UNLIKELY(real_param_num_ > stack_size)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("wrong number of input arguments on stack", K(real_param_num_), K(stack_size), K(ret));
  } else if (OB_LIKELY(row_dimension_ != NOT_ROW_DIMENSION)) {
    int32_t param_num = real_param_num_ * row_dimension_;
    if (OB_UNLIKELY(param_num > stack_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong number of input arguments on stack", K(param_num), K(stack_size));
      // vector operator
    } else if (OB_FAIL(this->calc_resultN(result, &stack[stack_size - param_num], param_num, expr_ctx))) {
      LOG_WARN("fail to calc resultN", K(ret), K(param_num), K(stack_size));
    } else {
      stack[stack_size - param_num] = result;
      stack_size -= (param_num - 1);
    }
  } else if (operand_auto_cast_ &&
             OB_FAIL(cast_operand_type(stack + stack_size - real_param_num_, real_param_num_, expr_ctx))) {
    LOG_WARN("fail convert operand types", K(stack_size), K(ret));
  } else {
    if (OB_UNLIKELY(param_num_ > 0 && param_num_ != real_param_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("bug! param_num is not equal than real_param_num",
          K_(name),
          K(get_type_name(type_)),
          K_(type),
          K_(param_num),
          K_(real_param_num),
          K_(row_dimension),
          K(input_types_.count()),
          K(stack_size),
          K(ret));
    }
    if (OB_UNLIKELY(real_param_num_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("bug! real_param_num is less than 0",
          K_(name),
          K(get_type_name(type_)),
          K_(type),
          K_(param_num),
          K_(real_param_num),
          K_(row_dimension),
          K(input_types_.count()),
          K(stack_size),
          K(ret));
    }
    if (OB_SUCC(ret)) {
      switch (param_num_) {
        case 0: {
          if (OB_FAIL(this->calc_result0(result, expr_ctx))) {
            LOG_WARN("fail to calc result0", K(ret), K(stack_size));
          } else {
            stack[stack_size] = result;
            ++stack_size;
          }
          break;
        }
        case 1: {
          if (OB_FAIL(this->calc_result1(result, stack[stack_size - 1], expr_ctx))) {
            LOG_WARN("fail to calc result1", K(ret), K(stack_size), N_FUNC, get_type_name(type_));
          } else {
            stack[stack_size - 1] = result;
          }
          break;
        }
        case 2: {
          if (OB_FAIL(this->calc_result2(result, stack[stack_size - 2], stack[stack_size - 1], expr_ctx))) {
            LOG_WARN("fail to calc result2", K(ret), K(stack_size), N_FUNC, get_type_name(type_));
          } else {
            stack[stack_size - 2] = result;
            stack_size--;
          }
          break;
        }
        case 3: {
          // org mode
          if (OB_FAIL(this->calc_result3(
                  result, stack[stack_size - 3], stack[stack_size - 2], stack[stack_size - 1], expr_ctx))) {
            LOG_WARN("fail to calc result3", K(ret), K(stack_size), N_FUNC, get_type_name(type_));
          } else {
            stack[stack_size - 3] = result;
            stack_size -= 2;
          }
          break;
        }
        default: {
          if (OB_FAIL(this->calc_resultN(result, &stack[stack_size - real_param_num_], real_param_num_, expr_ctx))) {
            LOG_WARN("fail to calc resultN", K(ret), K(stack_size), N_FUNC, get_type_name(type_));
          } else {
            stack[stack_size - real_param_num_] = result;
            stack_size -= real_param_num_ - 1;
          }
          break;
        }
      }  // end switch
      if (!stack[stack_size - 1].is_bit()) {
        stack[stack_size - 1].set_scale(result_type_.get_scale());
      }
    }
  }
  return ret;
}

int ObExprOperator::eval(
    common::ObExprCtx& expr_ctx, common::ObObj& val, common::ObObj* params, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  // some expr (e.g.: subtime) rely on the default value of ObObj, so we construct it again.
  // (see ObExprOperator::calc(), use an new stack ObObj variable and assign to val finally).
  new (&val) ObObj();
  if (OB_ISNULL(params) || OB_UNLIKELY(param_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(row_dimension_ != NOT_ROW_DIMENSION)) {
    // row operator
    if (OB_FAIL(calc_resultN(val, params, param_num, expr_ctx))) {
      LOG_WARN("calc result failed", K(ret), K(name_), K(get_type_name(type_)));
    }
  } else if (!is_param_lazy_eval() && operand_auto_cast_ && OB_FAIL(cast_operand_type(params, param_num, expr_ctx))) {
    LOG_WARN("cast operand failed", K(ret));
  } else {
    if (OB_UNLIKELY(param_num != real_param_num_) || OB_UNLIKELY(param_num_ > 0 && param_num_ != real_param_num_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("wrong param num", K(ret), K(param_num), K(param_num_), K(real_param_num_));
    } else {
      // Must use %param_num_, because it will set to negative number to indicate that
      // we should call calc_resultN here. e.g.: concat
      switch (param_num_) {
        case 0: {
          ret = calc_result0(val, expr_ctx);
          break;
        }
        case 1: {
          ret = calc_result1(val, params[0], expr_ctx);
          break;
        }
        case 2: {
          ret = calc_result2(val, params[0], params[1], expr_ctx);
          break;
        }
        case 3: {
          ret = calc_result3(val, params[0], params[1], params[2], expr_ctx);
          break;
        }
        default: {
          ret = calc_resultN(val, params, param_num, expr_ctx);
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("calc result failed", K(ret), K(param_num_), K(name_), K(get_type_name(type_)));
      }
      if (!val.is_bit() && !val.is_ext()) {
        val.set_scale(result_type_.get_scale());
      }
    }
  }
  return ret;
}

int ObExprOperator::param_eval(common::ObExprCtx& expr_ctx, const common::ObObj& param, const int64_t param_index) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.infix_expr_)) {
    // do nothing for postfix expression
  } else {
    if (OB_FAIL(expr_ctx.infix_expr_->param_eval(expr_ctx, param))) {
      LOG_WARN("param eval failed", K(ret));
    } else if (OB_LIKELY(row_dimension_ == NOT_ROW_DIMENSION) && OB_LIKELY(T_FUN_SYS_TIMESTAMP_NVL != type_) &&
               OB_LIKELY(param_index) < input_types_.count()) {
      // skip row operator and T_FUN_SYS_TIMESTAMP_NVL. (see cast_operand_type())
      if (operand_auto_cast_ &&
          OB_FAIL(cast_operand_type(const_cast<ObObj&>(param), input_types_.at(param_index), expr_ctx))) {
        LOG_WARN("cast failed", K(ret), K(param), K(input_types_.at(param_index)));
      }
    }
  }
  return ret;
}

bool ObExprOperator::is_valid_nls_param(const common::ObString& nls_param_str)
{
  bool bret = false;
  if (!nls_param_str.empty() && NULL != nls_param_str.find('=')) {
    static const common::ObString DEFAULT_VALUE_CALENDAR("GREGORIAN");
    static const common::ObString DEFAULT_VALUE_DATE_LANGUAGE("AMERICAN");
    static const common::ObString DEFAULT_VALUE_LANGUAGE("AMERICAN");
    static const common::ObString DEFAULT_VALUE_NUMERIC_CHARACTERS(".,");
    static const common::ObString DEFAULT_VALUE_SORT("BINARY");
    static const common::ObString DEFAULT_VALUE_COMP("BINARY");
    static const common::ObString DEFAULT_VALUE_CURRENCY("$");
    static const common::ObString DEFAULT_VALUE_ISO_CURRENCY("AMERICA");
    static const common::ObString DEFAULT_VALUE_DUAL_CURRENCY("$");

    static const common::ObString DEFAULT_NAME_CALENDAR(share::OB_SV_NLS_CALENDAR);
    static const common::ObString DEFAULT_NAME_DATE_LANGUAGE(share::OB_SV_NLS_DATE_LANGUAGE);
    static const common::ObString DEFAULT_NAME_LANGUAGE(share::OB_SV_NLS_LANGUAGE);
    static const common::ObString DEFAULT_NAME_NUMERIC_CHARACTERS(share::OB_SV_NLS_NUMERIC_CHARACTERS);
    static const common::ObString DEFAULT_NAME_SORT(share::OB_SV_NLS_SORT);
    static const common::ObString DEFAULT_NAME_COMP(share::OB_SV_NLS_COMP);
    static const common::ObString DEFAULT_NAME_CURRENCY(share::OB_SV_NLS_CURRENCY);
    static const common::ObString DEFAULT_NAME_ISO_CURRENCY(share::OB_SV_NLS_ISO_CURRENCY);
    static const common::ObString DEFAULT_NAME_DUAL_CURRENCY(share::OB_SV_NLS_DUAL_CURRENCY);

    ObString value_str = const_cast<common::ObString&>(nls_param_str).trim();
    ObString name_str = value_str.split_on('=');
    name_str = name_str.trim();
    value_str = value_str.trim();
    if (!name_str.empty() && !value_str.empty()) {
      if ((0 == name_str.case_compare(DEFAULT_NAME_CALENDAR) && 0 == value_str.case_compare(DEFAULT_VALUE_CALENDAR)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_DATE_LANGUAGE) &&
              0 == value_str.case_compare(DEFAULT_VALUE_DATE_LANGUAGE)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_LANGUAGE) && 0 == value_str.case_compare(DEFAULT_VALUE_LANGUAGE)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_NUMERIC_CHARACTERS) &&
              0 == value_str.case_compare(DEFAULT_VALUE_NUMERIC_CHARACTERS)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_SORT) && 0 == value_str.case_compare(DEFAULT_VALUE_SORT)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_COMP) && 0 == value_str.case_compare(DEFAULT_VALUE_COMP)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_CURRENCY) && 0 == value_str.case_compare(DEFAULT_VALUE_CURRENCY)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_ISO_CURRENCY) &&
              0 == value_str.case_compare(DEFAULT_VALUE_ISO_CURRENCY)) ||
          (0 == name_str.case_compare(DEFAULT_NAME_DUAL_CURRENCY) &&
              0 == value_str.case_compare(DEFAULT_VALUE_DUAL_CURRENCY))) {
        bret = true;
      }
    }
  }
  return bret;
}

/*
 * get default collation type from session for string type
 * do not use this function for non-string type
 * */
ObCollationType ObExprOperator::get_default_collation_type(ObObjType type, const ObBasicSessionInfo& session_info)
{
  ObCollationType collation_type = CS_TYPE_INVALID;
  if (ob_is_string_or_lob_type(type)) {
    if (share::is_mysql_mode()) {
      collation_type = static_cast<ObCollationType>(session_info.get_local_collation_connection());
    } else {
      if (ob_is_nstring(type)) {
        // nvarchar2 nchar nclob
        collation_type = session_info.get_nls_collation_nation();
      } else {
        // varchar2 char clob
        collation_type = session_info.get_nls_collation();
      }
    }
  }
  return collation_type;
}

OB_SERIALIZE_MEMBER(ObExprOperator, row_dimension_, real_param_num_, result_type_, input_types_, id_, extra_serialize_);

int ObExprOperator::aggregate_collations(
    ObObjMeta& type, const ObObjMeta* types, int64_t param_num, uint32_t flags, const ObCollationType conn_coll_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObCollationType coll_type = types[0].get_collation_type();
    ObCollationLevel coll_level = types[0].get_collation_level();
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      ret = ObCharset::aggregate_collation(
          coll_level, coll_type, types[i].get_collation_level(), types[i].get_collation_type(), coll_level, coll_type);
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY((flags & OB_COLL_DISALLOW_NONE) && CS_LEVEL_NONE == coll_level)) {
        // @todo () correct error code is OB_CANT_AGGREGATE_NCOLLATIONS
        ret = OB_CANT_AGGREGATE_2COLLATIONS;
      }
    }
    if (OB_SUCC(ret)) {
      /* If all arguments were numbers, reset to @@collation_connection */
      if (OB_UNLIKELY((flags & OB_COLL_ALLOW_NUMERIC_CONV) && CS_LEVEL_NUMERIC == coll_level)) {
        coll_type = conn_coll_type;
        coll_level = CS_LEVEL_COERCIBLE;
        // MySQL need charset converter here. We consider only two charset(binary
        // and utf8mb4), so no conversion is actually needed
      } else {
      }
      if (OB_SUCC(ret)) {
        type.set_collation_type(coll_type);
        type.set_collation_level(coll_level);
      } else {
      }
    } else {
    }
    if (OB_CANT_AGGREGATE_2COLLATIONS == ret) {
      if (3 == param_num) {
        ret = OB_CANT_AGGREGATE_3COLLATIONS;
      } else if (3 < param_num) {
        ret = OB_CANT_AGGREGATE_NCOLLATIONS;
      }
    }
  }
  return ret;
}

int ObExprOperator::aggregate_charsets_for_string_result(
    ObObjMeta& type, const ObObjMeta* types, int64_t param_num, const ObCollationType conn_coll_type)
{
  uint32_t flags = OB_COLL_ALLOW_NUMERIC_CONV;
  return aggregate_charsets(type, types, param_num, flags, conn_coll_type);
}

int ObExprOperator::aggregate_charsets_for_string_result(
    ObExprResType& type, const ObExprResType* types, int64_t param_num, const ObCollationType conn_coll_type)
{
  uint32_t flags = OB_COLL_ALLOW_NUMERIC_CONV;
  return aggregate_charsets(type, types, param_num, flags, conn_coll_type);
}

int ObExprOperator::aggregate_charsets_for_comparison(
    ObObjMeta& type, const ObObjMeta* types, int64_t param_num, const ObCollationType conn_coll_type)
{
  uint32_t flags = OB_COLL_DISALLOW_NONE;
  return aggregate_charsets(type, types, param_num, flags, conn_coll_type);
}

int ObExprOperator::aggregate_charsets_for_comparison(
    ObExprResType& type, const ObExprResType* types, int64_t param_num, const ObCollationType conn_coll_type)
{
  uint32_t flags = OB_COLL_DISALLOW_NONE;
  return aggregate_charsets(type.get_calc_meta(), types, param_num, flags, conn_coll_type);
}

int ObExprOperator::aggregate_charsets_for_string_result_with_comparison(
    ObObjMeta& type, const ObObjMeta* types, int64_t param_num, const ObCollationType conn_coll_type)
{
  uint32_t flags = OB_COLL_DISALLOW_NONE | OB_COLL_ALLOW_NUMERIC_CONV;
  return aggregate_charsets(type, types, param_num, flags, conn_coll_type);
}

int ObExprOperator::aggregate_charsets_for_string_result_with_comparison(
    ObObjMeta& type, const ObExprResType* types, int64_t param_num, const ObCollationType coll_type)
{
  uint32_t flags = OB_COLL_DISALLOW_NONE | OB_COLL_ALLOW_NUMERIC_CONV;
  return aggregate_charsets(type, types, param_num, flags, coll_type);
}

int ObExprOperator::aggregate_charsets(
    ObObjMeta& type, const ObObjMeta* types, int64_t param_num, uint32_t flags, const ObCollationType conn_coll_type)
{
  return aggregate_collations(type, types, param_num, flags, conn_coll_type);
}

int ObExprOperator::aggregate_charsets(ObObjMeta& type, const ObExprResType* types, int64_t param_num, uint32_t flags,
    const ObCollationType conn_coll_type)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(types), !OB_UNLIKELY(param_num < 1));

  if (OB_SUCC(ret)) {
    ObSEArray<ObObjMeta, 16> coll_types;
    ObObjMeta coll;
    for (int i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      coll.reset();
      coll.set_collation_type(types[i].get_collation_type());
      coll.set_collation_level(types[i].get_collation_level());
      ret = coll_types.push_back(coll);
    }  // end for

    OZ(aggregate_charsets(type, &coll_types.at(0), param_num, flags, conn_coll_type));
  }
  return ret;
}

int ObExprOperator::aggregate_string_type_and_charset_oracle(const ObBasicSessionInfo& session,
    const ObIArray<ObExprResType*>& params, ObExprResType& result, bool prefer_var_len_char)
{
  int ret = OB_SUCCESS;

  bool has_clob_type = false;
  bool has_character_type = false;
  bool has_varying_len_string_type = false;
  bool has_nation_string_type = false;

  CK(params.count() > 0);

  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    const ObExprResType* param_meta = params.at(i);
    CK(OB_NOT_NULL(param_meta));
    if (OB_SUCC(ret)) {
      if (param_meta->is_string_or_lob_locator_type()) {
        has_clob_type |= param_meta->is_clob();
        has_clob_type |= param_meta->is_clob_locator();
        has_character_type |= param_meta->is_character_type();
        has_nation_string_type |= param_meta->is_nstring();
        has_varying_len_string_type |= param_meta->is_varying_len_char_type();
      } else {
        has_varying_len_string_type = true;
      }
    }
  }

  // 1. deduce type + charset
  ObObjType result_type = ObMaxType;
  ObCollationType result_charset = CS_TYPE_INVALID;

  if (OB_SUCC(ret)) {
    if (has_clob_type) {
      if (has_nation_string_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "type nclob");
      } else {
        result_type = ObLongTextType;
        result_charset = session.get_nls_collation();
      }
    } else if (has_character_type) {
      if (has_nation_string_type) {
        result_type = has_varying_len_string_type || prefer_var_len_char ? ObNVarchar2Type : ObNCharType;
        result_charset = session.get_nls_collation_nation();
      } else {
        result_type = has_varying_len_string_type || prefer_var_len_char ? ObVarcharType : ObCharType;
        result_charset = session.get_nls_collation();
      }
    } else {  // blob and other types
      result_type = ObVarcharType;
      result_charset = session.get_nls_collation();
    }
  }

  if (OB_SUCC(ret)) {
    result.set_type_simple(result_type);
    if (ob_is_large_text(result_type)) {
      result.set_lob_inrow();
    }
    result.set_collation_type(result_charset);
    result.set_collation_level(CS_LEVEL_IMPLICIT);
  }

  //. deduce length sematics
  ObLengthSemantics result_ls = LS_INVALIED;

  if (OB_SUCC(ret)) {
    if (result.is_nstring() || result.is_clob() || result.is_clob_locator()) {
      result_ls = LS_CHAR;
    } else {
      bool is_all_the_same = true;
      for (int64_t i = 0; is_all_the_same && i < params.count(); ++i) {
        const ObExprResType* param_meta = params.at(i);
        if (param_meta->is_string_or_lob_locator_type()) {
          ObLengthSemantics curr_ls = param_meta->get_length_semantics();
          if (LS_INVALIED == result_ls) {
            result_ls = curr_ls;
          } else {
            is_all_the_same = (result_ls == curr_ls);
          }
        }
      }
      if (!is_all_the_same || LS_INVALIED == result_ls) {
        result_ls = session.get_actual_nls_length_semantics();
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.set_length_semantics(result_ls);
  }

  LOG_DEBUG("aggregate string charset", K(result), K(params));

  return ret;
}

int ObExprOperator::deduce_string_param_calc_type_and_charset(
    const ObBasicSessionInfo& session, const ObExprResType& result, ObIArray<ObExprResType*>& params)
{
  UNUSED(session);
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObExprResType* param_meta = params.at(i);
    CK(OB_NOT_NULL(param_meta));
    OX(param_meta->set_calc_meta(result));
  }
  return ret;
}

int ObExprOperator::is_same_kind_type_for_case(const ObExprResType& type1, const ObExprResType& type2, bool& match)
{
  int ret = OB_SUCCESS;
  match = false;
  if (OB_UNLIKELY(type1.get_type() >= ObMaxType || type2.get_type() >= ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the wrong type", K(type1), K(type2), K(ret));
  } else {
    if (ob_is_null(type1.get_type()) || ob_is_null(type2.get_type())) {
      match = true;
    } else if (ob_is_varchar_char_type(type1.get_type(), type1.get_collation_type())) {
      match = ob_is_varchar_char_type(type2.get_type(), type2.get_collation_type());
    } else if (ob_is_numeric_type(type1.get_type())) {
      match = ob_is_numeric_type(type2.get_type());
    } else if (ob_is_datetime_tc(type1.get_type()) || ob_is_otimestampe_tc(type1.get_type())) {
      match = ob_is_datetime_tc(type2.get_type()) || ob_is_otimestampe_tc(type2.get_type());
    } else if (ob_is_blob(type1.get_type(), type1.get_collation_type())) {
      match = ob_is_blob(type2.get_type(), type2.get_collation_type());
    } else if (ob_is_text(type1.get_type(), type1.get_collation_type())) {
      match = ob_is_text(type2.get_type(), type2.get_collation_type());
    } else if (ob_is_varbinary_type(type1.get_type(), type1.get_collation_type())) {
      match = ob_is_varbinary_type(type2.get_type(), type2.get_collation_type());
    } else if (ob_is_raw(type1.get_type())) {
      match = ob_is_raw(type2.get_type());
    } else if (ob_is_interval_ym(type1.get_type())) {
      match = ob_is_interval_ym(type2.get_type());
    } else if (ob_is_interval_ds(type1.get_type())) {
      match = ob_is_interval_ds(type2.get_type());
    } else if (ob_is_nstring_type(type1.get_type())) {
      match = ob_is_nstring_type(type2.get_type());
    } else if (ob_is_urowid(type1.get_type())) {
      match = ob_is_urowid(type2.get_type());
    } else if (ob_is_blob_locator(type1.get_type(), type1.get_collation_type())) {
      match = ob_is_blob_locator(type2.get_type(), type2.get_collation_type());
    } else if (ob_is_clob_locator(type1.get_type(), type1.get_collation_type())) {
      match = ob_is_clob_locator(type2.get_type(), type2.get_collation_type());
    }
  }
  return ret;
}

// coaesce expr, case when expr
int ObExprOperator::aggregate_result_type_for_case(ObExprResType& type, const ObExprResType* types, int64_t param_num,
    const ObCollationType conn_coll_type, bool is_oracle_mode, const ObLengthSemantics default_length_semantics,
    const ObSQLSessionInfo* session, bool need_merge_type, bool skip_null)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else if (1 == param_num && ob_is_enumset_tc(types[0].get_type())) {
  } else if (is_oracle_mode) {
    bool match = false;
    int64_t nth = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == nth && i < param_num; ++i) {
      if (!ob_is_null(types[i].get_type())) {
        nth = i;
      }
    }
    nth = OB_INVALID_ID == nth ? 0 : nth;
    const ObExprResType& res_type = types[nth];
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_FAIL(ObExprOperator::is_same_kind_type_for_case(res_type, types[i], match))) {
        LOG_WARN("fail to judge same type", K(i), K(res_type), K(types[i]), K(ret));
      } else if (!match) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("fail to judge same type", K(i), K(res_type), K(types[i]), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(aggregate_result_type_for_merge(type,
            types,
            param_num,
            conn_coll_type,
            is_oracle_mode,
            default_length_semantics,
            session,
            need_merge_type,
            skip_null))) {
      LOG_WARN("fail to aggregate result type", K(ret));
    }
  }
  return ret;
}

int ObExprOperator::aggregate_result_type_for_merge(ObExprResType& type, const ObExprResType* types, int64_t param_num,
    const ObCollationType conn_coll_type, bool is_oracle_mode, const ObLengthSemantics default_length_semantics,
    const ObSQLSessionInfo* session, bool need_merge_type, bool skip_null)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else if (1 == param_num && ob_is_enumset_tc(types[0].get_type())) {
    // this is for case when clause like case  when 1 then c1 end;
    type.set_type(ObVarcharType);
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(session->get_local_collation_connection());
  } else {
    ObObjType res_type = types[0].get_type();
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_FAIL(ObExprResultTypeUtil::get_merge_result_type(res_type, res_type, types[i].get_type()))) {
        // warn
      } else if (OB_UNLIKELY(ObMaxType == res_type)) {
        ret = OB_INVALID_ARGUMENT;  // not compatible input
        LOG_WARN("invalid argument. wrong type for merge", K(i), K(types[i].get_type()), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      type.set_type(res_type);
      if (ob_is_numeric_type(res_type)) {
        ret = aggregate_numeric_accuracy_for_merge(type, types, param_num, is_oracle_mode);
      } else if (ob_is_temporal_type(res_type) || ob_is_otimestamp_type(res_type)) {
        ret = aggregate_temporal_accuracy_for_merge(type, types, param_num);
      } else if (ob_is_string_type(res_type)) {
        if (OB_FAIL(aggregate_charsets_for_string_result(type, types, param_num, conn_coll_type))) {
        } else if (OB_FAIL(aggregate_max_length_for_string_result(
                       type, types, param_num, is_oracle_mode, default_length_semantics, need_merge_type, skip_null))) {
        } else { /*do nothing*/
        }
      } else if (ob_is_raw(res_type)) {
        type.set_collation_type(CS_TYPE_BINARY);
        type.set_collation_level(CS_LEVEL_NUMERIC);
      } else if (ob_is_interval_tc(res_type)) {
        ret = aggregate_accuracy_for_merge(type, types, param_num);
      }
    }
    LOG_DEBUG("merged type is", K(type), K(is_oracle_mode));
  }
  return ret;
}

int ObExprOperator::aggregate_max_length_for_string_result(ObExprResType& type, const ObExprResType* types,
    int64_t param_num, bool is_oracle_mode, const ObLengthSemantics default_length_semantics, bool need_merge_type,
    bool skip_null)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObLength max_length = -1;
    ObLength max_length_char = -1;
    ObLength length = -1;
    ObLengthSemantics length_semantics = -1;
    int64_t byte_length_count = 0;
    int64_t char_length_count = 0;
    bool len_in_byte = false;
    int64_t mbmaxlen = 1;
    if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(CS_TYPE_UTF8MB4_GENERAL_CI, mbmaxlen))) {
      LOG_WARN("fail to get mbmaxlen", K(ret));
    } else if (OB_FAIL(ObCharset::get_aggregate_len_unit(type.get_collation_type(), len_in_byte))) {
      LOG_WARN("get aggregate len unit failed", K(ret), K(type));
    } else if (len_in_byte) {
      // binary
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        if (ob_is_integer_type(types[i].get_type())) {
          length = MAX(ObAccuracy::DDL_DEFAULT_ACCURACY[types[i].get_type()].precision_, types[i].get_precision());
        } else {
          if (OB_FAIL(types[i].get_length_for_meta_in_bytes(length))) {
            LOG_WARN("get length failed", K(ret), K(types[i]));
          }
        }
        /* Oracle compatible: if prejudged result type is char and args' length are different, change result type to
         * varchar */
        LOG_DEBUG("cur len", K(length), K(max_length), K(types[i]));
        if (is_oracle_mode && need_merge_type &&
            ((ObCharType == type.get_type() && ObCharType == types[i].get_type()) ||
                (ob_is_nchar(type.get_type()) && ob_is_nchar(types[i].get_type()))) &&
            (max_length != -1) && (length != max_length)) {
          LOG_DEBUG("Merge type from Char to Varchar ", K(length), K(max_length), K(types[i]));
          type.set_type(ob_is_nchar(type.get_type()) ? ObNVarchar2Type : ObVarcharType);
        }
        /*no need to if(OB_SUCCE(ret)) here*/
        if (length > max_length) {
          if (types[i].is_null() && skip_null) {
            // skip
          } else {
            max_length = length;
          }
        }
      }  // end for
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        if (ob_is_integer_type(types[i].get_type())) {
          length = MAX(ObAccuracy::DDL_DEFAULT_ACCURACY[types[i].get_type()].precision_, types[i].get_precision());
          length_semantics = -1;
        } else {
          length = types[i].get_length();
          length_semantics = types[i].get_length_semantics();

          if (LS_BYTE == length_semantics) {
            byte_length_count++;
          } else if (LS_CHAR == length_semantics) {
            char_length_count++;
            if (length > max_length_char) {
              max_length_char = length;
            }
          }
        }
        /* Oracle compatible: if prejudged result type is char and args' length are different, change result type to
         * varchar */
        LOG_DEBUG("cur len", K(length), K(max_length), K(max_length_char), K(length_semantics), K(types[i]), K(type));
        if (is_oracle_mode && need_merge_type &&
            ((ObCharType == type.get_type() && ObCharType == types[i].get_type()) ||
                (ob_is_nchar(type.get_type()) && ob_is_nchar(types[i].get_type()))) &&
            (max_length != -1) && (length != max_length || (byte_length_count > 0 && char_length_count > 0))) {
          LOG_DEBUG("Merge type from Char to Varchar ", K(length), K(max_length), K(length_semantics), K(types[i]));
          type.set_type(ob_is_nchar(type.get_type()) ? ObNVarchar2Type : ObVarcharType);
        }
        if (length > max_length) {
          if (types[i].is_null() && skip_null) {
            // skip
          } else {
            max_length = length;
          }
        }
      }  // end for
    }
    // set length
    if (OB_SUCC(ret) && max_length < 0) {
      if (skip_null) {
        max_length = 0;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected max length.", K(ret), K(max_length), K(param_num), K(len_in_byte));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_oracle_mode && type.is_character_type()) {
        if (type.is_nstring()) {
          type.set_length_semantics(LS_CHAR);
        } else {
          if (byte_length_count > 0 && 0 == char_length_count) {
            type.set_length_semantics(LS_BYTE);
          } else if (char_length_count > 0 && 0 == byte_length_count) {
            type.set_length_semantics(LS_CHAR);
          } else {
            type.set_length_semantics(default_length_semantics);
          }
        }
        if (LS_CHAR == type.get_length_semantics()) {
          type.set_length(std::max(max_length_char, max_length));
        } else {
          type.set_length(std::max(static_cast<ObLength>(max_length_char * mbmaxlen), max_length));
        }
      } else {
        type.set_length(max_length);
      }
    }
  }
  return ret;
}

int ObExprOperator::aggregate_accuracy_for_merge(ObExprResType& type, const ObExprResType* types, int64_t param_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObScale scale = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      if (types[i].get_scale() > scale) {
        scale = types[i].get_scale();
      }
    }
    if (OB_UNLIKELY(scale < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected scale.", K(ret), K(scale));
    } else {
      type.set_scale(scale);
    }
  }
  return ret;
}

int ObExprOperator::aggregate_temporal_accuracy_for_merge(
    ObExprResType& type, const ObExprResType* types, int64_t param_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObScale scale = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      if ((ob_is_temporal_type(types[i].get_type()) || ob_is_otimestamp_type(types[i].get_type())) &&
          types[i].get_scale() > scale) {
        scale = types[i].get_scale();
      }
    }
    if (OB_UNLIKELY(scale < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected scale.", K(ret), K(scale));
    } else {
      type.set_scale(scale);
    }
  }
  return ret;
}

int ObExprOperator::aggregate_numeric_accuracy_for_merge(
    ObExprResType& type, const ObExprResType* types, int64_t param_num, bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else if (is_oracle_mode) {
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else {
    ObPrecision precision = 0;
    ObScale scale = 0;
    int16_t max_integer_digits = -1;
    int16_t max_decimal_digits = -1;
    bool has_real_type = false;
    for (int64_t i = 0; i < param_num && OB_SUCC(ret); ++i) {
      precision = PRECISION_UNKNOWN_YET;
      scale = SCALE_UNKNOWN_YET;
      if (OB_UNLIKELY(types[i].get_type() < ObNullType || types[i].get_type() >= ObMaxType)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected error. wrong type", K(ret), K(types[i]));
      } else if (ob_is_real_type(types[i].get_type())) {
        /*
          create table sb(a float);
          create table sc(c year);
          insert sb values (1.5);
          insert sc values(1923);
          a union c will result to 1.5 and 1923
        */
        has_real_type = true;
      } else {
        /*
          create table sb(a int(3));//3 ? display width ? precision? length?
          create table sc(c decimal(5,3));
          insert sb values (12345);
          insert sc values(123.33);
          a union c will result to 12345 and 123.33
        */
        if (ob_is_integer_type(types[i].get_type())) {
          precision = MAX(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode][types[i].get_type()].precision_,
              types[i].get_precision());
          scale = 0;
        } else if (ob_is_number_tc(types[i].get_type()) || ob_is_temporal_type(types[i].get_type())) {
          precision = types[i].get_precision();
          scale = types[i].get_scale();
        } else {
          // string type. precision and scale means nothing to string types
          // just let it go
        }

        if (precision >= 0 && precision - scale > max_integer_digits) {
          max_integer_digits = static_cast<int16_t>(precision - scale);
        }
        if (scale >= 0 && scale > max_decimal_digits) {
          max_decimal_digits = scale;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ob_is_real_type(type.get_type()) && has_real_type) {
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_scale(SCALE_UNKNOWN_YET);
    } else {
      if (max_integer_digits + max_decimal_digits >= 0) {
        precision = static_cast<ObPrecision>(max_integer_digits + max_decimal_digits);
        type.set_precision(MIN(precision, ObAccuracy::MAX_ACCURACY2[is_oracle_mode][type.get_type()].precision_));
      }
      if (max_decimal_digits >= 0) {
        type.set_scale(MIN(max_decimal_digits, ObAccuracy::MAX_ACCURACY2[is_oracle_mode][type.get_type()].scale_));
      }
    }
  }
  return ret;
}

ObObjType ObExprOperator::enumset_calc_types_[ObMaxTC] = {
    ObUInt64Type,  /*ObNullTC*/
    ObUInt64Type,  /*ObIntTC*/
    ObUInt64Type,  /*ObUIntTC*/
    ObUInt64Type,  /*ObFloatTC*/
    ObUInt64Type,  /*ObDoubleTC*/
    ObUInt64Type,  /*ObNumberTC*/
    ObVarcharType, /*ObDateTimeTC*/
    ObVarcharType, /*ObDateTC*/
    ObVarcharType, /*ObTimeTC*/
    ObUInt64Type,  /*ObYearTC*/
    ObVarcharType, /*ObStringTC*/
    ObMaxType,     /*ObExtendTC*/
    ObMaxType,     /*ObUnknownTC*/
    ObVarcharType, /*ObTextTC*/
    ObUInt64Type,  /*ObBitTC*/
    ObVarcharType, /*ObEnumSetTC*/
    ObVarcharType, /*ObEnumSetInnerTC*/
};
////////////////////////////////////////////////////////////////

int ObArithExprOperator::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObArithExprOperator* tmp_other = dynamic_cast<const ObArithExprOperator*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (this != tmp_other) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->result_type_func_ = tmp_other->result_type_func_;
      this->calc_type_func_ = tmp_other->calc_type_func_;
    }
  }
  return ret;
}

int ObArithExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_type_func_) || OB_ISNULL(calc_type_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("result_type_func_  or calc_type_func_is null", K(ret));
  } else {
    ObObjType calc_type = ObMaxType;
    ObObjType calc_ob1_type = ObMaxType;
    ObObjType calc_ob2_type = ObMaxType;
    if (OB_UNLIKELY(NOT_ROW_DIMENSION != row_dimension_)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    } else if (OB_FAIL(result_type_func_(type, type1, type2))) {
      LOG_WARN("fail to result_type_func_", K(ret));
    } else if (OB_UNLIKELY(ObMaxType == type.get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    } else if (OB_FAIL(calc_type_func_(calc_type, calc_ob1_type, calc_ob2_type, type1.get_type(), type2.get_type()))) {
      LOG_WARN("fail to calc_type_func_", K(ret));
    } else {
      type.set_calc_type(calc_type);
      type1.set_calc_type(get_calc_cast_type(type1.get_type(), calc_ob1_type));
      type2.set_calc_type(get_calc_cast_type(type2.get_type(), calc_ob2_type));
      ObExprOperator::calc_result_flag2(type, type1, type2);
    }

    LOG_DEBUG("arithmatic result type",
        K(type),
        K(type1),
        K(type2),
        K(calc_type),
        K(calc_ob1_type),
        K(calc_ob2_type),
        K(ret));
  }
  return ret;
}

int ObArithExprOperator::calc_result2(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx) const
{
  return calc_(
      result, left, right, expr_ctx, result_type_.get_calc_scale(), result_type_.get_calc_type(), arith_funcs_);
}

int ObArithExprOperator::calc(ObObj& result, const ObObj& left, const ObObj& right, ObIAllocator* allocator,
    ObScale scale, ObCalcTypeFunc calc_type_func, const ObArithFunc* arith_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_type_func) || OB_ISNULL(allocator) || OB_ISNULL(arith_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the pointer is null", K(calc_type_func), K(allocator), K(arith_funcs), K(ret));
  } else {
    ObObjType calc_type = ObMaxType;
    ObObjType calc_ob1_type = ObMaxType;
    ObObjType calc_ob2_type = ObMaxType;
    if (OB_FAIL(calc_type_func(calc_type, calc_ob1_type, calc_ob2_type, left.get_type(), right.get_type()))) {
    } else {
      ObExprCtx expr_ctx(NULL, NULL, NULL, allocator);
      ret = calc_(result, left, right, expr_ctx, scale, calc_type, arith_funcs);
    }
  }
  return ret;
}

int ObArithExprOperator::calc(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx,
    ObScale calc_scale, ObCalcTypeFunc calc_type_func, const ObArithFunc* arith_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_type_func) || OB_ISNULL(arith_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("calc_type_func is null", K(ret));
  } else {
    ObObjType calc_type = ObMaxType;
    ObObjType calc_ob1_type = ObMaxType;
    ObObjType calc_ob2_type = ObMaxType;
    if (OB_FAIL(calc_type_func(calc_type, calc_ob1_type, calc_ob2_type, left.get_type(), right.get_type()))) {
    } else {
      ret = calc_(result, left, right, expr_ctx, calc_scale, calc_type, arith_funcs);
    }
  }
  return ret;
}

int ObArithExprOperator::calc_(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx,
    ObScale calc_scale, ObObjType calc_type, const ObArithFunc* arith_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arith_funcs) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the pointer is null", K(arith_funcs), K(expr_ctx.calc_buf_), K(ret));
  } else if (OB_UNLIKELY(ObNullType == calc_type || ObMaxType == calc_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("result type is invalid", K(ret), K(calc_type));
  }

  if (OB_SUCC(ret) && (OB_UNLIKELY(ObNullType == left.get_type() || ObNullType == right.get_type()))) {
    result.set_null();
  } else if (OB_SUCC(ret)) {
    const ObObj* res_left = &left;
    const ObObj* res_right = &right;
    ObObj tmp_left_obj;
    ObObj tmp_right_obj;
    if (left.get_type() != calc_type || right.get_type() != calc_type) {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
      if (is_datetime_add_minus_calc(calc_type, arith_funcs)) {
        if (OB_FAIL(ObObjCaster::to_datetime(calc_type, cast_ctx, left, tmp_left_obj, res_left))) {
          LOG_WARN("cast failed.", K(ret), K(left), K(calc_type));
        } else if (OB_FAIL(ObObjCaster::to_datetime(calc_type, cast_ctx, right, tmp_right_obj, res_right))) {
          LOG_WARN("cast failed.", K(ret), K(right), K(calc_type));
        }
      } else if (OB_FAIL(ObObjCaster::to_type(calc_type, cast_ctx, left, tmp_left_obj, res_left))) {
        LOG_WARN("cast failed.", K(ret), K(left), K(calc_type));
      } else if (OB_FAIL(ObObjCaster::to_type(calc_type, cast_ctx, right, tmp_right_obj, res_right))) {
        LOG_WARN("cast failed.", K(ret), K(right), K(calc_type));
      }
    }
    // ok, let's calc it.
    if (OB_SUCC(ret)) {
      ObArithFunc arith_func = arith_funcs[res_left->get_type_class()];
      LOG_DEBUG("begin calc", K(left), KPC(res_left), K(right), KPC(res_right), K(calc_scale), K(calc_type), K(lbt()));
      if (OB_ISNULL(arith_func)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("arith_func is null", K(ret));
      } else {
        ret = arith_func(result, *res_left, *res_right, expr_ctx.calc_buf_, calc_scale);
        if (OB_ERR_UNEXPECTED == ret) {
          ob_abort();
        }
      }
    }
    //    }
  }
  return ret;
}

bool ObArithExprOperator::is_datetime_add_minus_calc(const common::ObObjType calc_type, const ObArithFunc* arith_funcs)
{
  return (NULL != arith_funcs && (ObExprAdd::add_datetime == arith_funcs[ob_obj_type_class(calc_type)] ||
                                     ObExprMinus::minus_datetime == arith_funcs[ob_obj_type_class(calc_type)]));
}

int ObRelationalExprOperator::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  cmp_op_func2_ = NULL;
  ret = ObExprOperator::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret) && (IS_COMMON_COMPARISON_OP(type_) || T_FUN_SYS_STRCMP == type_) &&
      ObExprOperator::NOT_ROW_DIMENSION == row_dimension_ && real_param_num_ == 2) {
    if (input_types_.count() != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid input types", K(ret), K(type_), K(real_param_num_), K(input_types_.count()));
    } else {
      ObObjType left_operand_type = input_types_.at(0).get_calc_type();
      ObObjType right_operand_type = input_types_.at(1).get_calc_type();
      if (OB_FAIL(set_cmp_func(left_operand_type, right_operand_type))) {
        LOG_WARN("set cmp func failed", K(ret), K(left_operand_type), K(right_operand_type), K(type_));
        cmp_op_func2_ = NULL;  // defensive code
      }
    }
  }
  return ret;
}

int ObRelationalExprOperator::compare(ObObj& result, const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx,
    ObCastCtx& cast_ctx, ObCmpOp cmp_op)
{
  int ret = OB_SUCCESS;
  bool need_cast = false;
  if (OB_FAIL(compare_nocast(result, obj1, obj2, cmp_ctx, cmp_op, need_cast))) {
    LOG_WARN("failed to compare objects", K(obj1), K(obj2));
  } else if (need_cast) {
    if (OB_FAIL(compare_cast(result, obj1, obj2, cmp_ctx, cast_ctx, cmp_op))) {
      LOG_WARN("failed to compare objects", K(obj1), K(obj2));
    }
  }
  return ret;
}

int ObRelationalExprOperator::compare_cast(ObObj& result, const ObObj& obj1, const ObObj& obj2,
    const ObCompareCtx& cmp_ctx, ObCastCtx& cast_ctx, ObCmpOp cmp_op)
{
  int ret = OB_SUCCESS;
  ObObj buf_obj1;
  ObObj buf_obj2;
  const ObObj* res_obj1 = NULL;
  const ObObj* res_obj2 = NULL;
  bool need_cast = false;
  cast_ctx.dest_collation_ = cmp_ctx.cmp_cs_type_;
  if (OB_FAIL(ObObjCaster::to_type(cmp_ctx.cmp_type_, cast_ctx, obj1, buf_obj1, res_obj1))) {
    LOG_WARN("failed to cast obj", K(ret), K(cmp_ctx.cmp_type_), K(obj1));
  } else if (OB_FAIL(ObObjCaster::to_type(cmp_ctx.cmp_type_, cast_ctx, obj2, buf_obj2, res_obj2))) {
    LOG_WARN("failed to cast obj", K(ret), K(cmp_ctx.cmp_type_), K(obj2));
  } else if (OB_ISNULL(res_obj1) || OB_ISNULL(res_obj2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_obj1 or res_obj2 is null");
  } else if (OB_FAIL(ObObjCmpFuncs::compare(result, *res_obj1, *res_obj2, cmp_ctx, cmp_op, need_cast))) {
    LOG_WARN("failed to compare objects", K(ret), K(*res_obj1), K(*res_obj2), K(cmp_op));
  } else if (need_cast) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to compare objects", K(ret), K(*res_obj1), K(*res_obj2), K(cmp_op));
  }
  return ret;
}

int ObRelationalExprOperator::compare_nullsafe(int64_t& result, const ObObj& obj1, const ObObj& obj2,
    ObCastCtx& cast_ctx, ObObjType cmp_type, ObCollationType cmp_cs_type)
{
  int ret = OB_SUCCESS;
  ObObj res_obj;
  int64_t tz_offset = 0;
  if (OB_FAIL(get_tz_offset(cast_ctx.dtc_params_.tz_info_, tz_offset))) {
    LOG_WARN("get tz_offset failed", K(ret), K(cast_ctx.dtc_params_.tz_info_), K(tz_offset));
  } else {
    ObCompareCtx cmp_ctx(cmp_type, cmp_cs_type, true, tz_offset, default_null_pos());
    if (OB_FAIL(compare(res_obj, obj1, obj2, cmp_ctx, cast_ctx, CO_CMP))) {
    } else {
      result = res_obj.get_int();
    }
  }
  return ret;
}

bool ObRelationalExprOperator::can_cmp_without_cast(
    ObExprResType type1, ObExprResType type2, ObCmpOp cmp_op, const ObSQLSessionInfo& session)
{
  bool need_no_cast = false;
  if (ob_is_enum_or_set_type(type1.get_type()) && ob_is_enum_or_set_type(type2.get_type())) {
    need_no_cast = false;
  } else if (session.use_static_typing_engine()) {
    if (ObDatumFuncs::is_string_type(type1.get_type()) && ObDatumFuncs::is_string_type(type2.get_type())) {
      need_no_cast = ObCharset::charset_type_by_coll(type1.get_collation_type()) ==
                     ObCharset::charset_type_by_coll(type2.get_collation_type());
    } else {
      auto func_ptr = ObExprCmpFuncsHelper::get_eval_expr_cmp_func(
          type1.get_type(), type2.get_type(), cmp_op, lib::is_oracle_mode(), CS_TYPE_BINARY);
      need_no_cast = (func_ptr != nullptr);
    }
  } else {
    obj_cmp_func cmp_func = NULL;
    need_no_cast = ObObjCmpFuncs::can_cmp_without_cast(type1, type2, cmp_op, cmp_func);
  }
  return need_no_cast;
}

/*
 * Note that, the original motivation of this function is that,
 *
 * Provided that we have a = b and b = c in which a , b and c
 * are of metainfo meta1, meta2, meta3, respectively
 *
 * So, can we deduce that a = c holds actually?
 *
 * this func tells you this via result when and only when the ret is OB_SUCCESS
 */

int ObRelationalExprOperator::is_equivalent(
    const ObObjMeta& meta1, const ObObjMeta& meta2, const ObObjMeta& meta3, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;
  ObObjMeta equal_meta12;
  ObObjMeta equal_meta23;
  ObObjMeta equal_meta13;
  if (OB_FAIL(get_equal_meta(equal_meta12, meta1, meta2))) {
    LOG_WARN("get equal meta failed", K(ret), K(meta1), K(meta2));
  } else if (OB_FAIL(get_equal_meta(equal_meta13, meta1, meta3))) {
    LOG_WARN("get equal meta failed", K(ret), K(meta1), K(meta3));
  } else if (OB_FAIL(get_equal_meta(equal_meta23, meta2, meta3))) {
    LOG_WARN("get equal meta failed", K(ret), K(meta2), K(meta3));
  } else if (OB_UNLIKELY(equal_meta12.get_type() == ObMaxType ||
                         equal_meta13.get_type() == ObMaxType /* no need to check ObNullType here*/
                         || equal_meta23.get_type() == ObMaxType)) {
    /*result = false;*/
  } else if (equal_meta12.get_type() == equal_meta13.get_type() && equal_meta13.get_type() == equal_meta23.get_type()) {
    if (OB_UNLIKELY(ob_is_string_or_lob_type(equal_meta12.get_type()))) {
      // all are string type
      result = equal_meta12.get_collation_type() == equal_meta13.get_collation_type() &&
               equal_meta13.get_collation_type() == equal_meta23.get_collation_type();
    } else {
      result = true;
    }
  }
  return ret;
}

int ObRelationalExprOperator::get_equal_meta(ObObjMeta& meta, const ObObjMeta& meta1, const ObObjMeta& meta2)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObMaxType;
  if (OB_FAIL(ObExprResultTypeUtil::get_relational_equal_type(type, meta1.get_type(), meta2.get_type()))) {
    LOG_WARN("get equal type failed", K(ret), K(meta1), K(meta2));
  } else if (ob_is_string_or_lob_type(type)) {
    ObObjMeta coll_types[2] = {meta1, meta2};
    ret = aggregate_charsets_for_comparison(meta, coll_types, 2, CS_TYPE_INVALID);
  }
  if (OB_SUCC(ret)) {
    meta.set_type(type);
  }
  return ret;
}

// MySQL use item_cmp_type to calc target compare type
int ObExprOperator::calc_cmp_type2(
    ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2, const ObCollationType coll_type) const
{
  int ret = OB_SUCCESS;
  ObObjType cmp_type = ObMaxType;
  // todo: we can add such code after plan cache add same optimization.
  //  if (is_int_cmp_const_str(&type1, &type2, cmp_type)) {
  //  } else
  if (is_oracle_mode() && (type1.is_blob() || type2.is_blob() || type1.is_blob_locator() || type2.is_blob_locator()) &&
      type_ != T_OP_IS && type_ != T_OP_IS_NOT) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()), ob_obj_type_str(type2.get_type()));
  } else if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, type1.get_type(), type2.get_type()))) {
    LOG_WARN("fail to get cmp_type", K(ret));
  } else if (OB_UNLIKELY(ObMaxType == cmp_type)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ObURowIDType == cmp_type) {
    // only support <urwoid, urowid>,<string, urwoid>, <urowid, string>,
    // <NULL, urowid>, <urowid, NULL>
    if (OB_UNLIKELY(ObStringTC != type1.get_type_class() && ObRowIDTC != type1.get_type_class() &&
                    ObNullTC != type1.get_type_class()) ||
        OB_UNLIKELY(ObStringTC != type2.get_type_class() && ObRowIDTC != type2.get_type_class() &&
                    ObNullTC != type2.get_type_class())) {
      ret = OB_INVALID_ROWID;
      LOG_WARN("not supported compare type", K(type1), K(type2));
    }
  }
  if (OB_SUCC(ret)) {
    type.set_calc_type(cmp_type);
    // collation
    if (ob_is_string_or_lob_type(cmp_type)) {
      ObObjMeta coll_types[2];
      coll_types[0].set_collation(type1);
      coll_types[1].set_collation(type2);
      ret = aggregate_charsets_for_comparison(type.get_calc_meta(), coll_types, 2, coll_type);
      if (OB_FAIL(ret)) {
        LOG_WARN("aggregate charset failed", K(type1), K(type2), K(type.get_calc_meta()));
      }
    } else if (ObRawType == cmp_type) {
      type.get_calc_meta().set_collation_type(CS_TYPE_BINARY);
    }
    LOG_DEBUG("calc cmp type", K(type1), K(type2), K(type.get_calc_meta()));
  }
  return ret;
}

int ObExprOperator::calc_cmp_type3(ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2,
    const ObExprResType& type3, const ObCollationType coll_type) const
{
  int ret = OB_SUCCESS;
  // cmp type
  ObObjType cmp_type = type1.get_type();
  if (is_oracle_mode() && (type1.is_blob() || type1.is_blob_locator() || type2.is_blob() || type2.is_blob_locator() ||
                              type3.is_blob() || type3.is_blob_locator())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()), ob_obj_type_str(type2.get_type()));
  } else if (OB_SUCC(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, type2.get_type(), cmp_type))) {
    if (OB_SUCC(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, cmp_type, type3.get_type()))) {
      if (OB_UNLIKELY(ObMaxType == cmp_type)) {
        ret = OB_INVALID_ARGUMENT;  // not compatible input
      }
    }
  }

  // collation
  if (OB_SUCC(ret)) {
    type.set_calc_type(cmp_type);
    if (ob_is_string_or_lob_type(cmp_type)) {
      ObObjMeta coll_types[3];
      coll_types[0].set_collation(type1);
      coll_types[1].set_collation(type2);
      coll_types[2].set_collation(type3);
      ret = aggregate_charsets_for_comparison(type.get_calc_meta(), coll_types, 3, coll_type);
    } else if (ObRawType == cmp_type) {
      type.get_calc_meta().set_collation_type(CS_TYPE_BINARY);
    }
  }
  return ret;
}

// for sqrt, ln, e functions
int ObExprOperator::calc_trig_function_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == type1.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else if (!share::is_oracle_mode()) {
    type.set_double();
  } else {
    if (ob_is_real_type(type1.get_type())) {
      type.set_double();
    } else {
      type.set_number();
    }
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  }
  type1.set_calc_type(type.get_type());
  ObExprOperator::calc_result_flag1(type, type1);
  return ret;
}

int ObExprOperator::calc_trig_function_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;  // arithmetic not support row
  } else if (ObMaxType == type1.get_type() || ObMaxType == type2.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else if (!share::is_oracle_mode()) {
    type.set_double();
  } else {
    if (ob_is_real_type(type1.get_type()) || ob_is_real_type(type2.get_type())) {
      type.set_double();
    } else {
      type.set_number();
    }
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  }
  type1.set_calc_type(type.get_type());
  type2.set_calc_type(type.get_type());
  ObExprOperator::calc_result_flag2(type, type1, type2);
  return ret;
}

ObCastMode ObExprOperator::get_cast_mode() const
{
  return CM_NONE;
}

ObExpr* ObExprOperator::get_rt_expr(const ObRawExpr& raw_expr) const
{
  return raw_expr.rt_expr_;
}

OB_SERIALIZE_MEMBER(ObIterExprOperator, expr_id_, expr_type_);

int ObRelationalExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  return deduce_cmp_type(*this, type, type1, type2, type_ctx);
}

int ObRelationalExprOperator::deduce_cmp_type(const ObExprOperator& expr, ObExprResType& type, ObExprResType& type1,
    ObExprResType& type2, ObExprTypeCtx& type_ctx)
{
  int ret = OB_SUCCESS;
  ObExprResType cmp_type;
  CK(NULL != type_ctx.get_session());
  if (OB_FAIL(ret)) {
  } else if (OB_SUCC(expr.calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()))) {
    type.set_int32();  // not tinyint, compatible with MySQL
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_calc_collation(cmp_type);
    type.set_calc_type(cmp_type.get_calc_type());
    ObExprOperator::calc_result_flag2(type, type1, type2);
    bool need_no_cast = can_cmp_without_cast(type1, type2, get_cmp_op(expr.get_type()), *type_ctx.get_session());
    type1.set_calc_type(need_no_cast ? type1.get_type() : cmp_type.get_calc_type());
    type2.set_calc_type(need_no_cast ? type2.get_type() : cmp_type.get_calc_type());
    if (ob_is_string_or_lob_type(cmp_type.get_calc_type())) {
      type1.set_calc_collation_type(cmp_type.get_calc_collation_type());
      type2.set_calc_collation_type(cmp_type.get_calc_collation_type());
    } else if (ObRawType == cmp_type.get_calc_type()) {
      type1.set_calc_collation_type(CS_TYPE_BINARY);
      type2.set_calc_collation_type(CS_TYPE_BINARY);
    }
  }
  return ret;
}

// TODO():invoke  type has not set calc_type
int ObRelationalExprOperator::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType cmp_type;
  if (OB_SUCC(calc_cmp_type3(cmp_type, type1, type2, type3, type_ctx.get_coll_type()))) {
    type.set_int32();
    type.set_calc_collation(cmp_type);
    type.set_calc_type(cmp_type.get_calc_type());
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    ObExprOperator::calc_result_flag3(type, type1, type2, type3);
    if (OB_FAIL(calc_calc_type3(type1, type2, type3, type_ctx, cmp_type.get_calc_type()))) {
      LOG_WARN("set calc type failed", K(type1), K(type2), K(type3), K(cmp_type), K(ret));
    }
    if (ob_is_string_or_lob_type(cmp_type.get_calc_type())) {
      type1.set_calc_collation_type(cmp_type.get_calc_collation_type());
      type2.set_calc_collation_type(cmp_type.get_calc_collation_type());
      type3.set_calc_collation_type(cmp_type.get_calc_collation_type());
    } else if (ObRawType == cmp_type.get_calc_type()) {
      type1.set_calc_collation_type(CS_TYPE_BINARY);
      type2.set_calc_collation_type(CS_TYPE_BINARY);
      type3.set_calc_collation_type(CS_TYPE_BINARY);
    }
  }
  return ret;
}

int ObRelationalExprOperator::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{

  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(0 >= row_dimension_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or row_dimension is less than 0", K(types), K(row_dimension_), K(ret));
  } else if (OB_UNLIKELY(param_num <= 0 || 2 != param_num / row_dimension_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("param_num is wrong", K(param_num), K(ret));
  } else if (OB_FAIL(type.init_row_dimension(row_dimension_))) {
    LOG_WARN("fail to init row dimemnsion", K(ret));
  } else {
    // () MySQL deal with (a,b) = (c, d) policy:
    // compare pair by pair, with different types
    // select (1, '1.000000') = ('1.00x', '1.000000');
    // 1 compare with '1.00x' as int,
    // '1.00000' compare with '1.000000' as string
    ObExprResType tmp_res_type;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_dimension_; ++i) {
      if (OB_FAIL(ObRelationalExprOperator::calc_result_type2(
              tmp_res_type, types[i], types[i + row_dimension_], type_ctx))) {
        LOG_WARN("failed to calc result types", K(ret));
      } else if (OB_FAIL(type.get_row_calc_cmp_types().push_back(tmp_res_type.get_calc_meta()))) {
        LOG_WARN("failed to push back cmp type", K(ret));
      } else {
        if (ob_is_string_or_lob_type(tmp_res_type.get_calc_type())) {
          types[i].set_calc_collation_type(tmp_res_type.get_calc_collation_type());
          types[i + row_dimension_].set_calc_collation_type(tmp_res_type.get_calc_collation_type());
        } else if (ObRawType == tmp_res_type.get_calc_type()) {
          types[i].set_calc_collation_type(CS_TYPE_BINARY);
          types[i + row_dimension_].set_calc_collation_type(CS_TYPE_BINARY);
        }
      }
    }
    // overall result type
    if (OB_SUCC(ret)) {
      type.set_int32();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      ObExprOperator::calc_result_flagN(type, types, param_num);
    }
  }
  return ret;
}

int ObRelationalExprOperator::calc_calc_type3(ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
    ObExprTypeCtx& type_ctx, const ObObjType cmp_type) const
{
  int ret = OB_SUCCESS;
  // a(type1) between b(type2) and c(type3) <==> b<=a && a <=c
  ObExprResType cmp_type21;  // b <= a
  ObExprResType cmp_type13;  // a <= c
  bool need_no_cast = false;
  CK(NULL != type_ctx.get_session());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_cmp_type2(cmp_type21, type2, type1, type_ctx.get_coll_type()))) {
    LOG_WARN("get cmp failed", K(ret), K(type2), K(type1));
  } else if (OB_FAIL(calc_cmp_type2(cmp_type13, type1, type3, type_ctx.get_coll_type()))) {
    LOG_WARN("get cmp failed", K(ret), K(type1), K(type3));
  } else {
    ObObjType type21 = cmp_type21.get_type();
    ObObjType type13 = cmp_type13.get_type();
    if (OB_LIKELY(type21 == type13 && type13 == cmp_type)) {
      // type21 == type13 == cmp_type
      bool need_no_cast_21 = can_cmp_without_cast(type2, type1, get_cmp_op(type_), *type_ctx.get_session());
      bool need_no_cast_13 = can_cmp_without_cast(type1, type3, get_cmp_op(type_), *type_ctx.get_session());
      need_no_cast = need_no_cast_21 && need_no_cast_13;
    }
  }
  if (OB_SUCC(ret)) {
    if (need_no_cast) {
      type1.set_calc_type(type1.get_type());
      type2.set_calc_type(type2.get_type());
      type3.set_calc_type(type3.get_type());
    } else {
      type1.set_calc_type(cmp_type);
      type2.set_calc_type(cmp_type);
      type3.set_calc_type(cmp_type);
    }
  }
  return ret;
}

int ObRelationalExprOperator::get_cmp_result_type3(ObExprResType& type, bool& need_no_cast, const ObExprResType* types,
    const int64_t param_num, const sql::ObSQLSessionInfo& my_session)
{
  int ret = OB_SUCCESS;
  need_no_cast = false;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1) || OB_UNLIKELY(param_num > 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else if (1 == param_num) {
    // this is for case when clause like case  when 1 then c1 end;
    type = types[0];
    need_no_cast = true;
  } else {
    ObCollationType coll_type = CS_TYPE_INVALID;
    const ObLengthSemantics default_ls = my_session.get_actual_nls_length_semantics();
    if (OB_FAIL(my_session.get_collation_connection(coll_type))) {
      LOG_WARN("fail to get_collation_connection", K(ret));
    } else if (2 == param_num) {
      need_no_cast = can_cmp_without_cast(types[1], types[0], CO_CMP, my_session);
      if (!need_no_cast) {
        ret = calc_cmp_type2(type, types[0], types[1], coll_type);
      }
    } else {
      const ObExprResType& type1 = types[0];
      const ObExprResType& type2 = types[1];
      const ObExprResType& type3 = types[2];
      // a(type1) between b(type2) and c(type3) <==> b<=a && a <=c
      ObExprResType cmp_type21;  // b <= a
      ObExprResType cmp_type13;  // a <= c
      if (OB_FAIL(calc_cmp_type3(type, type1, type2, type3, coll_type))) {
        LOG_WARN("fail to calc_cmp_type3", K(ret));
      } else if (OB_FAIL(calc_cmp_type2(cmp_type21, type2, type1, coll_type))) {
        LOG_WARN("get cmp failed", K(ret), K(type2), K(type1));
      } else if (OB_FAIL(calc_cmp_type2(cmp_type13, type1, type3, coll_type))) {
        LOG_WARN("get cmp failed", K(ret), K(type1), K(type3));
      } else if (cmp_type21.get_type() == cmp_type13.get_type() && cmp_type13.get_type() == type.get_calc_type()) {
        // type21 == type13 == cmp_type
        bool need_no_cast_21 = can_cmp_without_cast(type2, type1, CO_CMP, my_session);
        bool need_no_cast_13 = can_cmp_without_cast(type1, type3, CO_CMP, my_session);
        need_no_cast = need_no_cast_21 && need_no_cast_13;
      }
    }
  }
  return ret;
}

int ObRelationalExprOperator::calc_result2(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx, bool is_null_safe, ObCmpOp cmp_op) const
{
  int ret = OB_SUCCESS;
  // TODO:: raw
  //  bool need_cast = (share::is_oracle_mode() && obj1.get_collation_type() != obj2.get_collation_type());
  bool need_cast = false;
  EXPR_DEFINE_CMP_CTX(result_type_.get_calc_meta(), is_null_safe, expr_ctx);
  /*
   * FIX ME,please. It seems that we must check obj1 and obj2 are null or not here
   *
   * But this is so ugly and everything will be retarded
   */
  if (OB_LIKELY(!obj1.is_null() && !obj2.is_null() && cmp_op_func2_ != NULL && !need_cast)) {
    if (OB_FAIL(compare_nocast(result, obj1, obj2, cmp_ctx, cmp_op, cmp_op_func2_))) {
      LOG_WARN("failed to compare objects", K(ret), K(obj1), K(obj2));
    }
  } else if (!need_cast && OB_FAIL(compare_nocast(result, obj1, obj2, cmp_ctx, cmp_op, need_cast))) {
    LOG_WARN("failed to compare objects", K(ret), K(obj1), K(obj2));
  } else if (need_cast) {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(compare_cast(result, obj1, obj2, cmp_ctx, cast_ctx, cmp_op))) {
      LOG_WARN("failed to compare objects", K(ret), K(obj1), K(obj2));
    }
  }
  return ret;
}

/*If you know little about vector compare in mysql, before you start to read this function,
 *it is highly recommended for you to digg into this:
 *http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html
 *
 */
int ObRelationalExprOperator::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num,
    ObExprCtx& expr_ctx, bool is_null_safe, ObCmpOp cmp_op) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_dimension_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row_dimension_ is less or equal to 0", K(ret));
  } else if (OB_ISNULL(objs_array) || OB_UNLIKELY(0 >= param_num) || OB_UNLIKELY(0 != param_num % row_dimension_) ||
             OB_UNLIKELY(2 != param_num / row_dimension_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("objs_array or param_num or row_dimension_ is wrong", K(ret), K(param_num), K(row_dimension_));
  } else if (cmp_op < CO_EQ || cmp_op >= CO_CMP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cmp_op argument", K(ret), K(cmp_op));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    const ObIArray<ObExprCalcType>& cmp_types = result_type_.get_row_calc_cmp_types();
    int64_t i = 0;
    bool row_cnt_null = false;
    ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, is_null_safe, expr_ctx.tz_offset_, default_null_pos());
    // firstly, we try to locate the first non-equal pair
    for (i = 0; OB_SUCC(ret) && i < row_dimension_; ++i) {
      cmp_ctx.cmp_type_ = cmp_types.at(i).get_type();
      cmp_ctx.cmp_cs_type_ = cmp_types.at(i).get_collation_type();
      if (OB_FAIL(compare(result, objs_array[i], objs_array[i + row_dimension_], cmp_ctx, cast_ctx, CO_NE))) {
        LOG_WARN("compare failed", K(objs_array[i]), K(objs_array[i + row_dimension_]));
      } else if (result.is_null()) {
        row_cnt_null = true;
        // no break here
      } else if (result.is_true()) {
        break;
      }
    }  // end for
    if (OB_SUCC(ret)) {
      if (i == row_dimension_) {  // all pairs are null or equal
        if (row_cnt_null) {       // such as (1,2) op (null, 2) , (1,2) op (null, null)
          result.set_null();
        } else {  // all pairs are equal. such as (1,2) op (1,2)
          if (CO_EQ == cmp_op || CO_LE == cmp_op || CO_GE == cmp_op) {
            result.set_bool(true);
          } else {
            result.set_bool(false);
          }
        }
      } else {               // exist one pair whose comparison result is non-equal
        if (row_cnt_null) {  // such as (1,2) op (null, 3)
          if (CO_NE == cmp_op) {
            result.set_bool(true);
          } else if (CO_EQ == cmp_op) {
            result.set_bool(false);
          } else {
            result.set_null();
          }
        } else {  // such as (1,2,3) op (1,2,4)
          cmp_ctx.cmp_type_ = cmp_types.at(i).get_type();
          cmp_ctx.cmp_cs_type_ = cmp_types.at(i).get_collation_type();
          if (OB_FAIL(compare(result, objs_array[i], objs_array[i + row_dimension_], cmp_ctx, cast_ctx, cmp_op))) {
            LOG_WARN("compare failed", K(objs_array[i]), K(objs_array[i + row_dimension_]), K(cmp_op));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (false == result.is_null()) {
        // compatiable with MySQL,relational op result type is int32
        result.set_int(ObInt32Type, result.get_int());
      }
    }
  }
  return ret;
}

bool ObRelationalExprOperator::is_int_cmp_const_str(
    const ObExprResType* type1, const ObExprResType* type2, ObObjType& cmp_type)
{
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(cmp_type);
  return false;
}

int ObRelationalExprOperator::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObRelationalExprOperator* tmp_other = dynamic_cast<const ObRelationalExprOperator*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->cmp_op_func2_ = tmp_other->cmp_op_func2_;
    }
  }
  return ret;
}

int ObRelationalExprOperator::set_cmp_func(const ObObjType type1, const ObObjType type2)
{
  int ret = OB_SUCCESS;
  ObCmpOp cmp_op = get_cmp_op(type_);
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  if (OB_FAIL(ObObjCmpFuncs::get_cmp_func(tc1, tc2, cmp_op, cmp_op_func2_))) {
    LOG_WARN("get cmp func failed", K(type1), K(type2), K(tc1), K(tc2), K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObSubQueryRelationalExpr, ObExprOperator), subquery_key_, left_is_iter_, right_is_iter_);

int ObSubQueryRelationalExpr::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObSubQueryRelationalExpr* tmp_other = dynamic_cast<const ObSubQueryRelationalExpr*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->subquery_key_ = tmp_other->subquery_key_;
      this->left_is_iter_ = tmp_other->left_is_iter_;
      this->right_is_iter_ = tmp_other->right_is_iter_;
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObExprResType tmp_res_type;
  CK(NULL != type_ctx.get_session());
  OZ(type.init_row_dimension(1));
  if (OB_SUCC(ret) && !type_ctx.get_session()->use_static_typing_engine()) {
    // static typing engine not enabled, keep the old behavior:
    // type1, type2 are unchanged
    if (OB_FAIL(calc_result_type2_(tmp_res_type, type1, type2, type_ctx))) {
      LOG_WARN("failed to calc_result_type2_", K(ret));
    } else if (OB_FAIL(type.get_row_calc_cmp_types().push_back(tmp_res_type.get_calc_meta()))) {
      LOG_WARN("failed to push back cmp_type", K(ret));
    } else {
      type.set_tinyint();
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObTinyIntType].scale_);
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObTinyIntType].precision_);
      ObExprOperator::calc_result_flag2(type, type1, type2);
    }
  }
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    // static typing engine enabled, same with ObRelationalExprOperator::calc_result_type
    OZ(ObRelationalExprOperator::deduce_cmp_type(*this, type, type1, type2, type_ctx));
    OZ(type.get_row_calc_cmp_types().push_back(type.get_calc_meta()));
  }
  return ret;
}

int ObSubQueryRelationalExpr::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_session());
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(types) || OB_UNLIKELY(0 >= row_dimension_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or row_dimension_ is less equal than 0", K(types), K(row_dimension_), K(ret));
  } else if (OB_UNLIKELY(param_num <= 0 || 2 != param_num / row_dimension_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("param_num is wrong", K(ret));
  } else if (OB_FAIL(type.init_row_dimension(row_dimension_))) {
    LOG_WARN("fail to init row dimension", K(ret));
  }

  if (OB_SUCC(ret) && !type_ctx.get_session()->use_static_typing_engine()) {
    // static typing engine not enabled, keep the old behavior unchanged:
    // parameters' type is unchanged.

    // select (c1, c2) = (select b1, b2 from B);
    // c1 compare with b1 with type X,
    // c2  compare with b2 with type Y
    ObExprResType tmp_res_type;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_dimension_; ++i) {
      if (OB_FAIL(ObSubQueryRelationalExpr::calc_result_type2_(
              tmp_res_type, types[i], types[i + row_dimension_], type_ctx))) {
        LOG_WARN("fail to calc_result_type2_ when calc_result_typeN", K(ret));
      } else if (OB_FAIL(type.get_row_calc_cmp_types().push_back(tmp_res_type.get_calc_meta()))) {
        LOG_WARN("failed to push back cmp_type", K(ret));
      }
    }
    // overall result type
    if (OB_SUCC(ret)) {
      type.set_tinyint();
      ObExprOperator::calc_result_flagN(type, types, param_num);
    }
  }

  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_dimension_; i++) {
      ObExprResType tmp_res_type;
      OZ(ObRelationalExprOperator::deduce_cmp_type(*this, tmp_res_type, types[i], types[i + row_dimension_], type_ctx));
      OZ(type.get_row_calc_cmp_types().push_back(tmp_res_type.get_calc_meta()));
      if (OB_SUCC(ret)) {
        if (ob_is_string_type(tmp_res_type.get_calc_type())) {
          types[i].set_calc_collation_type(tmp_res_type.get_calc_collation_type());
          types[i + row_dimension_].set_calc_collation_type(tmp_res_type.get_calc_collation_type());
        } else if (ObRawType == tmp_res_type.get_calc_type()) {
          types[i].set_calc_collation_type(CS_TYPE_BINARY);
          types[i + row_dimension_].set_calc_collation_type(CS_TYPE_BINARY);
        }
      }
    }
    if (OB_SUCC(ret)) {
      type.set_int32();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      ObExprOperator::calc_result_flagN(type, types, param_num);
    }
  }

  return ret;
}
int ObSubQueryRelationalExpr::calc_result2(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!right_is_iter_) || OB_ISNULL(expr_ctx.subplan_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("right_is_iter_ is wrong or expr_ctx.subplan_iters_ is null",
        K(right_is_iter_),
        K(expr_ctx.subplan_iters_),
        K(ret));
  } else {
    int64_t subquery_idx = OB_INVALID_INDEX;
    ObNewRow tmp_row;
    ObNewRow* left_row = NULL;
    ObNewRowIterator* left_row_iter = NULL;
    if (left_is_iter_) {
      int64_t left_idx = OB_INVALID_INDEX;
      if (OB_FAIL(obj1.get_int(left_idx))) {
        LOG_WARN("get left subquery index failed", K(ret), K(obj1));
      } else if (OB_ISNULL(left_row_iter = expr_ctx.subplan_iters_->at(left_idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get row iterator failed", K(left_idx));
      } else if (OB_FAIL(left_row_iter->get_next_row(left_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          result.set_null();
        } else {
          LOG_WARN("get next row from left row iterator failed", K(ret));
        }
      }
    } else {
      tmp_row.cells_ = const_cast<ObObj*>(&obj1);
      tmp_row.count_ = 1;
      left_row = &tmp_row;
    }
    if (OB_SUCC(ret) && NULL != left_row) {
      if (OB_FAIL(obj2.get_int(subquery_idx))) {
        LOG_WARN("get subquery index failed", K(ret), K(obj2));
      } else if (T_WITH_ALL == subquery_key_) {
        if (OB_FAIL(calc_result_with_all(result, *left_row, subquery_idx, expr_ctx))) {
          LOG_WARN("calc result with all failed", K(ret));
        }
      } else if (T_WITH_ANY == subquery_key_) {
        if (OB_FAIL(calc_result_with_any(result, *left_row, subquery_idx, expr_ctx))) {
          LOG_WARN("calc result with any failed", K(ret));
        }
      } else if (T_WITH_NONE == subquery_key_) {
        // vector compare. scenario: both left right sides are subqueries
        if (OB_UNLIKELY(!left_is_iter_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("left_is_iter_ is wrong", K(ret));
        } else if (OB_FAIL(calc_result_with_none(result, *left_row, subquery_idx, expr_ctx))) {
          LOG_WARN("calc result with none failed", K(ret));
        }
      }
    }
    // for the comparison of subquery, if the left param is row iterator,
    // we must check the row count of the left row iterator
    // the left row iterator can only return one row
    if (OB_SUCC(ret) && left_is_iter_) {
      ObNewRow* tmp_left_row = NULL;
      if (OB_FAIL(left_row_iter->get_next_row(tmp_left_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row from iter failed", K(ret));
        }
      } else {
        ret = OB_SUBQUERY_TOO_MANY_ROW;
      }
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::calc_resultN(
    ObObj& result, const ObObj* param_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num <= 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("param_num is wrong", K(param_array), K(ret));
  } else if (OB_UNLIKELY(left_is_iter_ && right_is_iter_) || OB_ISNULL(expr_ctx.subplan_iters_) ||
             OB_ISNULL(param_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left_is_iter_ and right_is_iter cann't be same true",
        K(param_num),
        K(left_is_iter_),
        K(right_is_iter_),
        K(expr_ctx.subplan_iters_),
        K(param_array),
        K(ret));
  } else {
    int64_t subquery_idx = OB_INVALID_ID;
    ObNewRow tmp_row;
    ObNewRow* left_row = NULL;
    ObNewRowIterator* row_iter = NULL;
    if (left_is_iter_) {
      int64_t left_idx = OB_INVALID_INDEX;
      const ObObj& idx_obj = param_array[0];
      if (OB_FAIL(idx_obj.get_int(left_idx))) {
        LOG_WARN("get left subquery index failed", K(ret), K(idx_obj));
      } else if (OB_ISNULL(row_iter = expr_ctx.subplan_iters_->at(left_idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get row iterator failed", K(left_idx));
      } else if (OB_FAIL(row_iter->get_next_row(left_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          result.set_null();
        } else {
          LOG_WARN("get next row from left row iterator failed", K(ret));
        }
      } else {
        ObNewRow* tmp_left_row = NULL;
        tmp_row.cells_ = const_cast<ObObj*>(&param_array[1]);
        tmp_row.count_ = param_num - 1;
        if (OB_ISNULL(left_row)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("left_row is null", K(left_row), K(ret));
        } else if (OB_FAIL(compare_single_row(*left_row, tmp_row, expr_ctx, result))) {
          LOG_WARN("compare single row failed", K(ret));
        } else if (OB_FAIL(row_iter->get_next_row(tmp_left_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get next row from iter failed", K(ret));
          }
        } else {
          ret = OB_SUBQUERY_TOO_MANY_ROW;
        }
      }
    } else if (right_is_iter_) {
      tmp_row.cells_ = const_cast<ObObj*>(param_array);
      tmp_row.count_ = param_num - 1;
      left_row = &tmp_row;
      const ObObj& idx_obj = param_array[param_num - 1];
      if (OB_ISNULL(left_row)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("left_row is null", K(left_row), K(ret));
      } else if (OB_FAIL(idx_obj.get_int(subquery_idx))) {
        LOG_WARN("get subquery index failed", K(ret));
      } else if (T_WITH_ALL == subquery_key_) {
        if (OB_FAIL(calc_result_with_all(result, *left_row, subquery_idx, expr_ctx))) {
          LOG_WARN("calc result with all failed", K(ret));
        }
      } else if (T_WITH_ANY == subquery_key_) {
        if (OB_FAIL(calc_result_with_any(result, *left_row, subquery_idx, expr_ctx))) {
          LOG_WARN("calc result with any failed", K(ret));
        }
      } else if (T_WITH_NONE == subquery_key_) {
        if (OB_FAIL(calc_result_with_none(result, *left_row, subquery_idx, expr_ctx))) {
          LOG_WARN("calc result with none failed", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("left_is_iter_ and right_is_iter are all false", K(ret));
    }
  }

  return ret;
}

int ObSubQueryRelationalExpr::call(ObObj* stack, int64_t& stack_size, ObExprCtx& expr_ctx) const
{
  ObObj result;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stack) || OB_UNLIKELY(real_param_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stack is null or the param is wrong", K(stack), K(real_param_num_), K(ret));
  } else if (OB_UNLIKELY(real_param_num_ > stack_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stack is null or the param is wrong", K(stack_size), K(real_param_num_), K(ret));
  } else {
    switch (real_param_num_) {
      case 1: {
        if (OB_FAIL(calc_result1(result, stack[stack_size - 1], expr_ctx))) {
          LOG_WARN("fail to calc result1", K(ret), K(stack_size));
        } else {
          stack[stack_size - 1] = result;
        }
        break;
      }
      case 2: {
        if (OB_FAIL(calc_result2(result, stack[stack_size - 2], stack[stack_size - 1], expr_ctx))) {
          LOG_WARN("fail to calc result1", K(ret), K(stack_size));
        } else {
          stack[stack_size - 2] = result;
          stack_size--;
        }
        break;
      }
      default: {
        if (OB_FAIL(this->calc_resultN(result, &stack[stack_size - real_param_num_], real_param_num_, expr_ctx))) {
          LOG_WARN("fail to calc resultN", K(ret), K(stack_size));
        } else {
          stack[stack_size - real_param_num_] = result;
          stack_size -= real_param_num_ - 1;
        }
        break;
      }  // end switch
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::eval(
    common::ObExprCtx& expr_ctx, common::ObObj& val, common::ObObj* params, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  new (&val) ObObj();
  if (OB_ISNULL(params) || OB_UNLIKELY(param_num < 0) || OB_UNLIKELY(real_param_num_ != param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param_num), K(real_param_num_));
  } else {
    switch (real_param_num_) {
      case 1: {
        ret = calc_result1(val, params[0], expr_ctx);
        break;
      }
      case 2: {
        ret = calc_result2(val, params[0], params[1], expr_ctx);
        break;
      }
      default: {
        ret = calc_resultN(val, params, param_num, expr_ctx);
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("calc result failed", K(ret), K(param_num_), K(name_), K(get_type_name(type_)));
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::calc_result_with_none(
    ObObj& result, const ObNewRow& left_row, int64_t subquery_idx, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObNewRowIterator* row_iter = NULL;
  ObNewRow* row = NULL;
  if (OB_ISNULL(expr_ctx.subplan_iters_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr_ctx.subplan_iters_ is null");
  } else if (OB_UNLIKELY(subquery_idx < 0 || subquery_idx >= expr_ctx.subplan_iters_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery_idx is invalied", K(subquery_idx), "iter count", expr_ctx.subplan_iters_->count());
  } else if (OB_ISNULL(row_iter = expr_ctx.subplan_iters_->at(subquery_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery result iterator is null");
  } else if (OB_FAIL(row_iter->get_next_row(row))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      result.set_null();
    } else {
      LOG_WARN("get next row from row iterator failed", K(ret));
    }
  } else {
    if (OB_ISNULL(row)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("row is null");
    } else if (OB_UNLIKELY(left_row.get_count() != row->get_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("left_row and right row is not equal");
    } else {
      if (OB_FAIL(compare_single_row(left_row, *row, expr_ctx, result))) {
        LOG_WARN("compare single row failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row from iter failed", K(ret));
      }
    } else {
      ret = OB_SUBQUERY_TOO_MANY_ROW;
    }
  }
  return ret;
}

/**
 * ALL sematics: all true return true. if any false exists, return false
 * if NULL exists, return NULL if other values are true, return false if any values is false
 */
int ObSubQueryRelationalExpr::calc_result_with_all(
    ObObj& result, const ObNewRow& left_row, int64_t subquery_idx, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObNewRowIterator* row_iter = NULL;
  ObNewRow* row = NULL;
  ObObj tmp_result;
  bool cnt_null = false;
  if (OB_ISNULL(expr_ctx.subplan_iters_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery_idx is invalied", K(expr_ctx.subplan_iters_), K(ret));
  } else if (OB_UNLIKELY(subquery_idx < 0 || subquery_idx >= expr_ctx.subplan_iters_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery_idx is invalied", K(subquery_idx), "iter count", expr_ctx.subplan_iters_->count());
  } else if (OB_ISNULL(row_iter = expr_ctx.subplan_iters_->at(subquery_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery result iterator is null");
  } else {
    tmp_result.set_bool(true);  // empty set returns true
    while (OB_SUCC(ret) && OB_SUCC(row_iter->get_next_row(row))) {
      if (OB_ISNULL(row)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("row is null", K(ret));
      } else if (OB_UNLIKELY(left_row.get_count() != row->get_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("left_row and right_row is not equal", K(ret));
      } else {
        if (OB_FAIL(compare_single_row(left_row, *row, expr_ctx, tmp_result))) {
          LOG_WARN("compare single row failed", K(ret));
        } else if (OB_UNLIKELY(!tmp_result.is_int32() && !tmp_result.is_null())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result type type is invalid", K(tmp_result));
        } else if (tmp_result.is_false()) {
          break;
        } else if (tmp_result.is_null()) {
          cnt_null = true;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (tmp_result.is_true() && cnt_null) {
        tmp_result.set_null();
      }
      result = tmp_result;
    }
  }
  return ret;
}

/**
 * ANY sematics: return true if any value is true.
 * return NULL if all other values are false
 * return false if all values are false
 */
int ObSubQueryRelationalExpr::calc_result_with_any(
    ObObj& result, const ObNewRow& left_row, int64_t subquery_idx, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObNewRowIterator* row_iter = NULL;
  ObNewRow* row = NULL;
  ObObj tmp_result;
  bool cnt_null = false;
  if (OB_ISNULL(expr_ctx.subplan_iters_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery_idx is invalied", K(expr_ctx.subplan_iters_), K(ret));
  } else if (OB_UNLIKELY(subquery_idx < 0 || subquery_idx >= expr_ctx.subplan_iters_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery_idx is invalied", K(subquery_idx), "iter count", expr_ctx.subplan_iters_->count());
  } else if (OB_ISNULL(row_iter = expr_ctx.subplan_iters_->at(subquery_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery result iterator is null");
  } else {
    tmp_result.set_bool(false);
    while (OB_SUCC(ret) && OB_SUCC(row_iter->get_next_row(row))) {
      if (OB_UNLIKELY(left_row.get_count() != row->get_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("left_row and right_row is not equal", K(ret));
      } else {
        if (OB_FAIL(compare_single_row(left_row, *row, expr_ctx, tmp_result))) {
          LOG_WARN("compare single row failed", K(ret));
        } else if (OB_UNLIKELY(!tmp_result.is_int32() && !tmp_result.is_null())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result type type is invalid", K(tmp_result), K(ret));
        } else if (tmp_result.is_true()) {
          break;
        } else if (tmp_result.is_null()) {
          cnt_null = true;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (tmp_result.is_false() && cnt_null) {
        tmp_result.set_null();
      }
      result = tmp_result;
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::compare_obj(common::ObExprCtx& expr_ctx, const ObObj& obj1, const ObObj& obj2,
    const ObExprCalcType& cmp_type, bool is_null_safe, ObObj& result) const
{
  int ret = OB_SUCCESS;
  EXPR_DEFINE_CMP_CTX(cmp_type, is_null_safe, expr_ctx);
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
  if (OB_FAIL(ObRelationalExprOperator::compare_cast(result, obj1, obj2, cmp_ctx, cast_ctx, CO_CMP))) {
    LOG_WARN("Compare expression failed", K(ret));
  }
  return ret;
}

int ObSubQueryRelationalExpr::compare_single_row(
    const ObNewRow& left_row, const ObNewRow& right_row, ObExprCtx& expr_ctx, ObObj& result) const
{
  UNUSED(left_row);
  UNUSED(right_row);
  UNUSED(expr_ctx);
  UNUSED(result);
  return OB_NOT_IMPLEMENT;
}

int ObSubQueryRelationalExpr::get_param_types(
    const ObRawExpr& param, const bool is_iter, ObIArray<ObObjMeta>& types) const
{
  int ret = OB_SUCCESS;
  if (param.get_expr_type() == T_OP_ROW) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.get_param_count(); i++) {
      const ObRawExpr* e = param.get_param_expr(i);
      CK(NULL != e);
      OZ(types.push_back(e->get_result_meta()));
    }
  } else if (param.get_expr_type() == T_REF_QUERY && is_iter) {
    const ObQueryRefRawExpr& ref = static_cast<const ObQueryRefRawExpr&>(param);
    FOREACH_CNT_X(t, ref.get_column_types(), OB_SUCC(ret))
    {
      OZ(types.push_back(*t));
    }
  } else {
    OZ(types.push_back(param.get_result_meta()));
  }
  return ret;
}

int ObSubQueryRelationalExpr::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjMeta, 1> left_types;
  ObSEArray<ObObjMeta, 1> right_types;
  void** funcs = NULL;

  CK(NULL != op_cg_ctx.allocator_);
  CK(2 == raw_expr.get_param_count());
  CK(NULL != raw_expr.get_param_expr(0));
  CK(NULL != raw_expr.get_param_expr(1));

  // row_dimension_ is set for old expr engine in ObExprGeneratorImpl, can not used here
  OZ(get_param_types(*raw_expr.get_param_expr(0), left_is_iter_, left_types));
  OZ(get_param_types(*raw_expr.get_param_expr(1), right_is_iter_, right_types));
  if (OB_FAIL(ret)) {
  } else if (left_types.empty() || left_types.count() != right_types.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operand cnt mismatch", K(ret), K(left_types.count()), K(right_types.count()));
  } else if (OB_ISNULL(funcs = (void**)op_cg_ctx.allocator_->alloc(sizeof(void*) * left_types.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    rt_expr.inner_func_cnt_ = left_types.count();
    rt_expr.inner_functions_ = funcs;

    for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.inner_func_cnt_; i++) {
      auto& l = left_types.at(i);
      auto& r = right_types.at(i);
      if (ObDatumFuncs::is_string_type(l.get_type()) && ObDatumFuncs::is_string_type(r.get_type())) {
        CK(l.get_collation_type() == r.get_collation_type());
      }
      if (OB_SUCC(ret)) {
        funcs[i] = (void*)ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
            l.get_type(), r.get_type(), lib::is_oracle_mode(), l.get_collation_type());
        CK(NULL != funcs[i]);
      }
    }
    if (OB_SUCC(ret)) {
      ExtraInfo& info = ExtraInfo::get_info(rt_expr);
      info.subquery_key_ = subquery_key_;
      info.left_is_iter_ = left_is_iter_;
      info.right_is_iter_ = right_is_iter_;

      rt_expr.eval_func_ = &subquery_cmp_eval;
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::check_exists(const ObExpr& expr, ObEvalCtx& ctx, bool& exists)
{
  int ret = OB_SUCCESS;
  ObDatum* v = NULL;
  ObSubQueryIterator* iter = NULL;
  exists = false;
  if (1 != expr.arg_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument count", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, v))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL subquery ref info returned", K(ret));
  } else if (OB_FAIL(ObExprSubQueryRef::get_subquery_iter(
                 ctx, ObExprSubQueryRef::ExtraInfo::get_info(v->get_int()), iter))) {
    LOG_WARN("get subquery iterator failed", K(ret));
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL subquery iterator", K(ret));
  } else if (OB_FAIL(iter->start())) {
    LOG_WARN("start iterate failed", K(ret));
  } else if (OB_FAIL(iter->get_next_row())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      exists = false;
    } else {
      LOG_WARN("get next row failed", K(ret));
    }
  } else {
    exists = true;
  }
  return ret;
}

int ObSubQueryRelationalExpr::setup_row(ObExpr** expr, ObEvalCtx& ctx, const bool is_iter, const int64_t cmp_func_cnt,
    ObSubQueryIterator*& iter, ObExpr**& row)
{
  int ret = OB_SUCCESS;
  if (is_iter) {
    ObDatum* v = NULL;
    if (OB_FAIL(expr[0]->eval(ctx, v))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else if (v->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL subquery ref info returned", K(ret));
    } else if (OB_FAIL(ObExprSubQueryRef::get_subquery_iter(
                   ctx, ObExprSubQueryRef::ExtraInfo::get_info(v->get_int()), iter))) {
      LOG_WARN("get subquery iterator failed", K(ret));
    } else if (OB_ISNULL(iter) || cmp_func_cnt != iter->get_output().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL subquery iterator", K(ret), KP(iter), K(cmp_func_cnt));
    } else if (OB_FAIL(iter->start())) {
      LOG_WARN("start iterate failed", K(ret));
    } else {
      row = &const_cast<ExprFixedArray&>(iter->get_output()).at(0);
    }
  } else if (T_OP_ROW == expr[0]->type_) {
    if (cmp_func_cnt != expr[0]->arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp function count mismatch", K(ret), K(cmp_func_cnt), K(*expr[0]));
    } else {
      row = expr[0]->args_;
    }
  } else {
    row = expr;
  }
  return ret;
}

int ObSubQueryRelationalExpr::cmp_one_row(
    const ObExpr& expr, ObDatum& res, ObExpr** l_row, ObEvalCtx& l_ctx, ObExpr** r_row, ObEvalCtx& r_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_OP_SQ_NSEQ == expr.type_)) {
    ret = ObExprNullSafeEqual::ns_equal(expr, res, l_row, l_ctx, r_row, r_ctx);
  } else {
    ret = ObRelationalExprOperator::row_cmp(expr, res, l_row, l_ctx, r_row, r_ctx);
  }
  return ret;
}

int ObSubQueryRelationalExpr::subquery_cmp_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  const ExtraInfo& info = ExtraInfo::get_info(expr);
  ObSubQueryIterator* l_iter = NULL;
  ObSubQueryIterator* r_iter = NULL;
  ObExpr** l_row = NULL;
  ObExpr** r_row = NULL;
  if (2 != expr.arg_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument count", K(ret));
  } else if (OB_FAIL(setup_row(expr.args_, ctx, info.left_is_iter_, expr.inner_func_cnt_, l_iter, l_row))) {
    LOG_WARN("setup left row failed", K(ret));
  } else if (OB_FAIL(setup_row(expr.args_ + 1, ctx, info.right_is_iter_, expr.inner_func_cnt_, r_iter, r_row))) {
    LOG_WARN("setup right row failed", K(ret));
  } else if (OB_ISNULL(l_row) || OB_ISNULL(r_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null row", K(ret));
  } else {
    bool l_end = false;
    if (NULL != l_iter) {
      if (OB_FAIL(l_iter->get_next_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          l_end = true;
          // set row to NULL
          for (int64_t i = 0; i < expr.inner_func_cnt_; i++) {
            l_row[i]->locate_expr_datum(ctx).set_null();
            l_row[i]->get_eval_info(ctx).evaluated_ = true;
          }
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      switch (info.subquery_key_) {
        case T_WITH_NONE: {
          ret = subquery_cmp_eval_with_none(expr, ctx, expr_datum, l_row, r_row, r_iter);
          break;
        }
        case T_WITH_ANY: {
          ret = subquery_cmp_eval_with_any(expr, ctx, expr_datum, l_row, r_row, r_iter);
          break;
        }
        case T_WITH_ALL: {
          ret = subquery_cmp_eval_with_all(expr, ctx, expr_datum, l_row, r_row, r_iter);
          break;
        }
      }
    }
    if (OB_SUCC(ret) && NULL != l_iter && !l_end) {
      if (OB_FAIL(l_iter->get_next_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else {
        // only one row expected for left row
        ret = OB_SUBQUERY_TOO_MANY_ROW;
      }
    }
  }
  return ret;
}

int ObSubQueryRelationalExpr::subquery_cmp_eval_with_none(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res, ObExpr** l_row, ObExpr** r_row, ObSubQueryIterator* r_iter)
{
  int ret = OB_SUCCESS;
  // %l_row, %r_row is checked no need to check.
  // %iter may be NULL for with none.
  bool iter_end = false;
  if (NULL != r_iter) {
    if (OB_FAIL(r_iter->get_next_row())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        iter_end = true;
        // set row to NULL
        for (int64_t i = 0; i < expr.inner_func_cnt_; i++) {
          r_row[i]->locate_expr_datum(ctx).set_null();
          r_row[i]->get_eval_info(ctx).evaluated_ = true;
        }
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cmp_one_row(expr, res, l_row, ctx, r_row, ctx))) {
      LOG_WARN("compare one row failed", K(ret));
    }
  }
  if (NULL != r_iter && !iter_end && OB_SUCC(ret)) {
    if (OB_FAIL(r_iter->get_next_row())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    } else {
      // only one row expected for left row
      ret = OB_SUBQUERY_TOO_MANY_ROW;
    }
  }
  return ret;
}

// copy from ObSubQueryRelationalExpr::calc_result_with_any
int ObSubQueryRelationalExpr::subquery_cmp_eval_with_any(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res, ObExpr** l_row, ObExpr** r_row, ObSubQueryIterator* r_iter)
{
  // %l_row, %r_row is checked no need to check.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(r_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter should not be null", K(ret));
  } else {
    bool cnt_null = false;
    res.set_false();
    while (OB_SUCC(ret) && OB_SUCC(r_iter->get_next_row())) {
      // use subquery's eval ctx for right row to avoid ObEvalCtx::alloc_ expanding.
      if (OB_FAIL(cmp_one_row(expr, res, l_row, ctx, r_row, r_iter->get_op().get_eval_ctx()))) {
        LOG_WARN("compare single row failed", K(ret));
      } else if (res.is_true()) {
        break;
      } else if (res.is_null()) {
        cnt_null = true;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (res.is_false() && cnt_null) {
        res.set_null();
      }
    }
  }
  return ret;
}

// copy from ObSubQueryRelationalExpr::calc_result_with_all
int ObSubQueryRelationalExpr::subquery_cmp_eval_with_all(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res, ObExpr** l_row, ObExpr** r_row, ObSubQueryIterator* r_iter)
{
  // %l_row, %r_row is checked no need to check.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(r_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter should not be null", K(ret));
  } else {
    bool cnt_null = false;
    res.set_true();
    while (OB_SUCC(ret) && OB_SUCC(r_iter->get_next_row())) {
      // use subquery's eval ctx for right row to avoid ObEvalCtx::alloc_ expanding.
      if (OB_FAIL(cmp_one_row(expr, res, l_row, ctx, r_row, r_iter->get_op().get_eval_ctx()))) {
        LOG_WARN("compare single row failed", K(ret));
      } else if (res.is_false()) {
        break;
      } else if (res.is_null()) {
        cnt_null = true;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (res.is_true() && cnt_null) {
        res.set_null();
      }
    }
  }
  return ret;
}

int ObLogicalExprOperator::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  UNUSED(type1);
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_tinyint();
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObSubQueryRelationalExpr::calc_result_type2_(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType cmp_type;
  if (OB_SUCC(calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()))) {
    type.set_tinyint();
    type.set_calc_collation(cmp_type);
    type.set_calc_type(cmp_type.get_calc_type());
    ObExprOperator::calc_result_flag2(type, type1, type2);
  }
  return ret;
}

int ObLogicalExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type_ctx);
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_tinyint();
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObLogicalExprOperator::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type3);
  UNUSED(type_ctx);
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_tinyint();
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObLogicalExprOperator::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_tinyint();
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObLogicalExprOperator::is_true(const ObObj& obj, ObCastMode cast_mode, bool& result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObObjEvaluator::is_true(obj, cast_mode, result))) {
    // ugly...really ugly in order to be compatible with mysql.
    if (OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret) {
      // slow path. need twice calls for is_true_. not efficient but we will not reach here too frequently
      ret = ObObjEvaluator::is_true(obj, CM_WARN_ON_FAIL | CM_NO_RANGE_CHECK, result);
    }
  }
  return ret;
}

int ObVectorExprOperator::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type_ctx);
  LOG_WARN("operator in should not come here", K(ret));
  return ret;
}

int ObVectorExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  ObArenaAllocator alloc;
  ObExprResType types[2] = {alloc, alloc};
  types[0] = type1;
  types[1] = type2;
  return calc_result_typeN(type, types, 2, type_ctx);
}

int ObVectorExprOperator::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, ObExprTypeCtx& type_ctx) const
{
  ObArenaAllocator alloc;
  ObExprResType types[3] = {alloc, alloc, alloc};
  types[0] = type1;
  types[1] = type2;
  types[2] = type3;
  return calc_result_typeN(type, types, 3, type_ctx);
}

int ObVectorExprOperator::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num <= 0 || 0 >= row_dimension_) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("types is null or invalid param num or row_dimension_",
        K(types),
        K(param_num),
        K(row_dimension_),
        K(type_ctx.get_session()),
        K(ret));
  } else {
    int64_t left_start_idx = 0;
    int64_t right_start_idx = row_dimension_;
    int64_t right_element_count = param_num / row_dimension_ - 1;
    ObExprResType tmp_res_type;
    if (OB_FAIL(type.init_row_dimension(param_num))) {
      LOG_WARN("fail to init row dimension", K(ret));
    }
    bool is_nested_table_elem = true;
    // in maybe nest table, (nt1 in (nt2, nt3, nt4))
    for (int64_t k = 0; lib::is_oracle_mode() && k < param_num; ++k) {
      is_nested_table_elem &= types[k].is_ext();
    }
    if (lib::is_oracle_mode() && is_nested_table_elem) {
      if (row_dimension_ != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in condition of nest table can only have one left operator", K(ret), K(row_dimension_), K(param_num));
      } else {
        // extend type is unknown yet, get at runtime
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < right_element_count; ++i, right_start_idx += row_dimension_) {
        tmp_res_type.reset();
        for (int64_t j = 0; OB_SUCC(ret) && j < row_dimension_; ++j) {
          if (OB_FAIL(ObVectorExprOperator::calc_result_type2_(
                  tmp_res_type, types[left_start_idx + j], types[right_start_idx + j], type_ctx))) {
            LOG_WARN("failed to calc result types", K(ret));
          } else if (OB_FAIL(type.get_row_calc_cmp_types().push_back(tmp_res_type.get_calc_meta()))) {
            LOG_WARN("failed to push back cmp type", K(ret));
          }
        }
      }
    }
    // overall result type
    if (OB_SUCC(ret)) {
      type.set_int32();
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[type.get_type()].precision_);
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[type.get_type()].scale_);
      ObExprOperator::calc_result_flagN(type, types, param_num);
    }
  }
  return ret;
}

int ObVectorExprOperator::calc_result_type2_(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType cmp_type;
  if (OB_SUCC(calc_cmp_type2(cmp_type, type1, type2, type_ctx.get_coll_type()))) {
    type.set_int();  // not tinyint, compatiable with MySQL
    type.set_calc_collation(cmp_type);
    type.set_calc_type(cmp_type.get_calc_type());
    ObExprOperator::calc_result_flag2(type, type1, type2);

    if (GCONF.enable_static_engine_for_query()) {
      obj_cmp_func func_ptr = NULL;
      bool need_no_cast = ObRelationalExprOperator::can_cmp_without_cast(type1, type2, CO_EQ, func_ptr);
      type1.set_calc_type(need_no_cast ? type1.get_type() : cmp_type.get_calc_type());
      type2.set_calc_type(need_no_cast ? type2.get_type() : cmp_type.get_calc_type());
      if (ob_is_string_type(cmp_type.get_calc_type())) {
        if (OB_ISNULL(type_ctx.get_session())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null session", K(type_ctx.get_session()));
        } else if (share::is_oracle_mode()) {
          // oracle charset deduce
          ObExprResType tmp_res_type;
          ObSEArray<ObExprResType*, 2> tmp_param_array;
          if (OB_FAIL(tmp_param_array.push_back(&type1)) || OB_FAIL(tmp_param_array.push_back(&type2))) {
            LOG_WARN("failed to push back element", K(ret));
          } else if (OB_FAIL(aggregate_string_type_and_charset_oracle(
                         *type_ctx.get_session(), tmp_param_array, tmp_res_type))) {
            LOG_WARN("unexpected failed", K(ret));
          } else if (OB_FAIL(deduce_string_param_calc_type_and_charset(
                         *type_ctx.get_session(), tmp_res_type, tmp_param_array))) {
            LOG_WARN("unexpected failed", K(ret));
          }
        } else {
          type1.set_calc_collation_type(cmp_type.get_calc_collation_type());
          type2.set_calc_collation_type(cmp_type.get_calc_collation_type());
        }
      } else if (ObRawType == cmp_type.get_calc_type()) {
        type1.set_calc_collation_type(CS_TYPE_BINARY);
        type2.set_calc_collation_type(CS_TYPE_BINARY);
      }
    }
  }
  return ret;
}

int ObStringExprOperator::convert_result_collation(
    const ObExprResType& result_type, ObObj& in_obj, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  ObCollationType in_collation = in_obj.get_collation_type();
  ObString out_str;
  if (OB_UNLIKELY(!in_obj.is_string_or_lob_locator_type() || !result_type.is_string_or_lob_locator_type() ||
                  !ObCharset::is_valid_collation(in_collation) ||
                  !ObCharset::is_valid_collation(result_type.get_collation_type()) || OB_ISNULL(allocator))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_obj), K(result_type));
  } else if (OB_FAIL(ObExprUtil::convert_string_collation(
                 in_obj.get_string(), in_collation, out_str, result_type.get_collation_type(), *allocator))) {
    LOG_WARN("convert_string_collation failed", K(ret), K(in_obj), K(result_type));
  } else {
    in_obj.set_string(result_type.get_type(), out_str);
    in_obj.set_collation(result_type);
  }
  return ret;
}

ObObjType ObStringExprOperator::get_result_type_mysql(int64_t char_length) const
{
  /*
    drop table t1;
    drop table t2;
    create table t1 (
      col_varchar    varchar(1),
      col_varbinary  varbinary(1),
      col_char       char(1),
      col_binary     binary(1),
      col_tinytext   tinytext,
      col_tinyblob   tinyblob,
      col_mediumtext mediumtext,
      col_mediumblob mediumblob,
      col_text       text,
      col_blob       blob,
      col_count      bigint);
    create table t2 as
    select repeat('啊',   512),
           repeat('a',    513),
           repeat('啊', 21845),
           repeat('a',  21846),
           repeat(col_varchar, '512'),
           repeat(col_varchar, '513'),
           repeat(col_varchar, 65535),
           repeat(col_varchar, 65536),
           repeat(col_varbinary, '512'),
           repeat(col_varbinary, '513'),
           repeat(col_varbinary, 65535),
           repeat(col_varbinary, 65536),
           repeat(col_char, '512'),
           repeat(col_char, '513'),
           repeat(col_char, 65535),
           repeat(col_char, 65536),
           repeat(col_binary, '512'),
           repeat(col_binary, '513'),
           repeat(col_binary, 65535),
           repeat(col_binary, 65536),
           repeat(col_tinytext,   2),
           repeat(col_tinytext,   3),
           repeat(col_tinytext, 257),
           repeat(col_tinytext, 258),
           repeat(col_tinyblob,   2),
           repeat(col_tinyblob,   3),
           repeat(col_tinyblob, 257),
           repeat(col_tinyblob, 258),
           repeat(col_mediumtext, 1),
           repeat(col_mediumblob, 1),
           repeat(col_text, 1),
           repeat(col_text, 2),
           repeat(col_blob, 1),
           repeat(col_blob, 2),
           repeat('啊', col_count),
           repeat('a', col_count),
           repeat(col_varchar, col_count),
           repeat(col_varbinary, col_count),
           repeat(col_char, col_count),
           repeat(col_binary, col_count),
           repeat(col_tinytext, col_count),
           repeat(col_tinyblob, col_count),
           repeat(col_mediumtext, col_count),
           repeat(col_mediumblob, col_count),
           repeat(col_text, col_count),
           repeat(col_blob, col_count)
    from t1;
    desc t2;

    MySQL> desc t2;
    +-----------------------------------+----------------+------+-----+---------+-------+
    | Field                             | Type           | Null | Key | Default | Extra |
    +-----------------------------------+----------------+------+-----+---------+-------+
    | repeat('啊',   512)               | varchar(512)   | YES  |     | NULL    |       |
    | repeat('a',    513)               | text           | YES  |     | NULL    |       |
    | repeat('啊', 21845)               | text           | YES  |     | NULL    |       |
    | repeat('a',  21846)               | longtext       | YES  |     | NULL    |       |
    | repeat(col_varchar, '512')        | varchar(512)   | YES  |     | NULL    |       |
    | repeat(col_varchar, '513')        | text           | YES  |     | NULL    |       |
    | repeat(col_varchar, 65535)        | text           | YES  |     | NULL    |       |
    | repeat(col_varchar, 65536)        | longtext       | YES  |     | NULL    |       |
    | repeat(col_varbinary, '512')      | varbinary(512) | YES  |     | NULL    |       |
    | repeat(col_varbinary, '513')      | blob           | YES  |     | NULL    |       |
    | repeat(col_varbinary, 65535)      | blob           | YES  |     | NULL    |       |
    | repeat(col_varbinary, 65536)      | longblob       | YES  |     | NULL    |       |
    | repeat(col_char, '512')           | varchar(512)   | YES  |     | NULL    |       |
    | repeat(col_char, '513')           | text           | YES  |     | NULL    |       |
    | repeat(col_char, 65535)           | text           | YES  |     | NULL    |       |
    | repeat(col_char, 65536)           | longtext       | YES  |     | NULL    |       |
    | repeat(col_binary, '512')         | varbinary(512) | YES  |     | NULL    |       |
    | repeat(col_binary, '513')         | blob           | YES  |     | NULL    |       |
    | repeat(col_binary, 65535)         | blob           | YES  |     | NULL    |       |
    | repeat(col_binary, 65536)         | longblob       | YES  |     | NULL    |       |
    | repeat(col_tinytext,   2)         | varchar(510)   | YES  |     | NULL    |       |
    | repeat(col_tinytext,   3)         | text           | YES  |     | NULL    |       |
    | repeat(col_tinytext, 257)         | text           | YES  |     | NULL    |       |
    | repeat(col_tinytext, 258)         | longtext       | YES  |     | NULL    |       |
    | repeat(col_tinyblob,   2)         | varbinary(510) | YES  |     | NULL    |       |
    | repeat(col_tinyblob,   3)         | blob           | YES  |     | NULL    |       |
    | repeat(col_tinyblob, 257)         | blob           | YES  |     | NULL    |       |
    | repeat(col_tinyblob, 258)         | longblob       | YES  |     | NULL    |       |
    | repeat(col_mediumtext, 1)         | longtext       | YES  |     | NULL    |       |
    | repeat(col_mediumblob, 1)         | longblob       | YES  |     | NULL    |       |
    | repeat(col_text, 1)               | text           | YES  |     | NULL    |       |
    | repeat(col_text, 2)               | longtext       | YES  |     | NULL    |       |
    | repeat(col_blob, 1)               | blob           | YES  |     | NULL    |       |
    | repeat(col_blob, 2)               | longblob       | YES  |     | NULL    |       |
    | repeat('啊', col_count)           | longtext       | YES  |     | NULL    |       |
    | repeat('a', col_count)            | longtext       | YES  |     | NULL    |       |
    | repeat(col_varchar, col_count)    | longtext       | YES  |     | NULL    |       |
    | repeat(col_varbinary, col_count)  | longblob       | YES  |     | NULL    |       |
    | repeat(col_char, col_count)       | longtext       | YES  |     | NULL    |       |
    | repeat(col_binary, col_count)     | longblob       | YES  |     | NULL    |       |
    | repeat(col_tinytext, col_count)   | longtext       | YES  |     | NULL    |       |
    | repeat(col_tinyblob, col_count)   | longblob       | YES  |     | NULL    |       |
    | repeat(col_mediumtext, col_count) | longtext       | YES  |     | NULL    |       |
    | repeat(col_mediumblob, col_count) | longblob       | YES  |     | NULL    |       |
    | repeat(col_text, col_count)       | longtext       | YES  |     | NULL    |       |
    | repeat(col_blob, col_count)       | longblob       | YES  |     | NULL    |       |
    +-----------------------------------+----------------+------+-----+---------+-------+
    46 rows in set (0.00 sec)

   * summarize the rules that mysql decide the result type of some string functions,
   * which mainly depend on the MAX CHAR LENGTH of result:
   * 1. less than or equal to 512: varchar, otherwise
   * 2. less than or equal to 65535: text or blob, otherwise
   * 3. longtext or longblob (including UNKNOWN length when count param is not const).
   * do not care about the input data type, or whether the input is column or const.
   *
   * ATTENTION! we will ignore these exceptions:
   * 1. repeat('啊', 21845) => text, repeat('a',  21846) => longtext. we can not
   *    understand the magic numbers 21845 and 21846.
   * 2. repeat(col_mediumtext, 1) => longtext, repeat(col_mediumblob, 1) => longblob.
   *    text or blob is enough, see:
   *    repeat(col_text, 1) => text, repeat(col_blob, 1) => blob.
   */
  ObObjType res_type = ObMaxType;
  if (char_length <= MAX_CHAR_LENGTH_FOR_VARCAHR_RESULT) {
    res_type = ObVarcharType;
  } else if (char_length <= MAX_CHAR_LENGTH_FOR_TEXT_RESULT) {
    res_type = ObTextType;
  } else {
    res_type = ObLongTextType;
  }
  return res_type;
}

// for static_typing_engine
int ObBitwiseExprOperator::set_calc_type(ObExprResType& type)
{
  if (lib::is_oracle_mode()) {
    type.set_calc_type(ObNumberType);
    type.set_precision(DEFAULT_NUMBER_PRECISION_FOR_INTEGER);
    type.set_scale(DEFAULT_NUMBER_SCALE_FOR_INTEGER);
  } else {
    if (ObNumberType == type.get_type()) {
      type.set_calc_type(ObNumberType);
      type.set_precision(DEFAULT_NUMBER_PRECISION_FOR_INTEGER);
      type.set_scale(DEFAULT_NUMBER_SCALE_FOR_INTEGER);
    } else {
      type.set_calc_type(ObUInt64Type);
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    }
  }
  return OB_SUCCESS;
}

int ObBitwiseExprOperator::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    if (is_oracle_mode()) {
      type.set_number();
      type.set_precision(DEFAULT_NUMBER_PRECISION_FOR_INTEGER);
      type.set_scale(DEFAULT_NUMBER_SCALE_FOR_INTEGER);
    } else {
      type.set_uint64();
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    }
    ObExprOperator::calc_result_flag1(type, type1);
    const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session failed", K(ret));
    } else if (session->use_static_typing_engine()) {
      if (OB_FAIL(set_calc_type(type1))) {
        LOG_WARN("set_calc_type for type1 failed", K(ret), K(type1));
      } else {
        ObCastMode cm = lib::is_oracle_mode() ? CM_NONE : CM_STRING_INTEGER_TRUNC;
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK | cm);
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObBitwiseExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    if (is_oracle_mode()) {
      type.set_number();
      type.set_precision(DEFAULT_NUMBER_PRECISION_FOR_INTEGER);
      type.set_scale(DEFAULT_NUMBER_SCALE_FOR_INTEGER);
    } else {
      type.set_uint64();
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    }
    ObExprOperator::calc_result_flag2(type, type1, type2);
    const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session failed", K(ret));
    } else if (session->use_static_typing_engine()) {
      if (OB_FAIL(set_calc_type(type1))) {
        LOG_WARN("set_calc_type for type1 failed", K(ret), K(type1));
      } else if (OB_FAIL(set_calc_type(type2))) {
        LOG_WARN("set_calc_type for type2 failed", K(ret), K(type2));
      } else {
        ObCastMode cm = lib::is_oracle_mode() ? CM_NONE : CM_STRING_INTEGER_TRUNC;
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK | cm);
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  LOG_DEBUG("bitwise calc type2 done", K(ret), K(type), K(type1), K(type2));
  return ret;
}

int ObBitwiseExprOperator::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    if (is_oracle_mode()) {
      type.set_number();
      type.set_precision(DEFAULT_NUMBER_PRECISION_FOR_INTEGER);
      type.set_scale(DEFAULT_NUMBER_SCALE_FOR_INTEGER);
    } else {
      type.set_uint64();
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    }
    ObExprOperator::calc_result_flag3(type, type1, type2, type3);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObBitwiseExprOperator::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    if (is_oracle_mode()) {
      type.set_number();
      type.set_precision(DEFAULT_NUMBER_PRECISION_FOR_INTEGER);
      type.set_scale(DEFAULT_NUMBER_SCALE_FOR_INTEGER);
    } else {
      type.set_uint64();
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    }
    ObExprOperator::calc_result_flagN(type, types, param_num);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObBitwiseExprOperator::calc_(
    ObObj& res, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx, BitOperator op) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expr_ctx.calc_buf_ is null", K(expr_ctx.calc_buf_), K(ret));
  } else if (OB_UNLIKELY(op < 0 || op >= BIT_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(obj1), K(obj2), K(op));
  } else if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
    res.set_null();
  } else {
    if (share::is_mysql_mode()) {
      uint64_t uint64_v1 = 0;
      uint64_t uint64_v2 = 0;
      if (OB_FAIL(get_uint64(obj1, expr_ctx, true, uint64_v1))) {
        LOG_WARN("fail to get uint64", K(obj1), K(ret));
      } else if (OB_FAIL(get_uint64(obj2, expr_ctx, true, uint64_v2))) {
        LOG_WARN("fail to get uint64", K(obj2), K(ret));
      } else {
        // Do not worry too much about the efficiency
        // although we calc 5 results while only one is used here.
        // Bit operations take little time
        uint64_t bit_op_res[BIT_MAX] = {uint64_v1 & uint64_v2,
            uint64_v1 | uint64_v2,
            uint64_v1 ^ uint64_v2,
            uint64_v2 < sizeof(uint64_t) * 8 ? uint64_v1 << uint64_v2 : 0,
            uint64_v2 < sizeof(uint64_t) * 8 ? uint64_v1 >> uint64_v2 : 0};
        res.set_uint64(bit_op_res[op]);
      }
    } else {
      if (op != BIT_AND) {
        ret = OB_NOT_SUPPORTED;  // oracle mode support bitand op only
        LOG_WARN("not support bit op in oracle mode", K(ret), K(op));
      } else {
        int64_t int64_v1 = 0;
        int64_t int64_v2 = 0;
        if (OB_FAIL(get_int64(obj1, expr_ctx, false, int64_v1))) {
          LOG_WARN("failed to get int64", K(obj1), K(ret));
        } else if (OB_FAIL(get_int64(obj2, expr_ctx, false, int64_v2))) {
          LOG_WARN("failed to get int64", K(obj2), K(ret));
        } else {
          ObNumber result;
          result.from((int64_v1 & int64_v2), *expr_ctx.calc_buf_);
          res.set_number(result);
        }
      }
    }
  }
  return ret;
}

int ObBitwiseExprOperator::calc_result2_mysql(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)

{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  const BitOperator op = static_cast<const BitOperator>(expr.extra_);
  ObCastMode cast_mode = CM_NONE;
  if (OB_UNLIKELY(op < 0 || op >= BIT_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (left->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, right))) {
    LOG_WARN("eval arg 1 failed", K(ret));
  } else if (right->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, ctx.exec_ctx_.get_my_session(), cast_mode))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else {
    uint64_t left_uint = 0;
    uint64_t right_uint = 0;
    void* get_uint_func0 = NULL;
    void* get_uint_func1 = NULL;
    const ObObjType& left_type = expr.args_[0]->datum_meta_.type_;
    const ObObjType& right_type = expr.args_[1]->datum_meta_.type_;
    if (OB_FAIL(choose_get_int_func(left_type, get_uint_func0))) {
      LOG_WARN("choose_get_int_func failed", K(ret), K(left_type));
    } else if (OB_FAIL((reinterpret_cast<GetUIntFunc>(get_uint_func0)(*left, true, left_uint, cast_mode)))) {
      LOG_WARN("get uint64 failed", K(ret), K(*left));
    } else if (OB_FAIL(choose_get_int_func(right_type, get_uint_func1))) {
      LOG_WARN("choose_get_int_func failed", K(ret), K(right_type));
    } else if (OB_FAIL((reinterpret_cast<GetUIntFunc>(get_uint_func1)(*right, true, right_uint, cast_mode)))) {
      LOG_WARN("get uint64 failed", K(ret), K(*right));
    } else {
      // Do not worry too much about the efficiency
      // although we calc 5 results while only one is used here.
      // Bit operations take little time
      uint64_t bit_op_res[BIT_MAX] = {left_uint & right_uint,
          left_uint | right_uint,
          left_uint ^ right_uint,
          right_uint < sizeof(uint64_t) * 8 ? left_uint << right_uint : 0,
          right_uint < sizeof(uint64_t) * 8 ? left_uint >> right_uint : 0};
      res_datum.set_uint(bit_op_res[op]);
    }
  }
  return ret;
}

int ObBitwiseExprOperator::calc_result2_oracle(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)

{
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;
  const BitOperator op = static_cast<const BitOperator>(expr.extra_);
  ObCastMode cast_mode = CM_NONE;
  if (OB_UNLIKELY(BIT_AND != op)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported bit operator", K(ret), K(op), K(BIT_AND));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (left->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, right))) {
    LOG_WARN("eval arg 1 failed", K(ret));
  } else if (right->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, ctx.exec_ctx_.get_my_session(), cast_mode))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else {
    int64_t left_int = 0;
    int64_t right_int = 0;
    ObNumStackOnceAlloc tmp_alloc;
    ObNumber result;
    void* get_int_func0 = NULL;
    void* get_int_func1 = NULL;
    const ObObjType& left_type = expr.args_[0]->datum_meta_.type_;
    const ObObjType& right_type = expr.args_[1]->datum_meta_.type_;
    if (OB_FAIL(choose_get_int_func(left_type, get_int_func0))) {
      LOG_WARN("choose_get_int_func failed", K(ret), K(left_type));
    } else if (OB_FAIL((reinterpret_cast<GetIntFunc>(get_int_func0)(*left, false, left_int, cast_mode)))) {
      LOG_WARN("get uint64 failed", K(ret), K(*left));
    } else if (OB_FAIL(choose_get_int_func(right_type, get_int_func1))) {
      LOG_WARN("choose_get_int_func failed", K(ret), K(right_type));
    } else if (OB_FAIL((reinterpret_cast<GetIntFunc>(get_int_func1)(*right, false, right_int, cast_mode)))) {
      LOG_WARN("get uint64 failed", K(ret), K(*right));
    } else if (OB_FAIL(result.from((left_int & right_int), tmp_alloc))) {
      LOG_WARN("get ObNumber from int64 failed", K(ret), K(left_int & right_int));
    } else {
      res_datum.set_number(result);
    }
  }
  return ret;
}

// for static typing engine
int ObBitwiseExprOperator::cg_bitwise_expr(
    ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr, const BitOperator op)
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  // bit count, bit neg: 1 == arg_cnt_
  // bit and/or/xor/left shift/right shift: 2 == arg_cnt_
  if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(expr_cg_ctx.allocator_) ||
      OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args_ is NULL or arg_cnt_ is invalid", K(ret), K(rt_expr));
  } else {
    rt_expr.extra_ = static_cast<uint64_t>(op);
    if (2 == rt_expr.arg_cnt_) {
      if (lib::is_oracle_mode()) {
        rt_expr.eval_func_ = ObBitwiseExprOperator::calc_result2_oracle;
      } else {
        rt_expr.eval_func_ = ObBitwiseExprOperator::calc_result2_mysql;
      }
    } else {
      // must be set in its cg_expr method
      rt_expr.eval_func_ = NULL;
    }
  }
  return ret;
}

int ObBitwiseExprOperator::choose_get_int_func(const ObObjType type, void*& out_func)
{
  int ret = OB_SUCCESS;
  if (ObNumberTC == ob_obj_type_class(type)) {
    if (lib::is_oracle_mode()) {
      out_func = reinterpret_cast<void*>(ObBitwiseExprOperator::get_int64_from_number_type);
    } else {
      out_func = reinterpret_cast<void*>(ObBitwiseExprOperator::get_uint64_from_number_type);
    }
  } else {
    if (lib::is_oracle_mode()) {
      out_func = reinterpret_cast<void*>(ObBitwiseExprOperator::get_int64_from_int_tc);
    } else {
      out_func = reinterpret_cast<void*>(ObBitwiseExprOperator::get_uint64_from_int_tc);
    }
  }
  return ret;
}

int ObBitwiseExprOperator::get_int64_from_int_tc(
    const ObDatum& datum, bool is_round, int64_t& out, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(is_round);
  UNUSED(cast_mode);
  out = datum.get_int();
  return ret;
}

int ObBitwiseExprOperator::get_uint64_from_int_tc(
    const ObDatum& datum, bool is_round, uint64_t& out, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(is_round);
  UNUSED(cast_mode);
  out = datum.get_uint();
  return ret;
}

int ObBitwiseExprOperator::get_int64_from_number_type(
    const ObDatum& datum, bool is_round, int64_t& out, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t tmp_int = 0;
  number::ObNumber nmb(datum.get_number());
  if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(is_round ? nmb.round(0) : nmb.trunc(0)))) {
    LOG_WARN("round/trunc failed", K(ret), K(is_round), K(nmb));
  } else if (nmb.is_valid_int64(tmp_int)) {
    out = tmp_int;
  } else {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE;
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
      out = 0;
    }
  }
  return ret;
}

int ObBitwiseExprOperator::get_uint64_from_number_type(
    const ObDatum& datum, bool is_round, uint64_t& out, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  number::ObNumber nmb(datum.get_number());
  int64_t tmp_int = 0;
  uint64_t tmp_uint = 0;
  if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(is_round ? nmb.round(0) : nmb.trunc(0)))) {
    LOG_WARN("round/trunc failed", K(ret), K(is_round), K(nmb));
  } else if (nmb.is_valid_int64(tmp_int)) {
    out = static_cast<uint64_t>(tmp_int);
  } else if (nmb.is_valid_uint64(tmp_uint)) {
    out = tmp_uint;
  } else {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE;
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
      out = 0;
    }
  }
  return ret;
}

int ObBitwiseExprOperator::get_int64(const ObObj& obj, ObExprCtx& expr_ctx, bool is_round, int64_t& out)
{
  int ret = OB_SUCCESS;
  out = 0;
  ObObjType type = obj.get_type();
  if (OB_LIKELY(ob_is_int_tc(type))) {
    out = obj.get_int();
  } else if (OB_UNLIKELY(obj.is_number())) {
    int64_t tmp_int = 0;
    number::ObNumber nmb;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(nmb.from(obj.get_number(), cast_ctx))) {
      LOG_WARN("deep copy failed", K(ret), K(is_round), K(obj));
    } else if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(is_round ? nmb.round(0) : nmb.trunc(0)))) {
      LOG_WARN("round/trunc failed", K(ret), K(is_round), K(nmb));
    } else if (nmb.is_valid_int64(tmp_int)) {
      out = tmp_int;
    } else {
      ret = OB_ERR_TRUNCATED_WRONG_VALUE;
      if (CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
        ret = OB_SUCCESS;
        out = 0;
      }
    }
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_INT64_V2(obj, out);
  }
  return ret;
}

int ObBitwiseExprOperator::get_uint64(const ObObj& obj, ObExprCtx& expr_ctx, bool is_round, uint64_t& out)
{
  int ret = OB_SUCCESS;
  out = 0;
  ObObjType type = obj.get_type();
  if (OB_LIKELY(ob_is_int_tc(type))) {
    out = static_cast<uint64_t>(obj.get_int());
  } else if (ob_is_uint_tc(type)) {
    out = obj.get_uint64();
  } else if (OB_UNLIKELY(obj.is_number())) {
    uint64_t tmp_uint = 0;
    number::ObNumber value = obj.get_number();
    if (OB_LIKELY(value.is_valid_uint64(tmp_uint))) {
      out = tmp_uint;
    } else {
      int64_t tmp_int = 0;
      number::ObNumber nmb;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      if (OB_FAIL(nmb.from(value, cast_ctx))) {
        LOG_WARN("deep copy failed", K(ret), K(is_round), K(obj));
      } else if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(is_round ? nmb.round(0) : nmb.trunc(0)))) {
        LOG_WARN("round/trunc failed", K(ret), K(is_round), K(nmb));
      } else if (nmb.is_valid_int64(tmp_int)) {
        out = static_cast<uint64_t>(tmp_int);
      } else if (nmb.is_valid_uint64(tmp_uint)) {
        out = tmp_uint;
      } else {
        ret = OB_ERR_TRUNCATED_WRONG_VALUE;
        if (CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
          ret = OB_SUCCESS;
          out = 0;
        }
      }
    }
  } else {
    // need add CM_NO_RANGE_CHECK, otherwise 1 & -3.5(float) return 0.
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_RANGE_CHECK);
    EXPR_GET_UINT64_V2(obj, out);
  }
  return ret;
}

int ObMinMaxExprOperator::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObMinMaxExprOperator* tmp_other = dynamic_cast<const ObMinMaxExprOperator*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->need_cast_ = tmp_other->need_cast_;
    }
  }
  return ret;
}
int ObMinMaxExprOperator::aggregate_result_type_for_comparison(
    ObExprResType& type, const ObExprResType* types, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObObjType res_type = types[0].get_type();
    ObScale max_scale = types[0].get_scale();
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_FAIL(ObExprResultTypeUtil::get_relational_result_type(res_type, res_type, types[i].get_type()))) {
        // warn
      } else if (OB_UNLIKELY(ObMaxType == res_type)) {
        ret = OB_INVALID_ARGUMENT;  // not compatible input
      } else if (!ob_is_string_or_lob_type(types[i].get_type()) && types[i].get_scale() > max_scale) {
        max_scale = types[i].get_scale();
      }
    }
    if (OB_SUCC(ret)) {
      type.set_type(res_type);
      type.set_scale(max_scale);
    }
  }
  return ret;
}

int ObMinMaxExprOperator::aggregate_cmp_type_for_comparison(
    ObExprResType& type, const ObExprResType* types, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    ObObjType cmp_type = types[0].get_type();
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, cmp_type, types[i].get_type()))) {
        // warn
      } else if (OB_UNLIKELY(ObMaxType == cmp_type)) {
        ret = OB_INVALID_ARGUMENT;  // not compatible input
      }
    }
    if (OB_SUCC(ret)) {
      type.set_calc_type(cmp_type);
    }
  }

  return ret;
}

int ObMinMaxExprOperator::calc_result_meta_for_comparison(ObExprResType& type, ObExprResType* types_stack,
    int64_t param_num, const ObCollationType coll_type, const ObLengthSemantics default_length_semantics) const
{
  UNUSED(default_length_semantics);
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t byte_semantics_str_num = 0;
  int64_t char_semantics_str_num = 0;
  // bool all_string = true;
  if (OB_ISNULL(types_stack) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stack is null or param_num is wrong", K(types_stack), K(param_num), K(ret));
  }

  // result_type
  if (OB_SUCC(ret)) {
    ret = aggregate_result_type_for_comparison(type, types_stack, param_num);
  }

  if (OB_SUCC(ret)) {
    ret = aggregate_cmp_type_for_comparison(type, types_stack, param_num);
  }

  bool string_result = ob_is_string_or_enumset_type(type.get_type()) || ob_is_text_tc(type.get_type());
  bool string_cmp = ob_is_string_or_enumset_type(type.get_calc_type()) || ob_is_text_tc(type.get_calc_type());
  // compare collation
  if (OB_SUCC(ret) && string_cmp) {
    ret = aggregate_charsets_for_comparison(type, types_stack, param_num, coll_type);
  }

  // result collation
  if (OB_SUCC(ret) && string_result) {
    ret = aggregate_charsets_for_string_result(type, types_stack, param_num, coll_type);
  }

  if (OB_SUCC(ret)) {
    if (string_result) {
      int64_t max_length = 0;
      for (i = 0; i < param_num; ++i) {
        max_length = MAX(max_length, types_stack[i].get_length());
      }
      type.set_length(static_cast<ObLength>(max_length));
    } else {
      int64_t max_scale = 0;
      int64_t max_precision = 0;
      for (i = 0; i < param_num; ++i) {
        max_scale = MAX(max_scale, types_stack[i].get_mysql_compatible_scale());
        max_precision = MAX(max_precision, types_stack[i].get_precision());
      }
      ObScale result_scale = static_cast<ObScale>(NOT_FIXED_DEC == max_scale ? -1 : max_scale);
      const int64_t int32_max_precision = 11;
      if (result_scale > 0 && ob_is_int_tc(type.get_type())) {
        type.set_type(ObNumberType);
      } else if (max_precision > int32_max_precision && ObInt32Type == type.get_type()) {
        type.set_type(ObIntType);
      }
      type.set_scale(result_scale);
      type.set_precision(static_cast<ObPrecision>(max_precision + max_scale));  // esti, not accurate
    }
  }

  if (OB_SUCC(ret)) {
    ObObjType dest_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_calc_type()]];
    if (OB_UNLIKELY(ObMaxType == dest_type)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid type", K(type), K(ret));
    } else if (ObVarcharType == dest_type) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        if (ob_is_enumset_tc(types_stack[i].get_type())) {
          types_stack[i].set_calc_type(dest_type);
        }
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObMinMaxExprOperator::calc_(ObObj& result, const ObObj* objs_stack, int64_t param_num,
    const ObExprResType& result_type, ObExprCtx& expr_ctx, ObCmpOp cmp_op, bool need_cast)
{
  // todo:
  // we assume that result type / result collation / compare type / compare collation are CORRECT.
  // but who knowns? we should check it ASAP.
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(need_cast)) {
    ret = calc_with_cast(result, objs_stack, param_num, result_type, expr_ctx, cmp_op);
  } else {
    ret = calc_without_cast(result, objs_stack, param_num, result_type, expr_ctx, cmp_op);
  }
  return ret;
}

int ObMinMaxExprOperator::calc_without_cast(ObObj& result, const ObObj* objs_stack, int64_t param_num,
    const ObExprResType& result_type, ObExprCtx& expr_ctx, ObCmpOp cmp_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CO_LT != cmp_op && CO_GT != cmp_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the compare oper is wrong", K(ret), K(cmp_op));
  } else if (OB_ISNULL(objs_stack) || OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num < 1) ||
             OB_UNLIKELY(result_type.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stack is null or param_num is wrong", K(objs_stack), K(param_num), K(result_type), K(ret));
  } else {
    bool has_null = false;
    for (int i = 0; OB_SUCC(ret) && !has_null && i < param_num; ++i) {
      if (objs_stack[i].is_null()) {
        has_null = true;
        result.set_null();
      }
    }
    if (!has_null) {
      // compare all params.
      int res_idx = 0;
      ObCollationType cmp_cs_type = result_type.get_calc_collation_type();
      for (int i = 1; OB_SUCC(ret) && i < param_num; ++i) {
        if (ObObjCmpFuncs::compare_oper_nullsafe(objs_stack[i], objs_stack[res_idx], cmp_cs_type, cmp_op)) {
          res_idx = i;
        }
      }
      // ok, we got the least / greatest param.
      if (OB_SUCC(ret)) {
        if (OB_LIKELY(result_type.get_type() == objs_stack[res_idx].get_type() &&
                      result_type.get_collation_type() == objs_stack[res_idx].get_collation_type())) {
          result = objs_stack[res_idx];
        } else {  // slow path
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
          if (OB_FAIL(ObObjCaster::to_type(
                  result_type.get_type(), result_type.get_collation_type(), cast_ctx, objs_stack[res_idx], result))) {}
        }
      }
    }
  }
  return ret;
}

int ObMinMaxExprOperator::calc_with_cast(ObObj& result, const ObObj* objs_stack, int64_t param_num,
    const ObExprResType& result_type, ObExprCtx& expr_ctx, ObCmpOp cmp_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CO_LT != cmp_op && CO_GT != cmp_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the compare oper is wrong", K(ret), K(cmp_op));
  } else if (OB_ISNULL(objs_stack) || OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num < 1) ||
             OB_UNLIKELY(result_type.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stack is null or param_num is wrong", K(objs_stack), K(param_num), K(result_type), K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (ob_is_nstring_type(result_type.get_calc_type())) {
      cast_ctx.dest_collation_ = expr_ctx.my_session_->get_nls_collation_nation();
    } else if (share::is_mysql_mode() && CS_TYPE_INVALID != result_type.get_collation_type()) {
      cast_ctx.dest_collation_ = result_type.get_collation_type();
    }
    ObFixedArray<ObObj, ObIAllocator> buf_obj(expr_ctx.calc_buf_, param_num);
    ObFixedArray<const ObObj*, ObIAllocator> res_obj(expr_ctx.calc_buf_, param_num);  // inited in the for loop below.
    if (OB_FAIL(buf_obj.prepare_allocate(param_num))) {
      LOG_WARN("prepare allocate failed", K(param_num), K(ret));
    } else if (OB_FAIL(res_obj.prepare_allocate(param_num))) {
      LOG_WARN("prepare allocate failed", K(param_num), K(ret));
    }
    // ret status will be checked within OB_SUCC(ret)s
    bool has_null = false;
    // cast all params to cmp type, and check if exist null.
    for (int i = 0; OB_SUCC(ret) && !has_null && i < param_num; ++i) {
      res_obj[i] = NULL;
      if (objs_stack[i].is_null()) {
        has_null = true;
        result.set_null();
      } else if (OB_FAIL(ObObjCaster::to_type(
                     result_type.get_calc_type(), cast_ctx, objs_stack[i], buf_obj[i], res_obj[i]))) {

      } else if (OB_ISNULL(res_obj[i])) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret) && !has_null) {
      // compare all params.
      int res_idx = 0;
      ObCollationType cmp_cs_type = result_type.get_calc_collation_type();
      for (int i = 1; OB_SUCC(ret) && i < param_num; ++i) {
        if (ObObjCmpFuncs::compare_oper_nullsafe(*res_obj[i], *res_obj[res_idx], cmp_cs_type, cmp_op)) {
          res_idx = i;
        }
      }
      // ok, we got the least / greatest param.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObObjCaster::to_type(
                result_type.get_type(), result_type.get_collation_type(), cast_ctx, *res_obj[res_idx], result))) {}
      }
    }
  }
  return ret;
}

int ObArithExprOperator::interval_add_minus(
    ObObj& res, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx, ObScale scale, bool is_minus)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntervalTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    switch (left.get_type_class()) {
      case ObIntervalTC: {  // interval + interval
        if (OB_UNLIKELY(left.get_type() != right.get_type())) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;  // ORA-30081: invalid data type for datetime/interval arithmetic
          LOG_WARN("invalid data type interval arithmetic");
        } else if (left.is_interval_ym()) {
          ObIntervalYMValue value = is_minus ? left.get_interval_ym() - right.get_interval_ym()
                                             : left.get_interval_ym() + right.get_interval_ym();
          if (OB_FAIL(value.validate())) {
            LOG_WARN("value validate failed", K(ret), K(value));
          } else {
            res.set_interval_ym(value);
            res.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale());
          }
          LOG_DEBUG("add interval year to month result", K(value), K(res));
        } else {
          ObIntervalDSValue value = is_minus ? left.get_interval_ds() - right.get_interval_ds()
                                             : left.get_interval_ds() + right.get_interval_ds();
          if (OB_FAIL(value.validate())) {
            LOG_WARN("value validate failed", K(ret), K(value));
          } else {
            res.set_interval_ds(value);
            res.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale());
          }
          LOG_DEBUG("add interval day to second result", K(value), K(res));
        }
        break;
      }
      case ObDateTimeTC: {  // date +- interval
        int64_t date_v = left.get_datetime();
        int64_t result_v = 0;
        int64_t sign = is_minus ? -1 : 1;

        if (OB_FAIL(right.is_interval_ym() ? ObTimeConverter::date_add_nmonth(
                                                 date_v, right.get_interval_ym().get_nmonth() * sign, result_v)
                                           : ObTimeConverter::date_add_nsecond(date_v,
                                                 right.get_interval_ds().get_nsecond() * sign,
                                                 right.get_interval_ds().get_fs() * sign,
                                                 result_v))) {
          LOG_WARN("add value failed", K(ret), K(left), K(right));
        } else {
          res.set_datetime(result_v);
          res.set_scale(0);
        }
        break;
      }
      case ObOTimestampTC: {  // timestamp +- interval
        ObOTimestampData result_v;
        int32_t sign = is_minus ? -1 : 1;

        if (OB_FAIL(right.is_interval_ym() ? ObTimeConverter::otimestamp_add_nmonth(left.get_type(),
                                                 left.get_otimestamp_value(),
                                                 get_timezone_info(expr_ctx.my_session_),
                                                 right.get_interval_ym().get_nmonth() * sign,
                                                 result_v)
                                           : ObTimeConverter::otimestamp_add_nsecond(left.get_otimestamp_value(),
                                                 right.get_interval_ds().get_nsecond() * sign,
                                                 right.get_interval_ds().get_fs() * sign,
                                                 result_v))) {
          LOG_WARN("calc with timestamp value failed", K(ret), K(left), K(right));
        } else {
          res.set_otimestamp_value(left.get_type(), result_v);
          res.set_scale(MAX_SCALE_FOR_ORACLE_TEMPORAL);
        }
        break;
      }
      case ObNullTC: {
        res.set_null();
        break;
      }
      default: {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;  // ORA-30081: invalid data type for datetime/interval arithmetic
        LOG_WARN("invalid calc type", K(ret));
      }
    }
  }
  LOG_DEBUG("add interval", K(left), K(right), K(scale), K(res));
  UNUSED(scale);
  return ret;
}

int ObMinMaxExprOperator::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  need_cast_ = true;  // defensive code
  if (OB_FAIL(ObExprOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize in BASE class failed", K(ret));
  } else {
    OB_UNIS_DECODE(need_cast_);
  }
  return ret;
}

int ObMinMaxExprOperator::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprOperator::serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize in BASE class failed", K(ret));
  } else {
    OB_UNIS_ENCODE(need_cast_);
  }
  return ret;
}

int64_t ObMinMaxExprOperator::get_serialize_size() const
{
  int64_t len = 0;
  BASE_ADD_LEN((ObMinMaxExprOperator, ObExprOperator));
  OB_UNIS_ADD_LEN(need_cast_);
  return len;
}

// ObLocationExprOperator

int ObLocationExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprOperator::calc_result_flag2(type, type1, type2);
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  ObObjMeta types[2] = {type1, type2};
  OZ(aggregate_charsets_for_comparison(type.get_calc_meta(), types, 2, type_ctx.get_coll_type()));
  OX(type1.set_calc_collation_type(type.get_calc_collation_type()));
  OX(type2.set_calc_collation_type(type.get_calc_collation_type()));
  return ret;
}

int ObLocationExprOperator::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  ObObj position;
  position.set_int(1);
  return ObLocationExprOperator::calc_result3(result, obj1, obj2, position, expr_ctx);
}

int ObLocationExprOperator::calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
    const common::ObObj& obj3, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the pointer is null", K(expr_ctx.calc_buf_), K(ret));
  } else if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
    result.set_null();
  } else if (obj3.is_null()) {
    result.set_int(0);
  } else if (OB_UNLIKELY(!is_type_valid(obj1.get_type()) || !is_type_valid(obj2.get_type()) ||
                         !is_type_valid(obj3.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the param is not castable", K(obj1), K(obj2), K(obj3), K(ret));
  } else {
    int64_t pos = 0;
    ret = get_pos_int64(obj3, expr_ctx, pos);
    if (OB_SUCC(ret)) {
      TYPE_CHECK(obj1, ObVarcharType);
      TYPE_CHECK(obj2, ObVarcharType);
      ObString str1 = obj1.get_string();
      ObString str2 = obj2.get_string();
      uint32_t idx = ObCharset::locate(
          result_type_.get_calc_collation_type(), str2.ptr(), str2.length(), str1.ptr(), str1.length(), pos);
      result.set_int(static_cast<int64_t>(idx));
    }
  }
  return ret;
}

int ObLocationExprOperator::get_pos_int64(const ObObj& obj, ObExprCtx& expr_ctx, int64_t& out)
{
  int ret = OB_SUCCESS;
  out = 0;
  ObObjType type = obj.get_type();
  if (OB_LIKELY(ob_is_int_tc(type))) {
    out = obj.get_int();
  } else if (ob_is_uint_tc(type)) {
    uint64_t value = obj.get_uint64();
    out = (value > INT64_MAX) ? 0 : static_cast<int64_t>(value);
  } else if (OB_UNLIKELY(obj.is_number())) {
    int64_t tmp_int = 0;
    uint64_t tmp_uint = 0;
    number::ObNumber nmb = obj.get_number();
    number::ObNumber* pnmb = &nmb;
    number::ObNumber newmb;
    if (OB_UNLIKELY(!nmb.is_integer())) {
      // such as select locate('bar', 'foobarbar', 5.3); yeah, really ugly.
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      if (OB_FAIL(newmb.from(nmb, cast_ctx))) {  // copy is essential since if we did not do that, obj will be modified
        LOG_WARN("copy nmb failed", K(ret), K(nmb));
      } else if (OB_FAIL(newmb.round(0))) {
        LOG_WARN("round failed", K(ret), K(nmb));
      } else {
        pnmb = &newmb;
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(pnmb)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. null pointer", K(ret));
    } else if (pnmb->is_valid_int64(tmp_int)) {
      out = tmp_int;
    } else if (pnmb->is_valid_uint64(tmp_uint)) {
      out = 0;  // no errors  no warnings in mysql.
    } else {
      ret = OB_ERR_TRUNCATED_WRONG_VALUE;
      if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
        ret = OB_SUCCESS;
        out = 0;
      }
    }
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_INT64_V2(obj, out);
  }
  return ret;
}

int ObLocationExprOperator::get_calc_cs_type(const ObExpr& expr, ObCollationType& calc_cs_type)
{
  int ret = OB_SUCCESS;
  const ObCollationType cs_type1 = expr.args_[0]->datum_meta_.cs_type_;
  const ObCollationType cs_type2 = expr.args_[1]->datum_meta_.cs_type_;
  if (OB_UNLIKELY(cs_type1 != cs_type2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cs type should be same", K(ret), K(cs_type1), K(cs_type2));
  } else if (OB_UNLIKELY(!ObCharset::is_valid_collation(static_cast<int64_t>(cs_type1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cs_type", K(ret), K(cs_type1));
  } else {
    calc_cs_type = cs_type1;
  }
  return ret;
}

int ObLocationExprOperator::calc_location_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // locate(sub, ori, pos)
  if (OB_UNLIKELY(2 > expr.arg_cnt_ || 3 < expr.arg_cnt_) || OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[0]) ||
      OB_ISNULL(expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr", K(ret), K(expr));
  } else if (OB_FAIL(calc_(expr, *expr.args_[0], *expr.args_[1], ctx, res_datum))) {
    LOG_WARN("calc_ faied", K(ret));
  }
  return ret;
}

int ObLocationExprOperator::calc_(
    const ObExpr& expr, const ObExpr& sub_arg, const ObExpr& ori_arg, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* sub = NULL;
  ObDatum* ori = NULL;
  ObDatum* pos = NULL;
  bool has_result = false;
  if (OB_FAIL(sub_arg.eval(ctx, sub)) || OB_FAIL(ori_arg.eval(ctx, ori))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (sub->is_null() || ori->is_null()) {
    res_datum.set_null();
    has_result = true;
  }

  int64_t pos_int = 1;
  if (OB_SUCC(ret) && !has_result && 3 == expr.arg_cnt_) {
    if (OB_FAIL(expr.args_[2]->eval(ctx, pos))) {
      LOG_WARN("eval arg 2 failed", K(ret));
    } else if (pos->is_null()) {
      res_datum.set_int(0);
      has_result = true;
    } else {
      pos_int = pos->get_int();
    }
  }

  if (OB_SUCC(ret) && !has_result) {
    const ObString& ori_str = ori->get_string();
    const ObString& sub_str = sub->get_string();
    ObCollationType calc_cs_type = CS_TYPE_INVALID;
    if (OB_FAIL(get_calc_cs_type(expr, calc_cs_type))) {
      LOG_WARN("get_calc_cs_type failed", K(ret));
    } else {
      uint32_t idx =
          ObCharset::locate(calc_cs_type, ori_str.ptr(), ori_str.length(), sub_str.ptr(), sub_str.length(), pos_int);
      res_datum.set_int(static_cast<int64_t>(idx));
    }
  }
  return ret;
}

int ObLocationExprOperator::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_location_expr;
  return OB_SUCCESS;
}

int ObExprTRDateFormat::calc_hash(const char* p, int64_t len, uint64_t& hash)
{
  int ret = OB_SUCCESS;
  hash = 0;
  if (OB_ISNULL(p) || OB_UNLIKELY(len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error.invalid arguments", K(p), K(len));
  } else {
    for (int64_t i = 0; i < len; ++i) {
      hash = (hash << 7) + (hash << 1) + hash + toupper(p[i]);
    }
  }
  return ret;
}

int ObExprTRDateFormat::init()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < FORMAT_MAX_TYPE && OB_SUCC(ret); ++i) {
    ret = calc_hash(FORMATS_TEXT[i], strlen(FORMATS_TEXT[i]), FORMATS_HASH[i]);
    // validation
    for (int64_t j = 0; j < i && OB_SUCC(ret); ++j) {
      if (FORMATS_HASH[i] == FORMATS_HASH[j]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected error.hash func is not perfect hash", K(i), K(j));
        break;
      }
    }
  }
  return ret;
}

int ObExprTRDateFormat::trunc_new_obtime(ObTime& ob_time, const ObString& fmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fmt.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty string.", K(ret), K(fmt));
  } else {
    int64_t fmt_id = SYYYY;
    const char* ptr = fmt.ptr();
    int32_t ptr_len = fmt.length();
    uint64_t fmt_hash = 0;
    if (OB_FAIL(calc_hash(ptr, ptr_len, fmt_hash))) {
      LOG_WARN("calc hash failed", K(ret), K(fmt));
    } else if (FALSE_IT(get_format_id(fmt_hash, fmt_id))) {
    } else if (OB_UNLIKELY(fmt_id < 0 || fmt_id >= FORMAT_MAX_TYPE)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid format string", K(ret), K(fmt_id), K(fmt));
    } else if (OB_UNLIKELY(strncasecmp(ptr, FORMATS_TEXT[fmt_id], ptr_len))) {
      // what a pity ! same hash value, while not expected content
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid format string", K(ret), K(fmt_id), K(fmt));
    } else {
      switch (fmt_id) {
        case SYYYY:
          // go through
        case YYYY:
          // go through
        case YEAR:
        case SYEAR:
          // go through
        case YYY:
          // go through
        case YY:
          // go through
        case Y: {
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_YDAY] - 1);
          ob_time.parts_[DT_DATE] -= offset;
          break;
        }
        case Q: {
          set_time_part_to_zero(ob_time);
          int32_t quarter = (ob_time.parts_[DT_MON] + 2) / MONS_PER_QUAR;
          ob_time.parts_[DT_MON] = (quarter - 1) * MONS_PER_QUAR + 1;
          ob_time.parts_[DT_MDAY] = 1;
          ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
          break;
        }
        case MONTH:
          // go through
        case MON:
          // go through
        case MM:
          // go through
        case RM: {
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_MDAY] - 1);
          ob_time.parts_[DT_DATE] -= offset;
          break;
        }
        case WW: {
          // 01-01 is the first day of year
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_YDAY] - 1) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] -= offset;
          break;
        }
        case IW: {
          // within a week. monday is the first day
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_WDAY] - 1) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] -= offset;
          break;
        }
        case W: {
          // xx-01 is the first day
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_MDAY] - 1) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] -= offset;
          break;
        }
        case DDD:
          // go through
        case DD:
          // go through
        case J: {
          set_time_part_to_zero(ob_time);
          break;
        }
        case DAY:
          // go through
        case DY:
          // go through
        case D: {
          // within a week. sunday is the first day
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_WDAY]) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] -= offset;
          break;
        }
        case HH:
          // go through
        case HH12:
          // go through
        case HH24: {
          ob_time.parts_[DT_MIN] = 0;
          ob_time.parts_[DT_SEC] = 0;
          ob_time.parts_[DT_USEC] = 0;
          break;
        }
        case MI: {
          ob_time.parts_[DT_SEC] = 0;
          ob_time.parts_[DT_USEC] = 0;
          break;
        }
        case CC:
          // go through
        case SCC: {
          set_time_part_to_zero(ob_time);
          ob_time.parts_[DT_YEAR] = ob_time.parts_[DT_YEAR] / YEARS_PER_CENTURY * YEARS_PER_CENTURY + 1;
          ob_time.parts_[DT_MON] = 1;
          ob_time.parts_[DT_MDAY] = 1;
          ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
          break;
        }
        case IYYY:
          // go through
        case IY:
          // go through
        case I: {
          // not used heavily. so, do not care too much about performance !
          set_time_part_to_zero(ob_time);
          ObTimeConverter::get_first_day_of_isoyear(ob_time);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fmt_id", K(fmt_id), K(ret), K(fmt));
        }
      }  // end switch
    }
  }
  return ret;
}

int ObExprTRDateFormat::round_new_obtime(ObTime& ob_time, const ObString& fmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fmt.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty string.", K(ret), K(fmt));
  } else {
    int64_t fmt_id = SYYYY;
    const char* ptr = fmt.ptr();
    int32_t ptr_len = fmt.length();
    uint64_t fmt_hash = 0;
    if (OB_FAIL(calc_hash(ptr, ptr_len, fmt_hash))) {
      LOG_WARN("calc hash failed", K(ret), K(fmt));
    } else if (FALSE_IT(get_format_id(fmt_hash, fmt_id))) {
    } else if (OB_UNLIKELY(fmt_id < 0 || fmt_id >= FORMAT_MAX_TYPE)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid format string", K(ret), K(fmt_id), K(fmt));
    } else if (OB_UNLIKELY(strncasecmp(ptr, FORMATS_TEXT[fmt_id], ptr_len))) {
      // what a pity ! same hash value, while not expected content
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid format string", K(ret), K(fmt_id), K(fmt));
    } else {
      LOG_DEBUG("check value", K(ob_time), K(fmt_id), K(fmt));
      switch (fmt_id) {
        case SYYYY:
          // go through
        case YYYY:
          // go through
        case YEAR:
        case SYEAR:
          // go through
        case YYY:
          // go through
        case YY:
          // go through
        case Y: {
          //>6
          const int32_t add_year = (ob_time.parts_[DT_MON] > DT_PART_MAX[DT_MON] / 2) ? 1 : 0;
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_YDAY] - 1);
          ob_time.parts_[DT_DATE] =
              ob_time.parts_[DT_DATE] - offset + add_year * DAYS_PER_YEAR[IS_LEAP_YEAR(ob_time.parts_[DT_YEAR])];
          break;
        }
        case Q: {
          //>15
          const int32_t add_quarter = (((ob_time.parts_[DT_MON] - 1) % MONS_PER_QUAR > 1) ||
                                          (1 == (ob_time.parts_[DT_MON] - 1) % MONS_PER_QUAR &&
                                              ob_time.parts_[DT_MDAY] > DT_PART_MAX[DT_MDAY] / 2))
                                          ? 1
                                          : 0;
          set_time_part_to_zero(ob_time);
          int32_t quarter = (ob_time.parts_[DT_MON] + 2) / MONS_PER_QUAR + add_quarter;
          ob_time.parts_[DT_MON] = (quarter - 1) * MONS_PER_QUAR + 1;
          if (ob_time.parts_[DT_MON] > DT_PART_MAX[DT_MON]) {
            ob_time.parts_[DT_MON] -= static_cast<int32_t>(DT_PART_MAX[DT_MON]);
            ob_time.parts_[DT_YEAR] += 1;
          }
          ob_time.parts_[DT_MDAY] = 1;
          ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
          break;
        }
        case MONTH:
          // go through
        case MON:
          // go through
        case MM:
          // go through
        case RM: {
          //>15
          const int32_t add_month = (ob_time.parts_[DT_MDAY] > DT_PART_MAX[DT_MDAY] / 2) ? 1 : 0;
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_MDAY] - 1);
          ob_time.parts_[DT_DATE] =
              ob_time.parts_[DT_DATE] - offset +
              add_month * DAYS_PER_MON[IS_LEAP_YEAR(ob_time.parts_[DT_YEAR])][ob_time.parts_[DT_MON]];
          break;
        }
        case WW: {
          // 01-01 is the first day of year
          const int32_t add_ww = ((((ob_time.parts_[DT_YDAY] - 1) % DAYS_PER_WEEK) > DAYS_PER_WEEK / 2) ||
                                     (((ob_time.parts_[DT_YDAY] - 1) % DAYS_PER_WEEK) == DAYS_PER_WEEK / 2 &&
                                         ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR] / 2))
                                     ? 1
                                     : 0;
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_YDAY] - 1) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] = ob_time.parts_[DT_DATE] - offset + add_ww * DAYS_PER_WEEK;
          break;
        }
        case IW: {
          // within a week. monday is the first day
          const int32_t add_iw = ((((ob_time.parts_[DT_WDAY] - 1) % DAYS_PER_WEEK) > DAYS_PER_WEEK / 2) ||
                                     (((ob_time.parts_[DT_WDAY] - 1) % DAYS_PER_WEEK) == DAYS_PER_WEEK / 2 &&
                                         ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR] / 2))
                                     ? 1
                                     : 0;
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_WDAY] - 1) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] = ob_time.parts_[DT_DATE] - offset + add_iw * DAYS_PER_WEEK;
          break;
        }
        case W: {
          // xx-01 is the first day
          const int32_t add_w = ((((ob_time.parts_[DT_MDAY] - 1) % DAYS_PER_WEEK) > DAYS_PER_WEEK / 2) ||
                                    (((ob_time.parts_[DT_MDAY] - 1) % DAYS_PER_WEEK) == DAYS_PER_WEEK / 2 &&
                                        ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR] / 2))
                                    ? 1
                                    : 0;
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_MDAY] - 1) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] = ob_time.parts_[DT_DATE] - offset + add_w * DAYS_PER_WEEK;
          break;
        }
        case DDD:
          // go through
        case DD:
          // go through
        case J: {
          const int32_t add_d = (ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR] / 2) ? 1 : 0;
          set_time_part_to_zero(ob_time);
          ob_time.parts_[DT_DATE] += add_d;
          break;
        }
        case DAY:
          // go through
        case DY:
          // go through
        case D: {
          // within a week. sunday is the first day
          const int32_t add_dy = (((ob_time.parts_[DT_WDAY] % DAYS_PER_WEEK) > DAYS_PER_WEEK / 2) ||
                                     ((ob_time.parts_[DT_WDAY] % DAYS_PER_WEEK) == DAYS_PER_WEEK / 2 &&
                                         ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR] / 2))
                                     ? 1
                                     : 0;
          set_time_part_to_zero(ob_time);
          int32_t offset = (ob_time.parts_[DT_WDAY]) % DAYS_PER_WEEK;
          ob_time.parts_[DT_DATE] = ob_time.parts_[DT_DATE] - offset + add_dy * DAYS_PER_WEEK;
          break;
        }
        case HH:
          // go through
        case HH12:
          // go through
        case HH24: {
          const int32_t add_dh = (ob_time.parts_[DT_MIN] > DT_PART_MAX[DT_MIN] / 2) ? 1 : 0;
          ob_time.parts_[DT_MIN] = 0;
          ob_time.parts_[DT_SEC] = 0;
          ob_time.parts_[DT_USEC] = 0;
          ob_time.parts_[DT_HOUR] += add_dh;
          if (ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR]) {
            ob_time.parts_[DT_HOUR] -= static_cast<int32_t>(DT_PART_MAX[DT_HOUR]);
            ob_time.parts_[DT_DATE] += 1;
          }
          break;
        }
        case MI: {
          const int32_t add_mi = (ob_time.parts_[DT_SEC] >= DT_PART_MAX[DT_SEC] / 2) ? 1 : 0;
          ob_time.parts_[DT_SEC] = 0;
          ob_time.parts_[DT_USEC] = 0;
          ob_time.parts_[DT_MIN] += add_mi;
          if (ob_time.parts_[DT_MIN] > DT_PART_MAX[DT_MIN]) {
            ob_time.parts_[DT_MIN] -= static_cast<int32_t>(DT_PART_MAX[DT_MIN]);
            ob_time.parts_[DT_HOUR] += 1;

            if (ob_time.parts_[DT_HOUR] > DT_PART_MAX[DT_HOUR]) {
              ob_time.parts_[DT_HOUR] -= static_cast<int32_t>(DT_PART_MAX[DT_HOUR]);
              ob_time.parts_[DT_DATE] += 1;
            }
          }
          break;
        }
        case CC:
          // go through
        case SCC: {
          const int32_t add_cc = (ob_time.parts_[DT_YEAR] % YEARS_PER_CENTURY > YEARS_PER_CENTURY / 2) ? 1 : 0;
          set_time_part_to_zero(ob_time);
          ob_time.parts_[DT_YEAR] =
              ob_time.parts_[DT_YEAR] / YEARS_PER_CENTURY * YEARS_PER_CENTURY + 1 + add_cc * YEARS_PER_CENTURY;
          ob_time.parts_[DT_MON] = 1;
          ob_time.parts_[DT_MDAY] = 1;
          ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
          break;
        }
        case IYYY:
          // go through
        case IY:
          // go through
        case I: {
          // not used heavily. so, do not care too much about performance !
          set_time_part_to_zero(ob_time);
          ret = ObTimeConverter::get_round_day_of_isoyear(ob_time);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fmt_id", K(fmt_id), K(ret), K(fmt));
        }
      }  // end switch
    }
  }
  return ret;
}

int ObRelationalExprOperator::is_row_cmp(const ObRawExpr& raw_expr, int& row_dim)
{
  int ret = OB_SUCCESS;
  row_dim = -1;
  if (OB_UNLIKELY(2 != raw_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param count", K(ret), K(raw_expr.get_param_count()));
  } else if (T_OP_ROW == raw_expr.get_param_expr(0)->get_expr_type() &&
             T_OP_ROW == raw_expr.get_param_expr(1)->get_expr_type()) {
    if (raw_expr.get_param_expr(0)->get_param_count() != raw_expr.get_param_expr(1)->get_param_count()) {
      if (1 == raw_expr.get_param_expr(1)->get_param_count() &&
          T_OP_ROW == raw_expr.get_param_expr(1)->get_param_expr(0)->get_expr_type()) {
        // (c1, c2) = (c1, c2), (c1, c2) = ((c1, c2)) are both allowed in oralce mode
        if (raw_expr.get_param_expr(0)->get_param_count() ==
            raw_expr.get_param_expr(1)->get_param_expr(0)->get_param_count()) {
          row_dim = raw_expr.get_param_expr(0)->get_param_count();
        }
      }
    } else {
      row_dim = raw_expr.get_param_expr(0)->get_param_count();
    }
    if (OB_UNLIKELY(-1 == row_dim)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param cnt",
          K(raw_expr.get_param_expr(0)->get_param_count()),
          K(raw_expr.get_param_expr(1)->get_param_count()),
          K(raw_expr.get_param_expr(1)->get_expr_type()));
    }
  }
  return ret;
}

int ObRelationalExprOperator::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  int row_dim = -1;
  if (OB_FAIL(is_row_cmp(raw_expr, row_dim))) {
    LOG_WARN("failed to get row dimension", K(ret));
  } else if (OB_ISNULL(op_cg_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null allocator", K(ret), K(op_cg_ctx.allocator_));
  } else if (row_dim > 0) {
    ret = cg_row_cmp_expr(row_dim, *op_cg_ctx.allocator_, raw_expr, input_types_, rt_expr);
  } else {
    ret = cg_datum_cmp_expr(raw_expr, input_types_, rt_expr);
  }
  return ret;
}

int ObRelationalExprOperator::cg_datum_cmp_expr(
    const ObRawExpr& raw_expr, const ObExprOperatorInputTypeArray& input_types, ObExpr& rt_expr)
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_ || NULL == rt_expr.args_ || NULL == rt_expr.args_[0] ||
                  NULL == rt_expr.args_[1] || input_types.count() != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    rt_expr.inner_func_cnt_ = 0;
    rt_expr.inner_functions_ = NULL;

    const ObCmpOp cmp_op = get_cmp_op(raw_expr.get_expr_type());
    const ObObjType input_type1 = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType input_type2 = rt_expr.args_[1]->datum_meta_.type_;
    LOG_DEBUG("CG Datum CMP Expr", K(input_type1), K(input_type2), K(cmp_op));
    const ObCollationType cs_type = rt_expr.args_[0]->datum_meta_.cs_type_;
    if (ObDatumFuncs::is_string_type(input_type1) && ObDatumFuncs::is_string_type(input_type2)) {
      CK(rt_expr.args_[0]->datum_meta_.cs_type_ == rt_expr.args_[1]->datum_meta_.cs_type_);
      rt_expr.eval_func_ = ObExprCmpFuncsHelper::get_eval_expr_cmp_func(
          input_type1, input_type2, cmp_op, lib::is_oracle_mode(), cs_type);
    } else {
      rt_expr.eval_func_ = ObExprCmpFuncsHelper::get_eval_expr_cmp_func(
          input_type1, input_type2, cmp_op, lib::is_oracle_mode(), cs_type);
      CK(NULL != rt_expr.eval_func_);
    }
  }
  return ret;
}

int ObRelationalExprOperator::cg_row_cmp_expr(const int row_dimension, ObIAllocator& allocator,
    const ObRawExpr& raw_expr, const ObExprOperatorInputTypeArray& input_types, ObExpr& rt_expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 2 || row_dimension <= 0 || NULL == rt_expr.args_ || NULL == rt_expr.args_[0] ||
                  NULL == rt_expr.args_[1] || rt_expr.args_[0]->arg_cnt_ != row_dimension ||
                  input_types.count() != row_dimension * 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    void** inner_func_buf = NULL;
    if (OB_ISNULL(inner_func_buf = (void**)allocator.alloc(sizeof(void*) * row_dimension))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      rt_expr.inner_func_cnt_ = row_dimension;
      rt_expr.inner_functions_ = inner_func_buf;

      const ObCmpOp cmp_op = get_cmp_op(raw_expr.get_expr_type());

      ObExpr* left_row = rt_expr.args_[0];
      ObExpr* right_row = NULL;
      if (OB_LIKELY(T_OP_ROW == rt_expr.args_[1]->type_ && NULL != rt_expr.args_[1]->args_[0])) {
        if (T_OP_ROW == rt_expr.args_[1]->args_[0]->type_) {
          right_row = rt_expr.args_[1]->args_[0];
        } else {
          right_row = rt_expr.args_[1];
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      }

      if (OB_UNLIKELY(left_row->arg_cnt_ != right_row->arg_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row cnt", K(left_row->arg_cnt_), K(right_row->arg_cnt_));
      }
      LOG_DEBUG("CG ROW CMP Expr", K(input_types), K(cmp_op));

      for (int i = 0; OB_SUCC(ret) && i < row_dimension; i++) {
        const ObObjType type1 = left_row->args_[i]->datum_meta_.type_;
        const ObObjType type2 = right_row->args_[i]->datum_meta_.type_;
        const ObCollationType cs_type = left_row->args_[i]->datum_meta_.cs_type_;
        if (ObDatumFuncs::is_string_type(type1) && ObDatumFuncs::is_string_type(type2)) {
          CK(left_row->args_[i]->datum_meta_.cs_type_ == right_row->args_[i]->datum_meta_.cs_type_);
          rt_expr.inner_functions_[i] =
              (void*)ObExprCmpFuncsHelper::get_datum_expr_cmp_func(type1, type2, lib::is_oracle_mode(), cs_type);
        } else {
          rt_expr.inner_functions_[i] =
              (void*)ObExprCmpFuncsHelper::get_datum_expr_cmp_func(type1, type2, lib::is_oracle_mode(), cs_type);
          if (OB_ISNULL(rt_expr.inner_functions_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null function", K(ret), K(i), K(type1), K(type2));
          }
        }
      }  // for end
      if (OB_SUCC(ret)) {
        rt_expr.eval_func_ = &row_eval;
      }
    }
  }
  return ret;
}

int ObRelationalExprOperator::row_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != expr.arg_cnt_ || NULL == expr.args_ || expr.inner_func_cnt_ <= 0 ||
                  expr.args_[0]->arg_cnt_ != expr.inner_func_cnt_ || NULL == expr.args_[0]->args_ ||
                  NULL == expr.args_[1]->args_ || NULL == expr.inner_functions_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObExpr* left_row = expr.args_[0];
    ObExpr* right_row = NULL;
    if (1 == expr.args_[1]->arg_cnt_ && T_OP_ROW == expr.args_[1]->args_[0]->type_) {
      if (expr.args_[1]->args_[0]->arg_cnt_ != expr.inner_func_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg cnt", K(ret), K(expr.inner_func_cnt_), K(expr.args_[1]->args_[0]->arg_cnt_));
      } else {
        right_row = expr.args_[1]->args_[0];
      }
    } else if (OB_UNLIKELY(expr.inner_func_cnt_ != expr.args_[1]->arg_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg cnt", K(ret), K(expr.inner_func_cnt_), K(expr.args_[1]->arg_cnt_));
    } else {
      right_row = expr.args_[1];
    }
    ret = row_cmp(expr, expr_datum, left_row->args_, ctx, right_row->args_, ctx);
  }
  return ret;
}

int ObRelationalExprOperator::row_cmp(
    const ObExpr& expr, ObDatum& expr_datum, ObExpr** l_row, ObEvalCtx& l_ctx, ObExpr** r_row, ObEvalCtx& r_ctx)
{
  // performance critical, do not check pointer validity.
  int ret = OB_SUCCESS;
  ObDatum* left = NULL;
  ObDatum* right = NULL;

  bool cnt_row_null = false;
  int first_nonequal_cmp_ret = 0;
  int i = 0;
  // locate first non-equal pair
  for (; OB_SUCC(ret) && i < expr.inner_func_cnt_; i++) {
    if (OB_FAIL(l_row[i]->eval(l_ctx, left))) {
      LOG_WARN("failed to eval left in row cmp", K(ret));
    } else if (left->is_null()) {
      cnt_row_null = true;
    } else if (OB_FAIL(r_row[i]->eval(r_ctx, right))) {
      LOG_WARN("failed to eval right in row cmp", K(ret));
    } else if (right->is_null()) {
      cnt_row_null = true;
    } else if (0 != (first_nonequal_cmp_ret = ((DatumCmpFunc)expr.inner_functions_[i])(*left, *right))) {
      break;
    }
  }  // for end
  ObCmpOp cmp_op = get_cmp_op(expr.type_);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (i == expr.inner_func_cnt_) {
    if (cnt_row_null) {
      expr_datum.set_null();
    } else {
      expr_datum.set_int(is_expected_cmp_ret(cmp_op, 0));
    }
  } else {
    if (cnt_row_null) {
      if (CO_NE == cmp_op) {
        expr_datum.set_int(true);
      } else if (CO_EQ == cmp_op) {
        expr_datum.set_int(false);
      } else {
        expr_datum.set_null();
      }
    } else {
      expr_datum.set_int(is_expected_cmp_ret(cmp_op, first_nonequal_cmp_ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
