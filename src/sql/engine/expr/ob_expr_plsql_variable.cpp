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

#include "ob_expr_plsql_variable.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
using namespace pl;

namespace sql
{

OB_SERIALIZE_MEMBER((ObExprPLSQLVariable, ObFuncExprOperator), plsql_line_, plsql_variable_);

ObExprPLSQLVariable::ObExprPLSQLVariable(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_PLSQL_VARIABLE, N_PLSQL_VARIABLE, 0, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE),
      plsql_line_(OB_INVALID_INDEX),
      plsql_variable_(),
      allocator_(alloc)
{}

int ObExprPLSQLVariable::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprPLSQLVariable *tmp = dynamic_cast<const ObExprPLSQLVariable *>(&other);
  if (OB_UNLIKELY(OB_ISNULL(tmp))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(other), K(ret));
  } else if (OB_LIKELY(this != tmp)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(other), K(ret));
    } else {
      OX (this->plsql_line_ = tmp->plsql_line_);
      OZ (deep_copy_plsql_variable(tmp->plsql_variable_));
    }
  }
  return ret;
}

int ObExprPLSQLVariable::calc_result_type0(
  ObExprResType &type, common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (0 == plsql_variable_.case_compare("PLSQL_LINE")) {
    type.set_int32();
    type.set_precision(4);
    type.set_scale(0);
  } else if (0 == plsql_variable_.case_compare("PLSQL_UNIT")
    || 0 == plsql_variable_.case_compare("PLSQL_UNIT_OWNER")) {
    type.set_varchar();
    type.set_default_collation_type();
    type.set_length(common::OB_MAX_VARCHAR_LENGTH);
    type.set_length_semantics(LS_BYTE);
  } else {
    const sql::ObSQLSessionInfo *session_info = type_ctx.get_session();
    ObString plsql_ccflags;
    ObObj value;
    CK (OB_NOT_NULL(session_info));
    // may plsql relative system variables, try it.
    if (OB_FAIL(ret)) {
    } else if (0 == plsql_variable_.case_compare("PLSQL_CCFLAGS")
        || OB_FAIL(session_info->get_sys_variable_by_name(plsql_variable_, value))) {
      // not system variable
      if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret
          // plsql_ccflags may rewrite by itself.
          || 0 == plsql_variable_.case_compare("PLSQL_CCFLAGS")) {
        // then may plsql_ccflags, try it.
        ret = OB_SUCCESS;
        OX (plsql_ccflags = session_info->get_plsql_ccflags());
        OZ (get_key_value(plsql_ccflags, plsql_variable_, value));
        OX (type.set_meta(value.get_meta()));
        OX (type.set_collation_type(ObCharset::get_system_collation()));
        if (OB_SUCC(ret) && value.is_string_type()) {
          type.set_length(value.get_string_len());
          type.set_length_semantics(LS_BYTE);
        }
      } else {
        LOG_WARN("failed to get system variable by name", K(ret), K(plsql_variable_));
      }
    } else {
      type.set_meta(value.get_meta());
      if (value.is_string_type()) {
        type.set_length(value.get_string_len());
        type.set_length_semantics(LS_BYTE);
      }
    }
  }
  return ret;
}

int ObExprPLSQLVariable::get_plsql_unit(common::ObObj &result,
                                        common::ObIAllocator &alloc,
                                        ObSQLSessionInfo &session_info,
                                        const ObExprResType &result_type,
                                        const ObString &plsql_variable)
{
  int ret = OB_SUCCESS;
  pl::ObPLContext *pl_context = NULL;
  ObPLExecState *pl_state = NULL;
  ObString plsql_unit;
  CK (OB_NOT_NULL(pl_context = session_info.get_pl_context()));
  CK (pl_context->is_top_stack());
  CK (pl_context->get_exec_stack().count() > 0);
  CK (OB_NOT_NULL(pl_state = pl_context
    ->get_exec_stack().at(pl_context->get_exec_stack().count() - 1)));
  if (OB_FAIL(ret)) {
  } else if (0 == plsql_variable.case_compare("PLSQL_UNIT")) {
    if (pl_state->get_function().get_package_name().empty()) {
      OZ (ob_write_string(alloc,
                          pl_state->get_function().get_function_name(),
                          plsql_unit));
    } else {
      OZ (ob_write_string(alloc,
                          pl_state->get_function().get_package_name(),
                          plsql_unit));
    }
  } else if (0 == plsql_variable.case_compare("PLSQL_UNIT_OWNER")) {
    if (!pl_state->get_function().get_database_name().empty()) {
      OZ (ob_write_string(alloc,
                          pl_state->get_function().get_database_name(),
                          plsql_unit));
    }
  }
  OX (result.set_varchar(plsql_unit));
  OX (result.set_collation(result_type));
  return ret;
}

// check valid identifiled
int ObExprPLSQLVariable::check_key(const common::ObString &v)
{
  int ret = OB_SUCCESS;
  ObString key = v;
  bool valid = true;
  key = key.trim();
  if (!key.empty()
      && ((key.ptr()[0] >= 'a' && key.ptr()[0] <= 'z')
           || (key.ptr()[0] >= 'A' && key.ptr()[0] <= 'Z'))) {
    for (int64_t i = 1; valid && i < key.length(); ++i) {
      if (key.ptr()[i] == '_'
          || key.ptr()[i] == '$'
          || key.ptr()[i] == '#'
          || (key.ptr()[i] >= '0' && key.ptr()[i] <= '9')
          || ((key.ptr()[i] >= 'a' && key.ptr()[i] <= 'z')
               || (key.ptr()[i] >= 'A' && key.ptr()[i] <= 'Z'))) {
        // do nothing ...
      } else {
        valid = false;
      }
    }
  } else {
    valid = false;
  }
  if (!valid) {
    ret = OB_ERR_INVALID_PLSQL_CCFLAGS;
    LOG_WARN("ORA-39962: invalid parameter for PLSQL_CCFLAGS", K(ret), K(v));
  }
  return ret;
}

int ObExprPLSQLVariable::check_value(
  const common::ObString &v, ObObj &val_obj)
{
  int ret = OB_SUCCESS;
  ObString val = v;
  val = val.trim();
  if (0 == val.case_compare("NULL")) {
    val_obj.set_null();
  } else if (0 == val.case_compare("TRUE")) {
    val_obj.set_tinyint(1);
  } else if (0 == val.case_compare("FALSE")) {
    val_obj.set_tinyint(0);
  } else {
    char buf[OB_TMP_BUF_SIZE_256 + 1];
    int64_t int_val = 0;
    STRNCPY(buf, val.ptr(), val.length());
    OX (buf[val.length()] = '\0');
    OZ (ob_atoll(buf, int_val));
    CK (int_val > -2147483647 && int_val <= 2147483647);
    OX (val_obj.set_int32(int_val));
    if (OB_FAIL(ret)) {
      ret = OB_ERR_INVALID_PLSQL_CCFLAGS;
      LOG_WARN("ORA-39962: invalid parameter for PLSQL_CCFLAGS", K(ret), K(v));
    }
  }
  return ret;
}

int ObExprPLSQLVariable::add_to_array(
  const common::ObString &key, ObObj &val, ObIArray<std::pair<ObString, ObObj> > &result)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; i < result.count(); ++i) {
    if (0 == key.case_compare(result.at(i).first)) {
      result.at(i).second = val;
      break;
    }
  }
  if (i == result.count()) {
    OZ (result.push_back(std::make_pair(key, val)));
  }
  return ret;
}

int ObExprPLSQLVariable::check_plsql_ccflags(
  const common::ObString &v, ObIArray<std::pair<ObString, ObObj> > *result)
{
  int ret = OB_SUCCESS;
  ObString v1 = v;
  if (0 == v1.length()) {
    // do nothing ...
  } else if (v1.length() > OB_TMP_BUF_SIZE_256) {
    ret = OB_ERR_PARAMETER_TOO_LONG;
    LOG_WARN("ORA-32021: parameter value longer than string characters", K(ret), K(v));
    LOG_USER_ERROR(OB_ERR_PARAMETER_TOO_LONG, static_cast<int32_t>(OB_TMP_BUF_SIZE_256));
  } else {
    while (OB_SUCC(ret) && v1.length() > 0) {
      ObString value = v1.split_on(',');
      ObString key;
      ObObj val_obj;
      if (value.empty()) {
        value = v1;
        v1.reset();
      }
      key = value.split_on(':');
      key = key.trim();
      OZ (check_key(key));
      OZ (check_value(value, val_obj));
      if (OB_SUCC(ret) && OB_NOT_NULL(result)) {
        OZ (add_to_array(key, val_obj, *result));
      }
    }
  }
  return ret;
}

int ObExprPLSQLVariable::get_key_value(
  const common::ObString &plsql_ccflags, const common::ObString &key, ObObj &value)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<common::ObString, ObObj>, 4> result; 
  bool found = false;
  OZ (check_plsql_ccflags(plsql_ccflags, &result));
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; !found && i < result.count(); ++i) {
      if (0 == key.case_compare(result.at(i).first)) {
        value = result.at(i).second;
        found = true;
      }
    }
    if (!found) {
      if (0 == key.case_compare("PLSQL_CCFLAGS")) {
        value.set_varchar(plsql_ccflags);
      } else {
        value.set_null();
      }
    }
  }
  return ret;
}

int ObExprPLSQLVariable::cg_expr(ObExprCGCtx &op_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  const ObPLSQLVariableRawExpr &fun_sys = static_cast<const ObPLSQLVariableRawExpr &>(raw_expr);
  ObPLSQLVariableInfo *info = OB_NEWx(ObPLSQLVariableInfo, (&alloc), alloc, T_FUN_PLSQL_VARIABLE);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    OZ(info->from_raw_expr(fun_sys, alloc));
    info->result_type_ = result_type_;
    rt_expr.extra_info_ = info;
  }
  rt_expr.eval_func_ = eval_plsql_variable;
  return ret;
}
int ObExprPLSQLVariable::eval_plsql_variable(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  const ObPLSQLVariableInfo *info = static_cast<ObPLSQLVariableInfo *>(expr.extra_info_);
  ObObj result;
  if (OB_ISNULL(info) || OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get expr info", K(ret));
  } else if (0 == info->plsql_variable_.case_compare("PLSQL_LINE")) {
    result.set_int32(info->plsql_line_);
  } else if (0 == info->plsql_variable_.case_compare("PLSQL_UNIT") 
             || 0 == info->plsql_variable_.case_compare("PLSQL_UNIT_OWNER")) {
    OZ (get_plsql_unit(result, ctx.exec_ctx_.get_allocator(),
                       *ctx.exec_ctx_.get_my_session(), info->result_type_,
                       info->plsql_variable_));
  } else {
    ObString plsql_ccflags;
    // may plsql relative system variables, try it.
    if (OB_FAIL(ret)) {
    } else if (0 == info->plsql_variable_.case_compare("PLSQL_CCFLAGS")
        || OB_FAIL(ctx.exec_ctx_.get_my_session()->get_sys_variable_by_name(info->plsql_variable_,
                                                                            result))) {
      // not system variable
      if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret
          // plsql_ccflags may rewrite by itself.
          || 0 == info->plsql_variable_.case_compare("PLSQL_CCFLAGS")) {
        // then may plsql_ccflags, try it.
        ret = OB_SUCCESS;
        OX (plsql_ccflags = ctx.exec_ctx_.get_my_session()->get_plsql_ccflags());
        OZ (get_key_value(plsql_ccflags, info->plsql_variable_, result));
        OX (result.set_collation_type(ObCharset::get_system_collation()));
      } else {
        LOG_WARN("failed to get system variable by name", K(ret), K(info->plsql_variable_));
      }
    }
  }
  OZ(res.from_obj(result, expr.obj_datum_map_));
  if (is_lob_storage(result.get_type())) {
    OZ(ob_adjust_lob_datum(result, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), res));
  }
  OZ(expr.deep_copy_datum(ctx, res));
  return ret;
}

OB_DEF_SERIALIZE(ObPLSQLVariableInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              plsql_line_,
              plsql_variable_,
              result_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPLSQLVariableInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              plsql_line_,
              plsql_variable_,
              result_type_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPLSQLVariableInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              plsql_line_,
              plsql_variable_,
              result_type_);
  return len;
}

int ObPLSQLVariableInfo::deep_copy(common::ObIAllocator &allocator,
                         const ObExprOperatorType type,
                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObPLSQLVariableInfo &other = *static_cast<ObPLSQLVariableInfo *>(copied_info);
  other.plsql_line_ = plsql_line_;
  other.result_type_ = result_type_;
  OZ(ob_write_string(allocator, plsql_variable_, other.plsql_variable_));
  return ret;
}

template <typename RE>
int ObPLSQLVariableInfo::from_raw_expr(RE &raw_expr, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObPLSQLVariableRawExpr &expr 
    = const_cast<ObPLSQLVariableRawExpr &> (static_cast<const ObPLSQLVariableRawExpr&>(raw_expr));
  plsql_line_ = expr.get_plsql_line();
  OZ(ob_write_string(alloc, expr.get_plsql_variable(), plsql_variable_));
  return ret;
}

}
}
