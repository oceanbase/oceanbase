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

#include "ob_expr_pl_sqlcode_sqlerrm.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"

namespace oceanbase
{
using namespace common;
using namespace pl;

namespace sql
{
OB_SERIALIZE_MEMBER((ObExprPLSQLCodeSQLErrm, ObFuncExprOperator));

ObExprPLSQLCodeSQLErrm::ObExprPLSQLCodeSQLErrm(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_PL_SQLCODE_SQLERRM, N_PL_GET_SQLCODE_SQLERRM, ZERO_OR_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                        false, INTERNAL_IN_ORACLE_MODE)
      , is_sqlcode_(true)
{}

ObExprPLSQLCodeSQLErrm::~ObExprPLSQLCodeSQLErrm() {}

int ObExprPLSQLCodeSQLErrm::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprPLSQLCodeSQLErrm *tmp = dynamic_cast<const ObExprPLSQLCodeSQLErrm *>(&other);
  if (OB_UNLIKELY(OB_ISNULL(tmp))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(other), K(ret));
  } else if (OB_LIKELY(this != tmp)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(other), K(ret));
    } else {
      this->is_sqlcode_ = tmp->is_sqlcode_;
    }
  }
  return ret;
}

int ObExprPLSQLCodeSQLErrm::calc_result_typeN(ObExprResType &type,
                                             ObExprResType *types,
                                             int64_t param_num,
                                             common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number of arguments", K(param_num), K(ret));
  } else {
    if (is_sqlcode_) {
      type.set_int32();
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    } else {
      const ObLengthSemantics default_length_semantics
        = (OB_NOT_NULL(type_ctx.get_session())
          ? type_ctx.get_session()->get_actual_nls_length_semantics()
          : LS_BYTE);
      type.set_varchar();
      type.set_length_semantics(default_length_semantics);
      type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
      type.set_collation_level(common::CS_LEVEL_IMPLICIT);
      type.set_collation_type(type_ctx.get_coll_type());
    }
    if (1 == param_num) {
      types[0].set_calc_type(ObIntType);
    }
  }
  return ret;
}

int ObExprPLSQLCodeSQLErrm::cg_expr(
    ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  const ObPLSQLCodeSQLErrmRawExpr &pl_expr = static_cast<const ObPLSQLCodeSQLErrmRawExpr &>(raw_expr);
  rt_expr.extra_ = static_cast<uint64_t>(pl_expr.get_is_sqlcode());
  rt_expr.eval_func_ = &eval_pl_sql_code_errm;

  return ret;
}

int ObExprPLSQLCodeSQLErrm::eval_pl_sql_code_errm(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  pl::ObPLSqlCodeInfo *sqlcode_info = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  bool is_sqlcode = (expr.extra_ == 1);
  CK(OB_NOT_NULL(session));
  CK(expr.arg_cnt_ <= 1);
  CK (OB_NOT_NULL(sqlcode_info = session->get_pl_sqlcode_info()));
  if (OB_FAIL(ret)) {
  } else if (is_sqlcode) {
    CK (OB_LIKELY(0 == expr.arg_cnt_));
    expr_datum.set_int(sqlcode_info->get_sqlcode());
  } else {
    char *sqlerrm_result = NULL;
    int64_t pos = 0, max_buf_size = 200;
    if (0 == expr.arg_cnt_) {
      int64_t sqlcode = sqlcode_info->get_sqlcode();
      if (0 == sqlcode) {
        CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
        OZ (databuff_printf(sqlerrm_result, max_buf_size, pos, "ORA-0000: normal, successful completion"));
      } else if (sqlcode > 0) {
        CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
        OZ (databuff_printf(sqlerrm_result, max_buf_size, pos, "User-Defined Exception"));
      } else if (sqlcode >= OB_MIN_RAISE_APPLICATION_ERROR
                 && sqlcode <= OB_MAX_RAISE_APPLICATION_ERROR) {
        max_buf_size = 30 + sqlcode_info->get_sqlmsg().length(); // ORA-CODE: ERRMSG
        CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
        OZ (databuff_printf(sqlerrm_result, max_buf_size, pos,
                           "ORA%ld: %.*s", sqlcode,
                           sqlcode_info->get_sqlmsg().length(),
                           sqlcode_info->get_sqlmsg().ptr()));
      } else {
        const ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
        if (OB_LIKELY(NULL != wb) && wb->get_err_code() == sqlcode) {
          sqlerrm_result = const_cast<char *>(wb->get_err_msg());
        } else {
          const char* err_msg = ob_errpkt_strerror(sqlcode, true);
          if (NULL == err_msg) {
            CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
            OZ (databuff_printf(sqlerrm_result, max_buf_size, pos,
                "ORA%ld: Message error_code not found; product=RDBMS; facility=ORA", sqlcode));
          } else {
            sqlerrm_result = const_cast<char*>(err_msg);
          }
        }
      }
      OX (expr_datum.set_string(sqlerrm_result, strlen(sqlerrm_result)));
    } else if (1 == expr.arg_cnt_) {
      ObDatum *datum = NULL;
      int64_t sqlcode = 0;
      if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
        LOG_WARN("eval arg failed", K(ret), K(expr));
      } else if (FALSE_IT(sqlcode = datum->get_int())) {
        // do nothing
      } else if (sqlcode > 0) {
        CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
        OZ (databuff_printf(sqlerrm_result, 200, pos, "-%ld: non-ORACLE exception", sqlcode));
      } else if (sqlcode == 0) {
        CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
        OZ (databuff_printf(sqlerrm_result, 200, pos, "ORA-0000: normal, successful completion"));
      } else if (sqlcode >= OB_MIN_RAISE_APPLICATION_ERROR
                 && sqlcode <= OB_MAX_RAISE_APPLICATION_ERROR) {
        if (sqlcode_info->get_sqlcode() == sqlcode) {
          max_buf_size = 30 + sqlcode_info->get_sqlmsg().length(); // ORA-CODE: ERRMSG
          CK (OB_NOT_NULL(
            sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
          OZ (databuff_printf(sqlerrm_result, max_buf_size, pos,
                             "ORA%ld: %.*s", sqlcode,
                             sqlcode_info->get_sqlmsg().length(),
                             sqlcode_info->get_sqlmsg().ptr()));
        } else {
          CK (OB_NOT_NULL(sqlerrm_result
            = expr.get_str_res_mem(ctx, max_buf_size)));
          OZ (databuff_printf(sqlerrm_result, 200, pos, "ORA%ld:", sqlcode));
        }
      } else if (sqlcode < 0) {
        const ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
        if (OB_LIKELY(NULL != wb) && wb->get_err_code() == sqlcode) {
          sqlerrm_result = const_cast<char *>(wb->get_err_msg());
        } else {
          const char* err_msg = ob_errpkt_str_user_error(sqlcode, true);
          if (NULL == err_msg) {
            CK (OB_NOT_NULL(sqlerrm_result = expr.get_str_res_mem(ctx, max_buf_size)));
            OZ (databuff_printf(sqlerrm_result, 200, pos, "ORA%ld: Message error_code not found; product=RDBMS; facility=ORA", sqlcode));
          } else {
            sqlerrm_result = const_cast<char*>(err_msg);
          }
        }
      }
      OX (expr_datum.set_string(sqlerrm_result, strlen(sqlerrm_result)));
    }
  }

  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprPLSQLCodeSQLErrm, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
