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
#include "sql/engine/expr/ob_expr_whitespace_tokenize.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

////////////////////////////////////////////////////////////////
ObExprWhitespaceTokenize::ObExprWhitespaceTokenize(common::ObIAllocator& alloc)
    :ObStringExprOperator(alloc, T_FUN_SYS_WHITESPACE_TOKENIZE, "whitespace_tokenize", 1, NOT_VALID_FOR_GENERATED_COL)
{
}

int ObExprWhitespaceTokenize::calc_result_type1(ObExprResType& type, ObExprResType &text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(MAX_CHAR_LENGTH_FOR_VARCAHR_RESULT);
  return ret;
}

int ObExprWhitespaceTokenize::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  CK((rt_expr.arg_cnt_ == 1));
  if (OB_SUCC(ret)) {
    // do register
    rt_expr.eval_func_ = ObExprWhitespaceTokenize::eval_tokenize;
  }
  return OB_SUCCESS;
}

int ObExprWhitespaceTokenize::eval_tokenize(const ObExpr& expr, ObEvalCtx &ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObIAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(expr.args_[0]->eval(ctx, text))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (text->is_null()) {
    expr_datum.set_null();
  } else {
    ObExprStrResAlloc expr_res_alloc(expr, ctx);
    ObString output;
    ret = tokenize(output, text->get_string(), expr_res_alloc);
    if (OB_FAIL(ret)) {
      LOG_WARN("do tokenize failed", K(ret));
    } else {
      expr_datum.set_string(output);
    }
  }
  return ret;
}

int ObExprWhitespaceTokenize::tokenize(ObString &output, const ObString &text, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  int64_t tot_length = text.length() + 2;
  char *buf = static_cast<char *>(allocator.alloc(tot_length));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(tot_length));
  } else {
    output.assign_buffer(buf, static_cast<int32_t>(tot_length));
    output.write("[", 1);
    output.write(text.ptr(), text.length());
    output.write("]", 1);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase