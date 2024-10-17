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
#include "sql/engine/expr/ob_expr_hello_repeat.h"
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
const char ObExprHelloRepeat::hello_ob[] = "Hello, OceanBase!";
const int64_t ObExprHelloRepeat::hello_len = sizeof(hello_ob) - 1;
ObExprHelloRepeat::ObExprHelloRepeat(common::ObIAllocator& alloc)
    :ObStringExprOperator(alloc, T_FUN_SYS_HELLO_REPEAT, "hello_repeat", 1, NOT_VALID_FOR_GENERATED_COL)
{
}

int ObExprHelloRepeat::calc_result_type1(ObExprResType& type, ObExprResType &count, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(MAX_CHAR_LENGTH_FOR_VARCAHR_RESULT);
  return ret;
}

int ObExprHelloRepeat::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprHelloRepeat::eval;
  return OB_SUCCESS;
}

int ObExprHelloRepeat::eval(const ObExpr& expr, ObEvalCtx &ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *count = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObIAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObString text_str;
  if (OB_FAIL(expr.args_[0]->eval(ctx, count))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (count->is_null()) {
    expr_datum.set_null();
  } else {
    ObExprStrResAlloc expr_res_alloc(expr, ctx);
    ObString output;
    ret = repeat(output, count->get_int(), expr_res_alloc);
    if (OB_FAIL(ret)) {
      LOG_WARN("do repeat failed", K(ret));
    } else {
      expr_datum.set_string(output);
    }
  }
  return ret;
}

int ObExprHelloRepeat::repeat(ObString &output,
                         const int64_t count,
                         common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (count <= 0) {
    output.assign_ptr(NULL, 0);
  } else {
    int64_t tot_length = hello_len;
    char *buf = static_cast<char *>(allocator.alloc(tot_length));
    if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(tot_length));
    } else {
        MEMCPY(buf, hello_ob, hello_len);
        output.assign_ptr(buf, static_cast<int32_t>(tot_length));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase