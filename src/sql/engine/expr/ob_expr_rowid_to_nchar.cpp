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

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_rowid_to_nchar.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprRowIDToNChar::ObExprRowIDToNChar(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_ROWID_TO_NCHAR, N_ROWID_TO_NCHAR, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprRowIDToNChar::~ObExprRowIDToNChar()
{
}

int ObExprRowIDToNChar::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObBasicSessionInfo *session = type_ctx.get_session();

  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    type.set_nvarchar2();
    type.set_collation_type(session->get_nls_collation_nation());
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length_semantics(LS_CHAR);
    type.set_length(type1.get_length());

    type1.set_calc_type(ObURowIDType);
  }
  return ret;
}

int ObExprRowIDToNChar::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_rowid_to_nchar;
  return ret;
}

int ObExprRowIDToNChar::eval_rowid_to_nchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else {
    ObURowIDData rowid_data = arg->get_urowid();
    char *buf = NULL;
    char *utf16_buf = NULL;
    //const int32_t CharConvertFactorNum = 4; //最多使用4字节存储一个字符
    int64_t size = rowid_data.needed_base64_buffer_size();
    int64_t pos = 0;
    uint32_t res_len = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &alloc = alloc_guard.get_allocator();
    if (OB_ISNULL(buf = (char*)alloc.alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret), K(size));
    } else if (OB_FAIL(rowid_data.get_base64_str(buf, size, pos))) {
      LOG_WARN("encode rowid obj failed", K(ret));
    } else if (OB_ISNULL(utf16_buf = (char*)(expr.get_str_res_mem(ctx, pos * ObCharset::CharConvertFactorNum)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret), K(pos * ObCharset::CharConvertFactorNum));
    } else if (OB_FAIL(ObCharset::charset_convert(CS_TYPE_UTF8MB4_GENERAL_CI,
                                                  buf, pos, expr.datum_meta_.cs_type_,
                                                  utf16_buf, pos * ObCharset::CharConvertFactorNum, res_len))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      expr_datum.set_string(ObString(res_len, utf16_buf));
      LOG_DEBUG("calc rowid to nchar done", KPC(arg), K(expr_datum));
    }
  }
  return ret;
}

} /* sql */
} /* oceanbase */
