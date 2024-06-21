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
 * This file is for define of func rb expr helper
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_FUNC_HELPER_
#define OCEANBASE_SQL_OB_EXPR_RB_FUNC_HELPER_

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObRbExprHelper final
{
public:

  static int get_input_roaringbitmap_bin(ObEvalCtx &ctx, ObExpr *rb_arg, ObString &rb_bin, bool &is_rb_null);
  static int get_input_roaringbitmap(ObEvalCtx &ctx, ObExpr *rb_arg, ObRoaringBitmap *&rb, bool &is_rb_null);
  static int pack_rb_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const ObString &str);
  static uint64_t get_tenant_id(ObSQLSessionInfo *session);

private:
  // const static uint32_t RESERVE_MIN_BUFF_SIZE = 32;
  DISALLOW_COPY_AND_ASSIGN(ObRbExprHelper);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_FUNC_HELPER_