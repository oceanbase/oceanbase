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
 * This file is for implement of func json expr helper
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_rb_utils.h"

// #include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
// #include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
// #include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

int ObRbExprHelper::get_input_roaringbitmap_bin(ObEvalCtx &ctx, ObExpr *rb_arg,  ObString &rb_bin, bool &is_rb_null)
{
  INIT_SUCC(ret);
  ObDatum *rb_datum;
  ObEvalCtx::TempAllocGuard ctx_alloc_g(ctx);
  common::ObArenaAllocator &allocator = ctx_alloc_g.get_allocator();
  if (OB_FAIL(rb_arg->eval(ctx, rb_datum))) {
    LOG_WARN("eval roaringbitmap args failed", K(ret));
  } else if (rb_datum->is_null()) {
    is_rb_null = true;
  } else if (OB_FALSE_IT(rb_bin = rb_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                         allocator,
                         *rb_datum,
                         rb_arg->datum_meta_,
                         rb_arg->obj_meta_.has_lob_header(),
                         rb_bin))) {
    LOG_WARN("fail to get real string data", K(ret), K(rb_bin));
  } else if (rb_bin.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("roaringbitmap binary is empty", K(ret), K(rb_bin));
  }
  return ret;
}

int ObRbExprHelper::get_input_roaringbitmap(ObEvalCtx &ctx, ObExpr *rb_arg, ObRoaringBitmap *&rb, bool &is_rb_null)
{
  INIT_SUCC(ret);
  ObString rb_bin = nullptr;
  ObEvalCtx::TempAllocGuard ctx_alloc_g(ctx);
  common::ObArenaAllocator &allocator = ctx_alloc_g.get_allocator();
  if (OB_FAIL(get_input_roaringbitmap_bin(ctx, rb_arg, rb_bin, is_rb_null))) {
    LOG_WARN("failed to get input roaringbitmap binary", K(ret));
  } else if (!is_rb_null && OB_FAIL(ObRbUtils::rb_deserialize(allocator, rb_bin, rb))) {
    LOG_WARN("failed to deserialize roaringbitmap", K(ret));
  }
  return ret;
}

int ObRbExprHelper::pack_rb_res(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const ObString &str)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  if (OB_FAIL(text_result.init(str.length()))) {
    LOG_WARN("init lob result failed");
  } else if (OB_FAIL(text_result.append(str.ptr(), str.length()))) {
    LOG_WARN("failed to append realdata", K(ret), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

uint64_t ObRbExprHelper::get_tenant_id(ObSQLSessionInfo *session)
{
  uint64_t tenant_id = 0;
  if (OB_ISNULL(session)) {
  } else if (session->get_ddl_info().is_ddl_check_default_value()) {
    tenant_id = OB_SERVER_TENANT_ID;
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return tenant_id;
}

} // namespace sql
} // namespace oceanbase
