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
#include "ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_rb_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

int ObRbExprHelper::get_input_roaringbitmap_bin(ObEvalCtx &ctx, ObIAllocator &allocator, ObExpr *rb_arg,  ObString &rb_bin, bool &is_rb_null)
{
  INIT_SUCC(ret);
  ObDatum *rb_datum = nullptr;
  ObString get_str;
  if (OB_FAIL(rb_arg->eval(ctx, rb_datum))) {
    LOG_WARN("eval roaringbitmap args failed", K(ret));
  } else if (rb_datum->is_null()) {
    is_rb_null = true;
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                         allocator,
                         *rb_datum,
                         rb_arg->datum_meta_,
                         rb_arg->obj_meta_.has_lob_header(),
                         get_str))) {
    LOG_WARN("fail to get real string data", K(ret), K(get_str));
  } else if (rb_arg->datum_meta_.type_ != ObRoaringBitmapType) {
    if (OB_FAIL(ObRbUtils::build_binary(allocator, get_str, rb_bin))) {
      LOG_WARN("failed to build roaringbitmap from binary", K(ret), K(get_str));
    }
  } else {
    rb_bin.assign_ptr(get_str.ptr(), get_str.length());
  }
  return ret;
}

int ObRbExprHelper::get_input_roaringbitmap(ObEvalCtx &ctx, ObIAllocator &allocator, ObExpr *rb_arg, ObRoaringBitmap *&rb, bool &is_rb_null)
{
  INIT_SUCC(ret);
  ObDatum *rb_datum = nullptr;
  ObString get_str;
  if (OB_FAIL(rb_arg->eval(ctx, rb_datum))) {
    LOG_WARN("eval roaringbitmap args failed", K(ret));
  } else if (rb_datum->is_null()) {
    is_rb_null = true;
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                         allocator,
                         *rb_datum,
                         rb_arg->datum_meta_,
                         rb_arg->obj_meta_.has_lob_header(),
                         get_str))) {
    LOG_WARN("fail to get real string data", K(ret), K(get_str));
  } else if (rb_arg->datum_meta_.type_ != ObRoaringBitmapType) {
    bool need_validate = true;
    if (OB_FAIL(ObRbUtils::check_binary(get_str))) {
      LOG_WARN("invalid roaringbitmap binary string", K(ret));
    } else if (OB_FAIL(ObRbUtils::rb_deserialize(allocator, get_str, rb, need_validate))) {
      LOG_WARN("failed to deserialize roaringbitmap", K(ret));
    }
  } else {
    if (OB_FAIL(ObRbUtils::rb_deserialize(allocator, get_str, rb))) {
      LOG_WARN("failed to deserialize roaringbitmap", K(ret));
    }
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
