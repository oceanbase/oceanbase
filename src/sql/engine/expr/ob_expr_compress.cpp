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
#include "ob_expr_compress.h"

#include "sql/engine/ob_exec_context.h"
#include <zlib.h>
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

const int64_t COMPRESS_HEADER_LEN = 4;
const int32_t COMPRESS_HEADER_MASK = 0x3FFFFFFF;


ObExprCompress::ObExprCompress(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_COMPRESS, N_COMPRESS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCompress::~ObExprCompress()
{
}

int ObExprCompress::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  int64_t length = 0;
  int64_t mbmaxlen = 0;
  type.set_varbinary();
  type.set_collation_level(CS_LEVEL_COERCIBLE);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx.get_coll_type()));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));
  OZ (ObCharset::get_mbmaxlen_by_coll(tmp_type.get_collation_type(), mbmaxlen));
  OX (type.set_length(compressBound(type1.get_length() * mbmaxlen) + COMPRESS_HEADER_LEN + 1));
  return ret;
}

int ObExprCompress::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprCompress::eval_compress;
  return ret;
}

int ObExprCompress::eval_compress(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else if (arg->get_string().empty()) {
    expr_datum.set_string(ObString::make_empty_string());
  } else {
    const ObString& str_val = arg->get_string();
    uint64_t new_len = compressBound(str_val.length());
    const int64_t buf_len = new_len + COMPRESS_HEADER_LEN + 1;
    char *buf = expr.get_str_res_mem(ctx, buf_len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_UNLIKELY(Z_OK != compress(reinterpret_cast<unsigned char*>(buf + COMPRESS_HEADER_LEN), &new_len,
        reinterpret_cast<const unsigned char*>(str_val.ptr()), str_val.length()))) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LOG_WARN("fail to compress data", K(ret));
    } else {
      int32_t compress_header = str_val.length() & COMPRESS_HEADER_MASK;
      MEMCPY(buf, &compress_header, sizeof(compress_header));
      expr_datum.set_string(buf, new_len + COMPRESS_HEADER_LEN);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprCompress, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

ObExprUncompress::ObExprUncompress(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UNCOMPRESS, N_UNCOMPRESS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUncompress::~ObExprUncompress()
{
}

int ObExprUncompress::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_BINARY);
  type.set_blob();
  type.set_collation_level(CS_LEVEL_COERCIBLE);
  type.set_length(OB_MAX_MYSQL_VARCHAR_LENGTH);
  return ret;
}

int ObExprUncompress::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprUncompress::eval_uncompress;
  return ret;
}

static int eval_uncompress_length(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum,
                                  const ObString &str_val,
                                  uint64_t &orig_len,
                                  bool &not_final)
{
  int ret = OB_SUCCESS;
  if (str_val.empty()) {
    orig_len = 0;
    not_final = true;
  } else if (OB_UNLIKELY((str_val.length() <= COMPRESS_HEADER_LEN))) {
    expr_datum.set_null();
    LOG_USER_WARN(OB_ERR_ZLIB_DATA);
  } else {
    int32_t compress_header = 0;
    int64_t max_size = 0;
    MEMCPY(&compress_header, str_val.ptr(), sizeof(compress_header));
    orig_len = compress_header & COMPRESS_HEADER_MASK;
    if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_max_allowed_packet(max_size))) {
      LOG_WARN("get max allowed packet failed", K(ret));
    } else if (OB_UNLIKELY(orig_len > max_size)) {
      expr_datum.set_null();
      LOG_WARN("orig_len is larger than max_allow_packet", K(orig_len), K(max_size), K(ret));
      LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "uncompress", static_cast<int>(max_size));
    } else {
      not_final = true;
    }
  }
  return ret;
}

int ObExprUncompress::eval_uncompress(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObObjType type = expr.args_[0]->datum_meta_.type_;
  uint64_t orig_len = 0;
  bool not_final = false;
  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else {
    const ObString& str_val = arg->get_string();
    // input always varchar,  result always blob
    ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
    if (OB_FAIL(eval_uncompress_length(expr, ctx, expr_datum, str_val, orig_len, not_final))) {
      LOG_WARN("eval uncompress length failed", K(ret));
    } else if (not_final) {
      char *buf = NULL;
      int64_t buf_size = 0;
      if (OB_FAIL(output_result.init(orig_len))) {
        LOG_WARN("init stringtext result failed");
      } else if (orig_len > 0) {
        if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
          LOG_WARN("stringtext result reserve buffer failed");
        } else if (OB_UNLIKELY(Z_OK != uncompress(reinterpret_cast<unsigned char*>(buf), &orig_len,
            reinterpret_cast<const unsigned char*>(str_val.ptr() + COMPRESS_HEADER_LEN), str_val.length()))) {
          expr_datum.set_null();
          LOG_USER_WARN(OB_ERR_ZLIB_DATA);
        } else if (OB_FAIL(output_result.lseek(orig_len, 0))) {
          LOG_WARN("result lseek failed", K(ret));
        } else {
          output_result.set_result();
        }
      } else {
        output_result.set_result();
      }
    }
  }
  return ret;
}

ObExprUncompressedLength::ObExprUncompressedLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UNCOMPRESSED_LENGTH, N_UNCOMPRESSED_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUncompressedLength::~ObExprUncompressedLength()
{
}

int ObExprUncompressedLength::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_BINARY);
  type.set_int32();
  return ret;
}

int ObExprUncompressedLength::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprUncompressedLength::eval_uncompressed_length;
  return ret;
}

int ObExprUncompressedLength::eval_uncompressed_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else if (arg->get_string().empty()) {
    expr_datum.set_int(0);
  } else if (OB_UNLIKELY((arg->len_ <= COMPRESS_HEADER_LEN))) {
    expr_datum.set_int(0);
    LOG_USER_WARN(OB_ERR_ZLIB_DATA);
  } else {
    const ObString& str_val = arg->get_string();
    int32_t compress_header = 0;
    uint64_t orig_len = 0;
    MEMCPY(&compress_header, str_val.ptr(), sizeof(compress_header));
    orig_len = compress_header & COMPRESS_HEADER_MASK;
    expr_datum.set_int(orig_len);
  }
  return ret;
}

}
}
