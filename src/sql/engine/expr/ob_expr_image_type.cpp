/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_image_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "common/object/ob_object.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprImageType::ObExprImageType(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_IMAGE_TYPE, N_IMAGE_TYPE, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprImageType::~ObExprImageType()
{
}

int ObExprImageType::calc_result_type1(ObExprResType &type,
                                       ObExprResType &img,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  // Accept varchar, varbinary, binary, blob, text, longtext
  if (ob_is_null(img.get_type())
      || ob_is_varchar(img.get_type(), img.get_collation_type())
      || ob_is_varbinary_or_binary(img.get_type(), img.get_collation_type())
      || ob_is_blob(img.get_type(), img.get_collation_type())
      || ob_is_text_tc(img.get_type())
      || ob_is_json(img.get_type())) {
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    type.set_length(16); // max image type length is "x-icon" (6),预留16位扩展
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("wrong type of argument in function image_type", K(ret), K(img));
  }

  return ret;
}

int ObExprImageType::eval_image_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObString type_str;
  char *result_buf = NULL;

  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    ObExpr *input_expr = expr.args_[0];
    ObObjType input_type = input_expr->datum_meta_.get_type();
    ObCollationType input_cs_type = input_expr->datum_meta_.cs_type_;

    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();

    // Input as binary image data: varchar/char, varbinary/binary, blob, text
    if (ObVarcharType == input_type || ObCharType == input_type) {
      ObString input_str = arg->get_string();
      ObAiFuncImageUtils::ObImageType img_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
      ret = ObAiFuncImageUtils::get_type_from_binary(calc_alloc, input_str, img_type);
      if (OB_FAIL(ret)) {
        if (OB_INVALID_DATA == ret) {
          type_str = ObString(7, "UNKNOWN");
          ret = OB_SUCCESS;
        }
      } else {
        const char *s = ObAiFuncImageUtils::get_image_type_str(img_type);
        type_str = ObString(static_cast<int32_t>(strlen(s)), s);
      }
    } else if (ob_is_varbinary_or_binary(input_type, input_cs_type)) {
      ObString input_str = arg->get_string();
      ObAiFuncImageUtils::ObImageType img_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
      ret = ObAiFuncImageUtils::get_type_from_binary(calc_alloc, input_str, img_type);
      if (OB_FAIL(ret)) {
        if (OB_INVALID_DATA == ret) {
          type_str = ObString(7, "UNKNOWN");
          ret = OB_SUCCESS;
        }
      } else {
        const char *s = ObAiFuncImageUtils::get_image_type_str(img_type);
        type_str = ObString(static_cast<int32_t>(strlen(s)), s);
      }
    } else if (ob_is_blob(input_type, input_cs_type) ||
               ob_is_text_tc(input_type)) {
      ObString input_str = arg->get_string();
      ObString lob_data;
      // Use prefix read to avoid loading full lob data for huge images
      // For image type detection, only first 64 bytes are needed
      if (input_str.length() > 0) {
        ObTextStringIter str_iter(input_type, input_cs_type, input_str,
                                  input_expr->obj_meta_.has_lob_header());
        // Read only prefix bytes for type detection (64 bytes is enough for all supported formats)
        uint32_t prefix_byte_len = 64;
        if (OB_FAIL(str_iter.init(prefix_byte_len, NULL, &calc_alloc, NULL))) {
          LOG_WARN("text string iter init failed", K(ret), K(input_type));
        } else if (OB_FAIL(str_iter.get_inrow_or_outrow_prefix_data(lob_data, prefix_byte_len))) {
          LOG_WARN("get lob prefix data failed", K(ret), K(input_type));
        } else {
          ObAiFuncImageUtils::ObImageType img_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
          ret = ObAiFuncImageUtils::get_type_from_binary(calc_alloc, lob_data, img_type);
          if (OB_FAIL(ret)) {
            if (OB_INVALID_DATA == ret) {
              type_str = ObString(7, "UNKNOWN");
              ret = OB_SUCCESS;
            }
          } else {
            const char *s = ObAiFuncImageUtils::get_image_type_str(img_type);
            type_str = ObString(static_cast<int32_t>(strlen(s)), s);
          }
        }
      } else {
        type_str = ObString(7, "UNKNOWN");
      }
    } else {
      type_str = ObString(7, "UNKNOWN");
    }

    if (OB_SUCC(ret) && !type_str.empty()) {
      int64_t res_len = type_str.length();
      result_buf = static_cast<char*>(calc_alloc.alloc(res_len));
      if (OB_ISNULL(result_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(res_len));
      } else {
        MEMCPY(result_buf, type_str.ptr(), res_len);
        res.set_string(result_buf, res_len);
      }
    }
  }

  return ret;
}

int ObExprImageType::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_image_type;
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
