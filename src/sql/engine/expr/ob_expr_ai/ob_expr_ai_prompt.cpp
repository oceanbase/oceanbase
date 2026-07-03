/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_prompt.h"
#include "ob_ai_func.h"
#include "lib/utility/utility.h"
#include "lib/json_type/ob_json_common.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
namespace
{
inline bool is_binary_like_type(const ObObjType type, const ObCollationType cs_type)
{
  return ob_is_blob(type, cs_type) || ob_is_varbinary_or_binary(type, cs_type) || ob_is_raw(type);
}

inline bool is_text_string_type(const ObObjType type, const ObCollationType cs_type)
{
  return ob_is_string_type(type)
      && !ob_is_varbinary_or_binary(type, cs_type)
      && !ob_is_blob(type, cs_type)
      && !ob_is_raw(type);
}
} // namespace

ObExprAIPrompt::ObExprAIPrompt(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                    T_FUN_SYS_AI_PROMPT,
                    N_AI_PROMPT,
                    MORE_THAN_ZERO,
                    VALID_FOR_GENERATED_COL,
                    NOT_ROW_DIMENSION)
{
}

ObExprAIPrompt::~ObExprAIPrompt()
{
}

int ObExprAIPrompt::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType template_type = types_stack[0];
  if (!is_text_string_type(template_type.get_type(), template_type.get_collation_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid template type", K(ret), K(template_type.get_type()));
  } else {
    types_stack[0].set_calc_type(ObVarcharType);
    types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  }

  for (int64_t i = 1; i < param_num && OB_SUCC(ret); i++) {
    const ObObjType arg_type = types_stack[i].get_type();
    const ObCollationType arg_cs_type = types_stack[i].get_collation_type();
    if (is_text_string_type(arg_type, arg_cs_type)) {
      types_stack[i].set_calc_type(ObVarcharType);
      types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (is_binary_like_type(arg_type, arg_cs_type)) {
      // keep binary-like type for eval stage image placeholder processing.
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid data type", K(ret), K(arg_type), K(arg_cs_type));
    }
  }

  if (OB_SUCC(ret)) {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  }
  return ret;
}

/*
 * eval_ai_prompt flow:
 *
 *   [eval template arg]
 *            |
 *            v
 *   [parse placeholders -> arg_types]
 *            |
 *            v
 *   [for each expr arg]
 *      |        \
 *      |         +-- invalid runtime type -> error
 *      v
 *   [read arg data]
 *      |        \
 *      |         +-- is {img_x} -> build image object (URL/BINARY)
 *      v
 *   [text/unknown -> append json string]
 *            |
 *            v
 *   [construct prompt object]
 *            |
 *            v
 *   [pack json result]
 */
int ObExprAIPrompt::eval_ai_prompt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *template_datum = NULL;
  ObExpr *arg0 = expr.args_[0];
  ObString template_str;
  ObJsonArray *args_array = NULL;
  ObJsonObject *prompt_object = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor tmp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_PROMPT));

  if (OB_FAIL(tmp_allocator.eval_arg(arg0, ctx, template_datum))) {
    LOG_WARN("fail to eval template arg", K(ret), K(arg0->datum_meta_));
  } else if (template_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid template argument", K(ret), K(arg0->datum_meta_.type_));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *template_datum, arg0->datum_meta_, arg0->obj_meta_.has_lob_header(), template_str))) {
    LOG_WARN("fail to read real string data", K(ret), K(arg0->datum_meta_), K(template_str));
  }

  ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
  for (int64_t i = 0; i < expr.arg_cnt_ - 1 && OB_SUCC(ret); ++i) {
    if (OB_FAIL(arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN))) {
      LOG_WARN("fail to init arg types", K(ret), K(i));
    }
  }
  // Parse template placeholders once, then drive per-arg runtime checks by placeholder type.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, expr.arg_cnt_ - 1, arg_types))) {
    LOG_WARN("fail to parse template arg types", K(ret), K(template_str));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(tmp_allocator, args_array))) {
    LOG_WARN("fail to get json array", K(ret));
  } else {
    for (int64_t i = 1; i < expr.arg_cnt_ && OB_SUCC(ret); i++) {
      ObDatum *datum = NULL;
      ObExpr *arg = expr.args_[i];
      ObString arg_str;
      ObJsonString *arg_json_str = NULL;
      const ObAIFuncPromptObjectUtils::PromptArgType prompt_arg_type = arg_types.at(i - 1);
      const bool is_unknown_arg = (prompt_arg_type == ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN);
      const bool is_image_arg = (prompt_arg_type == ObAIFuncPromptObjectUtils::PromptArgType::IMAGE);
      const ObObjType arg_type = arg->datum_meta_.type_;
      const ObCollationType arg_cs_type = arg->datum_meta_.cs_type_;
      const bool is_raw_arg = ob_is_raw(arg_type);
      const bool is_binary_like_arg = is_binary_like_type(arg_type, arg_cs_type);
      const bool is_text_string_arg = is_text_string_type(arg_type, arg_cs_type);
      if (OB_FAIL(tmp_allocator.eval_arg(arg, ctx, datum))) {
        LOG_WARN("fail to eval arg", K(ret), K(arg->datum_meta_));
      } else if (datum->is_null()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("invalid data type", K(ret), K(arg->datum_meta_.type_));
      } else if (!is_text_string_arg && !is_binary_like_arg) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("invalid arg type for placeholder", K(ret), K(arg->datum_meta_.type_), K(i), K(is_image_arg));
      } else {
        arg_str = datum->get_string();
        if (!is_raw_arg && OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator,
                                                                             *datum,
                                                                             arg->datum_meta_,
                                                                             arg->obj_meta_.has_lob_header(),
                                                                             arg_str))) {
          LOG_WARN("fail to read real arg", K(ret), K(arg->datum_meta_), K(i));
        } else if (is_image_arg) {
          ObJsonObject *image_obj = nullptr;
          const bool is_url_by_content = arg_str.prefix_match_ci(ObAIFuncPromptObjectUtils::HTTPS_PREFIX)
              || arg_str.prefix_match_ci(ObAIFuncPromptObjectUtils::HTTP_PREFIX);
          ObAIFuncPromptObjectUtils::ImageArgType image_arg_type =
              is_url_by_content ? ObAIFuncPromptObjectUtils::ImageArgType::URL
                               : ObAIFuncPromptObjectUtils::ImageArgType::BINARY;
          if (OB_FAIL(ObAIFuncPromptObjectUtils::build_image_arg_object(tmp_allocator, arg_str, image_arg_type, image_obj))) {
            LOG_WARN("fail to build image arg", K(ret), K(i));
          } else if (OB_FAIL(args_array->append(image_obj))) {
            LOG_WARN("fail to append image arg", K(ret), K(i));
          }
        } else if (!is_text_string_arg && !is_unknown_arg) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("binary-like type is not allowed for text placeholder", K(ret), K(arg->datum_meta_.type_), K(i));
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(tmp_allocator, arg_str, arg_json_str))) {
          LOG_WARN("fail to get json string", K(ret), K(arg_str));
        } else if (OB_FAIL(args_array->append(arg_json_str))) {
          LOG_WARN("fail to add item", K(ret), K(arg_str));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObAIFuncPromptObjectUtils::construct_prompt_object(tmp_allocator, template_str, args_array, prompt_object))) {
    LOG_WARN("fail to construct prompt object", K(ret));
  } else {
    ObString raw_str;
    if (OB_FAIL(prompt_object->get_raw_binary(raw_str, &tmp_allocator))) {
      LOG_WARN("json extarct get result binary failed", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }
  return ret;
}



int ObExprAIPrompt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprAIPrompt::eval_ai_prompt;
  return ret;
}

} // namespace sql
} // namespace oceanbase