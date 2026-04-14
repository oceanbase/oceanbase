/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_complete.h"
#include "observer/omt/ob_tenant_ai_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAIComplete::ObExprAIComplete(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                        T_FUN_SYS_AI_COMPLETE,
                        N_AI_COMPLETE,
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL,
                        NOT_ROW_DIMENSION)
{
}

ObExprAIComplete::~ObExprAIComplete()
{
}

int ObExprAIComplete::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types_stack,
                                        int64_t param_num,
                                        common::ObExprTypeCtx &type_ctx) const
{

  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2 || param_num > 3)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (ob_is_string_type(types_stack[PROMPT_IDX].get_type())) {
      types_stack[PROMPT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (ob_is_json(types_stack[PROMPT_IDX].get_type())) {
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid param type", K(ret), K(types_stack[PROMPT_IDX]));
    }

    if (OB_FAIL(ret)) {
    } else if (param_num == 3) {
      ObObjType in_type = types_stack[CONFIG_IDX].get_type();
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, CONFIG_IDX, N_AI_COMPLETE))) {
        LOG_WARN("wrong type for json config.", K(ret), K(types_stack[CONFIG_IDX].get_type()));
      } else if (ob_is_string_type(in_type) && types_stack[CONFIG_IDX].get_collation_type() != CS_TYPE_BINARY) {
        if (types_stack[CONFIG_IDX].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[CONFIG_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
    if (OB_SUCC(ret)) {
      type.set_type(ObLongTextType);
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
    }
  }
  return ret;
}

int ObExprAIComplete::eval_ai_complete(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_prompt = nullptr;
  ObDatum *arg_config = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_prompt, arg_config))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (arg_model_id->is_null() || arg_prompt->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameters is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, parameters is null");
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_COMPLETE));
    ObString model_id = arg_model_id->get_string();
    ObString prompt;
    ObJsonObject *prompt_object = nullptr;
    ObJsonObject *config = nullptr;
    ObExpr *arg_expr_prompt = expr.args_[PROMPT_IDX];

    share::ObAIModelConfigInfo model_config;
    if (OB_FAIL(ObAIFuncUtils::get_model_config_info(temp_allocator, model_id, model_config))) {
      LOG_WARN("failed to get model config info", K(ret), K(model_id));
    } else if ( OB_ISNULL(arg_expr_prompt) ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg_expr_prompt is null", K(ret));
    } else if (arg_expr_prompt->datum_meta_.type_ == ObJsonType) {
      ObIJsonBase *j_base = nullptr;
      ObJsonString *template_json = nullptr;
      ObJsonArray *args_array = nullptr;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, PROMPT_IDX, j_base, is_null))) {
        LOG_WARN("get_json_doc failed", K(ret));
      } else if (is_null || OB_ISNULL(j_base)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("prompt json is null", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt is null");
      } else if (j_base->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("j_base is not json object", K(ret));
      } else if (OB_FALSE_IT(prompt_object = static_cast<ObJsonObject *>(j_base))) {
      } else if (!ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("prompt is not valid", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt is not valid");
      } else if (OB_ISNULL(template_json = static_cast<ObJsonString *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key)))
              || OB_ISNULL(args_array = static_cast<ObJsonArray *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prompt object fields is null", K(ret));
      } else {
        bool has_image_placeholder = false;
        ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
        const int64_t arg_cnt = args_array->element_count();
        for (int64_t i = 0; OB_SUCC(ret) && i < arg_cnt; ++i) {
          if (OB_FAIL(arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN))) {
            LOG_WARN("failed to push back prompt arg type", K(ret), K(i));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObAIFuncPromptObjectUtils::parse_template_arg_types(template_json->get_str(),
                                                                                arg_cnt,
                                                                                arg_types,
                                                                                &has_image_placeholder))) {
          LOG_WARN("failed to parse template arg types", K(ret));
        } else if (!has_image_placeholder) {
          if (OB_FAIL(ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(temp_allocator, prompt_object, prompt))) {
            LOG_WARN("failed to replace all str args in template", K(ret));
          } else {
            prompt_object = nullptr;
          }
        } else if (OB_FAIL(ObAIFuncPromptObjectUtils::validate_binary_image_sizes(prompt_object, model_config.get_max_image_size()))) {
          LOG_WARN("failed to validate binary image sizes", K(ret));
        }
      }
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *arg_prompt, expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), prompt))) {
      LOG_WARN("fail to get real string data", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_config)) {
      ObIJsonBase *j_base = nullptr;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, CONFIG_IDX, j_base, is_null))) {
        LOG_WARN("get_json_doc failed", K(ret));
      } else if (is_null || OB_ISNULL(j_base)) {
        // config is null, skip
      } else if (j_base->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("j_base is not json object", K(ret));
      } else {
        config = static_cast<ObJsonObject *>(j_base);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (model_id.empty() || (prompt.empty() && OB_ISNULL(prompt_object))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model id or input is empty", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, model id or input is empty");
    } else {
      ObAIServiceClient client(temp_allocator);
      ObString result;
      if (OB_FAIL(client.init(&model_config))) {
        LOG_WARN("fail to init model", K(ret), K(model_id));
      } else if (OB_FAIL(client.call_completion(prompt, prompt_object, config, result))) {
        LOG_WARN("fail to call model completion", K(ret), K(model_id), K(prompt), K(prompt_object), K(config));
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
        LOG_WARN("fail to set string result", K(ret), K(model_id), K(prompt), K(prompt_object), K(config));
      }
    }
  }
  return ret;
}

int ObExprAIComplete::pack_complete_response_to_indices(const ObExpr &expr,
                                                        ObEvalCtx &ctx,
                                                        common::ObIAllocator &allocator,
                                                        common::ObJsonObject *response,
                                                        const common::ObArray<int64_t> &row_indices,
                                                        const share::ObAiModelEndpointInfo &endpoint_info,
                                                        common::ObIVector *res_vec)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *output = nullptr;
  ObString raw_str;
  if (OB_FAIL(ObAIFuncUtils::parse_complete_output(allocator, endpoint_info, response, output))) {
    LOG_WARN("fail to parse complete output", K(ret));
  } else if (OB_ISNULL(output)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("complete output is null", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, output, raw_str))) {
    LOG_WARN("fail to print json to str", K(ret));
  } else {
    // Write the same result to all row indices (typically only 1)
    for (int64_t i = 0; OB_SUCC(ret) && i < row_indices.count(); ++i) {
      if (OB_FAIL(ObAIFuncJsonUtils::inner_pack_raw_str_to_res(raw_str, expr, ctx, res_vec, row_indices.at(i)))) {
        LOG_WARN("fail to pack raw str to res", K(ret), K(i), K(row_indices.at(i)));
      }
    }
  }
  return ret;
}

int ObExprAIComplete::eval_ai_complete_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                              const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[MODEL_IDX]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval model vector failed", K(ret));
  } else if (OB_FAIL(expr.args_[PROMPT_IDX]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval prompt vector failed", K(ret));
  } else if (expr.arg_cnt_ > CONFIG_IDX && OB_FAIL(expr.args_[CONFIG_IDX]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval config vector failed", K(ret));
  } else {
    ObIVector *model_vec = expr.args_[MODEL_IDX]->get_vector(ctx);
    ObIVector *prompt_vec = expr.args_[PROMPT_IDX]->get_vector(ctx);
    ObIVector *config_vec = expr.arg_cnt_ > CONFIG_IDX ? expr.args_[CONFIG_IDX]->get_vector(ctx) : nullptr;
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObExpr *arg_expr_prompt = expr.args_[PROMPT_IDX];
    const bool prompt_is_json = (arg_expr_prompt->datum_meta_.type_ == ObJsonType);
    const bool model_is_const = expr.args_[MODEL_IDX]->is_batch_result();
    const bool config_is_const = (expr.arg_cnt_ > CONFIG_IDX) && expr.args_[CONFIG_IDX]->is_batch_result();

    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor batch_arena(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    ObArenaAllocator const_arena(lib::ObMemAttr(tenant_id, "AIComplConst"));
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_COMPLETE));

    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    }

    ObString const_model_id;
    ObString const_config_str;
    ObJsonObject *const_config = nullptr;
    const bool config_is_json = (expr.arg_cnt_ > CONFIG_IDX)
                                && (expr.args_[CONFIG_IDX]->datum_meta_.type_ == ObJsonType);
    const ObJsonInType config_json_in_type = config_is_json
        ? ObJsonExprHelper::get_json_internal_type(expr.args_[CONFIG_IDX]->datum_meta_.type_)
        : ObJsonInType::JSON_TREE;
    share::ObAIModelConfigInfo cur_model_config;
    ObString cur_model_id;
    common::ObAIFuncBatchState pending;

    if (OB_FAIL(ret)) {
    } else if (!model_is_const) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                   batch_arena, model_vec, expr.args_[MODEL_IDX]->datum_meta_,
                   expr.args_[MODEL_IDX]->obj_meta_.has_lob_header(),
                   const_model_id, bound.start()))) {
      LOG_WARN("fail to read const model_id", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::get_model_config_info(batch_arena, const_model_id, cur_model_config))) {
      LOG_WARN("fail to get model config info for const model", K(ret), K(const_model_id));
    } else {
      cur_model_id = const_model_id;
    }

    if (OB_FAIL(ret)) {
    } else if (!config_is_const || OB_ISNULL(config_vec)) {
    } else if (config_vec->is_null(bound.start())) {
    } else {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                     const_arena, config_vec, expr.args_[CONFIG_IDX]->datum_meta_,
                     expr.args_[CONFIG_IDX]->obj_meta_.has_lob_header(),
                     const_config_str, bound.start()))) {
        LOG_WARN("fail to read const config", K(ret));
      } else if (!const_config_str.empty()) {
        ObIJsonBase *j_base = nullptr;
        if (OB_FAIL(ObJsonBaseFactory::get_json_base(&const_arena, const_config_str, config_json_in_type,
                                                     ObJsonInType::JSON_TREE, j_base, 0))) {
          LOG_WARN("fail to parse const config json", K(ret));
        } else if (OB_ISNULL(j_base) || j_base->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("const config is not json object", K(ret));
        } else {
          const_config = static_cast<ObJsonObject *>(j_base);
        }
      }
    }

    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (model_vec->is_null(idx) || prompt_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }

      ObString model_id_i;
      if (model_is_const) {
        model_id_i = const_model_id;
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                     batch_arena, model_vec, expr.args_[MODEL_IDX]->datum_meta_,
                     expr.args_[MODEL_IDX]->obj_meta_.has_lob_header(),
                     model_id_i, idx))) {
        LOG_WARN("fail to read model_id", K(ret), K(idx));
      }

      // Flush pending batch if model changed, then reuse arena before new allocations
      if (OB_FAIL(ret)) {
      } else if (pending.initialized_ && pending.model_id_ != model_id_i) {
        if (OB_FAIL(ObAIFuncBatchUtils::flush_pending_batch(
                       batch_arena, expr, ctx, pending, res_vec, eval_flags,
                       pack_complete_response_to_indices))) {
          LOG_WARN("fail to flush pending batch on model change", K(ret), K(idx));
        } else {
          pending.reset();
          batch_arena.reuse();
        }
      }

      // Refresh model config after arena reuse (must be after flush+reuse)
      if (OB_FAIL(ret)) {
      } else if (cur_model_id != model_id_i) {
        if (OB_FAIL(ObAIFuncUtils::get_model_config_info(batch_arena, model_id_i, cur_model_config))) {
          LOG_WARN("fail to get model config info", K(ret), K(model_id_i));
        } else {
          cur_model_id = model_id_i;
        }
      }

      ObString prompt_str;
      ObJsonObject *prompt_object = nullptr;
      if (OB_FAIL(ret)) {
      } else if (prompt_is_json) {
        ObString prompt_raw;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                       batch_arena, prompt_vec, arg_expr_prompt->datum_meta_,
                       arg_expr_prompt->obj_meta_.has_lob_header(),
                       prompt_raw, idx))) {
          LOG_WARN("fail to read prompt json data", K(ret), K(idx));
        } else {
          ObIJsonBase *j_base = nullptr;
          ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(arg_expr_prompt->datum_meta_.type_);
          if (OB_FAIL(ObJsonBaseFactory::get_json_base(&batch_arena, prompt_raw, j_in_type,
                                                       ObJsonInType::JSON_TREE, j_base, 0))) {
            LOG_WARN("fail to parse prompt json", K(ret), K(idx));
          } else if (OB_ISNULL(j_base) || j_base->json_type() != ObJsonNodeType::J_OBJECT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("prompt is not json object", K(ret), K(idx));
          } else if (OB_FALSE_IT(prompt_object = static_cast<ObJsonObject *>(j_base))) {
          } else if (!ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("prompt object is not valid", K(ret), K(idx));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt is not valid");
          } else {
            ObJsonString *template_json = static_cast<ObJsonString *>(
                prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key));
            ObJsonArray *args_array = static_cast<ObJsonArray *>(
                prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key));
            if (OB_ISNULL(template_json) || OB_ISNULL(args_array)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("prompt object fields is null", K(ret), K(idx));
            } else {
              bool has_image_placeholder = false;
              ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
              const int64_t arg_cnt = args_array->element_count();
              for (int64_t i = 0; OB_SUCC(ret) && i < arg_cnt; ++i) {
                if (OB_FAIL(arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN))) {
                  LOG_WARN("fail to push back arg type", K(ret), K(i));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(ObAIFuncPromptObjectUtils::parse_template_arg_types(
                             template_json->get_str(), arg_cnt, arg_types, &has_image_placeholder))) {
                LOG_WARN("fail to parse template arg types", K(ret), K(idx));
              } else if (!has_image_placeholder) {
                if (OB_FAIL(ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(
                               batch_arena, prompt_object, prompt_str))) {
                  LOG_WARN("fail to replace str args in template", K(ret), K(idx));
                } else {
                  prompt_object = nullptr;
                }
              } else if (OB_FAIL(ObAIFuncPromptObjectUtils::validate_binary_image_sizes(
                             prompt_object, cur_model_config.get_max_image_size()))) {
                LOG_WARN("fail to validate binary image sizes", K(ret), K(idx));
              }
            }
          }
        }
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                     batch_arena, prompt_vec, arg_expr_prompt->datum_meta_,
                     arg_expr_prompt->obj_meta_.has_lob_header(),
                     prompt_str, idx))) {
        LOG_WARN("fail to read prompt string data", K(ret), K(idx));
      }

      if (OB_FAIL(ret)) {
      } else if (prompt_str.empty() && OB_ISNULL(prompt_object)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }

      ObJsonObject *config_i = nullptr;
      if (OB_FAIL(ret)) {
      } else if (config_is_const) {
        config_i = const_config;
      } else if (OB_NOT_NULL(config_vec) && !config_vec->is_null(idx)) {
        ObString config_str_i;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                       batch_arena, config_vec, expr.args_[CONFIG_IDX]->datum_meta_,
                       expr.args_[CONFIG_IDX]->obj_meta_.has_lob_header(),
                       config_str_i, idx))) {
          LOG_WARN("fail to read config", K(ret), K(idx));
        } else if (!config_str_i.empty()) {
          ObIJsonBase *j_base = nullptr;
          if (OB_FAIL(ObJsonBaseFactory::get_json_base(&batch_arena, config_str_i, config_json_in_type,
                                                       ObJsonInType::JSON_TREE, j_base, 0))) {
            LOG_WARN("fail to parse config json", K(ret), K(idx));
          } else if (OB_ISNULL(j_base) || j_base->json_type() != ObJsonNodeType::J_OBJECT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("config is not json object", K(ret), K(idx));
          } else {
            config_i = static_cast<ObJsonObject *>(j_base);
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!pending.initialized_) {
        const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
        if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_id_i, endpoint_info))) {
          LOG_WARN("fail to get endpoint info", K(ret), K(model_id_i));
        } else if (OB_FAIL(ObAIFuncBatchUtils::init_pending_state(
                       batch_arena, model_id_i, endpoint_info, pending,
                       ObAIFuncUtils::check_info_type_completion))) {
          LOG_WARN("fail to init pending state", K(ret), K(model_id_i));
        }
      }

      if (OB_SUCC(ret)) {
        ObJsonObject *body = nullptr;
        ObString system_prompt;
        bool is_vl = OB_NOT_NULL(prompt_object);
        // Merge endpoint message_parameters with user config (user config takes priority)
        ObJsonObject *merged_config = nullptr;
        if (OB_FAIL(ObAIFuncUtils::merge_message_parameters_to_config(
                batch_arena, cur_model_config.get_message_parameters(), config_i, merged_config))) {
          LOG_WARN("fail to merge message parameters to config", K(ret), K(idx));
        } else if (OB_FAIL(ObAIFuncUtils::set_default_enable_thinking(batch_arena, merged_config))) {
          LOG_WARN("fail to set default enable_thinking", K(ret), K(idx));
        } else if (is_vl) {
          ObAIFuncIVLComplete *vl_provider = nullptr;
          ObString request_model_name = pending.info_->model_;
          if (!pending.endpoint_info_->get_request_model_name().empty()) {
            request_model_name = pending.endpoint_info_->get_request_model_name();
          }
          if (OB_FAIL(ObAIFuncUtils::get_vl_complete_provider(batch_arena,
                                                              pending.endpoint_info_->get_provider(),
                                                              vl_provider))) {
            LOG_WARN("fail to get vl complete provider", K(ret), K(idx));
          } else if (OB_FAIL(vl_provider->get_body(batch_arena, request_model_name, prompt_str,
                                                   prompt_object, merged_config, body))) {
            LOG_WARN("fail to get vl body", K(ret), K(idx));
          }
        } else {
          if (OB_FAIL(ObAIFuncUtils::get_complete_body(batch_arena, *pending.info_,
                                                       *pending.endpoint_info_, system_prompt,
                                                       prompt_str, merged_config, body))) {
            LOG_WARN("fail to get complete body", K(ret), K(idx));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(pending.bodies_.push_back(body))) {
          LOG_WARN("fail to push back body", K(ret), K(idx));
        } else if (OB_FAIL(pending.row_starts_.push_back(pending.row_indices_flat_.count()))) {
          LOG_WARN("fail to push back row start", K(ret), K(idx));
        } else if (OB_FAIL(pending.row_lens_.push_back(1))) {
          LOG_WARN("fail to push back row len", K(ret), K(idx));
        } else if (OB_FAIL(pending.row_indices_flat_.push_back(idx))) {
          LOG_WARN("fail to push back row index", K(ret), K(idx));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncBatchUtils::flush_pending_batch(
                   batch_arena, expr, ctx, pending, res_vec, eval_flags,
                   pack_complete_response_to_indices))) {
      LOG_WARN("fail to flush final pending batch", K(ret));
    } else {
      pending.reset();
      batch_arena.reuse();
    }
  }
  return ret;
}

int ObExprAIComplete::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIComplete::eval_ai_complete;
    rt_expr.eval_vector_func_ = ObExprAIComplete::eval_ai_complete_vector;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
