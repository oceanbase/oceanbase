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
 * This file contains implementation for ai_complete expression.
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
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parameters is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_COMPLETE));
    ObAIFuncExprInfo *info = nullptr;
    ObString model_id = arg_model_id->get_string();
    ObString prompt;
    ObJsonObject *config = nullptr;
    ObString config_str;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    ObExpr *arg_expr_prompt = expr.args_[1];
    if ( OB_ISNULL(arg_expr_prompt) ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg_expr_prompt is null", K(ret));
    } else if (arg_expr_prompt->datum_meta_.type_ == ObJsonType) {
      ObIJsonBase *j_base = nullptr;
      ObJsonObject *prompt_object = nullptr;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, PROMPT_IDX, j_base, is_null))) {
        LOG_WARN("get_json_doc failed", K(ret));
      } else if (j_base->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("j_base is not json object", K(ret));
      } else if (OB_FALSE_IT(prompt_object = static_cast<ObJsonObject *>(j_base))) {
      } else if (!ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("prompt is not valid", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "prompt is not valid");
        res.set_null();
      } else if (!ObAIFuncJsonUtils::ob_is_json_array_all_str(static_cast<ObJsonArray *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key)))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("prompt object is not support", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "prompt object is not support");
      } else if (OB_FAIL(ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(temp_allocator, prompt_object, prompt))) {
        LOG_WARN("fail to replace all str args in template", K(ret));
      }
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *arg_prompt, expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), prompt))) {
      LOG_WARN("fail to get real string data", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_config)) {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *arg_config, expr.args_[2]->datum_meta_, expr.args_[2]->obj_meta_.has_lob_header(), config_str))) {
        LOG_WARN("fail to get real string data", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(temp_allocator, config_str, config))) {
        LOG_WARN("fail to get json object", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (model_id.empty() || prompt.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model id or input is empty", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model id or input is empty");
      res.set_null();
    }

    if (OB_FAIL(ret)){
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
      LOG_WARN("fail to get ai func info", K(ret));
    } else if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_id, endpoint_info))) {
      LOG_WARN("failed to get endpoint info", K(ret), K(model_id));
    } else if (OB_ISNULL(endpoint_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("endpoint info is null", K(ret));
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_completion(prompt, config, result))) {
        LOG_WARN("fail to call completion", K(ret));
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
        LOG_WARN("fail to set string result", K(ret));
      }
    }
  }
  return ret;
}

int ObExprAIComplete::get_prompt_object_from_str(ObIAllocator &allocator,
                                                  const ObDatumMeta &meta,
                                                  ObArray<ObString> &prompts,
                                                  ObArray<ObJsonObject *> &prompt_objects,
                                                  bool& is_all_str)
{
  INIT_SUCC(ret);
  ObObjType val_type = meta.type_;
  ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(val_type);
  ObJsonInType expect_type = ObJsonInType::JSON_TREE;
  for (int64_t i = 0; OB_SUCC(ret) && i < prompts.count(); ++i) {
    ObString prompt = prompts.at(i);
    ObIJsonBase *j_base = nullptr;
    ObJsonObject *prompt_object = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, prompt, j_in_type,
                                                    expect_type, j_base, 0))) {
      LOG_WARN("fail to get json base", K(ret), K(j_in_type));
    } else if (OB_FALSE_IT(prompt_object = static_cast<ObJsonObject *>(j_base))) {
    } else if (OB_FAIL(prompt_objects.push_back(prompt_object))) {
      LOG_WARN("fail to push back prompt object", K(ret));
    } else if (!is_all_str && ObAIFuncJsonUtils::ob_is_json_array_all_str(static_cast<ObJsonArray *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key)))) {
      is_all_str = true;
    }
  }
  return ret;
}

int ObExprAIComplete::transform_prompt_object_to_str(ObIAllocator &allocator,
                                                    ObArray<ObJsonObject *> &prompt_objects,
                                                    ObArray<ObString> &prompts)
{
  INIT_SUCC(ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < prompt_objects.count(); ++i) {
    ObJsonObject *prompt_object = prompt_objects.at(i);
    ObString prompt;
    if (OB_FAIL(ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt_object, prompt))) {
      LOG_WARN("fail to replace all str args in template", K(ret));
    } else if (OB_FAIL(prompts.push_back(prompt))) {
      LOG_WARN("fail to push back prompt", K(ret), K(i));
    }
  }
  return ret;
}

int ObExprAIComplete::get_vector_params(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound,
                                        ObString &model_id,
                                        ObArray<ObString> &prompts,
                                        ObJsonObject *&config)
{
  INIT_SUCC(ret);
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval delimiter string failed", K(ret));
  } else if (OB_FAIL(expr.arg_cnt_ > 2 && expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval null string failed", K(ret));
  } else {
    ObIVector *model_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *prompt_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *config_vec = expr.arg_cnt_ > 2 ? expr.args_[2]->get_vector(ctx) : nullptr;
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObString config_str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_COMPLETE));
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          temp_allocator,
          model_vec,
          expr.args_[0]->datum_meta_,
          expr.args_[0]->obj_meta_.has_lob_header(),
          model_id,
          0))) {
      LOG_WARN("fail to get real string data", K(ret), K(model_id));
    } else if (OB_NOT_NULL(config_vec)) {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
        temp_allocator,
        config_vec,
        expr.args_[2]->datum_meta_,
        expr.args_[2]->obj_meta_.has_lob_header(),
        config_str,
        0))) {
        LOG_WARN("fail to get real string data", K(ret), K(config_str));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(temp_allocator, config_str, config))) {
        LOG_WARN("fail to get json object", K(ret), K(config_str));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        ObString prompt;
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else if (model_vec->is_null(idx) || prompt_vec->is_null(idx)) {
          res_vec->set_null(idx);
        } else {
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                temp_allocator,
                prompt_vec,
                expr.args_[1]->datum_meta_,
                expr.args_[1]->obj_meta_.has_lob_header(),
                prompt,
                idx))) {
            LOG_WARN("fail to get real string data", K(ret), K(prompt));
          } else if (prompt.empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("input is empty", K(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "input is empty");
            res_vec->set_null(idx);
          } else if (OB_FAIL(prompts.push_back(prompt))) {
            LOG_WARN("fail to push back prompt", K(ret), K(idx));
          }
        }
        eval_flags.set(idx);
      }
    }
  }
  return ret;
}

int ObExprAIComplete::pack_json_array_to_res_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObIAllocator &allocator,
                                      ObArray<ObJsonObject *> &responses,
                                      const ObBitVector &skip,
                                      const EvalBound &bound,
                                      const ObAiModelEndpointInfo &endpoint_info,
                                      ObIVector *res_vec)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObJsonObject *response_obj = nullptr;
  ObIJsonBase *output = nullptr;
  int64_t idx = bound.start();
  int64_t current_batch_size = 0;
  int64_t response_size = responses.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < response_size; ++i) {
    if (OB_ISNULL(response_obj = responses.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("response_obj is null", K(ret), K(i));
    } else if (OB_FAIL(ObAIFuncUtils::parse_complete_output(allocator, endpoint_info, response_obj, output))) {
      LOG_WARN("fail to parse output", K(ret), K(i));
    } else if (OB_ISNULL(output)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output is null", K(ret), K(i));
    } else if (output->json_type() == ObJsonNodeType::J_STRING) {
      ObString raw_str;
      if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, output, raw_str))) {
        LOG_WARN("fail to print json to str", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::inner_pack_raw_str_to_res(raw_str, expr, ctx, res_vec, idx))) {
        LOG_WARN("fail to pack json result", K(ret));
      }
      eval_flags.set(idx);
      idx++;
      current_batch_size++;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output is not array", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (current_batch_size != bound.batch_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current_batch_size not equal to batch_size", K(ret), K(current_batch_size), K(bound.batch_size()));
    }
  }
  return ret;
}

int ObExprAIComplete::pack_json_string_to_res_vector(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    ObIJsonBase *response,
                                                    const ObBitVector &skip,
                                                    const EvalBound &bound,
                                                    ObIVector *res_vec)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObJsonObject *response_obj = nullptr;
  ObIJsonBase *output = nullptr;
  if (OB_ISNULL(response)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("response is null", K(ret));
  } else if (response->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("response is not string", K(ret));
  } else {
    ObJsonBuffer j_buf(&allocator);
    ObString response_str;
    if (OB_FAIL(response->print(j_buf, 0))) {
      LOG_WARN("fail to print response", K(ret));
    } else if (OB_ISNULL(response_str = j_buf.string())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get response string value", K(ret));
    } else {
      ObJsonObject *response_obj = nullptr;
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(allocator, response_str, response_obj))) {
        LOG_WARN("fail to get json object", K(ret));
      } else {
        ObJsonNode *response_node = response_obj->get_value("item");
        if (OB_ISNULL(response_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("response_node is null", K(ret));
        } else if (response_node->json_type() == ObJsonNodeType::J_ARRAY) {
          int64_t idx = bound.start();
          ObString raw_str;
          ObJsonArray *response_array = static_cast<ObJsonArray *>(response_node);
          for (int64_t i = 0; OB_SUCC(ret) && i < response_array->element_count(); ++i) {
            if (idx < bound.end()) {
              ObIJsonBase *item = response_array->get_value(i);
              if (OB_ISNULL(item)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("item is null", K(ret));
              } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, item, raw_str))) {
                LOG_WARN("fail to print json to str", K(ret));
              } else if (OB_FAIL(ObAIFuncJsonUtils::inner_pack_raw_str_to_res(raw_str, expr, ctx, res_vec, idx))) {
                LOG_WARN("fail to pack json result", K(ret));
              }
              eval_flags.set(idx);
              idx++;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("response_node is not array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprAIComplete::eval_ai_complete_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  INIT_SUCC(ret);
  ObString model_id;
  ObString prompt;
  ObArray<ObString> prompts;
  ObJsonObject *config = nullptr;
  ObIVector *res_vec = expr.get_vector(ctx);
  ObExpr *arg_expr_prompt = expr.args_[1];
  if (OB_FAIL(get_vector_params(expr, ctx, skip, bound, model_id, prompts, config))) {
    LOG_WARN("fail to get vector params", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_COMPLETE));
    ObAIFuncExprInfo *info = nullptr;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    ObArray<ObString> header_array;
    ObArray<ObJsonObject *> bodies;
    ObJsonObject *body = nullptr;
    ObArray<ObJsonObject *> responses;
    ObAIFuncClient ai_client;
    if (arg_expr_prompt->datum_meta_.type_ == ObJsonType) {
      ObArray<ObJsonObject *> prompt_objects;
      bool is_all_str = false;
      if (OB_FAIL(get_prompt_object_from_str(temp_allocator, arg_expr_prompt->datum_meta_, prompts, prompt_objects, is_all_str))) {
        LOG_WARN("fail to get prompt object from str", K(ret));
      } else if (is_all_str) {
        prompts.reset();
        if (OB_FAIL(transform_prompt_object_to_str(temp_allocator, prompt_objects, prompts))) {
          LOG_WARN("fail to transform prompt object to str", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("prompt is not all str", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
      LOG_WARN("fail to get ai func info", K(ret));
    } else if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_id, endpoint_info))) {
      LOG_WARN("failed to get endpoint info", K(ret), K(model_id));
    } else if (OB_ISNULL(endpoint_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("endpoint info is null", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::check_info_type_completion(info))) {
      LOG_WARN("model type must be COMPLETION", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < prompts.count(); ++i) {
        if (OB_FAIL(ObAIFuncUtils::get_complete_body(temp_allocator, *info, *endpoint_info, prompt,
                                                     prompts.at(i), config, body))) {
          LOG_WARN("fail to get body", K(ret), K(i));
        } else if (OB_FAIL(bodies.push_back(body))) {
          LOG_WARN("fail to append body", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncUtils::get_header(temp_allocator, *info, *endpoint_info, header_array))) {
        LOG_WARN("fail to get header", K(ret));
      } else if (OB_FAIL(ai_client.send_post_batch(temp_allocator, endpoint_info->get_url(), header_array, bodies, responses))) {
        LOG_WARN("fail to send batch request", K(ret));
      } else if (OB_FAIL(pack_json_array_to_res_vector(expr, ctx, temp_allocator, responses, skip, bound, *endpoint_info, res_vec))) {
        LOG_WARN("fail to pack json to res", K(ret));
      }
    }
  }
  return ret;
}

int ObExprAIComplete::get_prompt_and_contents_contact_str(ObIAllocator &allocator,
                                      ObString &prompt,
                                      ObArray<ObString> &contents,
                                      ObString &prompt_and_contents)
{
  INIT_SUCC(ret);
  ObString meta_prompt(meta_prompt_);
  ObString prompt_meta_prompt(PROMPT_META_PROMPT);
  ObString tuples_meta_prompt(TUPLES_META_PROMPT);
  ObString body_str;
  if (OB_FAIL(construct_tuple_str(allocator, contents, body_str))) {
    LOG_WARN("fail to construct tuple str", K(ret));
  } else if (OB_FAIL(ObAIFuncPromptUtils::replace_meta_prompt(allocator, meta_prompt, prompt_meta_prompt, prompt, prompt_and_contents))) {
    LOG_WARN("fail to replace meta prompt", K(ret));
  } else if (OB_FAIL(ObAIFuncPromptUtils::replace_meta_prompt(allocator, prompt_and_contents, tuples_meta_prompt, body_str, prompt_and_contents))) {
    LOG_WARN("fail to replace meta prompt", K(ret));
  }
  return ret;
}

int ObExprAIComplete::get_tuple_str(ObIAllocator &allocator, ObString &content, ObString &tuple_str)
{
  INIT_SUCC(ret);
  ObStringBuffer tuple_buffer(&allocator);
  if (OB_FAIL(tuple_buffer.append("{\"tuple\":\""))) {
    LOG_WARN("fail to append tuple", K(ret));
  } else if (OB_FAIL(tuple_buffer.append(content.ptr(), content.length()))) {
    LOG_WARN("fail to append content", K(ret));
  } else if (OB_FAIL(tuple_buffer.append("\"}"))) {
    LOG_WARN("fail to append \"", K(ret));
  } else {
    tuple_str = tuple_buffer.string();
  }
  return ret;
}

int ObExprAIComplete::construct_tuple_str(ObIAllocator &allocator, ObArray<ObString> &contents, ObString &content_str)
{
  INIT_SUCC(ret);
  ObStringBuffer content_buffer(&allocator);
  if (OB_FAIL(content_buffer.append("["))) {
    LOG_WARN("fail to append [", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < contents.count(); ++i) {
      ObString tuple_str;
      if (OB_FAIL(get_tuple_str(allocator, contents.at(i), tuple_str))) {
        LOG_WARN("fail to get tuple str", K(ret));
      } else if (OB_FAIL(content_buffer.append(tuple_str.ptr(), tuple_str.length()))) {
        LOG_WARN("fail to append tuple str", K(ret));
      } else if (i < contents.count() - 1) {
        if (OB_FAIL(content_buffer.append(","))) {
          LOG_WARN("fail to append ,", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(content_buffer.append("]"))) {
      LOG_WARN("fail to append ]", K(ret));
    } else if (OB_ISNULL(content_str = content_buffer.string())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get string", K(ret));
    }
  }
  return ret;
}

int ObExprAIComplete::eval_ai_complete_vector_v2(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObString model_id;
  ObString prompt;
  ObArray<ObString> prompts;
  ObJsonObject *config = nullptr;
  ObIVector *res_vec = expr.get_vector(ctx);
  if (OB_FAIL(get_vector_params(expr, ctx, skip, bound, model_id, prompts, config))) {
    LOG_WARN("fail to get vector params", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_COMPLETE));
    ObAIFuncExprInfo *info = nullptr;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    const ObAiModelEndpointInfo *endpoint_info = nullptr;
    ObArray<ObString> header_array;
    ObJsonObject *body = nullptr;
    ObJsonObject *response = nullptr;
    ObAIFuncClient ai_client;
    ObJsonObject json_format_config(&temp_allocator);
    ObString no_prompt;
    ObString prompt_and_contents;
    ObIJsonBase *output = nullptr;
    if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
      LOG_WARN("fail to get ai func info", K(ret));
    } else if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_id, endpoint_info))) {
      LOG_WARN("failed to get endpoint info", K(ret), K(model_id));
    } else if (OB_ISNULL(endpoint_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("endpoint info is null", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::check_info_type_completion(info))) {
      LOG_WARN("model type must be COMPLETION", K(ret));
    } else if (OB_FAIL(get_prompt_and_contents_contact_str(temp_allocator, prompt, prompts, prompt_and_contents))) {
      LOG_WARN("fail to get prompt and contents contact str", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::set_json_format_config(temp_allocator, endpoint_info->get_provider(), &json_format_config))) {
      LOG_WARN("fail to get json format config", K(ret));
    } else {
      if (OB_ISNULL(config)) {
        config = &json_format_config;
      } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(temp_allocator, &json_format_config, config))) {
        LOG_WARN("fail to compact json object", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncUtils::get_header(temp_allocator, *info, *endpoint_info, header_array))) {
        LOG_WARN("fail to get header", K(ret));
      } else if (OB_FAIL(ObAIFuncUtils::get_complete_body(temp_allocator, *info, *endpoint_info, no_prompt, prompt_and_contents, config, body))) {
        LOG_WARN("fail to get body", K(ret));
      } else if (OB_FAIL(ai_client.send_post(temp_allocator, endpoint_info->get_url(), header_array, body, response))) {
        LOG_WARN("fail to send batch request", K(ret));
      } else if (OB_FAIL(ObAIFuncUtils::parse_complete_output(temp_allocator, *endpoint_info, response, output))) {
        LOG_WARN("fail to parse output", K(ret));
      } else if (OB_FAIL(pack_json_string_to_res_vector(expr, ctx, temp_allocator, output, skip, bound, res_vec))) {
        LOG_WARN("fail to pack json to res", K(ret));
      }
    }
  }
  return ret;
}

int ObExprAIComplete::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   bool got_data = false;
  //   ObObj const_data;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIComplete::eval_ai_complete;
    //rt_expr.eval_vector_func_ = ObExprAIComplete::eval_ai_complete_vector;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
