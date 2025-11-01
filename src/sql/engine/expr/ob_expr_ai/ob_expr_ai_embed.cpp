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
 * This file contains implementation for ai_embed expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_embed.h"
#include "observer/omt/ob_tenant_ai_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                        T_FUN_SYS_AI_EMBED,
                        N_AI_EMBED,
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL,
                        NOT_ROW_DIMENSION)
{
}

ObExprAIEmbed::~ObExprAIEmbed()
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dimension parameter must be an integer, not a decimal or float", K(ret), K(types_stack[DIM_IDX].get_type()));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (arg_model_id->is_null() || arg_content->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model id or content is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model id or content is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    ObString model_id = arg_model_id->get_string();
    ObString content = arg_content->get_string();
    if (model_id.empty() || content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model id or input is empty", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model id or input is empty");
      res.set_null();
    }
    int64_t dim = 0;
    ObJsonInt *dim_json = nullptr;
    ObJsonObject *config = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_dim)) {
      dim = arg_dim->get_int();
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dimension parameter must be a positive integer", K(ret), K(dim));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dimension parameter must be a positive integer");
        res.set_null();
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
        LOG_WARN("fail to get json object", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
        LOG_WARN("fail to get json int", K(ret));
      } else if (OB_FAIL(config->add("dimensions", dim_json))) {
        LOG_WARN("fail to add dimensions", K(ret));
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
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
        LOG_WARN("fail to call dense embedding", K(ret));
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
        LOG_WARN("fail to set string result", K(ret));
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::get_vector_params(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound,
                                    ObString &model_id,
                                    ObArray<ObString> &contents,
                                    int64_t &dim)
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
    ObIVector *content_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *dim_vec = expr.arg_cnt_ > 2 ? expr.args_[2]->get_vector(ctx) : nullptr;
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_EMBED));
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          temp_allocator,
          model_vec,
          expr.args_[0]->datum_meta_,
          expr.args_[0]->obj_meta_.has_lob_header(),
          model_id,
          0))) {
      LOG_WARN("fail to get real string data", K(ret), K(model_id));
    } else if (OB_NOT_NULL(dim_vec)) {
      dim = dim_vec->get_int(0);
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dimension parameter must be a positive integer", K(ret), K(dim));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dimension parameter must be a positive integer");
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        ObString content;
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else if (model_vec->is_null(idx) || content_vec->is_null(idx)) {
          res_vec->set_null(idx);
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                    temp_allocator,
                    content_vec,
                    expr.args_[1]->datum_meta_,
                    expr.args_[1]->obj_meta_.has_lob_header(),
                    content,
                    idx))) {
          LOG_WARN("fail to get real string data", K(ret), K(content));
        } else if (content.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("input is empty", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "input is empty");
        } else if (OB_FAIL(contents.push_back(content))) {
          LOG_WARN("fail to push back content", K(ret), K(idx));
        }
        eval_flags.set(idx);
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::pack_json_array_to_res_vector(const ObExpr &expr,
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
    } else if (OB_FAIL(ObAIFuncUtils::parse_embed_output(allocator, endpoint_info, response_obj, output))) {
      LOG_WARN("fail to parse output", K(ret), K(i));
    } else if (OB_ISNULL(output)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output is null", K(ret), K(i));
    } else if (output->json_type() == ObJsonNodeType::J_ARRAY) {
      ObJsonArray *embeddings_array = static_cast<ObJsonArray *>(output);
      ObString raw_str;
      ObIJsonBase *embedding = nullptr;
      for (int64_t j = 0; OB_SUCC(ret) && j < embeddings_array->element_count(); ++j) {
        ObStringBuffer embedding_buf(&allocator);
        if (idx < bound.end()) {
          if (OB_ISNULL(embedding = embeddings_array->get_value(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("embedding is null", K(ret));
          } else if (OB_FAIL(embedding->print(embedding_buf, 0))) {
            LOG_WARN("fail to print embedding", K(ret));
          } else if (OB_ISNULL(raw_str = embedding_buf.string())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get embedding string", K(ret));
          } else if (OB_FAIL(ObAIFuncJsonUtils::inner_pack_raw_str_to_res(raw_str, expr, ctx, res_vec, idx))) {
            LOG_WARN("fail to pack json result", K(ret));
          }
          eval_flags.set(idx);
          idx++;
        }
        current_batch_size++;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output is not array", K(ret), K(i));
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

int ObExprAIEmbed::eval_ai_embed_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  INIT_SUCC(ret);
  ObString model_id;
  ObArray<ObString> contents;
  int64_t dim = 0;
  ObIVector *res_vec = expr.get_vector(ctx);
  if (OB_FAIL(get_vector_params(expr, ctx, skip, bound, model_id, contents, dim))) {
    LOG_WARN("fail to get vector params", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    ObArray<ObString> header_array;
    ObArray<ObJsonObject *> bodies;
    ObJsonObject *body = nullptr;
    ObJsonObject *config = nullptr;
    ObJsonInt *dim_json = nullptr;
    ObArray<ObJsonObject *> responses;
    ObAIFuncClient ai_client;
    if (dim > 0) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
        LOG_WARN("fail to get json string", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
        LOG_WARN("fail to get json object", K(ret));
      } else if (OB_FAIL(config->add("dimensions", dim_json))) {
        LOG_WARN("fail to add dimensions", K(ret));
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
    } else if (!ObAIFuncUtils::is_dense_embedding_type(info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model type must be DENSE_EMBEDDING", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model type must be DENSE_EMBEDDING");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < contents.count(); ++i) {
        ObArray<ObString> contents_array;
        contents_array.push_back(contents[i]);
        if (OB_FAIL(ObAIFuncUtils::get_embed_body(temp_allocator, *info, *endpoint_info, contents_array, config, body))) {
          LOG_WARN("fail to get body", K(ret), K(i));
        } else if (OB_FAIL(bodies.push_back(body))) {
          LOG_WARN("fail to append body", K(ret), K(i));
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
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_vector_v2(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObIVector *res_vec = expr.get_vector(ctx);
  if (OB_ISNULL(res_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_vec is null", K(ret));
  } else {
    ObString model_id;
    ObArray<ObString> contents;
    int64_t dim = 0;
    if (OB_FAIL(get_vector_params(expr, ctx, skip, bound, model_id, contents, dim))) {
      LOG_WARN("fail to get vector params", K(ret));
    } else {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
      MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_EMBED));
      ObAIFuncExprInfo *info = nullptr;
      omt::ObAiServiceGuard ai_service_guard;
      omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
      const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
      ObArray<ObString> header_array;
      ObJsonObject *body = nullptr;
      ObJsonObject *response = nullptr;
      ObAIFuncClient ai_client;
      ObJsonObject *config = nullptr;
      ObJsonInt *dim_json = nullptr;
      if (dim > 0) {
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
          LOG_WARN("fail to get json string", K(ret));
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
          LOG_WARN("fail to get json object", K(ret));
        } else if (OB_FAIL(config->add("dimension", dim_json))) {
          LOG_WARN("fail to add dimensions", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
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
        } else if (OB_FAIL(ObAIFuncUtils::check_info_type_dense_embedding(info))) {
          LOG_WARN("fail to check model type", K(ret));
        } else if (OB_FAIL(ObAIFuncUtils::get_header(temp_allocator, *info, *endpoint_info, header_array))) {
            LOG_WARN("fail to get header", K(ret));
        } else if (OB_FAIL(ObAIFuncUtils::get_embed_body(temp_allocator, *info, *endpoint_info, contents, config, body))) {
          LOG_WARN("fail to get body", K(ret));
        } else if (OB_FAIL(ai_client.send_post(temp_allocator, endpoint_info->get_url(), header_array, body, response))) {
          LOG_WARN("fail to send batch request", K(ret));
        } else if (OB_FAIL(pack_json_object_to_res_vector(expr, ctx, temp_allocator, response, skip, bound, *endpoint_info, res_vec))) {
          LOG_WARN("fail to pack json to res", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::pack_json_object_to_res_vector(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                ObIAllocator &allocator,
                                                ObJsonObject *response,
                                                const ObBitVector &skip,
                                                const EvalBound &bound,
                                                const ObAiModelEndpointInfo &endpoint_info,
                                                ObIVector *res_vec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(response)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("response is null", K(ret));
  } else {
    ObArray<ObJsonObject *> responses;
    if (OB_FAIL(responses.push_back(response))) {
      LOG_WARN("fail to push back response", K(ret));
    } else if (OB_FAIL(pack_json_array_to_res_vector(expr, ctx, allocator, responses, skip, bound, endpoint_info, res_vec))) {
      LOG_WARN("fail to pack json to res", K(ret));
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   ObObj const_data;
  //   bool got_data = false;
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
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_vector_func_ = ObExprAIEmbed::eval_ai_embed_vector;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase