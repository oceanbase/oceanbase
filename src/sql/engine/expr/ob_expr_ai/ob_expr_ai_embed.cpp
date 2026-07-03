/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_embed.h"
#include "observer/omt/ob_tenant_ai_service.h"
#include "lib/allocator/page_arena.h"

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
  if (OB_UNLIKELY(param_num > 4 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (types_stack[CONTENT_IDX].get_collation_type() != CS_TYPE_BINARY) {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
      types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    if (param_num >= 3) {
      if (types_stack[DIM_IDX].get_type() == ObNullType) {
        // null, skip validation
      } else if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        // Third parameter as options (json string), e.g. '{"type": "image"}'
        types_stack[DIM_IDX].set_calc_type(ObVarcharType);
        types_stack[DIM_IDX].set_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_SUCC(ret) && param_num == 4) {
      types_stack[OPTIONS_IDX].set_calc_type(ObVarcharType);
      types_stack[OPTIONS_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
    type.set_length(65535);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  ObDatum *arg_options = nullptr;

  // Determine number of parameters (2-4)
  // When 3 params: third can be dim (int) or options (json string)
  int64_t param_count = expr.arg_cnt_;
  ObDatum *arg_third = nullptr;
  if (param_count == 4) {
    if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim, arg_options))) {
      LOG_WARN("evaluate parameters failed", K(ret));
    }
  } else if (param_count == 3) {
    if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_third))) {
      LOG_WARN("evaluate parameters failed", K(ret));
    } else if (OB_NOT_NULL(arg_third)) {
      if (ob_is_integer_type(expr.args_[DIM_IDX]->datum_meta_.type_)) {
        arg_dim = arg_third;
        arg_options = nullptr;
      } else {
        arg_dim = nullptr;
        arg_options = arg_third;
      }
    }
  } else { // param_count == 2
    if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
      LOG_WARN("evaluate parameters failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (arg_model_id->is_null() || arg_content->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model id or content is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or content is null");
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
    // Read real content for LOB/BLOB columns (e.g. image_binary); get_string() only gives locator
    if (OB_SUCC(ret) &&
        OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *arg_content,
                expr.args_[CONTENT_IDX]->datum_meta_,
                expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(),
                content, &ctx.exec_ctx_))) {
      LOG_WARN("fail to read content for ai_embed", K(ret));
    }

    ObAiEmbedInputType input_type = ObAiEmbedInputType::INPUT_TYPE_TEXT;
    if (OB_SUCC(ret) && OB_NOT_NULL(arg_options) && !arg_options->is_null()) {
      ObString options_str = arg_options->get_string();
      if (!options_str.empty()) {
        if (OB_FAIL(parse_embed_options(temp_allocator, options_str, input_type))) {
          LOG_WARN("fail to parse embed options", K(ret));
        }
      }
    }

    ObString processed_content = content;
    if (OB_SUCC(ret)) {
      if (input_type == ObAiEmbedInputType::INPUT_TYPE_IMAGE) {
        // Check if content is a URL (starts with http:// or https://)
        bool is_url = false;
        if (content.length() >= 5) {
          if (content.prefix_match_ci("http://") || content.prefix_match_ci("https://")) {
            is_url = true;
          }
        }
        if (!is_url) {
          // Binary image: enforce max_image_size from model config
          share::ObAIModelConfigInfo model_config;
          if (OB_FAIL(ObAIFuncUtils::get_model_config_info(temp_allocator, model_id, model_config))) {
            LOG_WARN("fail to get model config for max_image_size check", K(ret));
          } else if (model_config.get_max_image_size() > 0 &&
                     content.length() > static_cast<int64_t>(model_config.get_max_image_size())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("image binary size exceeds model max_image_size limit", K(ret),
                     K(content.length()), K(model_config.get_max_image_size()));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, image binary size exceeds max_image_size limit");
          }
        }
        if (OB_SUCC(ret) &&
            OB_FAIL(process_image_input(temp_allocator, content, is_url, processed_content))) {
          LOG_WARN("fail to process image input", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (model_id.empty() || processed_content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model id or input is empty", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
      res.set_null();
    } else {
      int64_t dim = 0;
      ObJsonInt *dim_json = nullptr;
      ObJsonObject *config = nullptr;
      if (OB_NOT_NULL(arg_dim)) {
        dim = arg_dim->get_int();
        if (dim <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("dimension parameter must be a positive integer", K(ret), K(dim));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
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
        // Set input_type based on parsed options
        ObString input_type_str = (input_type == ObAiEmbedInputType::INPUT_TYPE_IMAGE) ? ObString("image") : ObString("text");
        model.set_input_type(input_type_str);
        ObString result;
        if (OB_FAIL(model.call_dense_embedding(processed_content, config, result))) {
          LOG_WARN("fail to call dense embedding", K(ret));
        } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
          LOG_WARN("fail to set string result", K(ret));
        }
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

int ObExprAIEmbed::enqueue_group_to_pending(ObIAllocator &allocator,
                                            const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            omt::ObAiServiceGuard &ai_service_guard,
                                            ObIVector *res_vec,
                                            ObBitVector &eval_flags,
                                            const ObString &grp_model_id,
                                            int64_t grp_dim,
                                            ObArray<ObString> &grp_contents,
                                            ObArray<ObString> &grp_input_types,
                                            const ObArray<int64_t> &grp_row_idxs,
                                            common::ObAIFuncBatchState &pending,
                                            int64_t &pending_dim)
{
  int ret = OB_SUCCESS;
  ObString grp_input_type("text");
  bool has_text = false;
  bool has_image = false;
  if (!grp_contents.empty()) {
    for (int64_t k = 0; k < grp_input_types.count(); ++k) {
      if (grp_input_types.at(k).case_compare("text") == 0) {
        has_text = true;
      } else if (grp_input_types.at(k).case_compare("image") == 0) {
        has_image = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (pending.initialized_ &&
             (pending_dim != grp_dim || pending.model_id_ != grp_model_id)) {
    if (OB_FAIL(ObAIFuncBatchUtils::flush_pending_batch(
                    allocator, expr, ctx, pending, res_vec, eval_flags,
                    pack_embed_response_to_indices))) {
      LOG_WARN("fail to flush pending batch on key change", K(ret));
    } else {
      pending.reset();
      pending_dim = 0;
      allocator.reuse();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!pending.initialized_) {
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(grp_model_id, endpoint_info))) {
      LOG_WARN("fail to get endpoint info", K(ret), K(grp_model_id));
    } else if (OB_FAIL(ObAIFuncBatchUtils::init_pending_state(
                   allocator, grp_model_id, endpoint_info, pending,
                   ObAIFuncUtils::check_info_type_dense_embedding))) {
      LOG_WARN("fail to init pending state", K(ret), K(grp_model_id));
    } else {
      pending_dim = grp_dim;
    }
  }

  if (OB_SUCC(ret)) {
    // Copy grp_contents into allocator so pending bodies are self-contained; arena can be reused after flush.
    ObArray<ObString> grp_contents_in_batch;
    for (int64_t i = 0; OB_SUCC(ret) && i < grp_contents.count(); ++i) {
      const ObString &s = grp_contents.at(i);
      char *buf = static_cast<char *>(allocator.alloc(s.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc content copy in batch arena", K(ret), K(i), K(s.length()));
      } else {
        MEMCPY(buf, s.ptr(), s.length());
        if (OB_FAIL(grp_contents_in_batch.push_back(ObString(s.length(), buf)))) {
          LOG_WARN("fail to push back content copy", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // DashScope qwen2.x-vl models forbid duplicate types in one `contents` array:
      // "Each type can appear at most once." Send each row as its own request.
      // qwen3+ VL models do support batching multiple same-type items.
      ObString request_model_name = pending.info_->model_;
      if (!pending.endpoint_info_->get_request_model_name().empty()) {
        request_model_name = pending.endpoint_info_->get_request_model_name();
      }
      const bool is_vl_model = ObAIFuncUtils::is_multi_model(request_model_name);
      const bool needs_individual = is_vl_model &&
                                    ObAIFuncUtils::vl_model_needs_individual_requests(request_model_name);
      // VL models that don't need individual requests can support mixed text/image in one batch.
      const bool allow_mixed = is_vl_model && !needs_individual;
      if (has_text && has_image && !allow_mixed) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("mixed input types in one batch are not supported for non-VL models", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "ai_embed, mixed text and image in one batch");
      }

      if (needs_individual) {
        for (int64_t i = 0; OB_SUCC(ret) && i < grp_contents_in_batch.count(); ++i) {
          ObJsonObject *item_config = nullptr;
          ObJsonObject *item_body = nullptr;
          if (grp_dim > 0) {
            ObJsonInt *dim_json = nullptr;
            if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, grp_dim, dim_json))) {
              LOG_WARN("fail to get json int", K(ret));
            } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, item_config))) {
              LOG_WARN("fail to get json object", K(ret));
            } else if (OB_FAIL(item_config->add("dimensions", dim_json))) {
              LOG_WARN("fail to add dimensions", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            ObArray<ObString> single_content;
            ObString item_type = grp_input_types.at(i);
            if (OB_FAIL(single_content.push_back(grp_contents_in_batch.at(i)))) {
              LOG_WARN("fail to push single content", K(ret), K(i));
            } else if (OB_FAIL(ObAIFuncUtils::get_embed_body(allocator, *pending.info_,
                                                              *pending.endpoint_info_,
                                                              single_content, item_config,
                                                              item_type, item_body))) {
              LOG_WARN("fail to get embed body for vl item", K(ret), K(i));
            } else if (OB_FAIL(pending.bodies_.push_back(item_body))) {
              LOG_WARN("fail to push back body", K(ret));
            } else if (OB_FAIL(pending.row_starts_.push_back(pending.row_indices_flat_.count()))) {
              LOG_WARN("fail to push back row start", K(ret));
            } else if (OB_FAIL(pending.row_lens_.push_back(1))) {
              LOG_WARN("fail to push back row len", K(ret));
            } else if (OB_FAIL(pending.row_indices_flat_.push_back(grp_row_idxs.at(i)))) {
              LOG_WARN("fail to push back row index", K(ret), K(i));
            }
          }
        }
      } else {
        ObJsonObject *config = nullptr;
        ObJsonObject *body = nullptr;
        if (grp_dim > 0) {
          ObJsonInt *dim_json = nullptr;
          if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, grp_dim, dim_json))) {
            LOG_WARN("fail to get json int", K(ret));
          } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
            LOG_WARN("fail to get json object", K(ret));
          } else if (OB_FAIL(config->add("dimensions", dim_json))) {
            LOG_WARN("fail to add dimensions", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (allow_mixed && has_text && has_image) {
          // VL model with mixed text/image: use input_type_array version
          if (OB_FAIL(ObAIFuncUtils::get_embed_body(allocator, *pending.info_,
                                                     *pending.endpoint_info_, grp_contents_in_batch,
                                                     config, grp_input_types, body))) {
            LOG_WARN("fail to get embed body for mixed types", K(ret));
          }
        } else {
          // Non-mixed type: set grp_input_type based on content
          if (has_image) {
            grp_input_type = ObString("image");
          }
          if (OB_FAIL(ObAIFuncUtils::get_embed_body(allocator, *pending.info_,
                                                          *pending.endpoint_info_, grp_contents_in_batch,
                                                          config, grp_input_type, body))) {
            LOG_WARN("fail to get embed body", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(pending.bodies_.push_back(body))) {
          LOG_WARN("fail to push back body", K(ret));
        } else if (OB_FAIL(pending.row_starts_.push_back(pending.row_indices_flat_.count()))) {
          LOG_WARN("fail to push back row start", K(ret));
        } else if (OB_FAIL(pending.row_lens_.push_back(grp_row_idxs.count()))) {
          LOG_WARN("fail to push back row len", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < grp_row_idxs.count(); ++j) {
            if (OB_FAIL(pending.row_indices_flat_.push_back(grp_row_idxs.at(j)))) {
              LOG_WARN("fail to push back row index", K(ret), K(j));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObIVector *res_vec = expr.get_vector(ctx);
  if (OB_ISNULL(res_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_vec is null", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_EMBED));
    ObArenaAllocator batch_arena(lib::ObMemAttr(tenant_id, "AIEmbedBatch"));

    // Determine whether args[2] is dim (integer) or options (varchar)
    const bool args2_is_dim = expr.arg_cnt_ > 2 &&
                              ob_is_integer_type(expr.args_[DIM_IDX]->datum_meta_.type_);
    bool has_options_arg = false;
    int64_t options_arg_idx = -1;
    if (expr.arg_cnt_ == 4) {
      has_options_arg = true;
      options_arg_idx = OPTIONS_IDX;
    } else if (expr.arg_cnt_ == 3 && !args2_is_dim) {
      has_options_arg = true;
      options_arg_idx = DIM_IDX;
    }

    if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval model arg", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval content arg", K(ret));
    } else if (args2_is_dim && OB_FAIL(expr.args_[DIM_IDX]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval dim arg", K(ret));
    } else if (has_options_arg &&
               OB_FAIL(expr.args_[options_arg_idx]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval options arg", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObIVector *model_vec   = expr.args_[0]->get_vector(ctx);
      ObIVector *content_vec = expr.args_[1]->get_vector(ctx);
      ObIVector *dim_vec     = args2_is_dim ? expr.args_[DIM_IDX]->get_vector(ctx) : nullptr;
      ObIVector *options_vec = has_options_arg ?
                               expr.args_[options_arg_idx]->get_vector(ctx) : nullptr;
      ObBitVector &eval_flags     = expr.get_evaluated_flags(ctx);
      const bool model_is_batch   = expr.args_[MODEL_IDX]->is_batch_result();
      const bool dim_is_batch     = OB_NOT_NULL(dim_vec) ?
                                    expr.args_[DIM_IDX]->is_batch_result() : false;
      const bool options_is_batch = (has_options_arg && OB_NOT_NULL(options_vec)) ?
                                    expr.args_[options_arg_idx]->is_batch_result() : false;

      // Constant-column caches: each is read once and reused for every row.
      ObString const_model_id;
      bool const_model_id_ready = false;
      int64_t const_dim = 0;
      bool const_dim_ready = false;
      ObAiEmbedInputType const_input_type = ObAiEmbedInputType::INPUT_TYPE_TEXT;
      bool const_options_ready = false;

      omt::ObAiServiceGuard ai_service_guard;
      omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
      if (OB_ISNULL(ai_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ai service is null", K(ret));
      } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
        LOG_WARN("failed to get ai service guard", K(ret));
      }

      // Current in-flight group: rows grouped by (model_id, dim), flushed when key changes,
      // batch size is reached, or adding the row would exceed 100MB total bytes.
      ObString cur_grp_model_id;
      int64_t cur_grp_dim = 0;
      int64_t cur_grp_total_bytes = 0;
      int64_t cur_batch_size_limit = AI_EMBED_DEFAULT_BATCH_SIZE;
      int64_t cur_max_image_size = 0;
      ObArray<ObString> cur_contents;
      ObArray<ObString> cur_input_types;
      ObArray<int64_t>  cur_bound_idxs;
      bool cur_grp_initialized = false;

      common::ObAIFuncBatchState pending;
      int64_t pending_dim = 0;

      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        const int64_t model_idx   = model_is_batch ? idx : 0;
        const int64_t content_idx = idx;
        const int64_t dim_idx     = dim_is_batch ? idx : 0;
        const int64_t options_idx = options_is_batch ? idx : 0;
        if (skip.at(idx) || eval_flags.at(idx)) {
        } else if (model_vec->is_null(model_idx) || content_vec->is_null(content_idx)) {
          res_vec->set_null(idx);
        } else {
          // Read model_id (cache for constant column)
          ObString model_id_i;
          if (!model_is_batch) {
            if (!const_model_id_ready) {
              if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          temp_allocator, model_vec, expr.args_[MODEL_IDX]->datum_meta_,
                          expr.args_[MODEL_IDX]->obj_meta_.has_lob_header(),
                          const_model_id, model_idx))) {
                LOG_WARN("fail to read const model_id", K(ret), K(idx));
              } else if (const_model_id.empty()) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("const model_id is empty", K(ret), K(idx));
                LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is empty");
              } else {
                const_model_id_ready = true;
              }
            }
            if (OB_SUCC(ret)) {
              model_id_i = const_model_id;
            }
          } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                             temp_allocator, model_vec, expr.args_[MODEL_IDX]->datum_meta_,
                             expr.args_[MODEL_IDX]->obj_meta_.has_lob_header(),
                             model_id_i, model_idx))) {
            LOG_WARN("fail to read model_id", K(ret), K(idx));
          } else if (model_id_i.empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("model_id is empty", K(ret), K(idx));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is empty");
          }

          // Read dim (cache for constant column)
          int64_t dim_i = 0;
          if (OB_SUCC(ret) && OB_NOT_NULL(dim_vec)) {
            if (!dim_is_batch) {
              if (!const_dim_ready) {
                if (!dim_vec->is_null(dim_idx)) {
                  const_dim = dim_vec->get_int(dim_idx);
                  if (const_dim <= 0) {
                    ret = OB_INVALID_ARGUMENT;
                    LOG_WARN("const dimension must be a positive integer", K(ret), K(const_dim));
                    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                                   "ai_embed, dimension parameter must be a positive integer");
                  }
                }
                if (OB_SUCC(ret)) {
                  const_dim_ready = true;
                }
              }
              if (OB_SUCC(ret)) {
                dim_i = const_dim;
              }
            } else if (!dim_vec->is_null(dim_idx)) {
              dim_i = dim_vec->get_int(dim_idx);
              if (dim_i <= 0) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("dimension must be a positive integer", K(ret), K(dim_i), K(idx));
                LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                               "ai_embed, dimension parameter must be a positive integer");
            }
          }
          }

          // Fetch model config when model changes (for batch_size and max_image_size)
          if (OB_SUCC(ret) && (!cur_grp_initialized || cur_grp_model_id != model_id_i)) {
            share::ObAIModelConfigInfo model_config;
            if (OB_FAIL(ObAIFuncUtils::get_model_config_info(temp_allocator, model_id_i, model_config))) {
              LOG_WARN("fail to get model config for batch_size/max_image_size", K(ret), K(model_id_i));
            } else {
              cur_batch_size_limit = model_config.get_batch_size() > 0
                  ? model_config.get_batch_size()
                  : AI_EMBED_DEFAULT_BATCH_SIZE;
              cur_max_image_size = model_config.get_max_image_size();
            }
          }

          // Read content
          ObString raw_content;
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        temp_allocator, content_vec, expr.args_[CONTENT_IDX]->datum_meta_,
                        expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(),
                        raw_content, content_idx, &ctx.exec_ctx_))) {
              LOG_WARN("fail to read content", K(ret), K(idx));
            } else if (raw_content.empty()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("content is empty", K(ret), K(idx));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, input is empty");
            }
          }

          // Read options and resolve input type (cache for constant column)
          ObAiEmbedInputType input_type = ObAiEmbedInputType::INPUT_TYPE_TEXT;
          if (OB_SUCC(ret) && OB_NOT_NULL(options_vec)) {
            if (!options_is_batch) {
              if (!const_options_ready) {
                if (!options_vec->is_null(options_idx)) {
                  ObString options_str;
                  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                              temp_allocator, options_vec,
                              expr.args_[options_arg_idx]->datum_meta_,
                              expr.args_[options_arg_idx]->obj_meta_.has_lob_header(),
                              options_str, options_idx))) {
                    LOG_WARN("fail to read const options", K(ret), K(idx));
                  } else if (!options_str.empty() &&
                             OB_FAIL(parse_embed_options(temp_allocator, options_str,
                                                         const_input_type))) {
                    LOG_WARN("fail to parse const embed options", K(ret), K(idx));
                  }
                }
                if (OB_SUCC(ret)) {
                  const_options_ready = true;
                }
              }
              if (OB_SUCC(ret)) {
                input_type = const_input_type;
              }
            } else if (!options_vec->is_null(options_idx)) {
              ObString options_str;
              if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          temp_allocator, options_vec,
                          expr.args_[options_arg_idx]->datum_meta_,
                          expr.args_[options_arg_idx]->obj_meta_.has_lob_header(),
                          options_str, options_idx))) {
                LOG_WARN("fail to read options", K(ret), K(idx));
              } else if (!options_str.empty() &&
                         OB_FAIL(parse_embed_options(temp_allocator, options_str, input_type))) {
                LOG_WARN("fail to parse embed options", K(ret), K(idx));
              }
            }
          }

          // Pre-process image content (URL validation or binary → base64 data URI)
          ObString processed_content = raw_content;
          if (OB_SUCC(ret) && input_type == ObAiEmbedInputType::INPUT_TYPE_IMAGE) {
            const bool is_url = raw_content.prefix_match_ci("http://")
                             || raw_content.prefix_match_ci("https://");
            if (!is_url
                && cur_max_image_size > 0
                && raw_content.length() > cur_max_image_size) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("image binary size exceeds model max_image_size limit", K(ret), K(idx),
                       K(raw_content.length()), K(cur_max_image_size));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, image binary size exceeds max_image_size limit");
            }
            if (OB_SUCC(ret) &&
                OB_FAIL(process_image_input(temp_allocator, raw_content, is_url,
                                            processed_content))) {
              LOG_WARN("fail to process image input", K(ret), K(idx));
            }
          }

          if (OB_SUCC(ret)) {
            ObString type_str = (input_type == ObAiEmbedInputType::INPUT_TYPE_IMAGE) ?
                                ObString("image") : ObString("text");

            // Decide whether this row belongs to the current group.
            const int64_t row_bytes = processed_content.length();
            const bool same_grp = cur_grp_initialized &&
                                  model_id_i == cur_grp_model_id &&
                                  dim_i == cur_grp_dim &&
                                  cur_contents.count() < cur_batch_size_limit &&
                                  cur_grp_total_bytes + row_bytes <= AI_EMBED_BATCH_MAX_BYTES;

            if (same_grp) {
              if (OB_FAIL(cur_contents.push_back(processed_content)) ||
                  OB_FAIL(cur_input_types.push_back(type_str)) ||
                  OB_FAIL(cur_bound_idxs.push_back(idx))) {
                LOG_WARN("fail to push back to current group", K(ret), K(idx));
              } else {
                cur_grp_total_bytes += row_bytes;
              }
            } else {
              if (cur_grp_initialized &&
                  OB_FAIL(enqueue_group_to_pending(batch_arena, expr, ctx, ai_service_guard,
                                                   res_vec, eval_flags, cur_grp_model_id, cur_grp_dim,
                                                   cur_contents, cur_input_types,
                                                   cur_bound_idxs, pending, pending_dim))) {
                LOG_WARN("fail to enqueue current group", K(ret));
              } else if (OB_SUCC(ret)) {
                cur_contents.reuse();
                cur_input_types.reuse();
                cur_bound_idxs.reuse();
                cur_grp_model_id = model_id_i;
                cur_grp_dim = dim_i;
                cur_grp_total_bytes = row_bytes;
                cur_grp_initialized = true;
                if (OB_FAIL(cur_contents.push_back(processed_content)) ||
                    OB_FAIL(cur_input_types.push_back(type_str)) ||
                    OB_FAIL(cur_bound_idxs.push_back(idx))) {
                  LOG_WARN("fail to push back to new group", K(ret), K(idx));
                }
              }
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (cur_grp_initialized &&
                 OB_FAIL(enqueue_group_to_pending(batch_arena, expr, ctx, ai_service_guard,
                                                  res_vec, eval_flags, cur_grp_model_id, cur_grp_dim,
                                                  cur_contents, cur_input_types,
                                                  cur_bound_idxs, pending, pending_dim))) {
        LOG_WARN("fail to enqueue last group", K(ret));
      } else if (OB_FAIL(ObAIFuncBatchUtils::flush_pending_batch(
                     batch_arena, expr, ctx, pending, res_vec, eval_flags,
                     pack_embed_response_to_indices))) {
        LOG_WARN("fail to flush pending batch", K(ret));
      } else {
        pending.reset();
        batch_arena.reuse();
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

int ObExprAIEmbed::pack_embed_response_to_indices(const ObExpr &expr,
                                                  ObEvalCtx &ctx,
                                                  ObIAllocator &allocator,
                                                  ObJsonObject *response,
                                                  const ObArray<int64_t> &row_indices,
                                                  const ObAiModelEndpointInfo &endpoint_info,
                                                  ObIVector *res_vec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(response) || OB_ISNULL(res_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("response or res_vec is null", K(ret));
  } else {
    ObIJsonBase *output = nullptr;
    if (OB_FAIL(ObAIFuncUtils::parse_embed_output(allocator, endpoint_info, response, output))) {
      LOG_WARN("fail to parse embed output", K(ret));
    } else if (OB_ISNULL(output)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output is null", K(ret));
    } else if (output->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output is not array", K(ret));
    } else {
      ObJsonArray *embeddings_array = static_cast<ObJsonArray *>(output);
      int64_t embed_count = embeddings_array->element_count();
      if (embed_count != row_indices.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("embedding count does not match row count", K(ret),
                 K(embed_count), K(row_indices.count()));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < embed_count; ++j) {
          ObIJsonBase *embedding = embeddings_array->get_value(j);
          if (OB_ISNULL(embedding)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("embedding is null", K(ret), K(j));
          } else {
            ObStringBuffer embedding_buf(&allocator);
            ObString raw_str;
            if (OB_FAIL(embedding->print(embedding_buf, 0))) {
              LOG_WARN("fail to print embedding", K(ret));
            } else if (OB_ISNULL(raw_str = embedding_buf.string())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get embedding string", K(ret));
            } else if (OB_FAIL(ObAIFuncJsonUtils::inner_pack_raw_str_to_res(
                                raw_str, expr, ctx, res_vec, row_indices.at(j)))) {
              LOG_WARN("fail to pack embedding to res", K(ret), K(j));
            }
          }
        }
      }
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

int ObExprAIEmbed::parse_embed_options(ObIAllocator &allocator,
                                       const ObString &options_str,
                                       ObAiEmbedInputType &input_type)
{
  int ret = OB_SUCCESS;
  input_type = ObAiEmbedInputType::INPUT_TYPE_TEXT;  // default to TEXT type

  if (!options_str.empty()) {
    ObJsonObject *options_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(allocator, options_str, options_obj))) {
      LOG_WARN("fail to parse options json", K(ret));
    } else if (OB_NOT_NULL(options_obj)) {
      ObJsonNode *type_node = options_obj->get_value("type");
      if (OB_NOT_NULL(type_node) && type_node->json_type() == ObJsonNodeType::J_STRING) {
        ObString type_str = static_cast<ObJsonString *>(type_node)->get_str();
        if (type_str.case_compare("image") == 0) {
          input_type = ObAiEmbedInputType::INPUT_TYPE_IMAGE;
        } else if (type_str.case_compare("text") == 0) {
          input_type = ObAiEmbedInputType::INPUT_TYPE_TEXT;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid input type", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, invalid input type");
        }
      }
    }
  }
  // empty options_str is valid, use default TEXT type

  return ret;
}

int ObExprAIEmbed::process_image_input(ObIAllocator &allocator,
                                        const ObString &image_input,
                                        bool is_url,
                                        ObString &processed_content)
{
  int ret = OB_SUCCESS;

  if (is_url) {
    // URL validation: must start with http:// or https://
    if (image_input.length() < 8) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid image URL", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, image URL must start with http:// or https://");
    } else if (!image_input.prefix_match_ci("http://") && !image_input.prefix_match_ci("https://")) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("image URL must start with http:// or https://", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, image URL must start with http:// or https://");
    } else if (OB_FAIL(ObAIFuncUtils::check_url_security(image_input))) {
      // URL contains internal IP or disallowed hostname
      LOG_WARN("URL validation failed for security reason", K(ret));
    } else {
      // url is valid, use it directly as the processed content
      processed_content = image_input;
    }
  } else {
    // binary image data - convert to base64 data URL
    ObString base64_data_uri;
    common::ObAiFuncImageUtils::ObImageType image_type = common::ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;

    if (OB_FAIL(ObAiFuncImageUtils::get_type_from_binary(allocator, image_input, image_type))) {
      LOG_WARN("fail to get image type from binary", K(ret));
    } else if (image_type == common::ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown image type", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, unsupported image format");
    } else if (OB_FAIL(ObAiFuncImageUtils::get_base64_data_uri_from_binary(allocator, image_input, base64_data_uri))) {
      LOG_WARN("fail to convert image to base64 data URL", K(ret));
    } else {
      processed_content = base64_data_uri;
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase