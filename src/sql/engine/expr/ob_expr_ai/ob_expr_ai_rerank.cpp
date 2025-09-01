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
 * This file contains implementation for ai_rerank expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_rerank.h"
#include "lib/utility/utility.h"
#include "lib/json_type/ob_json_common.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprAIRerank::ObExprAIRerank(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc,
                    T_FUN_SYS_AI_RERANK,
                    N_AI_RERANK,
                    MORE_THAN_ZERO,
                    NOT_VALID_FOR_GENERATED_COL,
                    NOT_ROW_DIMENSION)
{
}

ObExprAIRerank::~ObExprAIRerank()
{
}

int ObExprAIRerank::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const {
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num != 3)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    types_stack[QUERY_IDX].set_calc_type(ObVarcharType);
    types_stack[QUERY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObObjType in_type = types_stack[DOCUMENTS_IDX].get_type();
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, DOCUMENTS_IDX, N_AI_RERANK))) {
      LOG_WARN("wrong type for json config.", K(ret), K(types_stack[DOCUMENTS_IDX].get_type()));
    } else if (ob_is_string_type(in_type) && types_stack[DOCUMENTS_IDX].get_collation_type() != CS_TYPE_BINARY) {
      if (types_stack[DOCUMENTS_IDX].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[DOCUMENTS_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  }
  return ret;
}

int ObExprAIRerank::eval_ai_rerank(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_query = nullptr;
  ObDatum *arg_documents = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_query, arg_documents))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (arg_model_id->is_null() || arg_query->is_null() || arg_documents->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model id or query or documents is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model id or query or documents is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_RERANK));
    ObAIFuncExprInfo *info = static_cast<ObAIFuncExprInfo *>(expr.extra_info_);
    ObString model_id = arg_model_id->get_string();
    ObString query = arg_query->get_string();
    ObArray<ObString> header_array;
    ObJsonArray *document_array = nullptr;
    ObJsonArray *result_array = nullptr;
    ObIJsonBase *j_base = nullptr;
    int64_t batch_size = 20; // max batch size for rerank
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                   &temp_allocator, arg_documents->get_string(),
                   ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base))) {
      LOG_WARN("fail to get json base", K(ret), K(arg_documents->get_string()));
    } else if (OB_ISNULL(j_base)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("j_base is null", K(ret));
    } else if (j_base->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("j_base is not array", K(ret));
    } else if (OB_ISNULL(document_array = static_cast<ObJsonArray *>(j_base))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("document_array is null", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info is null", K(ret));
      } else if (OB_FAIL(info->init(ctx.exec_ctx_, model_id))) {
        LOG_WARN("fail to get model info", K(ret));
      } else if (OB_FAIL(ObAIFuncUtils::check_info_type_rerank(info))) {
        LOG_WARN("model type must be rerank", K(ret));
      } else if (OB_FAIL(ObAIFuncUtils::get_header(temp_allocator, info, header_array))) {
        LOG_WARN("fail to get header", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(temp_allocator, result_array))) {
        LOG_WARN("fail to get json array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObString score_key(SCORE_KEY);
      int64_t end_idx = 0;
      ObJsonArray *compact_array = nullptr;
      ObJsonArray *batch_result_array = nullptr;
      ObJsonArray *batch_document_array = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < document_array->element_count(); i += batch_size) {
        end_idx = i + batch_size;
        if (end_idx > document_array->element_count()) {
          end_idx = document_array->element_count();
        }
        if (OB_FAIL(construct_batch_document_array(temp_allocator, document_array, i, end_idx, batch_document_array))) {
          LOG_WARN("fail to construct batch document array", K(ret));
        } else if (OB_FAIL(inner_eval_ai_rerank(temp_allocator, info, header_array, query, batch_document_array, batch_result_array))) {
          LOG_WARN("fail to eval ai rerank", K(ret));
        } else if (OB_FAIL(batch_result_add_base(temp_allocator, batch_result_array, i))) {
          LOG_WARN("fail to add base", K(ret));
        } else if (OB_FAIL(compact_json_array_by_key(temp_allocator, result_array, batch_result_array, score_key, compact_array))) {
          LOG_WARN("fail to compact json array", K(ret));
        } else {
          result_array = compact_array;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString raw_str;
      if (OB_FAIL(result_array->get_raw_binary(raw_str, &temp_allocator))) {
        LOG_WARN("json extarct get result binary failed", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
        LOG_WARN("fail to pack json result", K(ret));
      }
    }
  }
  return ret;
}

int ObExprAIRerank::construct_batch_document_array(ObIAllocator &allocator, ObJsonArray *document_array, int64_t start_idx, int64_t end_idx, ObJsonArray *&batch_document_array)
{
  INIT_SUCC(ret);
  ObJsonArray *result_array = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
    LOG_WARN("fail to get json array", K(ret));
  } else {
    for (int64_t i = start_idx; i < end_idx; i++) {
      if (OB_FAIL(result_array->append(document_array->get_value(i)))) {
        LOG_WARN("fail to append", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    batch_document_array = result_array;
  }
  return ret;
}

int ObExprAIRerank::inner_eval_ai_rerank(ObIAllocator &allocator,
                                          ObAIFuncExprInfo *info,
                                          ObArray<ObString> &header_array,
                                          ObString &query,
                                          ObJsonArray *document_array,
                                          ObJsonArray *&result_array)
{
  INIT_SUCC(ret);
  ObJsonArray *res = nullptr;
  ObJsonObject *config_json = nullptr;
  ObJsonObject *body = nullptr;
  ObJsonObject *http_response = nullptr;
  ObIJsonBase *response = nullptr;
  ObAIFuncClient ai_client;
  if (OB_FAIL(ObAIFuncUtils::get_rerank_body(allocator, info, query, document_array, config_json, body))) {
    LOG_WARN("fail to get body", K(ret));
  } else if (OB_FAIL(ai_client.send_post(allocator, info->url_, header_array, body, http_response))) {
    LOG_WARN("fail to send post", K(ret));
  } else if (OB_FAIL(ObAIFuncUtils::parse_rerank_output(allocator, info, http_response, response))) {
    LOG_WARN("fail to parse response", K(ret));
  } else if (response->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("response is not array", K(ret));
  } else if (OB_ISNULL(res = static_cast<ObJsonArray *>(response))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res is null", K(ret));
  } else {
    result_array = res;
  }
  return ret;
}

int ObExprAIRerank::batch_result_add_base(ObIAllocator &allocator, ObJsonArray *array, int64_t start_idx)
{
  INIT_SUCC(ret);
  int64_t count = array->element_count();
  ObString index_key(INDEX_KEY);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObJsonObject *obj = static_cast<ObJsonObject *>(array->get_value(i));
    if (OB_ISNULL(obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj is null", K(ret));
    } else {
      ObJsonInt *int_obj = static_cast<ObJsonInt *>(obj->get_value(index_key));
      if (OB_ISNULL(int_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("int_obj is null", K(ret));
      } else {
        int64_t id = int_obj->value() + start_idx;
        int_obj->set_value(id);
      }
    }
  }
  return ret;
}

int ObExprAIRerank::compact_json_array_by_key(ObIAllocator &allocator, ObJsonArray *array1, ObJsonArray *array2, ObString &score_key, ObJsonArray *&compact_array)
{
  INIT_SUCC(ret);
  ObJsonArray *result_array = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
    LOG_WARN("fail to get json array", K(ret));
  } else {
    int64_t count1 = array1->element_count();
    int64_t count2 = array2->element_count();
    int64_t min_count = count1 < count2 ? count1 : count2;
    int64_t i = 0;
    int64_t j = 0;
    while (i < min_count && j < min_count) {
      ObJsonObject *obj1 = static_cast<ObJsonObject *>(array1->get_value(i));
      ObJsonObject *obj2 = static_cast<ObJsonObject *>(array2->get_value(j));
      if (OB_ISNULL(obj1) || OB_ISNULL(obj2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj1 or obj2 is null", K(ret));
      } else {
        ObJsonDouble *double1 = static_cast<ObJsonDouble *>(obj1->get_value(score_key));
        ObJsonDouble *double2 = static_cast<ObJsonDouble *>(obj2->get_value(score_key));
        if (OB_ISNULL(double1) || OB_ISNULL(double2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("double1 or double2 is null", K(ret));
        } else {
          if (double1->value() > double2->value()) {
            if (OB_FAIL(result_array->append(obj1))) {
              LOG_WARN("fail to append", K(ret));
            }
            i++;
          } else {
            if (OB_FAIL(result_array->append(obj2))) {
              LOG_WARN("fail to append", K(ret));
            }
            j++;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      while (i < count1) {
        if (OB_FAIL(result_array->append(array1->get_value(i)))) {
          LOG_WARN("fail to append", K(ret));
        }
        i++;
      }
    }
    if (OB_SUCC(ret)) {
      while (j < count2) {
        if (OB_FAIL(result_array->append(array2->get_value(j)))) {
          LOG_WARN("fail to append", K(ret));
        }
        j++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    compact_array = result_array;
  }
  return ret;
}

int ObExprAIRerank::construct_config_json(ObIAllocator &allocator, int64_t top_n, int64_t return_doc, ObJsonObject *&config_json)
{
  INIT_SUCC(ret);
  ObJsonObject *config_obj = nullptr;
  ObJsonInt *top_n_obj = nullptr;
  ObJsonBoolean *return_doc_obj = nullptr;
  bool return_doc_value = true;
  if (return_doc <= 0) {
    return_doc_value = false;
  }
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config_obj))) {
    LOG_WARN("fail to get json object", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_boolean(allocator, return_doc_value, return_doc_obj))) {
    LOG_WARN("fail to get return_doc", K(ret));
  } else if (OB_FAIL(config_obj->add("return_documents", return_doc_obj))) {
    LOG_WARN("fail to add return_doc", K(ret));
  } else if (top_n > 0) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, top_n, top_n_obj))) {
      LOG_WARN("fail to get top_n", K(ret));
    } else if (OB_FAIL(config_obj->add("top_n", top_n_obj))) {
      LOG_WARN("fail to add top_n", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    config_json = config_obj;
  }
  return ret;
}

int ObExprAIRerank::cg_expr(ObExprCGCtx &expr_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = *expr_cg_ctx.allocator_;
  ObAIFuncExprInfo *info = OB_NEWx(ObAIFuncExprInfo, (&allocator), allocator, T_FUN_SYS_AI_RERANK);
  if (OB_ISNULL(info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ai rerank info", K(ret));
  } else {
    rt_expr.extra_info_ = info;
  }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIRerank::eval_ai_rerank;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase