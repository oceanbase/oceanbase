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

#ifndef OB_AI_FUNC_UTILS_H_
#define OB_AI_FUNC_UTILS_H_

#include "ob_ai_func.h"

namespace oceanbase
{
namespace common
{

class ObOpenAIUtils
{
public:
  class ObOpenAIComplete : public ObAIFuncIComplete
  {
  public:
    ObOpenAIComplete() {}
    virtual ~ObOpenAIComplete() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &prompt,
                         common::ObString &content,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
    virtual int set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config) override;
    static int construct_messages_array(ObIAllocator &allocator, ObString &prompt, ObString &content, ObJsonArray *&messages);
    static int construct_message_obj(ObIAllocator &allocator, ObString &role, ObString &content, ObJsonObject *&message);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObOpenAIComplete);
  };

  class ObOpenAIEmbed : public ObAIFuncIEmbed
  {
  public:
    ObOpenAIEmbed() {}
    virtual ~ObOpenAIEmbed() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObOpenAIEmbed);
  };
  static int get_header(common::ObIAllocator &allocator,
                        common::ObString &api_key,
                        common::ObArray<ObString> &headers);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOpenAIUtils);
};

class ObOllamaUtils
{
public:
  class ObOllamaComplete : public ObAIFuncIComplete
  {
  public:
    ObOllamaComplete() {}
    virtual ~ObOllamaComplete() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &prompt,
                         common::ObString &content,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
    virtual int set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObOllamaComplete);
  };

  class ObOllamaEmbed : public ObAIFuncIEmbed
  {
  public:
    ObOllamaEmbed() {}
    virtual ~ObOllamaEmbed() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObOllamaEmbed);
  };
  static int get_header(common::ObIAllocator &allocator, common::ObArray<ObString> &headers);
  DISALLOW_COPY_AND_ASSIGN(ObOllamaUtils);
};

class ObDashscopeUtils
{
public:
  class ObDashscopeComplete : public ObAIFuncIComplete
  {
  public:
    ObDashscopeComplete() {}
    virtual ~ObDashscopeComplete() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &prompt,
                         common::ObString &content,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
    virtual int set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config) override;
    static int set_config_result_format(common::ObIAllocator &allocator, common::ObJsonObject *config);
    static int construct_input_obj(ObIAllocator &allocator, ObString &prompt, ObString &content, ObJsonObject *&input_obj);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObDashscopeComplete);
  };
  class ObDashscopeEmbed : public ObAIFuncIEmbed
  {
  public:
    ObDashscopeEmbed() {}
    virtual ~ObDashscopeEmbed() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObDashscopeEmbed);
  };
  class ObDashscopeRerank : public ObAIFuncIRerank
  {
  public:
    ObDashscopeRerank() {}
    virtual ~ObDashscopeRerank() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &query,
                         common::ObJsonArray *document_array,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    static int get_input_obj(common::ObIAllocator &allocator,
                             common::ObString &query,
                             common::ObJsonArray *document_array,
                             common::ObJsonObject *&input_obj);
    DISALLOW_COPY_AND_ASSIGN(ObDashscopeRerank);
  };
  static int get_header(common::ObIAllocator &allocator, common::ObString &api_key, common::ObArray<ObString> &headers);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDashscopeUtils);
};

class ObSiliconflowUtils
{
public:
  class ObSiliconflowRerank : public ObAIFuncIRerank
  {
  public:
    ObSiliconflowRerank() {}
    virtual ~ObSiliconflowRerank() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &query,
                         common::ObJsonArray *document_array,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObSiliconflowRerank);
  };
  static int get_header(common::ObIAllocator &allocator,
                        common::ObString &api_key,
                        common::ObArray<ObString> &headers);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSiliconflowUtils);
};
class ObAIFuncJsonUtils
{
public:
  ObAIFuncJsonUtils() {}
  virtual ~ObAIFuncJsonUtils() {}
  static bool ob_is_json_array_all_str(ObJsonArray* json_array);
  static int transform_array_to_json_array(ObIAllocator &allocator, ObArray<ObString> &contents, ObJsonArray *&array);
  static int get_json_object(ObIAllocator &allocator, ObJsonObject *&obj_node);
  static int get_json_array(ObIAllocator &allocator, ObJsonArray *&array_node);
  static int get_json_string(ObIAllocator &allocator, ObString &str, ObJsonString *&str_node);
  static int get_json_int(ObIAllocator &allocator, int64_t num, ObJsonInt *&int_node);
  static int get_json_boolean(ObIAllocator &allocator, bool value, ObJsonBoolean *&boolean_node);
  static int get_json_object_form_str(ObIAllocator &allocator, ObString &str, ObJsonObject *&obj_node);
  static int print_json_to_str(ObIAllocator &allocator, ObIJsonBase *base_node, ObString &str);
  static int compact_json_object(ObIAllocator &allocator, ObJsonObject *obj_node, ObJsonObject *compact_obj);
  static int copy_raw_str_to_res(ObString &raw_str, char *res_buf, int64_t res_buf_len)
  {
    int ret = OB_SUCCESS;
    if (res_buf_len < raw_str.length()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get invalid res buf len", K(ret), K(res_buf_len), K(raw_str.length()));
    } else {
      MEMCPY(res_buf, raw_str.ptr(), raw_str.length());
    }
    return ret;
  }
  template <typename ResVec>
  static int set_raw_str_res(ObString &raw_str, const ObExpr &expr, ObEvalCtx &ctx,
                           ResVec *res_vec, int64_t batch_idx)
  {
    int ret = OB_SUCCESS;
    int32_t res_size = raw_str.length();
    char *res_buf = nullptr;
    int64_t res_buf_len = 0;
    ObTextStringVectorResult<ResVec> str_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, batch_idx);
    if (OB_FAIL(str_result.init_with_batch_idx(res_size, batch_idx))) {
      SQL_ENG_LOG(WARN, "fail to init result", K(ret), K(res_size));
    } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
      SQL_ENG_LOG(WARN, "fail to get reserver buffer", K(ret));
    } else if (res_buf_len < res_size) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
    } else if (OB_FAIL(ObAIFuncJsonUtils::copy_raw_str_to_res(raw_str, res_buf, res_buf_len))) {
      SQL_ENG_LOG(WARN, "get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
    } else if (OB_FAIL(str_result.lseek(res_size, 0))) {
      SQL_ENG_LOG(WARN, "failed to lseek res.", K(ret), K(str_result), K(res_size));
    } else {
      str_result.set_result();
    }
    return ret;
  }
  static int inner_pack_raw_str_to_res(ObString &raw_str, const ObExpr &expr, ObEvalCtx &ctx,
                              ObIVector *res_vec, int64_t batch_idx)
  {
    int ret = OB_SUCCESS;
    VectorFormat res_format = expr.get_format(ctx);
    switch (res_format) {
      case VEC_DISCRETE: {
        ret = set_raw_str_res<ObDiscreteFormat>(raw_str, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), batch_idx);
        break;
      }
      case VEC_CONTINUOUS: {
        ret = set_raw_str_res<ObContinuousFormat>(raw_str, expr, ctx, static_cast<ObContinuousFormat *>(res_vec), batch_idx);
        break;
      }
      case VEC_UNIFORM: {
        ret = set_raw_str_res<ObUniformFormat<false>>(raw_str, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), batch_idx);
        break;
      }
      case VEC_UNIFORM_CONST: {
        ret = set_raw_str_res<ObUniformFormat<true>>(raw_str, expr, ctx, static_cast<ObUniformFormat<true> *>(res_vec), batch_idx);
        break;
      }
      default: {
        ret = set_raw_str_res<ObVectorBase>(raw_str, expr, ctx, static_cast<ObVectorBase *>(res_vec), batch_idx);
      }
    }
    if (OB_FAIL(ret)) {
      SQL_ENG_LOG(WARN, "fail to set json res", K(ret));
    }
    return ret;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncJsonUtils);
};

class ObAIFuncPromptUtils
{
public:
  ObAIFuncPromptUtils() {}
  virtual ~ObAIFuncPromptUtils() {}
  static int replace_meta_prompt(ObIAllocator &allocator, ObString &meta_prompt, ObString &key, ObString &content, ObString &result);
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncPromptUtils);
};

class ObAIFuncProviderUtils
{
public:
  ObAIFuncProviderUtils() {}
  virtual ~ObAIFuncProviderUtils() {}
  static constexpr char OPENAI[20] = "OPENAI";
  static constexpr char OLLAMA[20] = "OLLAMA";
  static constexpr char ALIYUN[20] = "ALIYUN-OPENAI";
  static constexpr char DASHSCOPE[20] = "ALIYUN-DASHSCOPE";
  static constexpr char SILICONFLOW[20] = "SILICONFLOW";
  static constexpr char HUNYUAN[20] = "HUNYUAN-OPENAI";
  static constexpr char DEEPSEEK[20] = "DEEPSEEK";

private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncProviderUtils);
};

class ObAIFuncUtils
{
public:
  ObAIFuncUtils() {}
  virtual ~ObAIFuncUtils() {}
  static bool is_completion_type(ObAIFuncExprInfo *info) {return info->type_ == share::EndpointType::COMPLETION;}
  static bool is_dense_embedding_type(ObAIFuncExprInfo *info) {return info->type_ == share::EndpointType::DENSE_EMBEDDING;}
  static bool is_rerank_type(ObAIFuncExprInfo *info) {return info->type_ == share::EndpointType::RERANK;}
  static int check_info_type_completion(ObAIFuncExprInfo *info);
  static int check_info_type_dense_embedding(ObAIFuncExprInfo *info);
  static int check_info_type_rerank(ObAIFuncExprInfo *info);
  static bool ob_provider_check(ObString &provider, const char *str_const) {return ob_check_string_equal_char(provider, str_const);}
  static bool ob_type_check(ObString &type, const char *type_const) {return ob_check_string_equal_char(type, type_const);}
  static bool ob_check_string_equal_char(ObString &str, const char *str_const) {return str.case_compare(str_const) == 0;}
  static int get_complete_provider(ObIAllocator &allocator, ObString &provider, ObAIFuncIComplete *&complete_provider);
  static int get_embed_provider(ObIAllocator &allocator, ObString &provider, ObAIFuncIEmbed *&embed_provider);
  static int get_rerank_provider(ObIAllocator &allocator, ObString &provider, ObAIFuncIRerank *&rerank_provider);
  static int get_header(ObIAllocator &allocator,
                        ObAIFuncExprInfo *info,
                        ObArray<ObString> &headers);
  static int get_complete_body(ObIAllocator &allocator,
                               ObAIFuncExprInfo *info,
                               ObString &prompt,
                               ObString &content,
                               ObJsonObject *config,
                               ObJsonObject *&body);
  static int set_json_format_config(ObIAllocator &allocator, ObString &provider, ObJsonObject *config);
  static int get_embed_body(ObIAllocator &allocator,
                            ObAIFuncExprInfo *info,
                            ObArray<ObString> &contents,
                            ObJsonObject *config,
                            ObJsonObject *&body);
  static int get_rerank_body(ObIAllocator &allocator,
                             ObAIFuncExprInfo *info,
                             ObString &query,
                             ObJsonArray *document_array,
                             ObJsonObject *config,
                             ObJsonObject *&body);
  static int parse_complete_output(ObIAllocator &allocator,
                                   ObAIFuncExprInfo *info,
                                   ObJsonObject *http_response,
                                   ObIJsonBase *&result);
  static int parse_embed_output(ObIAllocator &allocator,
                                ObAIFuncExprInfo *info,
                                ObJsonObject *http_response,
                                ObIJsonBase *&result);
  static int parse_rerank_output(ObIAllocator &allocator,
                                 ObAIFuncExprInfo *info,
                                 ObJsonObject *http_response,
                                 ObIJsonBase *&result);
  static int set_string_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObString &res_str);
  static int get_ai_func_info(ObIAllocator &allocator, ObString &model_id, ObAIFuncExprInfo *&info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncUtils);
};


class ObAIFuncModel
{
public:
   ObAIFuncModel(ObIAllocator &allocator, ObAIFuncExprInfo &info) : allocator_(&allocator), info_(info) {}
   virtual ~ObAIFuncModel() {}
   // completion
   int call_completion(ObString &prompt, ObJsonObject *config, ObString &result);
   int call_completion_vector(ObArray<ObString> &prompts, ObJsonObject *config, ObArray<ObString> &results);
   // dense_embedding
   int call_dense_embedding(ObString &content, ObJsonObject *config, ObString &result);
   int call_dense_embedding_vector(ObArray<ObString> &contents, ObJsonObject *config, ObArray<ObString> &results);
   int call_dense_embedding_vector_v2(ObArray<ObString> &contents, ObJsonObject *config, ObArray<ObString> &results);
   // rerank
   int call_rerank(ObString &query, ObJsonArray *contents, ObJsonArray *&results);
 private:
   bool is_completion_type() {return info_.type_ == share::EndpointType::COMPLETION;}
   bool is_dense_embedding_type() {return info_.type_ == share::EndpointType::DENSE_EMBEDDING;}
   bool is_rerank_type() {return info_.type_ == share::EndpointType::RERANK;}
   ObIAllocator *allocator_;
   ObAIFuncExprInfo &info_;
   DISALLOW_COPY_AND_ASSIGN(ObAIFuncModel);
};

class ObAIFuncPromptObjectUtils
{
public:
  ObAIFuncPromptObjectUtils() {}
  virtual ~ObAIFuncPromptObjectUtils() {}
  static bool is_valid_prompt_object(ObJsonObject* prompt_object);
  static int replace_all_str_args_in_template(ObIAllocator &allocator, ObJsonObject* prompt_object, ObString& replaced_prompt_str);
  static int construct_prompt_object(ObIAllocator &allocator, ObString &template_str, ObJsonArray *args_array, ObJsonObject *&prompt_object);
  static constexpr char prompt_template_key[20] = "template";
  static constexpr char prompt_args_key[20] = "args";
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncPromptObjectUtils);
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_UTILS_H_ */
