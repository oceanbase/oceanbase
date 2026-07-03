/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_AI_FUNC_UTILS_H_
#define OB_AI_FUNC_UTILS_H_

#include "ob_ai_func.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/vector_index/ob_json_helper.h"

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

  class ObOpenAIVLComplete : public ObAIFuncIVLComplete
  {
  public:
    ObOpenAIVLComplete() {}
    virtual ~ObOpenAIVLComplete() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &prompt,
                         common::ObJsonObject *prompt_object,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObOpenAIVLComplete);
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
                         common::ObString input_type,
                         common::ObJsonObject *&body) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObArray<ObString> &input_type_array,
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
                         common::ObString input_type,
                         common::ObJsonObject *&body) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObArray<ObString> &input_type_array,
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
  class ObDashscopeVLComplete : public ObAIFuncIVLComplete
  {
  public:
    ObDashscopeVLComplete() {}
    virtual ~ObDashscopeVLComplete() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &prompt,
                         common::ObJsonObject *prompt_object,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObDashscopeVLComplete);
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
                         common::ObString input_type,
                         common::ObJsonObject *&body) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObArray<ObString> &input_type_array,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObDashscopeEmbed);
  };
  // Embed provider for Dashscope multi-modal (vl/flush) models: always uses input.contents with {"text"/"image"} nodes.
  class ObDashscopeVLEmbed : public ObAIFuncIEmbed
  {
  public:
    ObDashscopeVLEmbed() {}
    virtual ~ObDashscopeVLEmbed() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObString input_type,
                         common::ObJsonObject *&body) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObArray<ObString> &input_type_array,
                         common::ObJsonObject *&body) override;
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObDashscopeVLEmbed);
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
  static int get_json_object_form_str(ObIAllocator &allocator, const ObString &str, ObJsonObject *&obj_node);
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
  static constexpr char COHERE[20] = "COHERE";
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
  static bool is_completion_type(const ObAIFuncExprInfo *info) {return info->type_ == share::EndpointType::COMPLETION;}
  static bool is_dense_embedding_type(const ObAIFuncExprInfo *info) {return info->type_ == share::EndpointType::DENSE_EMBEDDING;}
  static bool is_rerank_type(const ObAIFuncExprInfo *info) {return info->type_ == share::EndpointType::RERANK;}
  static int check_info_type_completion(const ObAIFuncExprInfo *info);
  static int check_info_type_dense_embedding(const ObAIFuncExprInfo *info);
  static int check_info_type_rerank(const ObAIFuncExprInfo *info);
  static bool ob_provider_check(const ObString &provider, const char *str_const) { return provider.case_compare(str_const) == 0; }
  static int get_complete_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIComplete *&complete_provider);
  static int get_vl_complete_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIVLComplete *&complete_provider);
  static int get_embed_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIEmbed *&embed_provider);
  // When model_name is non-empty and provider is DASHSCOPE, returns ObDashscopeVLEmbed for multi-modal (vl/flush) models.
  static int get_embed_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIEmbed *&embed_provider,
                                bool is_multi_model);
  static int get_rerank_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIRerank *&rerank_provider);
  static int get_header(ObIAllocator &allocator,
                        const ObAIFuncExprInfo &info,
                        const ObAiModelEndpointInfo &endpoint_info,
                        ObArray<ObString> &headers);
  static int get_complete_body(ObIAllocator &allocator,
                               const ObAIFuncExprInfo &info,
                               const ObAiModelEndpointInfo &endpoint_info,
                               ObString &prompt,
                               ObString &content,
                               ObJsonObject *config,
                               ObJsonObject *&body);
  static int set_json_format_config(ObIAllocator &allocator, const ObString &provider, ObJsonObject *config);
  static int get_embed_body(ObIAllocator &allocator,
                            const ObAIFuncExprInfo &info,
                            const ObAiModelEndpointInfo &endpoint_info,
                            ObArray<ObString> &contents,
                            ObJsonObject *config,
                            ObString input_type,
                            ObJsonObject *&body);
  // Overload for mixed content types (text/image).
  static int get_embed_body(ObIAllocator &allocator,
                            const ObAIFuncExprInfo &info,
                            const ObAiModelEndpointInfo &endpoint_info,
                            ObArray<ObString> &contents,
                            ObJsonObject *config,
                            ObArray<ObString> &input_type_array,  // input type per content, e.g. text/image
                            ObJsonObject *&body);
  static int get_rerank_body(ObIAllocator &allocator,
                             const ObAIFuncExprInfo &info,
                             const ObAiModelEndpointInfo &endpoint_info,
                             ObString &query,
                             ObJsonArray *document_array,
                             ObJsonObject *config,
                             ObJsonObject *&body);
  static int parse_complete_output(ObIAllocator &allocator,
                                   const ObAiModelEndpointInfo &endpoint_info,
                                   ObJsonObject *http_response,
                                   ObIJsonBase *&result);
  static int parse_embed_output(ObIAllocator &allocator,
                                const ObAiModelEndpointInfo &endpoint_info,
                                ObJsonObject *http_response,
                                ObIJsonBase *&result);
  static int parse_rerank_output(ObIAllocator &allocator,
                                 const ObAiModelEndpointInfo &endpoint_info,
                                 ObJsonObject *http_response,
                                 ObIJsonBase *&result);
  static int set_string_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObString &res_str);
  static int get_ai_func_info(ObIAllocator &allocator, const ObString &model_id, ObAIFuncExprInfo *&info);
  static bool is_provider_support_base64(const ObString &provider);
  static int decode_base64_embedding_array(const ObIJsonBase &embedding_jbase, ObIAllocator &allocator,
                                           const int64_t dimension, float *&vector);
  static int decode_float_embedding_array(const ObIJsonBase &embedding_jbase, ObIAllocator &allocator,
                                          ObJsonReaderHelper &json_reader, const int64_t dimension, float *&vector);
  static int get_ai_func_info(ObIAllocator &allocator, const ObString &model_id,
                              share::schema::ObSchemaGetterGuard &guard, ObAIFuncExprInfo *&info);
  static int get_model_config_info(ObIAllocator &allocator, const ObString &model_key, ObAIModelConfigInfo &config);
  // URL security check for preventing SSRF attacks
  static int check_url_security(const ObString &url);
  // True if model name contains "vl" or "flush" (case-insensitive); used for multi-modal message format.
  static bool is_multi_model(const ObString &model_name);
  // True for VL models that reject same-type batching (e.g. qwen2.x-vl-*).
  // These models require each row to be sent as a separate HTTP request.
  // qwen3+ VL models support batch; only qwen2.x-vl is known to be restricted.
  static bool vl_model_needs_individual_requests(const ObString &model_name);
  // Merge endpoint message_parameters with user config. User config overrides endpoint config on same key.
  static int merge_message_parameters_to_config(ObIAllocator &allocator,
                                                ObJsonObject *message_parameters,
                                                ObJsonObject *config,
                                                ObJsonObject *&merged_config);
  static int set_default_enable_thinking(ObIAllocator &allocator, ObJsonObject *&config);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncUtils);
};


class ObAIFuncModel
{
public:
  ObAIFuncModel(ObIAllocator &allocator, const ObAIFuncExprInfo &info, const ObAiModelEndpointInfo &endpoint_info)
  : allocator_(&allocator),
    info_(info),
    endpoint_info_(endpoint_info),
    input_type_()
  {}
   virtual ~ObAIFuncModel() {}
   void set_input_type(const ObString &input_type) { input_type_ = input_type; }
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
   const ObString get_request_model_name();
   ObIAllocator *allocator_;
   const ObAIFuncExprInfo &info_;
   const ObAiModelEndpointInfo &endpoint_info_;
   ObString input_type_;
   DISALLOW_COPY_AND_ASSIGN(ObAIFuncModel);
};

class ObAIServiceClient
{
public:
  ObAIServiceClient(ObIAllocator &allocator)
    : is_inited_(false),
      allocator_(allocator),
      model_config_info_(nullptr)
  {}
  virtual ~ObAIServiceClient() {}
  // init by model_key: look up config internally
  int init(const ObString &model_key);
  // init by externally provided config pointer (caller manages lifetime)
  int init(share::ObAIModelConfigInfo *model_config_info);
  // completion
  int call_completion(ObString &prompt, ObJsonObject *prompt_object, ObJsonObject *config, ObString &result);
private:
  // Merge endpoint message_parameters with user config. User input overrides endpoint config on same key.
  static int merge_message_parameters_to_config(ObIAllocator &allocator,
                                                ObJsonObject *message_parameters,
                                                ObJsonObject *config,
                                                ObJsonObject *&merged_config);

  bool is_inited_;
  ObIAllocator &allocator_;
  share::ObAIModelConfigInfo *model_config_info_;
  DISALLOW_COPY_AND_ASSIGN(ObAIServiceClient);
};

class ObAIFuncPromptObjectUtils
{
public:
  ObAIFuncPromptObjectUtils() {}
  virtual ~ObAIFuncPromptObjectUtils() {}
  enum class PromptArgType : int8_t {
    TEXT = 0,
    IMAGE = 1,
    UNKNOWN = 2
  };
  enum class ImageArgType : int8_t {
    URL = 0,
    BINARY = 1
  };
  static bool is_valid_prompt_object(ObJsonObject* prompt_object);
  static int replace_all_str_args_in_template(ObIAllocator &allocator, ObJsonObject* prompt_object, ObString& replaced_prompt_str);
  static int construct_prompt_object(ObIAllocator &allocator, ObString &template_str, ObJsonArray *args_array, ObJsonObject *&prompt_object);
  static int parse_template_arg_types(const ObString &template_str,
                                      const int64_t arg_cnt,
                                      ObIArray<PromptArgType> &arg_types,
                                      bool *has_image = nullptr);
  static int build_image_arg_object(ObIAllocator &allocator,
                                    const ObString &arg_str,
                                    const ImageArgType image_arg_type,
                                    ObJsonObject *&image_obj);
  static int validate_binary_image_sizes(ObJsonObject *prompt_object,
                                         const int64_t max_image_size);
  static constexpr const char *HTTP_PREFIX = "http://";
  static constexpr const char *HTTPS_PREFIX = "https://";
  static constexpr char prompt_template_key[20] = "template";
  static constexpr char prompt_args_key[20] = "args";
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncPromptObjectUtils);
};

class ObAiFuncImageUtils
{
public:
  ObAiFuncImageUtils() {}
  virtual ~ObAiFuncImageUtils() {}
public:
  // image type enum
  enum ObImageType
  {
    IMAGE_TYPE_JPEG = 0,
    IMAGE_TYPE_PNG,
    IMAGE_TYPE_GIF,
    IMAGE_TYPE_WEBP,
    IMAGE_TYPE_BMP,
    IMAGE_TYPE_TIFF,
    IMAGE_TYPE_ICO,
    IMAGE_TYPE_DIB,
    IMAGE_TYPE_ICNS,
    IMAGE_TYPE_SGI,
    IMAGE_TYPE_UNKNOWN
  };

  // get image type from binary by magic number
  static int get_type_from_binary(ObIAllocator &allocator, const ObString &binary_image_str, ObImageType &image_type);
  // image binary to base64 data uri
  static int get_base64_data_uri_from_binary(ObIAllocator &allocator, const ObString &binary_image_str, ObString &base64_data_uri);
  // get image type string by image type enum
  static const char* get_image_type_str(ObImageType image_type);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAiFuncImageUtils);
};

struct ObAIModelConfigKey
{
public:
  ObAIModelConfigKey() {}
  virtual ~ObAIModelConfigKey() {}
  uint64_t hash() const
  {
    uint64_t hash_val = murmurhash(provider_.ptr(), static_cast<int32_t>(provider_.length()), 0);
    return murmurhash(model_name_.ptr(), static_cast<int32_t>(model_name_.length()), hash_val);
  }
  int hash(uint64_t &res) const
  {
    res = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObAIModelConfigKey &other) const
  {
    return 0 == provider_.case_compare(other.provider_) && 0 == model_name_.case_compare(other.model_name_);
  }
  TO_STRING_KV(K_(provider), K_(model_name));
public:
  ObString provider_;
  ObString model_name_;
};

class ObAIModelConfigInfoManager
{
public:
  typedef common::hash::ObHashMap<ObAIModelConfigKey, share::ObAIModelConfigItem,
                                  common::hash::NoPthreadDefendMode> ObAIModelConfigMap;
  static const int64_t DEFAULT_BUCKET_NUM = 128;
  static ObAIModelConfigInfoManager &get_instance()
  {
    static ObAIModelConfigInfoManager instance;
    return instance;
  }
  int init();
  void reset();
  OB_INLINE bool is_inited() const { return ATOMIC_LOAD(&is_inited_); }
  int get_model_config(const ObString &provider,
                       const ObString &request_model_name,
                       share::ObAIModelConfigItem &item) const;
  int get_config(const ObString &provider,
                 const ObString &request_model_name,
                 share::ObAIModelConfigItem &item) const;
  int64_t size() const { return config_map_.size(); }
private:
  ObAIModelConfigInfoManager() : is_inited_(false), allocator_(), config_map_(), lock_(common::ObLatchIds::AI_MODEL_CONFIG_MGR_LOCK) {}
  ~ObAIModelConfigInfoManager();
  int set_config(const ObString &provider,
                 const ObString &model_name,
                 const share::ObAIModelConfigItem &item,
                const bool overwrite = true);
  int register_default_model_configs_();
  int register_aliyun_model_configs_();
private:
  mutable bool is_inited_;
  mutable ObArenaAllocator allocator_;
  mutable ObAIModelConfigMap config_map_;
  mutable common::ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObAIModelConfigInfoManager);
};

struct ObAIFuncBatchState
{
  bool initialized_;
  ObString model_id_;
  ObAIFuncExprInfo *info_;
  const share::ObAiModelEndpointInfo *endpoint_info_;
  int64_t min_concurrency_;
  int64_t max_concurrency_;
  ObArray<ObString> headers_;
  ObArray<ObJsonObject *> bodies_;
  ObArray<int64_t> row_indices_flat_;
  ObArray<int64_t> row_starts_;
  ObArray<int64_t> row_lens_;

  ObAIFuncBatchState()
      : initialized_(false),
        info_(nullptr),
        endpoint_info_(nullptr),
        min_concurrency_(share::ObAIModelConfigItem::DEFAULT_MIN_CONCURRENCY),
        max_concurrency_(share::ObAIModelConfigItem::DEFAULT_MAX_CONCURRENCY) {}

  void reset()
  {
    initialized_ = false;
    model_id_.reset();
    info_ = nullptr;
    endpoint_info_ = nullptr;
    min_concurrency_ = share::ObAIModelConfigItem::DEFAULT_MIN_CONCURRENCY;
    max_concurrency_ = share::ObAIModelConfigItem::DEFAULT_MAX_CONCURRENCY;
    headers_.reuse();
    bodies_.reuse();
    row_indices_flat_.reuse();
    row_starts_.reuse();
    row_lens_.reuse();
  }
};

typedef int (*CheckModelTypeFn)(const ObAIFuncExprInfo *info);

typedef int (*ParseBatchResponseFn)(const sql::ObExpr &expr,
                                    sql::ObEvalCtx &ctx,
                                    ObIAllocator &allocator,
                                    ObJsonObject *response,
                                    const ObArray<int64_t> &row_indices,
                                    const share::ObAiModelEndpointInfo &endpoint_info,
                                    ObIVector *res_vec);

class ObAIFuncBatchUtils
{
public:
  static int flush_pending_batch(ObIAllocator &allocator,
                                 const sql::ObExpr &expr,
                                 sql::ObEvalCtx &ctx,
                                 ObAIFuncBatchState &pending,
                                 ObIVector *res_vec,
                                 ObBitVector &eval_flags,
                                 ParseBatchResponseFn parse_fn);

  static int init_pending_state(ObIAllocator &allocator,
                                const ObString &model_id,
                                const share::ObAiModelEndpointInfo *endpoint_info,
                                ObAIFuncBatchState &pending,
                                CheckModelTypeFn check_fn);
private:
  // AIMD concurrency adjustment: increase on success, decrease on failure
  static int64_t adjust_concurrency(int64_t current, int64_t min_concurrency,
                                    int64_t max_concurrency, bool all_success);
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncBatchUtils);
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_UTILS_H_ */
