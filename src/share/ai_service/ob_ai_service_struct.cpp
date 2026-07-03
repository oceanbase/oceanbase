/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ai_service/ob_ai_service_struct.h"
#include "lib/json/ob_json.h"
#include "share/ob_encryption_util.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_ai_model_mgr.h"

#define USING_LOG_PREFIX SHARE

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::json;

namespace oceanbase
{
namespace share
{
const ObString ObAiModelEndpointInfo::DEFAULT_SCOPE = "ALL";

const char *VALID_PROVIDERS[] = {
  "ALIYUN-OPENAI",
  "ALIYUN-DASHSCOPE",
  "DEEPSEEK",
  "SILICONFLOW",
  "HUNYUAN-OPENAI",
  "OPENAI"
};

#define EXTRACT_JSON_ELEM_STR(json_key, member) \
  EXTRACT_JSON_ELEM_STR_WITH_PROCESS(json_key, member, "void")

#define EXTRACT_JSON_ELEM_STR_WITH_PROCESS(json_key, member, post_process) \
      if (elem.first.case_compare(json_key) == 0) { \
        if (elem.second->json_type() != ObJsonNodeType::J_STRING) { \
          ret = OB_AI_FUNC_PARAM_VALUE_INVALID; \
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, elem.first.length(), elem.first.ptr()); \
          LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
        } else { \
          member = ObString(elem.second->get_data_length(), elem.second->get_data()); \
          post_process; \
        } \
      } else

#define EXTRACT_JSON_ELEM_INT(json_key, int_val) \
if (elem.first.case_compare(json_key) == 0) { \
  if (elem.second->json_type() != ObJsonNodeType::J_INT && elem.second->json_type() != ObJsonNodeType::J_UINT) { \
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID; \
    LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
    FORWARD_USER_ERROR(ret, "invalid parameters type for parameters." #json_key ", " #json_key " must be an integer"); \
  } else { \
    int_val = elem.second->get_int(); \
  } \
} else

#define EXTRACT_JSON_ELEM_DOUBLE(json_key, double_val) \
if (elem.first.case_compare(json_key) == 0) { \
  if (elem.second->json_type() != ObJsonNodeType::J_DOUBLE && \
      elem.second->json_type() != ObJsonNodeType::J_INT && \
      elem.second->json_type() != ObJsonNodeType::J_UINT) { \
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID; \
    LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
    FORWARD_USER_ERROR(ret, "invalid parameters type for parameters." #json_key ", " #json_key " must be a number"); \
  } else { \
    if (elem.second->json_type() == ObJsonNodeType::J_DOUBLE) { \
      double_val = elem.second->get_double(); \
    } else { \
      double_val = static_cast<double>(elem.second->get_int()); \
    } \
  } \
} else

#define EXTRACT_JSON_ELEM_END() \
  { \
    ret = OB_AI_FUNC_PARAM_INVALID; \
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr()); \
    LOG_WARN("unknown json key param", K(ret), K(elem.first)); \
  }

#define EXTRACT_JSON_ELEM_NO_CHECK_END() \
  { \
  }

static int check_endpoint_parameters_valid(const ObString &parameters)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AIEpParamChk");
  ObIJsonBase *params_base = nullptr;
  int64_t batch_size = 0;
  int64_t max_image_size = 0;
  int64_t max_tokens = 0;
  int64_t min_concurrency = 0;
  int64_t max_concurrency = 0;
  double temperature = 0.0;
  double top_p = 0.0;
  if (parameters.empty()) {
    // do nothing
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                      parameters,
                                                      ObJsonInType::JSON_TREE,
                                                      ObJsonInType::JSON_TREE,
                                                      params_base))) {
    LOG_WARN("failed to parse endpoint parameters", K(ret), K(parameters));
  } else if (OB_ISNULL(params_base) || params_base->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("parameters"), "parameters");
    LOG_WARN("endpoint parameters should be json object", K(ret), K(parameters));
  } else {
    JsonObjectIterator iter = params_base->object_iterator();
    while (!iter.end() && OB_SUCC(ret)) {
      ObJsonObjPair elem;
      if (OB_FAIL(iter.get_elem(elem))) {
        LOG_WARN("failed to get elem", K(ret));
      } else {
        EXTRACT_JSON_ELEM_INT("batch_size", batch_size)
        EXTRACT_JSON_ELEM_INT("max_image_size", max_image_size)
        EXTRACT_JSON_ELEM_INT("max_tokens", max_tokens)
        EXTRACT_JSON_ELEM_INT("min_concurrency", min_concurrency)
        EXTRACT_JSON_ELEM_INT("max_concurrency", max_concurrency)
        EXTRACT_JSON_ELEM_DOUBLE("temperature", temperature)
        EXTRACT_JSON_ELEM_DOUBLE("top_p", top_p)
        EXTRACT_JSON_ELEM_NO_CHECK_END()
      }
      iter.next();
    }
  }
  if (OB_SUCC(ret)) {
    if (batch_size < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("batch_size"), "batch_size");
      LOG_WARN("batch size is invalid", K(ret), K(batch_size));
    } else if (max_image_size < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("max_image_size"), "max_image_size");
      LOG_WARN("max image size is invalid", K(ret), K(max_image_size));
    } else if (max_tokens < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("max_tokens"), "max_tokens");
      LOG_WARN("max_tokens is invalid", K(ret), K(max_tokens));
    } else if (temperature < 0.0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("temperature"), "temperature");
      LOG_WARN("temperature is invalid, should be in range [0.0, 2.0]", K(ret), K(temperature));
    } else if (top_p < 0.0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("top_p"), "top_p");
      LOG_WARN("top_p is invalid, should be in range [0.0, 1.0]", K(ret), K(top_p));
    } else if (min_concurrency < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("min_concurrency"), "min_concurrency");
      LOG_WARN("min_concurrency is invalid, must be a positive integer", K(ret), K(min_concurrency));
    } else if (max_concurrency < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("max_concurrency"), "max_concurrency");
      LOG_WARN("max_concurrency is invalid, must be a positive integer", K(ret), K(max_concurrency));
    } else if (min_concurrency > 0 && max_concurrency > 0 && min_concurrency > max_concurrency) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("min_concurrency"), "min_concurrency");
      LOG_WARN("min_concurrency must not exceed max_concurrency",
               K(ret), K(min_concurrency), K(max_concurrency));
    }
  }
  return ret;
}

int ObAiModelEndpointInfo::parse_from_json_base(common::ObArenaAllocator &allocator, const ObString &name, const ObIJsonBase &params_jbase)
{
  int ret = OB_SUCCESS;
  reset();
  name_ = name;
  if (OB_FAIL(merge_delta_endpoint(allocator, params_jbase))) {
    LOG_WARN("failed to merge delta endpoint", K(ret), K(params_jbase));
  }
  LOG_INFO("parse from json base", K(ret), K(params_jbase), K(params_jbase.json_type()), K(params_jbase.element_count()));
  return ret;
}

int ObAiModelEndpointInfo::check_valid() const
{
  int ret = OB_SUCCESS;
  if (name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("name"), "name");
    LOG_WARN("name is empty", K(ret), K(*this));
  } else if (scope_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("scope"), "scope");
    LOG_WARN("scope is empty", K(ret), K(*this));
  } else if (scope_.case_compare(DEFAULT_SCOPE) != 0) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("scope"), "scope");
    LOG_WARN("scope value is invalid", K(ret), K(*this));
  } else if (ai_model_name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("ai_model_name"), "ai_model_name");
    LOG_WARN("ai_model_name is empty", K(ret), K(*this));
  } else if (url_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("url"), "url");
    LOG_WARN("url is empty", K(ret), K(*this));
  } else if (access_key_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("access_key"), "access_key");
    LOG_WARN("access_key is empty", K(ret), K(*this));
  } else if (provider_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("provider"), "provider");
    LOG_WARN("provider is empty", K(ret), K(*this));
  } else if (!is_valid_provider(provider_)) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("provider"), "provider");
    LOG_WARN("provider is invalid", K(ret), K(*this));
  } else if (OB_FAIL(check_endpoint_parameters_valid(parameters_))) {
    LOG_WARN("parameters is invalid", K(ret), K(*this), K(parameters_));
  } else if (!request_transform_fn_.empty()) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("request_transform_fn"), "request_transform_fn");
    LOG_WARN("request_transform_fn is not empty", K(ret), K(*this));
  } else if (!response_transform_fn_.empty()) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("response_transform_fn"), "response_transform_fn");
    LOG_WARN("response_transform_fn is not empty", K(ret), K(*this));
  }
  return ret;
}

bool ObAiModelEndpointInfo::is_valid_provider(const ObString &provider)
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(VALID_PROVIDERS); i++) {
    if (provider.case_compare(VALID_PROVIDERS[i]) == 0) {
      is_valid = true;
      break;
    }
  }
  return is_valid;
}

int ObAiModelEndpointInfo::merge_delta_endpoint(common::ObArenaAllocator &allocator, const ObIJsonBase &delta_jbase)
{
  int ret = OB_SUCCESS;
  JsonObjectIterator iter = delta_jbase.object_iterator();
  bool has_api_key = false;
  while (!iter.end() && OB_SUCC(ret)) {
    ObJsonObjPair elem;
    if (OB_FAIL(iter.get_elem(elem))) {
      LOG_WARN("failed to get elem", K(ret));
    } else {
      EXTRACT_JSON_ELEM_STR("scope", scope_)
      EXTRACT_JSON_ELEM_STR("ai_model_name", ai_model_name_)
      EXTRACT_JSON_ELEM_STR("url", url_)
      EXTRACT_JSON_ELEM_STR_WITH_PROCESS("access_key", access_key_, has_api_key = true)
      EXTRACT_JSON_ELEM_STR("provider", provider_)
      EXTRACT_JSON_ELEM_STR("request_model_name", request_model_name_)
      EXTRACT_JSON_ELEM_STR("parameters", parameters_)
      EXTRACT_JSON_ELEM_STR("request_transform_fn", request_transform_fn_)
      EXTRACT_JSON_ELEM_STR("response_transform_fn", response_transform_fn_)
      EXTRACT_JSON_ELEM_END()
    }
    iter.next();
  }

  if (OB_SUCC(ret)) {
    if (has_api_key && !access_key_.empty() && OB_FAIL(encrypt_access_key_(allocator, access_key_, access_key_))) {
      LOG_WARN("failed to encrypt access key", K(ret));
    } else if (OB_FAIL(check_valid())) {
      LOG_WARN("invalid endpoint", K(ret), K(delta_jbase));
    }
  }

  LOG_INFO("merge delta endpoint", K(ret), K(delta_jbase), K(delta_jbase.json_type()), K(delta_jbase.element_count()));
  return ret;
}

int ObAiModelEndpointInfo::encrypt_access_key_(ObIAllocator &allocator, const ObString &access_key, ObString &encrypted_access_key)
{
#ifdef OB_BUILD_TDE_SECURITY
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t out_len = 0;
  int64_t encrypted_buf_length = ObEncryptionUtil::sys_encrypted_length(access_key.length());
  char *encrypted_key_buf = NULL;
  char *encrypted_hex_key_buf = NULL;
  int64_t hex_buf_length = encrypted_buf_length * 2 + 1;
  int64_t hex_buf_pos = 0;

  if (OB_ISNULL(encrypted_key_buf = static_cast<char *>(allocator.alloc(encrypted_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc encrypted key buf", K(ret), K(encrypted_buf_length));
  } else if (OB_FAIL(ObEncryptionUtil::encrypt_sys_data(tenant_id, access_key.ptr(), access_key.length(),
                                                        encrypted_key_buf, encrypted_buf_length, out_len))) {
    LOG_WARN("failed to encrypt access key", K(ret));
  } else if (0 >= out_len || out_len > encrypted_buf_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encrypted out length", K(ret), K(out_len), K(encrypted_buf_length));
  } else if (OB_ISNULL(encrypted_hex_key_buf = static_cast<char *>(allocator.alloc(hex_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc encrypted hex key buf", K(ret), K(hex_buf_length));
  } else if (OB_FAIL(hex_print(encrypted_key_buf, out_len, encrypted_hex_key_buf, hex_buf_length, hex_buf_pos))) {
    LOG_WARN("failed to convert encrypted key to hex", K(ret), K(encrypted_key_buf), K(hex_buf_length));
  } else {
    encrypted_access_key.assign_ptr(encrypted_hex_key_buf, hex_buf_pos);
  }
  return ret;
#else
  return encrypt_access_key_no_tde_(allocator, access_key, encrypted_access_key);
#endif
}

int ObAiModelEndpointInfo::encrypt_access_key_no_tde_(ObIAllocator &allocator, const ObString &access_key, ObString &encrypted_access_key)
{
  int ret = OB_SUCCESS;
  int64_t hex_buf_length = access_key.length() * 2 + 1;
  int64_t hex_buf_pos = 0;
  char *hex_buf = NULL;
  if (OB_ISNULL(hex_buf = static_cast<char *>(allocator.alloc(hex_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc hex key buf", K(ret), K(hex_buf_length));
  } else if (OB_FAIL(hex_print(access_key.ptr(), access_key.length(), hex_buf, hex_buf_length, hex_buf_pos))) {
    LOG_WARN("failed to convert access key to hex", K(ret), K(access_key), K(hex_buf_length));
  } else {
    encrypted_access_key.assign_ptr(hex_buf, hex_buf_pos);
  }
  return ret;
}

int ObAiModelEndpointInfo::decrypt_access_key_(ObIAllocator &allocator, const ObString &encrypted_access_key, ObString &unencrypted_access_key) const
{
#ifdef OB_BUILD_TDE_SECURITY
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t deser_buf_length = encrypted_access_key.length() / 2 + 1;
  char *deser_buf = NULL;
  int64_t deser_buf_pos = 0;
  int64_t access_key_buf_length = encrypted_access_key.length() / 2;
  char *access_key_buf = NULL;
  int64_t decrypted_length = 0;
  if (encrypted_access_key.length() % 2 != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid encrypted access key", K(ret), K(encrypted_access_key.length()));
  } else if (OB_ISNULL(deser_buf = static_cast<char *>(allocator.alloc(deser_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc deserialized key buf", K(ret), K(deser_buf_length));
  } else if (OB_FAIL(hex_to_cstr(encrypted_access_key.ptr(), encrypted_access_key.length(),
                                 deser_buf, deser_buf_length, deser_buf_pos))) {
    LOG_WARN("failed to deserialize encrypted key", K(ret), K(encrypted_access_key), K(deser_buf_length));
  } else if (deser_buf_pos != deser_buf_length-1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid encrypted access key", K(ret), K(encrypted_access_key.length()));
  } else if (OB_ISNULL(access_key_buf = static_cast<char *>(allocator.alloc(access_key_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc decrypted key buf", K(ret), K(access_key_buf_length));
  } else if (OB_FAIL(ObEncryptionUtil::decrypt_sys_data(tenant_id, deser_buf, deser_buf_length - 1,
                                                        access_key_buf, access_key_buf_length, decrypted_length))) {
    LOG_WARN("failed to decrypt access key", K(ret));
  } else {
    unencrypted_access_key.assign_ptr(access_key_buf, decrypted_length);
  }
  return ret;
#else
  return decrypt_access_key_no_tde_(allocator, encrypted_access_key, unencrypted_access_key);
#endif
}

int ObAiModelEndpointInfo::decrypt_access_key_no_tde_(ObIAllocator &allocator, const ObString &encrypted_access_key, ObString &unencrypted_access_key) const
{
  int ret = OB_SUCCESS;
  int64_t unencrypted_buf_length = encrypted_access_key.length() / 2 + 1;
  int64_t unencrypted_buf_pos = 0;
  char *unencrypted_buf = NULL;
  if (OB_ISNULL(unencrypted_buf = static_cast<char *>(allocator.alloc(unencrypted_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc unencrypted key buf", K(ret), K(unencrypted_buf_length));
  } else if (OB_FAIL(hex_to_cstr(encrypted_access_key.ptr(), encrypted_access_key.length(),
                                 unencrypted_buf, unencrypted_buf_length, unencrypted_buf_pos))) {
    LOG_WARN("failed to convert encrypted key to unencrypted key", K(ret), K(encrypted_access_key), K(unencrypted_buf_length));
  } else {
    unencrypted_access_key.assign_ptr(unencrypted_buf, unencrypted_buf_pos);
  }
  return ret;
}

const char *EndpointType::ENDPOINT_TYPE_STR[] = {
  "DENSE_EMBEDDING",
  "SPARSE_EMBEDDING",
  "COMPLETION",
  "RERANK",
};

EndpointType::TYPE EndpointType::str_to_endpoint_type(const ObString &type_str)
{
  STATIC_ASSERT(static_cast<int64_t>(EndpointType::MAX_TYPE) == ARRAYSIZEOF(ENDPOINT_TYPE_STR) + 1, "endpoint type str len is mismatch");
  EndpointType::TYPE endpoint_type = EndpointType::INVALID_TYPE;
  bool is_found = false;
  for (int i = 1; i < EndpointType::MAX_TYPE && !is_found; i++) {
    if (type_str.case_compare(ENDPOINT_TYPE_STR[i-1]) == 0) {
      endpoint_type = static_cast<EndpointType::TYPE>(i);
      is_found = true;
    }
  }
  return endpoint_type;
}

int ObAiModelEndpointInfo::get_unencrypted_access_key(common::ObIAllocator &allocator, ObString &unencrypted_access_key) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decrypt_access_key_(allocator, access_key_, unencrypted_access_key))) {
    LOG_WARN("failed to decrypt access key", K(ret));
  }
  return ret;
}

int ObAiServiceModelInfo::parse_from_json_base(const ObString &name, const common::ObIJsonBase &params_jbase)
{
  int ret = OB_SUCCESS;
  reset();
  name_ = name;
  JsonObjectIterator iter = params_jbase.object_iterator();
  ObString type_str;
  while (!iter.end() && OB_SUCC(ret)) {
    ObJsonObjPair elem;
    if (OB_FAIL(iter.get_elem(elem))) {
      LOG_WARN("failed to get elem", K(ret));
    } else {
      EXTRACT_JSON_ELEM_STR("model_name", model_name_)
      EXTRACT_JSON_ELEM_STR_WITH_PROCESS("type", type_str, type_ = EndpointType::str_to_endpoint_type(type_str))
      EXTRACT_JSON_ELEM_END()
    }
    iter.next();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_valid())) {
      LOG_WARN("invalid model", K(ret), K(params_jbase));
    }
  }

  LOG_TRACE("parse from json base", K(ret), K(params_jbase), K(params_jbase.json_type()), K(params_jbase.element_count()));
  return ret;
}

int ObAiServiceModelInfo::check_valid() const
{
  int ret = OB_SUCCESS;
  if (name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("name"), "name");
    LOG_WARN("name is empty", K(ret), K(*this));
  } else if (model_name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("model_name"), "model_name");
    LOG_WARN("model_name is empty", K(ret), K(*this));
  } else if (type_ == EndpointType::MAX_TYPE) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("type"), "type");
    LOG_WARN("model type is empty", K(ret), K(*this), K(type_));
  } else if (type_ == EndpointType::INVALID_TYPE) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("type"), "type");
    LOG_WARN("model type is invalid", K(ret), K(*this), K(type_));
  }
  return ret;
}

int ObAIModelConfigInfo::init(ObIAllocator &allocator,
                              const schema::ObAiModelSchema &ai_model_schema,
                              const ObAiModelEndpointInfo &endpoint_info)
{
  int ret = OB_SUCCESS;
  reset();
  const ObString request_model_name = endpoint_info.get_request_model_name().empty()
                                      ? ai_model_schema.get_model_name()
                                      : endpoint_info.get_request_model_name();
  if (OB_FAIL(ob_write_string(allocator, ai_model_schema.get_name(), model_key_, true))) {
    LOG_WARN("failed to deep copy model key", K(ret), K(ai_model_schema));
  } else if (OB_FAIL(ob_write_string(allocator, ai_model_schema.get_model_name(), model_name_, true))) {
    LOG_WARN("failed to deep copy model name", K(ret), K(ai_model_schema));
  } else if (OB_FAIL(ob_write_string(allocator, endpoint_info.get_provider(), provider_, true))) {
    LOG_WARN("failed to deep copy provider", K(ret), K(endpoint_info));
  } else if (OB_FAIL(ob_write_string(allocator, endpoint_info.get_url(), url_, true))) {
    LOG_WARN("failed to deep copy endpoint url", K(ret), K(endpoint_info));
  } else if (OB_FAIL(ob_write_string(allocator, request_model_name, request_model_name_, true))) {
    LOG_WARN("failed to deep copy request model name", K(ret), K(request_model_name));
  } else if (OB_FAIL(endpoint_info.get_unencrypted_access_key(allocator, api_key_))) {
    LOG_WARN("failed to get unencrypted access key", K(ret), K(endpoint_info));
  } else if (OB_FALSE_IT(model_type_ = ai_model_schema.get_type())) {
  } else {
    const ObString &parameters = endpoint_info.get_parameters();
    if (!parameters.empty()) {
      ObIJsonBase *params_base = nullptr;
      if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                    parameters,
                                                    ObJsonInType::JSON_TREE,
                                                    ObJsonInType::JSON_TREE,
                                                    params_base))) {
        LOG_WARN("failed to parse endpoint parameters", K(ret), K(parameters));
      } else if (OB_ISNULL(params_base) || params_base->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("endpoint parameters should be json object", K(ret), K(parameters));
      } else {
        // get endpoint parameters
        JsonObjectIterator iter = params_base->object_iterator();
        while (!iter.end() && OB_SUCC(ret)) {
          ObJsonObjPair elem;
          if (OB_FAIL(iter.get_elem(elem))) {
            LOG_WARN("failed to get elem", K(ret));
          } else {
            EXTRACT_JSON_ELEM_INT("batch_size", batch_size_)
            EXTRACT_JSON_ELEM_INT("max_image_size", max_image_size_)
            EXTRACT_JSON_ELEM_INT("min_concurrency", min_concurrency_)
            EXTRACT_JSON_ELEM_INT("max_concurrency", max_concurrency_)
            EXTRACT_JSON_ELEM_NO_CHECK_END()
          }
          iter.next();
        }
        // get message parameters
        ObJsonObject *message_parameters = static_cast<ObJsonObject *>(params_base);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(message_parameters->remove("batch_size"))) {
            LOG_WARN("failed to remove batch size", K(ret));
          } else if (OB_FAIL(message_parameters->remove("max_image_size"))) {
            LOG_WARN("failed to remove max image size", K(ret));
          } else if (OB_FAIL(message_parameters->remove("min_concurrency"))) {
            LOG_WARN("failed to remove min concurrency", K(ret));
          } else if (OB_FAIL(message_parameters->remove("max_concurrency"))) {
            LOG_WARN("failed to remove max concurrency", K(ret));
          } else {
            message_parameters_ = message_parameters;
          }
        }
      }
    }
  }
  return ret;
}

int ObAIModelConfigInfo::merge_default_config(ObIAllocator &allocator, const share::ObAIModelConfigItem &default_config)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  batch_size_ = batch_size_ == 0 ? default_config.batch_size_ : batch_size_;
  max_image_size_ = max_image_size_ == 0 ? default_config.max_image_size_ : max_image_size_;
  min_concurrency_ = min_concurrency_ == 0 ? default_config.min_concurrency_ : min_concurrency_;
  max_concurrency_ = max_concurrency_ == 0 ? default_config.max_concurrency_ : max_concurrency_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObAiServiceModelInfo, name_, type_, model_name_);

} // namespace share
} // namespace oceanbase