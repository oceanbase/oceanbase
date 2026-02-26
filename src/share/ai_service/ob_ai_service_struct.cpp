/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/ai_service/ob_ai_service_struct.h"
#include "lib/json/ob_json.h"
#include "share/ob_encryption_util.h"
#include "share/rc/ob_tenant_base.h"

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

#define EXTRACT_JSON_ELEM_END() \
  { \
    ret = OB_AI_FUNC_PARAM_INVALID; \
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr()); \
    LOG_WARN("unknown json key param", K(ret), K(elem.first)); \
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
  } else if (!parameters_.empty()) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("parameters"), "parameters");
    LOG_WARN("parameters is not empty", K(ret), K(*this));
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

OB_SERIALIZE_MEMBER(ObAiServiceModelInfo, name_, type_, model_name_);

} // namespace share
} // namespace oceanbase