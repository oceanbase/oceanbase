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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ai_func_utils.h"
#include "ob_ai_func_client.h"

namespace oceanbase
{
namespace common
{

int ObOpenAIUtils::get_header(common::ObIAllocator &allocator,
                              ObString &api_key,
                              common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (api_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("API key is empty", K(ret));
  } else {
    //["Authorization: Bearer %.*s", "Content-Type: application/json"]
    int auth_header_len = 1024;
    char *auth_header_str = (char *)allocator.alloc(auth_header_len);
    ObString content_type_str("Content-Type: application/json");
    ObString content_type_c_str;
    if (OB_FAIL(ob_write_string(allocator, content_type_str, content_type_c_str, true))) {
      LOG_WARN("fail to write content type string", K(ret));
    } else if (OB_ISNULL(auth_header_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for auth header string and content type string", K(ret));
    } else {
      int auth_header_pos = snprintf(auth_header_str, auth_header_len,
                         "Authorization: Bearer %.*s", api_key.length(), api_key.ptr());
      if (auth_header_pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to snprintf", K(ret));
      } else if (OB_FAIL(headers.push_back(ObString(auth_header_str)))) {
        LOG_WARN("Failed to push back auth header", K(ret));
      } else if (OB_FAIL(headers.push_back(content_type_c_str))) {
        LOG_WARN("Failed to push back content type", K(ret));
      }
    }
  }
  return ret;
}
int ObOpenAIUtils::ObOpenAIComplete::get_header(common::ObIAllocator &allocator,
                                                ObString &api_key,
                                                common::ObArray<ObString> &headers)
{
  return ObOpenAIUtils::get_header(allocator, api_key, headers);
}

int ObOpenAIUtils::ObOpenAIComplete::get_body(common::ObIAllocator &allocator,
                                              common::ObString &model,
                                              common::ObString &prompt,
                                              common::ObString &content,
                                              common::ObJsonObject *config,
                                              common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or content is empty", K(ret));
  } else {
    // {"model": "*", "messages": [{"role": "system", "content": "*"}, {"role": "user", "content": "*"}]}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *messages_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(construct_messages_array(allocator, prompt, content, messages_array))) {
      LOG_WARN("Failed to construct messages", K(ret));
    } else if (OB_FAIL(body_obj->add("messages", messages_array))) {
      LOG_WARN("Failed to add messages", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::construct_messages_array(ObIAllocator &allocator, ObString &prompt, ObString &content, ObJsonArray *&messages)
{
  int ret = OB_SUCCESS;
  if (content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("content is empty", K(ret));
  } else {
    //messages: [{"role": "system", "content": "You are a helpful assistant."}, {"role": "user", "content": "What is the capital of France?"}]
    ObJsonArray *messages_array = nullptr;
    ObJsonObject *sys_message_obj = nullptr;
    ObJsonObject *user_message_obj = nullptr;
    ObString system_str("system");
    ObString user_str("user");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, messages_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if(!prompt.empty()) {
      if (OB_FAIL(construct_message_obj(allocator, system_str, prompt, sys_message_obj))) {
        LOG_WARN("Failed to construct message object", K(ret));
      } else if (OB_FAIL(messages_array->append(sys_message_obj))) {
        LOG_WARN("Failed to append member", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(construct_message_obj(allocator, user_str, content, user_message_obj))) {
        LOG_WARN("Failed to construct message object", K(ret));
      } else if (OB_FAIL(messages_array->append(user_message_obj))) {
        LOG_WARN("Failed to append member", K(ret));
      } else {
        messages = messages_array;
      }
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::construct_message_obj(ObIAllocator &allocator, ObString &role, ObString &content, ObJsonObject *&message)
{
  int ret = OB_SUCCESS;
  if (role.empty() || content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("role or content is empty", K(ret));
  } else {
    ObJsonObject *message_obj = nullptr;
    ObJsonString *role_json_str = nullptr;
    ObJsonString *content_json_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, message_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, role, role_json_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(message_obj->add("role", role_json_str))) {
      LOG_WARN("Failed to add member", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, content_json_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(message_obj->add("content", content_json_str))) {
      LOG_WARN("Failed to add member", K(ret));
    } else {
      message = message_obj;
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else {
    // {"response_format":{"type":"json_object"}}
    ObString json_str("json_object");
    ObJsonString *json_str_obj = nullptr;
    ObJsonObject *json_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, json_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, json_str, json_str_obj))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(json_obj->add("type", json_str_obj))) {
      LOG_WARN("Failed to add member", K(ret));
    } else if (OB_FAIL(config->add("response_format", json_obj))) {
      LOG_WARN("Failed to add member", K(ret));
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::parse_output(common::ObIAllocator &allocator,
                                                  common::ObJsonObject *http_response,
                                                  common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.choices[0].message.content");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hit is empty", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIEmbed::get_header(common::ObIAllocator &allocator,
                                             ObString &api_key,
                                             common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *input_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::transform_array_to_json_array(allocator, contents, input_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_array))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIEmbed::parse_output(common::ObIAllocator &allocator,
                                               common::ObJsonObject *http_response,
                                               common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObJsonArray *result_array = nullptr;
    ObJsonNode *data_node = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_ISNULL(data_node = http_response->get_value("data"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get data", K(ret));
    } else {
      ObJsonArray *data_array = static_cast<ObJsonArray *>(data_node);
      ObJsonNode *embedding_node = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < data_array->element_count(); i++) {
        if (OB_ISNULL(embedding_node = data_array->get_value(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to get embedding", K(ret));
        } else if (embedding_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to get embedding node", K(ret));
        } else {
          ObJsonObject *embedding_obj = static_cast<ObJsonObject *>(embedding_node);
          ObJsonNode *embedding = embedding_obj->get_value("embedding");
          if (OB_ISNULL(embedding)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to get embedding", K(ret));
          } else if (OB_FAIL(result_array->append(embedding))) {
            LOG_WARN("Failed to append embedding", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        result = result_array;
      }
    }
  }
  return ret;
}

int ObOllamaUtils::get_header(common::ObIAllocator &allocator,
                              common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  // ollama header is empty, do nothing
  return ret;
}

int ObOllamaUtils::ObOllamaComplete::get_header(common::ObIAllocator &allocator,
                                                common::ObString &api_key,
                                                common::ObArray<ObString> &headers)
{
  return ObOllamaUtils::get_header(allocator, headers);
}

int ObOllamaUtils::ObOllamaComplete::get_body(common::ObIAllocator &allocator,
                                              common::ObString &model,
                                              common::ObString &prompt,
                                              common::ObString &content,
                                              common::ObJsonObject *config,
                                              common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or content is empty", K(ret));
  } else {
    // {"model": "llama3.1", "prompt": "What is the capital of France?"}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonString *content_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, content_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("prompt", content_str))) {
      LOG_WARN("Failed to add prompt", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaComplete::set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else {
    // {"format": "json"}
    ObString json_str("json");
    ObJsonString *json_str_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, json_str, json_str_obj))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(config->add("format", json_str_obj))) {
      LOG_WARN("Failed to add format", K(ret));
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaComplete::parse_output(common::ObIAllocator &allocator,
                                                  common::ObJsonObject *http_response,
                                                  common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.response");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hit is empty", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaEmbed::get_header(common::ObIAllocator &allocator,
                                             ObString &api_key,
                                             common::ObArray<ObString> &headers)
{
  return ObOllamaUtils::get_header(allocator, headers);
}

int ObOllamaUtils::ObOllamaEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *input_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::transform_array_to_json_array(allocator, contents, input_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_array))) {
      LOG_WARN("Failed to add input", K(ret));
    } else {
      body = body_obj;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body))) {
        LOG_WARN("Failed to compact json object", K(ret));
      }
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaEmbed::parse_output(common::ObIAllocator &allocator,
                                               common::ObJsonObject *http_response,
                                               common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.embeddings");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hit is empty", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObDashscopeUtils::get_header(common::ObIAllocator &allocator,
                                 common::ObString &api_key,
                                 common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::get_header(common::ObIAllocator &allocator,
                                                      common::ObString &api_key,
                                                      common::ObArray<ObString> &headers)
{
  return ObOpenAIUtils::get_header(allocator, api_key, headers);
}

int ObDashscopeUtils::ObDashscopeComplete::get_body(common::ObIAllocator &allocator,
                                                    common::ObString &model,
                                                    common::ObString &prompt,
                                                    common::ObString &content,
                                                    common::ObJsonObject *config,
                                                    common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || OB_ISNULL(content)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or content is null", K(ret));
  } else {
    // {"model": "*", "input": {"messages": [{"role": "system", "content": "*"}, {"role": "user", "content": "*"}]}, "parameters": {}}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;

    if (OB_ISNULL(config)) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
        LOG_WARN("Failed to get json object", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
        LOG_WARN("Failed to get json object", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
        LOG_WARN("Failed to get json string", K(ret));
      } else if (OB_FAIL(body_obj->add("model", model_str))) {
        LOG_WARN("Failed to add model", K(ret));
      } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeComplete::construct_input_obj(allocator, prompt, content, input_obj))) {
        LOG_WARN("Failed to construct input object", K(ret));
      } else if (OB_FAIL(body_obj->add("input", input_obj))) {
        LOG_WARN("Failed to add input", K(ret));
      } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeComplete::set_config_result_format(allocator, config))) {
        LOG_WARN("Failed to set config result format", K(ret));
      } else if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      } else {
        body = body_obj;
      }
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::set_config_result_format(ObIAllocator &allocator, ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else if (OB_NOT_NULL(config->get_value("result_format"))) {
    if (OB_FAIL(config->remove("result_format"))) {
      LOG_WARN("Failed to remove result_format", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // {"result_format": "message"}
    ObJsonString *result_format_str = nullptr;
    ObString result_format_str_val("message");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, result_format_str_val, result_format_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(config->add("result_format", result_format_str))) {
      LOG_WARN("Failed to add result_format", K(ret));
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::construct_input_obj(ObIAllocator &allocator, ObString &prompt, ObString &content, ObJsonObject *&input_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(content)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("content is null", K(ret));
  } else {
    ObJsonObject *obj = nullptr;
    ObJsonArray *messages_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObOpenAIUtils::ObOpenAIComplete::construct_messages_array(allocator, prompt, content, messages_array))) {
      LOG_WARN("Failed to construct messages array", K(ret));
    } else if (OB_FAIL(obj->add("messages", messages_array))) {
      LOG_WARN("Failed to add messages", K(ret));
    } else {
      input_obj = obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::set_config_json_format(ObIAllocator &allocator, ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else {
    //{"type": "json_object"}
    ObJsonString *type_str = nullptr;
    ObString type_str_val("json_object");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, type_str_val, type_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(config->add("type", type_str))) {
      LOG_WARN("Failed to add type", K(ret));
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::parse_output(ObIAllocator &allocator, ObJsonObject *http_response, ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObJsonObject *output_obj = nullptr;
    ObJsonArray *choices_array = nullptr;
    ObJsonObject *choice_obj = nullptr;
    ObJsonObject *message_obj = nullptr;
    ObJsonString *content_str = nullptr;
    ObString response_str;
    if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output_obj is null", K(ret));
    } else if (OB_ISNULL(choices_array = static_cast<ObJsonArray *>(output_obj->get_value("choices")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("choices_array is null", K(ret));
    } else if (OB_ISNULL(choice_obj = static_cast<ObJsonObject *>(choices_array->get_value(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("choice_obj is null", K(ret));
    } else if (OB_ISNULL(message_obj = static_cast<ObJsonObject *>(choice_obj->get_value("message")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("message_obj is null", K(ret));
    } else if (OB_ISNULL(content_str = static_cast<ObJsonString *>(message_obj->get_value("content")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("content_str is null", K(ret));
    } else {
      result = content_str;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeEmbed::get_header(common::ObIAllocator &allocator,
                                                   common::ObString &api_key,
                                                   common::ObArray<ObString> &headers)
{
  return ObDashscopeUtils::get_header(allocator, api_key, headers);
}

int ObDashscopeUtils::ObDashscopeEmbed::get_body(common::ObIAllocator &allocator,
                                                 common::ObString &model,
                                                 common::ObArray<ObString> &contents,
                                                 common::ObJsonObject *config,
                                                 common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or contents is empty", K(ret));
  } else {
    // {"model": "*", "input": {"texts": ["*"]}, "parameters": {}}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    ObJsonArray *texts_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, input_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::transform_array_to_json_array(allocator, contents, texts_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(input_obj->add("texts", texts_array))) {
      LOG_WARN("Failed to add texts", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_obj))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (config != nullptr && config->element_count() > 0) {
      ObJsonNode *dimensions_node = config->get_value("dimensions");
      if (OB_ISNULL(dimensions_node)) {
        // do nothing
      } else if (OB_FAIL(config->rename_key("dimensions", "dimension"))) {
        LOG_WARN("Failed to rename key", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(body_obj->add("parameters", config))) {
          LOG_WARN("Failed to add parameters", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeEmbed::parse_output(common::ObIAllocator &allocator,
                                                    common::ObJsonObject *http_response,
                                                    common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    // {"output": {"embeddings": [{"embedding": ["*"]}]}}
    ObJsonObject *output_obj = nullptr;
    ObJsonArray *embeddings_array = nullptr;
    ObJsonObject *embedding_obj = nullptr;
    ObJsonArray *embedding_array = nullptr;
    ObJsonArray *result_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output_obj is null", K(ret));
    } else if (OB_ISNULL(embeddings_array = static_cast<ObJsonArray *>(output_obj->get_value("embeddings")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("embeddings_array is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < embeddings_array->element_count(); ++i) {
        if (OB_ISNULL(embedding_obj = static_cast<ObJsonObject *>(embeddings_array->get_value(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("embedding_obj is null", K(ret));
        } else if (OB_ISNULL(embedding_array = static_cast<ObJsonArray *>(embedding_obj->get_value("embedding")))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("embedding_array is null", K(ret));
        } else if (OB_FAIL(result_array->append(embedding_array))) {
          LOG_WARN("Failed to append embedding array", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        result = result_array;
      }
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::get_header(common::ObIAllocator &allocator,
                                                    common::ObString &api_key,
                                                    common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::get_body(common::ObIAllocator &allocator,
                                                  common::ObString &model,
                                                  common::ObString &query,
                                                  common::ObJsonArray *document_array,
                                                  common::ObJsonObject *config,
                                                  common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || OB_ISNULL(query) || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or query or document_array is null", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeRerank::get_input_obj(allocator, query, document_array, input_obj))) {
      LOG_WARN("Failed to get input object", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_obj))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (config != nullptr && config->element_count() > 0) {
      if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::get_input_obj(common::ObIAllocator &allocator,
                                                      common::ObString &query,
                                                      common::ObJsonArray *document_array,
                                                      common::ObJsonObject *&input_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query) || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query or document_array is null", K(ret));
  } else {
    ObJsonObject *obj = nullptr;
    ObJsonString *query_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, query, query_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(obj->add("query", query_str))) {
      LOG_WARN("Failed to add query", K(ret));
    } else if (OB_FAIL(obj->add("documents", document_array))) {
      LOG_WARN("Failed to add documents", K(ret));
    } else {
      input_obj = obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::parse_output(common::ObIAllocator &allocator,
                                                      common::ObJsonObject *http_response,
                                                      common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObJsonObject *output_obj = nullptr;
  ObJsonArray *results_array = nullptr;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output_obj is null", K(ret));
  } else if (OB_ISNULL(results_array = static_cast<ObJsonArray *>(output_obj->get_value("results")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results_array is null", K(ret));
  } else {
    result = results_array;
  }
  return ret;
}

int ObSiliconflowUtils::get_header(common::ObIAllocator &allocator,
                                 common::ObString &api_key,
                                 common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObSiliconflowUtils::ObSiliconflowRerank::get_header(common::ObIAllocator &allocator,
                                                    common::ObString &api_key,
                                                    common::ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObSiliconflowUtils::ObSiliconflowRerank::get_body(common::ObIAllocator &allocator,
                                                  common::ObString &model,
                                                  common::ObString &query,
                                                  common::ObJsonArray *document_array,
                                                  common::ObJsonObject *config,
                                                  common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || OB_ISNULL(query) || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or query or document_array is null", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonString *query_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, query, query_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("query", query_str))) {
      LOG_WARN("Failed to add query", K(ret));
    } else if (OB_FAIL(body_obj->add("documents", document_array))) {
      LOG_WARN("Failed to add documents", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObSiliconflowUtils::ObSiliconflowRerank::parse_output(common::ObIAllocator &allocator,
                                                      common::ObJsonObject *http_response,
                                                      common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObJsonArray *results_array = nullptr;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else if (OB_ISNULL(results_array = static_cast<ObJsonArray *>(http_response->get_value("results")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results_array is null", K(ret));
  } else {
    result = results_array;
  }
  return ret;
}


int ObAIFuncUtils::get_header(ObIAllocator &allocator,
                              ObAIFuncExprInfo *info,
                              ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else if (ObAIFuncUtils::is_completion_type(info)) {
    ObAIFuncIComplete *complete_provider = nullptr;
    if (OB_FAIL(get_complete_provider(allocator, info->provider_, complete_provider))) {
      LOG_WARN("Failed to get complete provider", K(ret));
    } else if (OB_FAIL(complete_provider->get_header(allocator, info->api_key_, headers))) {
      LOG_WARN("Failed to get header from complete provider", K(ret));
    }
  } else if (ObAIFuncUtils::is_dense_embedding_type(info)) {
    ObAIFuncIEmbed *embed_provider = nullptr;
    if (OB_FAIL(get_embed_provider(allocator, info->provider_, embed_provider))) {
      LOG_WARN("Failed to get embed provider", K(ret));
    } else if (OB_FAIL(embed_provider->get_header(allocator, info->api_key_, headers))) {
      LOG_WARN("Failed to get header from embed provider", K(ret));
    }
  } else if (ObAIFuncUtils::is_rerank_type(info)) {
    ObAIFuncIRerank *rerank_provider = nullptr;
    if (OB_FAIL(get_rerank_provider(allocator, info->provider_, rerank_provider))) {
      LOG_WARN("Failed to get rerank provider", K(ret));
    } else if (OB_FAIL(rerank_provider->get_header(allocator, info->api_key_, headers))) {
      LOG_WARN("Failed to get header from rerank provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::get_complete_body(ObIAllocator &allocator,
                                    ObAIFuncExprInfo *info,
                                    ObString &prompt,
                                    ObString &content,
                                    ObJsonObject *config,
                                    ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else {
    ObAIFuncIComplete *complete_provider = nullptr;
    if (OB_FAIL(get_complete_provider(allocator, info->provider_, complete_provider))) {
      LOG_WARN("Failed to get complete provider", K(ret));
    } else if (OB_FAIL(complete_provider->get_body(allocator, info->model_, prompt, content, config, body))) {
      LOG_WARN("Failed to get body from complete provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::set_json_format_config(ObIAllocator &allocator, ObString &provider, ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  ObAIFuncIComplete *complete_provider = nullptr;
  if (OB_FAIL(get_complete_provider(allocator, provider, complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->set_config_json_format(allocator, config))) {
    LOG_WARN("Failed to set json format config from complete provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_embed_body(ObIAllocator &allocator,
                                  ObAIFuncExprInfo *info,
                                  ObArray<ObString> &contents,
                                  ObJsonObject *config,
                                  ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else {
    ObAIFuncIEmbed *embed_provider = nullptr;
    if (OB_FAIL(get_embed_provider(allocator, info->provider_, embed_provider))) {
      LOG_WARN("Failed to get embed provider", K(ret));
    } else if (OB_FAIL(embed_provider->get_body(allocator, info->model_, contents, config, body))) {
      LOG_WARN("Failed to get body from embed provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::get_rerank_body(ObIAllocator &allocator,
                                   ObAIFuncExprInfo *info,
                                   ObString &query,
                                   ObJsonArray *document_array,
                                   ObJsonObject *config,
                                   ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else {
    ObAIFuncIRerank *rerank_provider = nullptr;
    if (OB_FAIL(get_rerank_provider(allocator, info->provider_, rerank_provider))) {
      LOG_WARN("Failed to get rerank provider", K(ret));
    } else if (OB_FAIL(rerank_provider->get_body(allocator, info->model_, query, document_array, config, body))) {
      LOG_WARN("Failed to get body from rerank provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::parse_complete_output(ObIAllocator &allocator,
                                        ObAIFuncExprInfo *info,
                                        ObJsonObject *http_response,
                                        ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else {
    ObAIFuncIComplete *complete_provider = nullptr;
    if (OB_FAIL(get_complete_provider(allocator, info->provider_, complete_provider))) {
      LOG_WARN("Failed to get complete provider", K(ret));
    } else if (OB_FAIL(complete_provider->parse_output(allocator, http_response, result))) {
      LOG_WARN("Failed to parse output from complete provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::parse_embed_output(ObIAllocator &allocator,
                                      ObAIFuncExprInfo *info,
                                      ObJsonObject *http_response,
                                      ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else {
    ObAIFuncIEmbed *embed_provider = nullptr;
    if (OB_FAIL(get_embed_provider(allocator, info->provider_, embed_provider))) {
      LOG_WARN("Failed to get embed provider", K(ret));
    } else if (OB_FAIL(embed_provider->parse_output(allocator, http_response, result))) {
      LOG_WARN("Failed to parse output from embed provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::parse_rerank_output(ObIAllocator &allocator,
                                       ObAIFuncExprInfo *info,
                                       ObJsonObject *http_response,
                                       ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
  } else {
    ObAIFuncIRerank *rerank_provider = nullptr;
    if (OB_FAIL(get_rerank_provider(allocator, info->provider_, rerank_provider))) {
      LOG_WARN("Failed to get rerank provider", K(ret));
    } else if (OB_FAIL(rerank_provider->parse_output(allocator, http_response, result))) {
      LOG_WARN("Failed to parse output from rerank provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_object(ObIAllocator &allocator, ObJsonObject *&obj_node)
{
  int ret = OB_SUCCESS;
  ObJsonObject *j_obj = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(j_obj)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_obj", K(ret));
  } else {
    obj_node = j_obj;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_array(ObIAllocator &allocator, ObJsonArray *&array_node)
{
  int ret = OB_SUCCESS;
  ObJsonArray *j_array = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(j_array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_array", K(ret));
  } else {
    array_node = j_array;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_string(ObIAllocator &allocator, ObString &str, ObJsonString *&str_node)
{
  int ret = OB_SUCCESS;
  ObJsonString *j_str = OB_NEWx(ObJsonString, &allocator,str);
  if (OB_ISNULL(j_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_str", K(ret));
  } else {
    str_node = j_str;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_int(ObIAllocator &allocator, int64_t num, ObJsonInt *&int_node)
{
  int ret = OB_SUCCESS;
  ObJsonInt *j_int = OB_NEWx(ObJsonInt, &allocator, num);
  if (OB_ISNULL(j_int)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_int", K(ret));
  } else {
    int_node = j_int;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_boolean(ObIAllocator &allocator, bool value, ObJsonBoolean *&boolean_node)
{
  int ret = OB_SUCCESS;
  ObJsonBoolean *j_bool = OB_NEWx(ObJsonBoolean, &allocator, value);
  if (OB_ISNULL(j_bool)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_bool", K(ret));
  } else {
    boolean_node = j_bool;
  }
  return ret;
}

int ObAIFuncJsonUtils::print_json_to_str(ObIAllocator &allocator, ObIJsonBase *base_node, ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonBuffer j_buf(&allocator);
  if (OB_FAIL(base_node->print(j_buf, 0))) {
    LOG_WARN("Failed to print json", K(ret));
  } else {
    str = j_buf.string();
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_object_form_str(ObIAllocator &allocator, ObString &str, ObJsonObject *&obj_node)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = NULL;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, str, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base))) {
    LOG_WARN("fail to get json base", K(ret), K(str));
  } else {
    obj_node = static_cast<ObJsonObject *>(j_base);
  }
  return ret;
}

int ObAIFuncJsonUtils::compact_json_object(ObIAllocator &allocator, ObJsonObject *obj_node, ObJsonObject *compact_obj)
{
  int ret = OB_SUCCESS;
  // add all members of obj_node to compact_obj
  if (OB_ISNULL(compact_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact_obj is null", K(ret));
  } else if (OB_ISNULL(obj_node)) {
    // do nothing
  } else {
    ObString key;
    int64_t count = obj_node->element_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObJsonNode *j_node = obj_node->get_value(i);
      if (OB_ISNULL(j_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("j_node is null", K(ret));
      } else if (OB_FAIL(obj_node->get_key(i, key))) {
        LOG_WARN("Failed to get key", K(ret));
      } else if (OB_FAIL(compact_obj->add(key, j_node))) {
        LOG_WARN("Failed to add member", K(ret));
      }
    }
  }
  return ret;
}

int ObAIFuncPromptUtils::replace_meta_prompt(ObIAllocator &allocator, ObString &meta_prompt, ObString &key, ObString &content, ObString &result)
{
  int ret = OB_SUCCESS;
  if (meta_prompt.empty() || key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_prompt or key is empty", K(ret));
  } else {
    const char *key_pos = nullptr;
    if (OB_ISNULL(key_pos = STRSTR(meta_prompt.ptr(), key.ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key not found in meta_prompt", K(ret));
    } else {
      int64_t before_key_len = key_pos - meta_prompt.ptr();
      int64_t after_key_len = meta_prompt.length() - before_key_len - key.length();
      int64_t new_len = before_key_len + content.length() + after_key_len;
      char *new_str = static_cast<char *>(allocator.alloc(new_len));
      if (OB_ISNULL(new_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for new string", K(ret));
      } else {
        MEMCPY(new_str, meta_prompt.ptr(), before_key_len);
        MEMCPY(new_str + before_key_len, content.ptr(), content.length());
        MEMCPY(new_str + before_key_len + content.length(), key_pos + key.length(), after_key_len);
        result.assign_ptr(new_str, static_cast<ObString::obstr_size_t>(new_len));
      }
    }
  }
  return ret;
}

int ObAIFuncJsonUtils::transform_array_to_json_array(ObIAllocator &allocator, ObArray<ObString> &contents, ObJsonArray *&array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array = OB_NEWx(ObJsonArray, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for array", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < contents.count(); i++) {
      ObString content = contents[i];
      ObJsonString *j_str = OB_NEWx(ObJsonString, &allocator, content);
      if (OB_ISNULL(j_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for j_str", K(ret));
      } else {
        array->append(j_str);
      }
    }
  }
  return ret;
}

int ObAIFuncUtils::get_complete_provider(ObIAllocator &allocator, ObString &provider, ObAIFuncIComplete *&complete_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI)
      || ob_provider_check(provider, ObAIFuncProviderUtils::ALIYUN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::DEEPSEEK)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)
      || ob_provider_check(provider, ObAIFuncProviderUtils::HUNYUAN)) {
    complete_provider = OB_NEWx(ObOpenAIUtils::ObOpenAIComplete, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    complete_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeComplete, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  }
  if (OB_SUCC(ret) && OB_ISNULL(complete_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for complete_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_embed_provider(ObIAllocator &allocator, ObString &provider, ObAIFuncIEmbed *&embed_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI)
      || ob_provider_check(provider, ObAIFuncProviderUtils::ALIYUN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::HUNYUAN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)) {
    embed_provider = OB_NEWx(ObOpenAIUtils::ObOpenAIEmbed, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    embed_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeEmbed, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  }
  if (OB_SUCC(ret) && OB_ISNULL(embed_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for embed_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_rerank_provider(ObIAllocator &allocator, ObString &provider, ObAIFuncIRerank *&rerank_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)) {
    rerank_provider = OB_NEWx(ObSiliconflowUtils::ObSiliconflowRerank, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    rerank_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeRerank, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  }
  if (OB_SUCC(ret) && OB_ISNULL(rerank_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for rerank_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_completion(ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info is null");
  } else if (!is_completion_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not completion");
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_dense_embedding(ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info is null");
  } else if (!is_dense_embedding_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_rerank(ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info is null");
  } else if (!is_rerank_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not rerank", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not rerank");
  }
  return ret;
}

int ObAIFuncUtils::set_string_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObString &res_str)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  int64_t res_len = res_str.length();
  if (OB_FAIL(text_result.init(res_len))) {
    LOG_WARN("fail to init string result length", K(ret), K(text_result), K(res_len));
  } else if (OB_FAIL(text_result.append(res_str))) {
    LOG_WARN("fail to append string", K(ret), K(res_str), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

int ObAIFuncUtils::get_ai_func_info(ObIAllocator &allocator, ObString &model_id, ObAIFuncExprInfo *&info)
{
  int ret = OB_SUCCESS;
  if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model_id is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model_id is empty");
  } else {
    ObAIFuncExprInfo *info_obj = OB_NEWx(ObAIFuncExprInfo, (&allocator), allocator, T_FUN_SYS_AI_COMPLETE);
    if (OB_ISNULL(info_obj)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for info_obj", K(ret));
    } else if (OB_FAIL(info_obj->init(allocator, model_id))) {
      LOG_WARN("Failed to init info_obj", K(ret));
    } else {
      info = info_obj;
    }
  }
  return ret;
}

int ObAIFuncModel::call_completion(ObString &prompt, ObJsonObject *config, ObString &result)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIComplete *complete_provider = nullptr;
  ObString prompt_str;
  ObString result_str;
  ObAIFuncClient client;
  if (!is_completion_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not completion");
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(*allocator_, info_.provider_, complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->get_header(*allocator_, info_.api_key_, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(complete_provider->get_body(*allocator_, info_.model_, prompt_str, prompt, config, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, info_.url_, headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(complete_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(*allocator_, result_base, result_str))) {
    LOG_WARN("Failed to print json to string", K(ret));
  } else {
    result = result_str;
  }
  return ret;
}

int ObAIFuncModel::call_completion_vector(ObArray<ObString> &prompts, ObJsonObject *config, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObArray<ObJsonObject *> body_array;
  ObArray<ObJsonObject *> response_array;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIComplete *complete_provider = nullptr;
  ObString prompt_str;
  ObString result_str;
  ObAIFuncClient client;
  if (!is_completion_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not completion");
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(*allocator_, info_.provider_, complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->get_header(*allocator_, info_.api_key_, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < prompts.count(); i++) {
      ObString prompt = prompts[i];
      if (OB_FAIL(complete_provider->get_body(*allocator_, info_.model_, prompt_str, prompt, config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(body_array.push_back(body))) {
        LOG_WARN("Failed to append body", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(client.send_post_batch(*allocator_, info_.url_, headers, body_array, response_array))) {
    LOG_WARN("Failed to send post", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < response_array.count(); i++) {
      ObJsonObject *response = response_array[i];
      ObStringBuffer result_buf(allocator_);
      if (OB_FAIL(complete_provider->parse_output(*allocator_, response, result_base))) {
        LOG_WARN("Failed to parse output", K(ret));
      } else if (OB_FAIL(result_base->print(result_buf, 0))) {
        LOG_WARN("Failed to print json", K(ret));
      } else {
        results.push_back(result_buf.string());
      }
    }
  }
  return ret;
}

int ObAIFuncModel::call_dense_embedding(ObString &content, ObJsonObject *config, ObString &result)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> contents;
  ObArray<ObString> results;
  if (content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("content is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "input is empty");
  } else if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  } else if (OB_FAIL(contents.push_back(content))) {
    LOG_WARN("Failed to push back content", K(ret));
  } else if (OB_FAIL(call_dense_embedding_vector_v2(contents, config, results))) {
    LOG_WARN("Failed to call dense embedding vector v2", K(ret));
  } else if (results.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results is not equal to 1", K(ret));
  } else {
    result = results[0];
  }
  return ret;
}

int ObAIFuncModel::call_dense_embedding_vector(ObArray<ObString> &contents, ObJsonObject *config, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObArray<ObJsonObject *> body_array;
  ObArray<ObJsonObject *> response_array;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIEmbed *embed_provider = nullptr;
  ObAIFuncClient client;
  if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  } else if (OB_FAIL(ObAIFuncUtils::get_embed_provider(*allocator_, info_.provider_, embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->get_header(*allocator_, info_.api_key_, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < contents.count(); i++) {
      ObString content = contents[i];
      ObArray<ObString> content_array;
      if (OB_FAIL(content_array.push_back(content))) {
        LOG_WARN("Failed to push back content", K(ret));
      } else if (OB_FAIL(embed_provider->get_body(*allocator_, info_.model_, content_array, config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(body_array.push_back(body))) {
        LOG_WARN("Failed to append body", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(client.send_post_batch(*allocator_, info_.url_, headers, body_array, response_array))) {
    LOG_WARN("Failed to send post", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < response_array.count(); i++) {
      ObJsonObject *response = response_array[i];
      ObStringBuffer result_buf(allocator_);
      if (OB_FAIL(embed_provider->parse_output(*allocator_, response, result_base))) {
        LOG_WARN("Failed to parse output", K(ret));
      } else if (OB_FAIL(result_base->print(result_buf, 0))) {
        LOG_WARN("Failed to print json", K(ret));
      } else {
        results.push_back(result_buf.string());
      }
    }
  }
  return ret;
}


int ObAIFuncModel::call_dense_embedding_vector_v2(ObArray<ObString> &content, ObJsonObject *config, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIEmbed *embed_provider = nullptr;
  ObString result_str;
  ObAIFuncClient client;
  if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  } else if (OB_FAIL(ObAIFuncUtils::get_embed_provider(*allocator_, info_.provider_, embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->get_header(*allocator_, info_.api_key_, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(embed_provider->get_body(*allocator_, info_.model_, content, config, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, info_.url_, headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(embed_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else {
    ObJsonArray *result_array = static_cast<ObJsonArray *>(result_base);
    int64_t count = result_array->element_count();
    if (content.count() != count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("content count is not equal to result array count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        ObIJsonBase *j_base = result_array->get_value(i);
        if (OB_ISNULL(j_base)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("j_base is null", K(ret));
        } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(*allocator_, j_base, result_str))) {
          LOG_WARN("Failed to print json to string", K(ret));
        } else {
          results.push_back(result_str);
        }
      }
    }
  }
  return ret;
}

int ObAIFuncModel::call_rerank(ObString &query, ObJsonArray *contents, ObJsonArray *&results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIRerank *rerank_provider = nullptr;
  ObAIFuncClient client;
  if (!is_rerank_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not rerank", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not rerank");
  } else if (OB_FAIL(ObAIFuncUtils::get_rerank_provider(*allocator_, info_.provider_, rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(rerank_provider->get_header(*allocator_, info_.api_key_, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(rerank_provider->get_body(*allocator_, info_.model_, query, contents, nullptr, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, info_.url_, headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(rerank_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else {
    results = static_cast<ObJsonArray *>(result_base);
  }
  return ret;
}

bool ObAIFuncJsonUtils::ob_is_json_array_all_str(ObJsonArray* json_array)
{
  bool is_all_str = true;
  if (OB_ISNULL(json_array)) {
    is_all_str = false;
  }
  for (int64_t i = 0; is_all_str && i < json_array->element_count(); i++) {
    ObJsonNode *node = json_array->get_value(i);
    if (OB_ISNULL(node)) {
      is_all_str = false;
    } else if (node->json_type() != ObJsonNodeType::J_STRING) {
      is_all_str = false;
    }
  }
  return is_all_str;
}

int ObAIFuncPromptObjectUtils::construct_prompt_object(ObIAllocator &allocator, ObString &template_str, ObJsonArray *args_array, ObJsonObject *&prompt_object)
{
  INIT_SUCC(ret);
  ObJsonObject *prompt_obj = NULL;
  ObJsonString *template_json_str = NULL;
  if (template_str.empty() || args_array == NULL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, template_str, template_json_str))) {
    LOG_WARN("fail to get json string", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, prompt_obj))) {
    LOG_WARN("fail to get json object", K(ret));
  } else if (OB_FAIL(prompt_obj->add(ObAIFuncPromptObjectUtils::prompt_template_key, template_json_str))) {
    LOG_WARN("fail to set template", K(ret));
  } else if (OB_FAIL(prompt_obj->add(ObAIFuncPromptObjectUtils::prompt_args_key, args_array))) {
    LOG_WARN("fail to set args", K(ret), K(args_array));
  }
  if (OB_SUCC(ret)) {
    prompt_object = prompt_obj;
  }
  return ret;
}

bool ObAIFuncPromptObjectUtils::is_valid_prompt_object(ObJsonObject *prompt_object)
{
  bool is_valid = true;
  ObJsonNode *template_node = NULL;
  ObJsonNode *args_node = NULL;
  if (OB_ISNULL(prompt_object)) {
    is_valid = false;
  } else if (prompt_object->element_count() != 2) {
    is_valid = false;
  } else if (OB_ISNULL(template_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key)) ||
            (OB_ISNULL(args_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key)))) {
    is_valid = false;
  } else if (template_node->json_type() != ObJsonNodeType::J_STRING || args_node->json_type() != ObJsonNodeType::J_ARRAY) {
    is_valid = false;
  }
  if (is_valid) {
    ObJsonArray *args_array = static_cast<ObJsonArray *>(args_node);
    for (int64_t i = 0; is_valid && i < args_array->element_count(); i++) {
      ObJsonNode *node = args_array->get_value(i);
      if (OB_ISNULL(node)) {
        is_valid = false;
      } else if (node->json_type() != ObJsonNodeType::J_STRING && node->json_type() != ObJsonNodeType::J_OBJECT) {
        is_valid = false;
      }
    }
  }
  return is_valid;
}

int ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(ObIAllocator &allocator, ObJsonObject* prompt_object, ObString& replaced_prompt_str)
{
  INIT_SUCC(ret);
  ObJsonString *template_json_str = NULL;
  ObJsonArray *args_array = NULL;
  ObString template_str;
  ObString result_str;
  if (OB_ISNULL(prompt_object) ||
      OB_ISNULL(template_json_str = static_cast<ObJsonString *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key))) ||
      OB_ISNULL(args_array = static_cast<ObJsonArray *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key))) ||
      OB_ISNULL(template_str = template_json_str->get_str()) ||
      (template_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    uint64_t args_count = args_array->element_count();
    int64_t max_result_len = template_str.length();
    char *result_buf = NULL;
    for (uint64_t i = 0; OB_SUCC(ret) && i < args_count; i++) {
      ObJsonNode *arg_node = args_array->get_value(i);
      if (OB_NOT_NULL(arg_node) && arg_node->json_type() == ObJsonNodeType::J_STRING) {
        ObJsonString *arg_str = static_cast<ObJsonString *>(arg_node);
        max_result_len += arg_str->get_str().length();
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(result_buf = static_cast<char *>(allocator.alloc(max_result_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for result buffer", K(ret), K(max_result_len));
    } else {
      int64_t result_pos = 0;
      const char *template_ptr = template_str.ptr();
      int64_t template_len = template_str.length();

      for (int64_t i = 0; i < template_len && OB_SUCC(ret); i++) {
        if (template_ptr[i] == '{') {
          int64_t start_pos = i;
          int64_t end_pos = start_pos;
          bool found_end = false;

          for (int64_t j = start_pos + 1; j < template_len && !found_end; j++) {
            if (template_ptr[j] == '}') {
              end_pos = j;
              found_end = true;
            } else if (template_ptr[j] < '0' || template_ptr[j] > '9') {
              break;
            }
          }

          if (found_end && end_pos > start_pos + 1) {
            ObString index_str;
            index_str.assign_ptr(template_ptr + start_pos + 1, static_cast<int32_t>(end_pos - start_pos - 1));

            int64_t index = 0;
            bool valid_index = true;
            for (int64_t k = 0; k < index_str.length() && valid_index; k++) {
              if (index_str.ptr()[k] >= '0' && index_str.ptr()[k] <= '9') {
                index = index * 10 + (index_str.ptr()[k] - '0');
              } else {
                valid_index = false;
              }
            }

            if (valid_index && index >= 0 && static_cast<uint64_t>(index) < args_count) {
              ObJsonNode *arg_node = args_array->get_value(static_cast<uint64_t>(index));
              if (OB_NOT_NULL(arg_node) && arg_node->json_type() == ObJsonNodeType::J_STRING) {
                ObJsonString *arg_str = static_cast<ObJsonString *>(arg_node);
                ObString arg_value = arg_str->get_str();

                if (result_pos + arg_value.length() <= max_result_len) {
                  MEMCPY(result_buf + result_pos, arg_value.ptr(), arg_value.length());
                  result_pos += arg_value.length();
                } else {
                  ret = OB_BUF_NOT_ENOUGH;
                  LOG_WARN("result buffer not enough", K(ret), K(result_pos), K(arg_value.length()), K(max_result_len));
                }
              } else {
                //do nothing
              }

              i = end_pos;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid placeholder index", K(ret), K(index), K(args_count));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt: invalid placeholder index");
            }
          } else {
            if (result_pos < max_result_len) {
              result_buf[result_pos++] = template_ptr[i];
            } else {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN("result buffer not enough", K(ret), K(result_pos), K(max_result_len));
            }
          }
        } else {
          if (result_pos < max_result_len) {
            result_buf[result_pos++] = template_ptr[i];
          } else {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("result buffer not enough", K(ret), K(result_pos), K(max_result_len));
          }
        }
      }

      if (OB_SUCC(ret)) {
        replaced_prompt_str.assign_ptr(result_buf, static_cast<int32_t>(result_pos));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase