/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ai_func_utils.h"
#include "ob_ai_func_client.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/random/ob_random.h"
#include <cctype>
#include <algorithm>

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

static int get_image_url_from_prompt_arg_object(ObIAllocator &allocator,
                                                ObJsonObject *image_obj,
                                                ObString &image_url)
{
  int ret = OB_SUCCESS;
  ObJsonNode *type_node = nullptr;
  ObJsonNode *url_node = nullptr;
  ObJsonNode *format_node = nullptr;
  ObJsonNode *data_node = nullptr;
  ObString image_type("image");
  if (OB_ISNULL(image_obj)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(type_node = image_obj->get_value("type"))
              || type_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
  } else if (static_cast<ObJsonString *>(type_node)->get_str().case_compare(image_type) != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_NOT_NULL(url_node = image_obj->get_value("url"))) {
    if (url_node->json_type() != ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      image_url = static_cast<ObJsonString *>(url_node)->get_str();
      if (image_url.empty()) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  } else if (OB_ISNULL(format_node = image_obj->get_value("format"))
              || format_node->json_type() != ObJsonNodeType::J_STRING
              || OB_ISNULL(data_node = image_obj->get_value("data"))
              || data_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString format = static_cast<ObJsonString *>(format_node)->get_str();
    ObString data = static_cast<ObJsonString *>(data_node)->get_str();
    if (format.empty() || data.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const int64_t prefix_len = 11; // data:image/
      const int64_t middle_len = 8;  // ;base64,
      const int64_t total_len = prefix_len + format.length() + middle_len + data.length();
      char *buf = static_cast<char *>(allocator.alloc(total_len));
      int64_t pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf + pos, "data:image/", prefix_len);
        pos += prefix_len;
        MEMCPY(buf + pos, format.ptr(), format.length());
        pos += format.length();
        MEMCPY(buf + pos, ";base64,", middle_len);
        pos += middle_len;
        MEMCPY(buf + pos, data.ptr(), data.length());
        pos += data.length();
        image_url.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  if (OB_INVALID_ARGUMENT == ret) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, image prompt argument is invalid");
  }
  return ret;
}

static int append_openai_text_item(ObIAllocator &allocator, const ObString &text, ObJsonArray *content_array)
{
  int ret = OB_SUCCESS;
  ObJsonObject *text_obj = nullptr;
  ObJsonString *type_json = nullptr;
  ObJsonString *text_json = nullptr;
  ObString type_text("text");
  ObString text_copy = text;
  if (OB_ISNULL(content_array)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, text_obj))) {
    LOG_WARN("fail to alloc text obj", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, type_text, type_json))) {
    LOG_WARN("fail to alloc type text", K(ret));
  } else if (OB_FAIL(text_obj->add("type", type_json))) {
    LOG_WARN("fail to set text type", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, text_copy, text_json))) {
    LOG_WARN("fail to alloc text", K(ret));
  } else if (OB_FAIL(text_obj->add("text", text_json))) {
    LOG_WARN("fail to set text", K(ret));
  } else if (OB_FAIL(content_array->append(text_obj))) {
    LOG_WARN("fail to append text item", K(ret));
  }
  return ret;
}

static int append_openai_image_item(ObIAllocator &allocator, const ObString &url, ObJsonArray *content_array)
{
  int ret = OB_SUCCESS;
  ObJsonObject *image_obj = nullptr;
  ObJsonString *type_json = nullptr;
  ObJsonObject *image_url_obj = nullptr;
  ObJsonString *url_json = nullptr;
  ObString type_image_url("image_url");
  ObString url_copy = url;
  if (OB_ISNULL(content_array) || url.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, image_obj))) {
    LOG_WARN("fail to alloc image obj", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, type_image_url, type_json))) {
    LOG_WARN("fail to alloc image type", K(ret));
  } else if (OB_FAIL(image_obj->add("type", type_json))) {
    LOG_WARN("fail to set image type", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, image_url_obj))) {
    LOG_WARN("fail to alloc image_url obj", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, url_copy, url_json))) {
    LOG_WARN("fail to alloc image url", K(ret));
  } else if (OB_FAIL(image_url_obj->add("url", url_json))) {
    LOG_WARN("fail to set image url", K(ret));
  } else if (OB_FAIL(image_obj->add("image_url", image_url_obj))) {
    LOG_WARN("fail to set image_url field", K(ret));
  } else if (OB_FAIL(content_array->append(image_obj))) {
    LOG_WARN("fail to append image item", K(ret));
  }
  return ret;
}

static int append_dashscope_text_item(ObIAllocator &allocator, const ObString &text, ObJsonArray *content_array)
{
  int ret = OB_SUCCESS;
  ObJsonObject *text_obj = nullptr;
  ObJsonString *text_json = nullptr;
  ObString text_copy = text;
  if (OB_ISNULL(content_array)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, text_obj))) {
    LOG_WARN("fail to alloc text obj", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, text_copy, text_json))) {
    LOG_WARN("fail to alloc text", K(ret));
  } else if (OB_FAIL(text_obj->add("text", text_json))) {
    LOG_WARN("fail to set text", K(ret));
  } else if (OB_FAIL(content_array->append(text_obj))) {
    LOG_WARN("fail to append text item", K(ret));
  }
  return ret;
}

static int append_dashscope_image_item(ObIAllocator &allocator, const ObString &url, ObJsonArray *content_array)
{
  int ret = OB_SUCCESS;
  ObJsonObject *image_obj = nullptr;
  ObJsonString *image_json = nullptr;
  ObString url_copy = url;
  if (OB_ISNULL(content_array) || url.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, image_obj))) {
    LOG_WARN("fail to alloc image obj", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, url_copy, image_json))) {
    LOG_WARN("fail to alloc image", K(ret));
  } else if (OB_FAIL(image_obj->add("image", image_json))) {
    LOG_WARN("fail to set image", K(ret));
  } else if (OB_FAIL(content_array->append(image_obj))) {
    LOG_WARN("fail to append image item", K(ret));
  }
  return ret;
}

static int construct_openai_content_from_prompt_object(ObIAllocator &allocator,
                                                       ObJsonObject *prompt_object,
                                                       ObJsonArray *&content_array)
{
  int ret = OB_SUCCESS;
  ObJsonNode *template_node = nullptr;
  ObJsonNode *args_node = nullptr;
  ObJsonString *template_json = nullptr;
  ObJsonArray *args_array = nullptr;
  ObString template_str;
  if (OB_ISNULL(prompt_object)
      || OB_ISNULL(template_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key))
      || ObJsonNodeType::J_STRING != template_node->json_type()
      || OB_ISNULL(args_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key))
      || ObJsonNodeType::J_ARRAY != args_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt object field type is invalid");
  } else if (OB_FALSE_IT(template_json = static_cast<ObJsonString *>(template_node))) {
  } else if (OB_FALSE_IT(args_array = static_cast<ObJsonArray *>(args_node))) {
  } else if (OB_FALSE_IT(template_str = template_json->get_str())) {
  } else if (template_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, content_array))) {
    LOG_WARN("fail to alloc openai content array", K(ret));
  } else {
    const char *ptr = template_str.ptr();
    const int64_t len = template_str.length();
    int64_t seg_start = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (ptr[i] != '{') {
        continue;
      }
      bool is_img = false;
      int64_t digit_start = i + 1;
      if (i + 4 < len
          && ptr[i + 1] == 'i'
          && ptr[i + 2] == 'm'
          && ptr[i + 3] == 'g'
          && ptr[i + 4] == '_') {
        is_img = true;
        digit_start = i + 5;
      }
      int64_t j = digit_start;
      int64_t idx = 0;
      bool has_digit = false;
      while (j < len && ptr[j] >= '0' && ptr[j] <= '9') {
        has_digit = true;
        idx = idx * 10 + (ptr[j] - '0');
        ++j;
      }
      if (!has_digit || j >= len || ptr[j] != '}') {
        continue;
      }
      if (seg_start < i) {
        ObString text_seg;
        text_seg.assign_ptr(ptr + seg_start, static_cast<int32_t>(i - seg_start));
        if (OB_FAIL(append_openai_text_item(allocator, text_seg, content_array))) {
          LOG_WARN("fail to append openai text seg", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (idx < 0 || idx >= args_array->element_count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt placeholder index out of range");
      } else {
        ObJsonNode *arg_node = args_array->get_value(idx);
        if (OB_ISNULL(arg_node)) {
          ret = OB_INVALID_ARGUMENT;
        } else if (arg_node->json_type() == ObJsonNodeType::J_STRING) {
          ObString arg_text = static_cast<ObJsonString *>(arg_node)->get_str();
          if (OB_FAIL(append_openai_text_item(allocator, arg_text, content_array))) {
            LOG_WARN("fail to append openai arg text", K(ret), K(idx));
          }
        } else if (arg_node->json_type() == ObJsonNodeType::J_OBJECT) {
          ObString image_url;
          if (OB_FAIL(get_image_url_from_prompt_arg_object(allocator, static_cast<ObJsonObject *>(arg_node), image_url))) {
            LOG_WARN("fail to parse image arg", K(ret), K(idx));
          } else if (OB_FAIL(append_openai_image_item(allocator, image_url, content_array))) {
            LOG_WARN("fail to append openai image arg", K(ret), K(idx));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
        }
        if (OB_INVALID_ARGUMENT == ret) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt args type is invalid");
        }
      }
      seg_start = j + 1;
      i = j;
      UNUSED(is_img);
    }
    if (OB_SUCC(ret) && seg_start < len) {
      ObString text_seg;
      text_seg.assign_ptr(ptr + seg_start, static_cast<int32_t>(len - seg_start));
      if (OB_FAIL(append_openai_text_item(allocator, text_seg, content_array))) {
        LOG_WARN("fail to append openai trailing text seg", K(ret));
      }
    }
  }
  return ret;
}

static int construct_dashscope_content_from_prompt_object(ObIAllocator &allocator,
                                                          ObJsonObject *prompt_object,
                                                          ObJsonArray *&content_array)
{
  int ret = OB_SUCCESS;
  ObJsonArray *openai_content = nullptr;
  if (OB_FAIL(construct_openai_content_from_prompt_object(allocator, prompt_object, openai_content))) {
    LOG_WARN("fail to construct openai content from prompt object", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, content_array))) {
    LOG_WARN("fail to alloc dashscope content array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < openai_content->element_count(); ++i) {
      ObJsonObject *item = static_cast<ObJsonObject *>(openai_content->get_value(i));
      ObJsonString *type_json = static_cast<ObJsonString *>(item->get_value("type"));
      if (OB_ISNULL(type_json)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (0 == type_json->get_str().case_compare("text")) {
        ObJsonString *text_json = static_cast<ObJsonString *>(item->get_value("text"));
        if (OB_ISNULL(text_json)) {
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(append_dashscope_text_item(allocator, text_json->get_str(), content_array))) {
          LOG_WARN("fail to append dashscope text", K(ret), K(i));
        }
      } else if (0 == type_json->get_str().case_compare("image_url")) {
        ObJsonObject *image_url_obj = static_cast<ObJsonObject *>(item->get_value("image_url"));
        ObJsonString *url_json = OB_ISNULL(image_url_obj) ? nullptr : static_cast<ObJsonString *>(image_url_obj->get_value("url"));
        if (OB_ISNULL(url_json)) {
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(append_dashscope_image_item(allocator, url_json->get_str(), content_array))) {
          LOG_WARN("fail to append dashscope image", K(ret), K(i));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_INVALID_ARGUMENT == ret) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt args type is invalid");
      }
    }
  }
  return ret;
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
      ret = OB_INVALID_DATA;
      LOG_WARN("http response format is not as expected, failed to get content", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIVLComplete::get_header(common::ObIAllocator &allocator,
                                                  ObString &api_key,
                                                  common::ObArray<ObString> &headers)
{
  return ObOpenAIUtils::get_header(allocator, api_key, headers);
}

int ObOpenAIUtils::ObOpenAIVLComplete::get_body(common::ObIAllocator &allocator,
                                                common::ObString &model,
                                                common::ObString &prompt,
                                                common::ObJsonObject *prompt_object,
                                                common::ObJsonObject *config,
                                                common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || OB_ISNULL(prompt_object)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model is empty or prompt object is null", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *messages_array = nullptr;
    ObJsonArray *content_array = nullptr;
    ObJsonObject *user_message_obj = nullptr;
    ObJsonString *user_role_json = nullptr;
    ObString user_role("user");
    ObString system_role("system");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("fail to alloc body obj", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("fail to alloc model json", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("fail to set model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, messages_array))) {
      LOG_WARN("fail to alloc messages array", K(ret));
    } else if (!prompt.empty()) {
      ObJsonObject *sys_message_obj = nullptr;
      if (OB_FAIL(ObOpenAIUtils::ObOpenAIComplete::construct_message_obj(allocator, system_role, prompt, sys_message_obj))) {
        LOG_WARN("fail to construct system message", K(ret));
      } else if (OB_FAIL(messages_array->append(sys_message_obj))) {
        LOG_WARN("fail to append system message", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(construct_openai_content_from_prompt_object(allocator, prompt_object, content_array))) {
        LOG_WARN("fail to construct openai content from prompt object", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, user_message_obj))) {
        LOG_WARN("fail to alloc user message", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, user_role, user_role_json))) {
        LOG_WARN("fail to alloc user role", K(ret));
      } else if (OB_FAIL(user_message_obj->add("role", user_role_json))) {
        LOG_WARN("fail to set user role", K(ret));
      } else if (OB_FAIL(user_message_obj->add("content", content_array))) {
        LOG_WARN("fail to set user content", K(ret));
      } else if (OB_FAIL(messages_array->append(user_message_obj))) {
        LOG_WARN("fail to append user message", K(ret));
      } else if (OB_FAIL(body_obj->add("messages", messages_array))) {
        LOG_WARN("fail to set messages", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
        LOG_WARN("fail to compact config", K(ret));
      } else {
        body = body_obj;
      }
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIVLComplete::parse_output(common::ObIAllocator &allocator,
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
      ret = OB_INVALID_DATA;
      LOG_WARN("http response format is not as expected, failed to get content", K(ret));
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
                                           common::ObString input_type,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else if (input_type.case_compare("image") == 0) {
    // OpenAI provider does not support image embedding
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("OpenAI provider does not support image embedding", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "OpenAI provider image embedding");
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

int ObOpenAIUtils::ObOpenAIEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObArray<ObString> &input_type_array,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else if (input_type_array.count() != contents.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input_type_array count is not equal to contents count", K(ret),
             K(input_type_array.count()), K(contents.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_type_array.count(); ++i) {
      if (input_type_array.at(i).case_compare("image") == 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("OpenAI provider does not support image embedding", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "OpenAI provider image embedding");
      }
    }
    if (OB_SUCC(ret)) {
      ObString text_input_type("text");
      if (OB_FAIL(get_body(allocator, model, contents, config, text_input_type, body))) {
        LOG_WARN("Failed to get body", K(ret));
      }
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
      ret = OB_INVALID_DATA;
      LOG_WARN("Failed to get data", K(ret));
    } else {
      ObJsonArray *data_array = static_cast<ObJsonArray *>(data_node);
      ObJsonNode *embedding_node = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < data_array->element_count(); i++) {
        if (OB_ISNULL(embedding_node = data_array->get_value(i))) {
          ret = OB_INVALID_DATA;
          LOG_WARN("Failed to get embedding", K(ret));
        } else if (embedding_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_DATA;
          LOG_WARN("Failed to get embedding node", K(ret));
        } else {
          ObJsonObject *embedding_obj = static_cast<ObJsonObject *>(embedding_node);
          ObJsonNode *embedding = embedding_obj->get_value("embedding");
          if (OB_ISNULL(embedding)) {
            ret = OB_INVALID_DATA;
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
                                           common::ObString input_type,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else if (input_type.case_compare("image") == 0) {
    // Ollama provider does not support image embedding
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Ollama provider does not support image embedding", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Ollama provider does not support image embedding");
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

int ObOllamaUtils::ObOllamaEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObArray<ObString> &input_type_array,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else if (input_type_array.count() != contents.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input_type_array count is not equal to contents count", K(ret),
             K(input_type_array.count()), K(contents.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_type_array.count(); ++i) {
      if (input_type_array.at(i).case_compare("image") == 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Ollama provider does not support image embedding", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Ollama provider does not support image embedding");
      }
    }
    if (OB_SUCC(ret)) {
      ObString text_input_type("text");
      if (OB_FAIL(get_body(allocator, model, contents, config, text_input_type, body))) {
        LOG_WARN("Failed to get body", K(ret));
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
  if (model.empty() || content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or content is empty", K(ret));
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
    ObIJsonBase *content_base = nullptr;
    ObJsonArray *content_array = nullptr;
    ObJsonString *content_str = nullptr;
    ObJsonNode *output_node = nullptr;
    ObJsonNode *choices_node = nullptr;
    ObJsonNode *choice_node = nullptr;
    ObJsonNode *message_node = nullptr;
    if (OB_ISNULL(output_node = http_response->get_value("output"))
        || output_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("output_obj is null or invalid", K(ret));
    } else if (OB_FALSE_IT(output_obj = static_cast<ObJsonObject *>(output_node))) {
    } else if (OB_ISNULL(choices_node = output_obj->get_value("choices"))
               || choices_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_DATA;
      LOG_WARN("choices_array is null or invalid", K(ret));
    } else if (OB_FALSE_IT(choices_array = static_cast<ObJsonArray *>(choices_node))) {
    } else if (OB_ISNULL(choice_node = choices_array->get_value(0))
               || choice_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("choice_obj is null or invalid", K(ret));
    } else if (OB_FALSE_IT(choice_obj = static_cast<ObJsonObject *>(choice_node))) {
    } else if (OB_ISNULL(message_node = choice_obj->get_value("message"))
               || message_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("message_obj is null or invalid", K(ret));
    } else if (OB_FALSE_IT(message_obj = static_cast<ObJsonObject *>(message_node))) {
    } else if (OB_ISNULL(content_base = message_obj->get_value("content"))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("content_base is null", K(ret));
    } else if (content_base->json_type() == ObJsonNodeType::J_ARRAY) {
      ObJsonObject *content_obj = nullptr;
      ObJsonString *text_str = nullptr;
      ObJsonNode *content_node = nullptr;
      ObJsonNode *text_node = nullptr;
      content_array = static_cast<ObJsonArray *>(content_base);
      if (OB_ISNULL(content_node = content_array->get_value(0))
          || content_node->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
        LOG_WARN("content_obj is null or invalid", K(ret));
      } else if (OB_FALSE_IT(content_obj = static_cast<ObJsonObject *>(content_node))) {
      } else if (OB_ISNULL(text_node = content_obj->get_value("text"))
                 || text_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_DATA;
        LOG_WARN("text_str is null or invalid", K(ret));
      } else if (OB_FALSE_IT(text_str = static_cast<ObJsonString *>(text_node))) {
      } else {
        result = text_str;
      }
    } else if (content_base->json_type() == ObJsonNodeType::J_STRING) {
      content_str = static_cast<ObJsonString *>(content_base);
      result = content_str;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("content is neither array nor string", K(ret));
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeVLComplete::get_header(common::ObIAllocator &allocator,
                                                        common::ObString &api_key,
                                                        common::ObArray<ObString> &headers)
{
  return ObDashscopeUtils::get_header(allocator, api_key, headers);
}

int ObDashscopeUtils::ObDashscopeVLComplete::get_body(common::ObIAllocator &allocator,
                                                      common::ObString &model,
                                                      common::ObString &prompt,
                                                      common::ObJsonObject *prompt_object,
                                                      common::ObJsonObject *config,
                                                      common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || OB_ISNULL(prompt_object)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model is empty or prompt object is null", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    ObJsonArray *messages_array = nullptr;
    ObJsonArray *content_array = nullptr;
    ObJsonObject *user_message_obj = nullptr;
    ObJsonString *user_role = nullptr;
    ObString user_role_str("user");
    if (OB_ISNULL(config)) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
        LOG_WARN("failed to alloc config", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
        LOG_WARN("failed to alloc body", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
        LOG_WARN("failed to alloc model json", K(ret));
      } else if (OB_FAIL(body_obj->add("model", model_str))) {
        LOG_WARN("failed to set model", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, input_obj))) {
        LOG_WARN("failed to alloc input", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, messages_array))) {
        LOG_WARN("failed to alloc messages", K(ret));
      } else if (OB_FAIL(construct_dashscope_content_from_prompt_object(allocator, prompt_object, content_array))) {
        LOG_WARN("failed to construct dashscope content", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, user_message_obj))) {
        LOG_WARN("failed to alloc user message", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, user_role_str, user_role))) {
        LOG_WARN("failed to alloc role", K(ret));
      } else if (OB_FAIL(user_message_obj->add("role", user_role))) {
        LOG_WARN("failed to set role", K(ret));
      } else if (OB_FAIL(user_message_obj->add("content", content_array))) {
        LOG_WARN("failed to set content", K(ret));
      } else if (OB_FAIL(messages_array->append(user_message_obj))) {
        LOG_WARN("failed to append message", K(ret));
      } else if (OB_FAIL(input_obj->add("messages", messages_array))) {
        LOG_WARN("failed to set messages", K(ret));
      } else if (OB_FAIL(body_obj->add("input", input_obj))) {
        LOG_WARN("failed to set input", K(ret));
      } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeComplete::set_config_result_format(allocator, config))) {
        LOG_WARN("failed to set config result format", K(ret));
      } else if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("failed to add parameters", K(ret));
      } else {
        body = body_obj;
      }
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeVLComplete::parse_output(common::ObIAllocator &allocator,
                                                          common::ObJsonObject *http_response,
                                                          common::ObIJsonBase *&result)
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
    ObIJsonBase *content_base = nullptr;
    ObJsonArray *content_array = nullptr;
    ObJsonString *content_str = nullptr;
    ObJsonNode *output_node = nullptr;
    ObJsonNode *choices_node = nullptr;
    ObJsonNode *choice_node = nullptr;
    ObJsonNode *message_node = nullptr;
    if (OB_ISNULL(output_node = http_response->get_value("output"))
        || output_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("output_obj is null or invalid", K(ret));
    } else if (OB_FALSE_IT(output_obj = static_cast<ObJsonObject *>(output_node))) {
    } else if (OB_ISNULL(choices_node = output_obj->get_value("choices"))
               || choices_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_DATA;
      LOG_WARN("choices_array is null or invalid", K(ret));
    } else if (OB_FALSE_IT(choices_array = static_cast<ObJsonArray *>(choices_node))) {
    } else if (OB_ISNULL(choice_node = choices_array->get_value(0))
               || choice_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("choice_obj is null or invalid", K(ret));
    } else if (OB_FALSE_IT(choice_obj = static_cast<ObJsonObject *>(choice_node))) {
    } else if (OB_ISNULL(message_node = choice_obj->get_value("message"))
               || message_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("message_obj is null or invalid", K(ret));
    } else if (OB_FALSE_IT(message_obj = static_cast<ObJsonObject *>(message_node))) {
    } else if (OB_ISNULL(content_base = message_obj->get_value("content"))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("content_base is null", K(ret));
    } else if (content_base->json_type() == ObJsonNodeType::J_ARRAY) {
      ObJsonObject *content_obj = nullptr;
      ObJsonString *text_str = nullptr;
      ObJsonNode *content_node = nullptr;
      ObJsonNode *text_node = nullptr;
      content_array = static_cast<ObJsonArray *>(content_base);
      if (OB_ISNULL(content_node = content_array->get_value(0))
          || content_node->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
        LOG_WARN("content_obj is null or invalid", K(ret));
      } else if (OB_FALSE_IT(content_obj = static_cast<ObJsonObject *>(content_node))) {
      } else if (OB_ISNULL(text_node = content_obj->get_value("text"))
                 || text_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_DATA;
        LOG_WARN("text_str is null or invalid", K(ret));
      } else if (OB_FALSE_IT(text_str = static_cast<ObJsonString *>(text_node))) {
      } else {
        result = text_str;
      }
    } else if (content_base->json_type() == ObJsonNodeType::J_STRING) {
      content_str = static_cast<ObJsonString *>(content_base);
      result = content_str;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("content is neither array nor string", K(ret));
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
                                                 common::ObString input_type,
                                                 common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or contents is empty", K(ret));
  } else if (input_type.case_compare("image") == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ObDashscopeEmbed supports text only; use a VL model (name contains 'vl' or 'flash') for image", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ai_embed, image input requires a VL model (model name containing 'vl' or 'flash')");
  } else {
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
      } else if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeEmbed::get_body(common::ObIAllocator &allocator,
                                                 common::ObString &model,
                                                 common::ObArray<ObString> &contents,
                                                 common::ObJsonObject *config,
                                                 common::ObArray<ObString> &input_type_array,
                                                 common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or contents is empty", K(ret));
  } else if (input_type_array.count() != contents.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input_type_array count is not equal to contents count", K(ret),
             K(input_type_array.count()), K(contents.count()));
  } else {
    bool all_text = true;
    for (int64_t i = 0; i < input_type_array.count(); ++i) {
      if (input_type_array.at(i).case_compare("text") != 0) {
        all_text = false;
        break;
      }
    }
    if (!all_text) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ObDashscopeEmbed supports text only; use a VL model for image or mixed input", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ai_embed, image or mixed input requires a VL model (model name containing 'vl' or 'flash')");
    } else {
      ObString text_input_type("text");
      if (OB_FAIL(get_body(allocator, model, contents, config, text_input_type, body))) {
        LOG_WARN("Failed to get body", K(ret));
      }
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
      ret = OB_INVALID_DATA;
      LOG_WARN("output_obj is null", K(ret));
    } else if (OB_ISNULL(embeddings_array = static_cast<ObJsonArray *>(output_obj->get_value("embeddings")))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("embeddings_array is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < embeddings_array->element_count(); ++i) {
        if (OB_ISNULL(embedding_obj = static_cast<ObJsonObject *>(embeddings_array->get_value(i)))) {
          ret = OB_INVALID_DATA;
          LOG_WARN("embedding_obj is null", K(ret));
        } else if (OB_ISNULL(embedding_array = static_cast<ObJsonArray *>(embedding_obj->get_value("embedding")))) {
          ret = OB_INVALID_DATA;
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

int ObDashscopeUtils::ObDashscopeVLEmbed::get_header(common::ObIAllocator &allocator,
                                                     common::ObString &api_key,
                                                     common::ObArray<ObString> &headers)
{
  return ObDashscopeUtils::get_header(allocator, api_key, headers);
}

int ObDashscopeUtils::ObDashscopeVLEmbed::get_body(common::ObIAllocator &allocator,
                                                   common::ObString &model,
                                                   common::ObArray<ObString> &contents,
                                                   common::ObJsonObject *config,
                                                   common::ObString input_type,
                                                   common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or contents is empty", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    ObJsonArray *contents_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, input_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, contents_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else {
      bool is_image = (input_type.case_compare("image") == 0);
      const char *content_key = is_image ? "image" : "text";
      for (int64_t i = 0; OB_SUCC(ret) && i < contents.count(); ++i) {
        ObJsonObject *item_obj = nullptr;
        ObJsonString *content_str = nullptr;
        ObString content = contents.at(i);
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, item_obj))) {
          LOG_WARN("Failed to get json object", K(ret));
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, content_str))) {
          LOG_WARN("Failed to get json string", K(ret));
        } else if (OB_FAIL(item_obj->add(content_key, content_str))) {
          LOG_WARN("Failed to add content item", K(ret));
        } else if (OB_FAIL(contents_array->append(item_obj))) {
          LOG_WARN("Failed to append content item", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(input_obj->add("contents", contents_array))) {
        LOG_WARN("Failed to add contents", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(body_obj->add("input", input_obj))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (config != nullptr && config->element_count() > 0) {
      ObJsonNode *dimensions_node = config->get_value("dimensions");
      if (OB_ISNULL(dimensions_node)) {
        // do nothing
      } else if (OB_FAIL(config->rename_key("dimensions", "dimension"))) {
        LOG_WARN("Failed to rename key", K(ret));
      } else if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeVLEmbed::get_body(common::ObIAllocator &allocator,
                                                   common::ObString &model,
                                                   common::ObArray<ObString> &contents,
                                                   common::ObJsonObject *config,
                                                   common::ObArray<ObString> &input_type_array,
                                                   common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or contents is empty", K(ret));
  } else if (input_type_array.count() != contents.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input_type_array count is not equal to contents count", K(ret),
             K(input_type_array.count()), K(contents.count()));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    ObJsonArray *contents_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, input_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, contents_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < contents.count(); ++i) {
        ObJsonObject *item_obj = nullptr;
        ObJsonString *content_str = nullptr;
        ObString content = contents.at(i);
        bool is_image = (input_type_array.at(i).case_compare("image") == 0);
        const char *content_key = is_image ? "image" : "text";
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, item_obj))) {
          LOG_WARN("Failed to get json object", K(ret));
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, content_str))) {
          LOG_WARN("Failed to get json string", K(ret));
        } else if (OB_FAIL(item_obj->add(content_key, content_str))) {
          LOG_WARN("Failed to add content item", K(ret));
        } else if (OB_FAIL(contents_array->append(item_obj))) {
          LOG_WARN("Failed to append content item", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(input_obj->add("contents", contents_array))) {
        LOG_WARN("Failed to add contents", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(body_obj->add("input", input_obj))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (OB_SUCC(ret) && config != nullptr && config->element_count() > 0) {
      ObJsonNode *dimensions_node = config->get_value("dimensions");
      if (OB_ISNULL(dimensions_node)) {
        // do nothing
      } else if (OB_FAIL(config->rename_key("dimensions", "dimension"))) {
        LOG_WARN("Failed to rename key", K(ret));
      } else if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeVLEmbed::parse_output(common::ObIAllocator &allocator,
                                                        common::ObJsonObject *http_response,
                                                        common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObJsonObject *output_obj = nullptr;
    ObJsonArray *embeddings_array = nullptr;
    ObJsonObject *embedding_obj = nullptr;
    ObJsonArray *embedding_array = nullptr;
    ObJsonArray *result_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("output_obj is null", K(ret));
    } else if (OB_ISNULL(embeddings_array = static_cast<ObJsonArray *>(output_obj->get_value("embeddings")))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("embeddings_array is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < embeddings_array->element_count(); ++i) {
        if (OB_ISNULL(embedding_obj = static_cast<ObJsonObject *>(embeddings_array->get_value(i)))) {
          ret = OB_INVALID_DATA;
          LOG_WARN("embedding_obj is null", K(ret));
        } else if (OB_ISNULL(embedding_array = static_cast<ObJsonArray *>(embedding_obj->get_value("embedding")))) {
          ret = OB_INVALID_DATA;
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
    ret = OB_INVALID_DATA;
    LOG_WARN("output_obj is null", K(ret));
  } else if (OB_ISNULL(results_array = static_cast<ObJsonArray *>(output_obj->get_value("results")))) {
    ret = OB_INVALID_DATA;
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
    ret = OB_INVALID_DATA;
    LOG_WARN("results_array is null", K(ret));
  } else {
    result = results_array;
  }
  return ret;
}


int ObAIFuncUtils::get_header(ObIAllocator &allocator,
                              const ObAIFuncExprInfo &info,
                              const ObAiModelEndpointInfo &endpoint_info,
                              ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  ObString unencrypted_access_key;
  if (OB_FAIL(endpoint_info.get_unencrypted_access_key(allocator, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (ObAIFuncUtils::is_completion_type(&info)) {
    ObAIFuncIComplete *complete_provider = nullptr;
    if (OB_FAIL(get_complete_provider(allocator, endpoint_info.get_provider(), complete_provider))) {
      LOG_WARN("Failed to get complete provider", K(ret));
    } else if (OB_FAIL(complete_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("Failed to get header from complete provider", K(ret));
    }
  } else if (ObAIFuncUtils::is_dense_embedding_type(&info)) {
    ObAIFuncIEmbed *embed_provider = nullptr;
    ObString request_model_name = info.model_;
    if (!endpoint_info.get_request_model_name().empty()) {
      request_model_name = endpoint_info.get_request_model_name();
    }
    bool is_multi_model = ObAIFuncUtils::is_multi_model(request_model_name);
    if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider, is_multi_model))) {
      LOG_WARN("Failed to get embed provider", K(ret));
    } else if (OB_FAIL(embed_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("Failed to get header from embed provider", K(ret));
    }
  } else if (ObAIFuncUtils::is_rerank_type(&info)) {
    ObAIFuncIRerank *rerank_provider = nullptr;
    if (OB_FAIL(get_rerank_provider(allocator, endpoint_info.get_provider(), rerank_provider))) {
      LOG_WARN("Failed to get rerank provider", K(ret));
    } else if (OB_FAIL(rerank_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("Failed to get header from rerank provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::get_complete_body(ObIAllocator &allocator,
                                    const ObAIFuncExprInfo &info,
                                    const ObAiModelEndpointInfo &endpoint_info,
                                    ObString &prompt,
                                    ObString &content,
                                    ObJsonObject *config,
                                    ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }

  ObAIFuncIComplete *complete_provider = nullptr;
  if (OB_FAIL(get_complete_provider(allocator, endpoint_info.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->get_body(allocator, request_model_name, prompt, content, config, body))) {
    LOG_WARN("Failed to get body from complete provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::set_json_format_config(ObIAllocator &allocator, const ObString &provider, ObJsonObject *config)
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
                                  const ObAIFuncExprInfo &info,
                                  const ObAiModelEndpointInfo &endpoint_info,
                                  ObArray<ObString> &contents,
                                  ObJsonObject *config,
                                  ObString input_type,
                                  ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }

  ObAIFuncIEmbed *embed_provider = nullptr;
  if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider, is_multi_model(request_model_name)))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->get_body(allocator, request_model_name, contents, config, input_type, body))) {
    LOG_WARN("Failed to get body from embed provider", K(ret));
  }
  return ret;
}

// Overload for mixed content types (text/image).
int ObAIFuncUtils::get_embed_body(ObIAllocator &allocator,
                                  const ObAIFuncExprInfo &info,
                                  const ObAiModelEndpointInfo &endpoint_info,
                                  ObArray<ObString> &contents,
                                  ObJsonObject *config,
                                  ObArray<ObString> &input_type_array,
                                  ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }
  bool is_multi_model = ObAIFuncUtils::is_multi_model(request_model_name);
  ObAIFuncIEmbed *embed_provider = nullptr;
  if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider, is_multi_model))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->get_body(allocator, request_model_name, contents, config, input_type_array, body))) {
    LOG_WARN("Failed to get body from embed provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_rerank_body(ObIAllocator &allocator,
                                   const ObAIFuncExprInfo &info,
                                   const ObAiModelEndpointInfo &endpoint_info,
                                   ObString &query,
                                   ObJsonArray *document_array,
                                   ObJsonObject *config,
                                   ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }

  ObAIFuncIRerank *rerank_provider = nullptr;
  if (OB_FAIL(get_rerank_provider(allocator, endpoint_info.get_provider(), rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(rerank_provider->get_body(allocator, request_model_name, query, document_array, config, body))) {
    LOG_WARN("Failed to get body from rerank provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::parse_complete_output(ObIAllocator &allocator,
                                        const ObAiModelEndpointInfo &endpoint_info,
                                        ObJsonObject *http_response,
                                        ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObAIFuncIComplete *complete_provider = nullptr;
  if (OB_FAIL(get_complete_provider(allocator, endpoint_info.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->parse_output(allocator, http_response, result))) {
    LOG_WARN("Failed to parse output from complete provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::parse_embed_output(ObIAllocator &allocator,
                                      const ObAiModelEndpointInfo &endpoint_info,
                                      ObJsonObject *http_response,
                                      ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObAIFuncIEmbed *embed_provider = nullptr;
  if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->parse_output(allocator, http_response, result))) {
    LOG_WARN("Failed to parse output from embed provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::parse_rerank_output(ObIAllocator &allocator,
                                       const ObAiModelEndpointInfo &endpoint_info,
                                       ObJsonObject *http_response,
                                       ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObAIFuncIRerank *rerank_provider = nullptr;
  if (OB_FAIL(get_rerank_provider(allocator, endpoint_info.get_provider(), rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(rerank_provider->parse_output(allocator, http_response, result))) {
    LOG_WARN("Failed to parse output from rerank provider", K(ret));
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

int ObAIFuncJsonUtils::get_json_object_form_str(ObIAllocator &allocator, const ObString &str, ObJsonObject *&obj_node)
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

int ObAIFuncUtils::get_complete_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIComplete *&complete_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, provider is empty");
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
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current is");
  }
  if (OB_SUCC(ret) && OB_ISNULL(complete_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for complete_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_vl_complete_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIVLComplete *&complete_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI)
      || ob_provider_check(provider, ObAIFuncProviderUtils::ALIYUN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::DEEPSEEK)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)
      || ob_provider_check(provider, ObAIFuncProviderUtils::HUNYUAN)) {
    complete_provider = OB_NEWx(ObOpenAIUtils::ObOpenAIVLComplete, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    complete_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeVLComplete, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current is");
  }
  if (OB_SUCC(ret) && OB_ISNULL(complete_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for vl complete_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_embed_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIEmbed *&embed_provider)
{
  return get_embed_provider(allocator, provider, embed_provider, false);
}

int ObAIFuncUtils::get_embed_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIEmbed *&embed_provider,
                                      bool is_multi_model)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI)
      || ob_provider_check(provider, ObAIFuncProviderUtils::ALIYUN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::HUNYUAN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)) {
    embed_provider = OB_NEWx(ObOpenAIUtils::ObOpenAIEmbed, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    if (is_multi_model) {
      embed_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeVLEmbed, &allocator);
    } else {
      embed_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeEmbed, &allocator);
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current is");
  }
  if (OB_SUCC(ret) && OB_ISNULL(embed_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for embed_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_rerank_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIRerank *&rerank_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)) {
    rerank_provider = OB_NEWx(ObSiliconflowUtils::ObSiliconflowRerank, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    rerank_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeRerank, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "rerank support siliconflow and ailiyun-dashscope, this provider current is");
  }
  if (OB_SUCC(ret) && OB_ISNULL(rerank_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for rerank_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_completion(const ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, info is null");
  } else if (!is_completion_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, info type is not completion");
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_dense_embedding(const ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, info is null");
  } else if (!is_dense_embedding_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, info type is not dense embedding");
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_rerank(const ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, info is null");
  } else if (!is_rerank_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not rerank", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, info type is not rerank");
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

int ObAIFuncUtils::get_ai_func_info(ObIAllocator &allocator, const ObString &model_id,
                                    share::schema::ObSchemaGetterGuard &guard,
                                    ObAIFuncExprInfo *&info)
{
  int ret = OB_SUCCESS;
  if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model_id is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, model_id is empty");
  } else {
    ObAIFuncExprInfo *info_obj = OB_NEWx(ObAIFuncExprInfo, (&allocator), allocator, T_FUN_SYS_AI_COMPLETE);
    if (OB_ISNULL(info_obj)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for info_obj", K(ret));
    } else if (OB_FAIL(info_obj->init(allocator, model_id, guard))) {
      LOG_WARN("Failed to init info_obj", K(ret));
    } else {
      info = info_obj;
    }
  }
  return ret;
}

bool ObAIFuncUtils::is_provider_support_base64(const ObString &provider)
{
  return ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW);
}

// TODO: a formal configuration will be used later to determine whether it is a multimodal model
bool ObAIFuncUtils::is_multi_model(const ObString &model_name)
{
  bool bret = false;
  if (model_name.empty()) {
    bret = false;
  } else {
    // 1. Exact match (case-insensitive) against known multi-model names
    static const char *const MULTI_MODEL_NAMES[] = {
      "qwen3-vl-embedding",
      "qwen2.5-vl-embedding",
      "tongyi-embedding-vision-plus",
      "tongyi-embedding-vision-flash",
      "multimodal-embedding-v1",
    };
    static const int64_t NUM_MULTI_MODEL_NAMES =
        static_cast<int64_t>(sizeof(MULTI_MODEL_NAMES) / sizeof(MULTI_MODEL_NAMES[0]));
    for (int64_t k = 0; !bret && k < NUM_MULTI_MODEL_NAMES; ++k) {
      if (model_name.case_compare_equal(MULTI_MODEL_NAMES[k])) {
        bret = true;
      }
    }
    // 2. If not in list, treat as multi-model if name contains "vl", "vision", "multimodal"
    if (!bret) {
      const char *ptr = model_name.ptr();
      const int64_t len = model_name.length();
      if (OB_NOT_NULL(ptr) && len >= 2) {
        static const char vl[] = "vl";
        static const char vision[] = "vision";
        static const char multimodal[] = "multimodal";
        const int64_t vl_len = 2;
        const int64_t vision_len = 6;
        const int64_t multimodal_len = 10;
        for (int64_t i = 0; !bret && i <= len - vl_len; ++i) {
          if (ObString(vl_len, ptr + i).case_compare_equal(ObString(vl_len, vl))) {
            bret = true;
          }
        }
        for (int64_t i = 0; !bret && i <= len - vision_len; ++i) {
          if (ObString(vision_len, ptr + i).case_compare_equal(ObString(vision_len, vision))) {
            bret = true;
          }
        }
        for (int64_t i = 0; !bret && i <= len - multimodal_len; ++i) {
          if (ObString(multimodal_len, ptr + i).case_compare_equal(ObString(multimodal_len, multimodal))) {
            bret = true;
          }
        }
      }
    }
  }
  return bret;
}

bool ObAIFuncUtils::vl_model_needs_individual_requests(const ObString &model_name)
{
  // DashScope qwen2.x-vl models reject same-type batching:
  // "Duplicate input type, Each type can appear at most once."
  // qwen3+ VL models support batching multiple items of the same type.
  bool bret = false;
  if (!model_name.empty()) {
    const char *ptr = model_name.ptr();
    const int64_t len = model_name.length();
    static const char qwen2_pattern[] = "qwen2.";
    const int64_t pattern_len = static_cast<int64_t>(sizeof(qwen2_pattern) - 1);
    if (OB_NOT_NULL(ptr) && len >= pattern_len) {
      for (int64_t i = 0; !bret && i <= len - pattern_len; ++i) {
        if (ObString(pattern_len, ptr + i).case_compare_equal(ObString(pattern_len, qwen2_pattern))) {
          bret = true;
        }
      }
    }
  }
  return bret;
}

int ObAIFuncUtils::merge_message_parameters_to_config(ObIAllocator &allocator,
                                                      ObJsonObject *message_parameters,
                                                      ObJsonObject *config,
                                                      ObJsonObject *&merged_config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    // user has no config, use endpoint message_parameters as-is
    merged_config = message_parameters;
  } else if (OB_ISNULL(message_parameters)) {
    // endpoint has no message_parameters, use user config only
    merged_config = config;
  } else {
    // Both present: clone message_parameters to avoid mutating the template,
    // then merge user config into the clone (user config overrides on same key)
    ObJsonNode *cloned_node = message_parameters->clone(&allocator, true /*is_deep_copy*/);
    if (OB_ISNULL(cloned_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to clone message_parameters", K(ret));
    } else {
      ObJsonObject *cloned_mp = static_cast<ObJsonObject *>(cloned_node);
      if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, cloned_mp))) {
        LOG_WARN("fail to merge user config into message_parameters", K(ret));
      } else {
        merged_config = cloned_mp;
      }
    }
  }
  return ret;
}

int ObAIFuncUtils::set_default_enable_thinking(ObIAllocator &allocator, ObJsonObject *&config)
{
  int ret = OB_SUCCESS;
  const ObString enable_thinking_key("enable_thinking");
  if (OB_ISNULL(config)) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
      LOG_WARN("fail to get json object", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(config) && OB_ISNULL(config->get_value(enable_thinking_key))) {
    ObJsonBoolean *enable_thinking_val = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_boolean(allocator, false, enable_thinking_val))) {
      LOG_WARN("fail to get json boolean", K(ret));
    } else if (OB_FAIL(config->add(enable_thinking_key, enable_thinking_val))) {
      LOG_WARN("fail to add enable_thinking", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::decode_base64_embedding_array(const ObIJsonBase &embedding_jbase,
                                                 ObIAllocator &allocator,
                                                 const int64_t dimension,
                                                 float *&vector)
{
  int ret = OB_SUCCESS;
  if (embedding_jbase.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("embedding_jbase is not a string", K(ret));
  } else {
    const char *encoded_embedding = embedding_jbase.get_data();
    uint64_t encoded_embedding_len = embedding_jbase.get_data_length();
    uint64_t decoded_buf_len = ObBase64Encoder::needed_decoded_length(encoded_embedding_len);
    uint8_t *decoded_buf = nullptr;
    int64_t pos = 0;
    if (decoded_buf_len <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("decoded_buf_len is not valid", K(ret), K(decoded_buf_len));
    } else if (OB_ISNULL(decoded_buf = static_cast<uint8_t *>(allocator.alloc(decoded_buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(decoded_buf_len));
    } else if (OB_FAIL(ObBase64Encoder::decode(encoded_embedding, encoded_embedding_len,
                                               decoded_buf, decoded_buf_len, pos))) {
      LOG_WARN("failed to decode encoded_embedding", K(ret));
    } else if (pos != dimension * sizeof(float)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decode result length mismatch", K(ret), K(pos), K(dimension));
    } else {
      vector = reinterpret_cast<float *>(decoded_buf);
    }
  }
  return ret;
}

int ObAIFuncUtils::decode_float_embedding_array(const ObIJsonBase &embedding_jbase,
                                                ObIAllocator &allocator,
                                                ObJsonReaderHelper &json_reader,
                                                const int64_t dimension,
                                                float *&vector)
{
  int ret = OB_SUCCESS;
  float *tmp_vector = nullptr;
  if (!ObJsonHelper::is_array_type(&embedding_jbase)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("embedding field is not an array", K(ret));
  } else {
    uint64_t embedding_size = json_reader.get_array_size(&embedding_jbase);
    if (embedding_size != dimension) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("embedding size mismatch", K(ret), K(embedding_size), K(dimension));
    } else {
      tmp_vector = static_cast<float*>(allocator.alloc(dimension * sizeof(float)));
      if (OB_ISNULL(tmp_vector)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        for (uint64_t i = 0; i < dimension && OB_SUCC(ret); i++) {
          ObIJsonBase *value = nullptr;
          if (OB_FAIL(json_reader.get_array_element(&embedding_jbase, i, value))) {
            LOG_WARN("failed to get array element", K(ret), K(i));
          } else if (!ObJsonHelper::is_number_type(value)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("value is not a number", K(ret), K(i));
          } else {
            float f_value = 0.0;
            if (OB_FAIL(json_reader.get_float_value(value, f_value))) {
              LOG_WARN("failed to get float value", K(ret), K(i));
            } else {
              tmp_vector[i] = f_value;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    vector = tmp_vector;
  }
  return ret;
}

int ObAIFuncUtils::get_ai_func_info(ObIAllocator &allocator, const ObString &model_id, ObAIFuncExprInfo *&info)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_ai_func_info(allocator, model_id, guard, info))) {
    LOG_WARN("Failed to init info_obj", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_model_config_info(ObIAllocator &allocator, const ObString &model_key, ObAIModelConfigInfo &config)
{
  int ret = OB_SUCCESS;
  config.reset();
  if (model_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model_key is empty", K(ret));
  } else {
    schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
    schema::ObSchemaGetterGuard guard;
    uint64_t tenant_id = MTL_ID();
    const ObAiModelSchema *ai_model_schema = nullptr;
    // get ai model schema
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret));
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(guard.get_ai_model_schema(tenant_id, model_key, ai_model_schema))) {
      LOG_WARN("fail to get ai model schema", KR(ret), K(tenant_id), K(model_key));
    } else if (OB_ISNULL(ai_model_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ai model schema is null", KR(ret), K(tenant_id), K(model_key));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_function, ai model not found, please check if the model exists");
    }

    // get endpoint info
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_key, endpoint_info))) {
      LOG_WARN("failed to get endpoint info", K(ret), K(model_key));
    } else if (OB_ISNULL(endpoint_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("endpoint info is null", K(ret), K(model_key));
    }

    // init config
    if (OB_SUCC(ret)) {
      if (OB_FAIL(config.init(allocator, *ai_model_schema, *endpoint_info))) {
        LOG_WARN("failed to init config", K(ret));
      }
    }

    // get default config from info manager
    if (OB_SUCC(ret)) {
      share::ObAIModelConfigItem default_config;
      if (OB_FAIL(ObAIModelConfigInfoManager::get_instance().get_model_config(config.get_provider(),
                                                                              config.get_request_model_name(),
                                                                              default_config))) {
        LOG_WARN("failed to get default config", K(ret));
      } else if (OB_FAIL(config.merge_default_config(allocator, default_config))) {
        LOG_WARN("failed to merge default config", K(ret));
      }
    }
  }
  return ret;
}

int ObAIFuncUtils::check_url_security(const ObString &url)
{
  int ret = OB_SUCCESS;

  // Extract hostname from URL for security check
  // URL format: http://hostname:port/path or https://hostname:port/path
  ObString url_str = url;
  int64_t pos = 0;

  // Skip protocol prefix
  if (url_str.prefix_match_ci("https://")) {
    pos = 8;
  } else if (url_str.prefix_match_ci("http://")) {
    pos = 7;
  }

  if (pos > 0 && pos < url_str.length()) {
    // Find hostname (up to '/' or ':' or end)
    int64_t host_start = pos;
    int64_t host_end = url_str.length();
    for (int64_t i = pos; i < url_str.length(); ++i) {
      char c = url_str[i];
      if (c == '/' || c == ':' || c == '?' || c == '#') {
        host_end = i;
        break;
      }
    }

    if (host_start < host_end) {
      ObString hostname;
      hostname.assign_ptr(url_str.ptr() + host_start, static_cast<int32_t>(host_end - host_start));

      // Check for localhost variants
      if (hostname.case_compare("localhost") == 0 ||
          hostname.case_compare("localhost.localdomain") == 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("localhost is not allowed for security reason", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, localhost URL is not allowed");
      }
      // Check for IP addresses
      else if (OB_SUCC(ret)) {
        // Check for IPv4 loopback (127.x.x.x)
        if (hostname.prefix_match_ci("127.")) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("loopback IP is not allowed for security reason", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, loopback IP URL is not allowed");
        }
        // Check for IPv4 private ranges: 10.x.x.x, 192.168.x.x, 172.16-31.x.x
        else if (hostname.prefix_match_ci("10.")) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("private IP is not allowed for security reason", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, private IP URL is not allowed");
        }
        else if (hostname.prefix_match_ci("192.168.")) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("private IP is not allowed for security reason", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, private IP URL is not allowed");
        }
        else if (hostname.prefix_match_ci("172.")) {
          // Check 172.16-31.x.x range (private 172.16.0.0/12)
          if (hostname.length() > 4) {
            const char *p = hostname.ptr() + 4;
            const int64_t rest_len = hostname.length() - 4;
            int64_t octet_end = 0;
            while (octet_end < rest_len && p[octet_end] != '.') {
              ++octet_end;
            }
            int second_octet = 0;
            for (int64_t i = 0; i < octet_end && p[i] >= '0' && p[i] <= '9'; ++i) {
              second_octet = second_octet * 10 + (p[i] - '0');
            }
            if (second_octet >= 16 && second_octet <= 31) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("private IP is not allowed for security reason", K(ret));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, private IP URL is not allowed");
            }
          }
        }
        // Check for cloud metadata endpoints
        else if (hostname.contains("metadata.google.internal") ||
                 hostname.contains("169.254.169.254") ||  // AWS/GCP/Azure metadata
                 hostname.contains("metadata.azure.com") ||
                 hostname.contains("kubernetes.default.svc")) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("cloud metadata endpoint is not allowed for security reason", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, cloud metadata endpoint URL is not allowed");
        }
      }
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
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_completion_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, info type is not completion");
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(*allocator_, endpoint_info_.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(complete_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(complete_provider->get_body(*allocator_, request_model_name, prompt_str, prompt, config, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, endpoint_info_.get_url(), headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(complete_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(*allocator_, result_base, result_str))) {
    LOG_WARN("Failed to print json to string", K(ret));
  } else {
    result = result_str;
  }
  if (ret == OB_INVALID_DATA) {
    ObString response_str;
    if (OB_SUCCESS == ObAIFuncJsonUtils::print_json_to_str(*allocator_, response, response_str)) {
      char http_message_str[1024];
      snprintf(http_message_str, sizeof(http_message_str), "unexpected http message: %s", response_str.ptr());
      ObString ob_http_message_str(http_message_str);
      LOG_WARN("unexpected http message", K(ret), K(ob_http_message_str));
      FORWARD_USER_ERROR(ret, ob_http_message_str.ptr());
    } else {
      LOG_WARN("unexpected http message", K(ret));
      FORWARD_USER_ERROR(ret, "unexpected http message");
    }
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
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_completion_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, info type is not completion");
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(*allocator_, endpoint_info_.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(complete_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < prompts.count(); i++) {
      ObString prompt = prompts[i];
      if (OB_FAIL(complete_provider->get_body(*allocator_, request_model_name, prompt_str, prompt, config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(body_array.push_back(body))) {
        LOG_WARN("Failed to append body", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(client.send_post_batch(*allocator_, endpoint_info_.get_url(), headers, body_array, response_array))) {
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
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, input is empty");
  } else if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, info type is not dense embedding");
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
  ObString request_model_name = get_request_model_name();
  ObString unencrypted_access_key;
  bool is_multi_model = ObAIFuncUtils::is_multi_model(request_model_name);
  if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, info type is not dense embedding");
  } else if (OB_FAIL(ObAIFuncUtils::get_embed_provider(*allocator_, endpoint_info_.get_provider(), embed_provider, is_multi_model))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(embed_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < contents.count(); i++) {
      ObString content = contents[i];
      ObArray<ObString> content_array;
      if (OB_FAIL(content_array.push_back(content))) {
        LOG_WARN("Failed to push back content", K(ret));
      } else if (OB_FAIL(embed_provider->get_body(*allocator_, request_model_name, content_array, config, input_type_, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(body_array.push_back(body))) {
        LOG_WARN("Failed to append body", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(client.send_post_batch(*allocator_, endpoint_info_.get_url(), headers, body_array, response_array))) {
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
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  bool is_multi_model = ObAIFuncUtils::is_multi_model(request_model_name);
  int64_t dimension = 0;
  if (OB_NOT_NULL(config)) {
    ObJsonNode *dimension_node = config->get_value("dimensions");
    if (OB_ISNULL(dimension_node)) {
      // do nothing
    } else if (dimension_node->json_type() != ObJsonNodeType::J_INT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dimension is not int", K(ret));
    } else {
      dimension = static_cast<ObJsonInt *>(dimension_node)->get_int();
    }
  }

  if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, info type is not dense embedding");
  } else if (OB_FAIL(ObAIFuncUtils::get_embed_provider(*allocator_, endpoint_info_.get_provider(), embed_provider, is_multi_model))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(embed_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(embed_provider->get_body(*allocator_, request_model_name, content, config, input_type_, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, endpoint_info_.get_url(), headers, body, response))) {
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
        } else if (dimension > 0 && static_cast<ObJsonArray *>(j_base)->element_count() != dimension) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("result array is not equal to dimension", K(ret), K(dimension), K(static_cast<ObJsonArray *>(j_base)->element_count()));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, result dimension is not equal to dimension");
        } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(*allocator_, j_base, result_str))) {
          LOG_WARN("Failed to print json to string", K(ret));
        } else {
          results.push_back(result_str);
        }
      }
    }
  }
  if (ret == OB_INVALID_DATA) {
    ObString response_str;
    if (OB_SUCCESS == ObAIFuncJsonUtils::print_json_to_str(*allocator_, response, response_str)) {
      char http_message_str[1024];
      snprintf(http_message_str, sizeof(http_message_str), "unexpected http message: %s", response_str.ptr());
      ObString ob_http_message_str(http_message_str);
      LOG_WARN("unexpected http message", K(ret), K(ob_http_message_str));
      FORWARD_USER_ERROR(ret, ob_http_message_str.ptr());
    } else {
      LOG_WARN("unexpected http message", K(ret));
      FORWARD_USER_ERROR(ret, "unexpected http message");
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
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_rerank_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not rerank", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, info type is not rerank");
  } else if (OB_FAIL(ObAIFuncUtils::get_rerank_provider(*allocator_, endpoint_info_.get_provider(), rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(rerank_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(rerank_provider->get_body(*allocator_, request_model_name, query, contents, nullptr, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, endpoint_info_.get_url(), headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(rerank_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else {
    results = static_cast<ObJsonArray *>(result_base);
  }

  if (ret == OB_INVALID_DATA) {
    ObString response_str;
    if (OB_SUCCESS == ObAIFuncJsonUtils::print_json_to_str(*allocator_, response, response_str)) {
      char http_message_str[1024];
      snprintf(http_message_str, sizeof(http_message_str), "unexpected http message: %s", response_str.ptr());
      ObString ob_http_message_str(http_message_str);
      LOG_WARN("unexpected http message", K(ret), K(ob_http_message_str));
      FORWARD_USER_ERROR(ret, ob_http_message_str.ptr());
    } else {
      LOG_WARN("unexpected http message", K(ret));
      FORWARD_USER_ERROR(ret, "unexpected http message");
    }
  }
  return ret;
}

int ObAIServiceClient::init(const ObString &model_key)
{
  int ret = OB_SUCCESS;
  share::ObAIModelConfigInfo *config = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("model already initialized", K(ret));
  } else if (OB_ISNULL(config = OB_NEWx(share::ObAIModelConfigInfo, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate model config info", K(ret));
  } else if (OB_FAIL(ObAIFuncUtils::get_model_config_info(allocator_, model_key, *config))) {
    LOG_WARN("fail to get model config info", K(ret));
  } else {
    model_config_info_ = config;
    is_inited_ = true;
  }
  return ret;
}

int ObAIServiceClient::init(share::ObAIModelConfigInfo *model_config_info)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("model already initialized", K(ret));
  } else if (OB_ISNULL(model_config_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model config info is null", K(ret));
  } else {
    model_config_info_ = model_config_info;
    is_inited_ = true;
  }
  return ret;
}

int ObAIServiceClient::merge_message_parameters_to_config(ObIAllocator &allocator,
                                                      ObJsonObject *message_parameters,
                                                      ObJsonObject *config,
                                                      ObJsonObject *&merged_config)
{
  return ObAIFuncUtils::merge_message_parameters_to_config(allocator, message_parameters, config, merged_config);
}

int ObAIServiceClient::call_completion(ObString &prompt,
                                   ObJsonObject *prompt_object,
                                   ObJsonObject *config,
                                   ObString &result)
{
  int ret = OB_SUCCESS;
  bool is_vl = false;
  bool is_completion_type = false;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObString result_str;
  ObAIFuncClient client;
  ObAIFuncIComplete *complete_provider = nullptr;
  ObAIFuncIVLComplete *vl_complete_provider = nullptr;
  ObString system_prompt;
  ObString api_key;
  ObString model_name;
  ObString url;
  ObString provider;
  ObJsonObject *merged_config = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("model not initialized", K(ret));
  } else {
    is_vl = OB_NOT_NULL(prompt_object);
    is_completion_type = model_config_info_->get_model_type() == share::EndpointType::COMPLETION;
    api_key = model_config_info_->get_api_key();
    model_name = model_config_info_->get_request_model_name();
    url = model_config_info_->get_url();
    provider = model_config_info_->get_provider();
    if (!is_completion_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("info type is not completion", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, info type is not completion");
    } else if (OB_FAIL(merge_message_parameters_to_config(allocator_, model_config_info_->get_message_parameters(), config, merged_config))) {
      LOG_WARN("Failed to merge message parameters to config", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::set_default_enable_thinking(allocator_, merged_config))) {
      LOG_WARN("fail to set default enable_thinking", K(ret));
    } else if (is_vl) {
      if (OB_FAIL(ObAIFuncUtils::get_vl_complete_provider(allocator_, provider, vl_complete_provider))) {
        LOG_WARN("Failed to get vl complete provider", K(ret));
      } else if (OB_FAIL(vl_complete_provider->get_header(allocator_, api_key, headers))) {
        LOG_WARN("Failed to get header", K(ret));
      } else if (OB_FAIL(vl_complete_provider->get_body(allocator_, model_name, prompt, prompt_object, merged_config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(client.send_post(allocator_, url, headers, body, response))) {
        LOG_WARN("Failed to send post", K(ret));
      } else if (OB_FAIL(vl_complete_provider->parse_output(allocator_, response, result_base))) {
        LOG_WARN("Failed to parse output", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator_, result_base, result_str))) {
        LOG_WARN("Failed to print json to string", K(ret));
      } else {
        result = result_str;
      }
    } else {
      // Non-VL path
      if (OB_FAIL(ObAIFuncUtils::get_complete_provider(allocator_, provider, complete_provider))) {
        LOG_WARN("Failed to get complete provider", K(ret));
      } else if (OB_FAIL(complete_provider->get_header(allocator_, api_key, headers))) {
        LOG_WARN("Failed to get header", K(ret));
      } else if (OB_FAIL(complete_provider->get_body(allocator_, model_name, system_prompt, prompt, merged_config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(client.send_post(allocator_, url, headers, body, response))) {
        LOG_WARN("Failed to send post", K(ret));
      } else if (OB_FAIL(complete_provider->parse_output(allocator_, response, result_base))) {
        LOG_WARN("Failed to parse output", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator_, result_base, result_str))) {
        LOG_WARN("Failed to print json to string", K(ret));
      } else {
        result = result_str;
      }
    }
    if (OB_INVALID_DATA == ret) {
      ObString response_str;
      if (OB_SUCCESS == ObAIFuncJsonUtils::print_json_to_str(allocator_, response, response_str)) {
        char http_message_str[1024];
        int64_t copy_len = std::min(static_cast<int64_t>(response_str.length()),
                                    static_cast<int64_t>(sizeof(http_message_str) - 32));
        if (copy_len > 0) {
          snprintf(http_message_str, sizeof(http_message_str), "unexpected http message: %.*s",
                   static_cast<int>(copy_len), response_str.ptr());
        } else {
          snprintf(http_message_str, sizeof(http_message_str), "unexpected http message");
        }
        ObString ob_http_message_str;
        ob_http_message_str.assign_ptr(http_message_str, static_cast<int32_t>(strlen(http_message_str)));
        LOG_WARN("unexpected http message", K(ret), K(ob_http_message_str));
      }
      FORWARD_USER_ERROR(ret, "unexpected http response from AI service");
    }
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

static int encode_base64(ObIAllocator &allocator, const ObString &binary_str, ObString &base64_str)
{
  int ret = OB_SUCCESS;
  if (binary_str.empty()) {
    base64_str.assign_ptr(nullptr, 0);
  } else {
    int64_t out_len = ObBase64Encoder::needed_encoded_length(binary_str.length());
    if (out_len > INT32_MAX) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("base64 encoded length exceeds INT32_MAX", K(ret), K(binary_str.length()), K(out_len));
    } else {
      char *out_buf = static_cast<char *>(allocator.alloc(out_len));
      int64_t pos = 0;
      if (OB_ISNULL(out_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate base64 buf", K(ret), K(out_len));
      } else if (OB_FAIL(ObBase64Encoder::encode(reinterpret_cast<const uint8_t *>(binary_str.ptr()),
                                                 binary_str.length(),
                                                 out_buf,
                                                 out_len,
                                                 pos,
                                                 0 /* no line break */))) {
        LOG_WARN("failed to encode base64", K(ret), K(binary_str.length()));
      } else {
        base64_str.assign_ptr(out_buf, static_cast<int32_t>(pos));
      }
    }
  }
  return ret;
}

int ObAIFuncPromptObjectUtils::parse_template_arg_types(const ObString &template_str,
                                                        const int64_t arg_cnt,
                                                        ObIArray<PromptArgType> &arg_types,
                                                        bool *has_image)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(has_image)) {
    *has_image = false;
  }
  const char *ptr = template_str.ptr();
  const int64_t len = template_str.length();
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    if (ptr[i] != '{') {
      continue;
    }
    bool is_img = false;
    int64_t digit_start = i + 1;
    if (i + 4 < len
        && ptr[i + 1] == 'i'
        && ptr[i + 2] == 'm'
        && ptr[i + 3] == 'g'
        && ptr[i + 4] == '_') {
      is_img = true;
      digit_start = i + 5;
    }
    int64_t j = digit_start;
    int64_t idx = 0;
    bool has_digit = false;
    while (j < len && ptr[j] >= '0' && ptr[j] <= '9') {
      has_digit = true;
      const int64_t digit = ptr[j] - '0';
      if (idx > (INT64_MAX - digit) / 10) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("placeholder index overflow", K(ret), K(template_str));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt: invalid placeholder index");
      } else {
        idx = idx * 10 + digit;
      }
      ++j;
    }
    if (has_digit && j < len && ptr[j] == '}') {
      if (idx < 0 || idx >= arg_cnt) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid placeholder index", K(ret), K(idx), K(arg_cnt), K(template_str));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt: placeholder index out of range");
      } else if (arg_types.at(idx) != PromptArgType::UNKNOWN) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("placeholder index duplicated", K(ret), K(idx), K(template_str));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt: duplicated placeholder index");
      } else {
        arg_types.at(idx) = is_img ? PromptArgType::IMAGE : PromptArgType::TEXT;
        if (is_img && OB_NOT_NULL(has_image)) {
          *has_image = true;
        }
      }
      i = j;
    }
  }
  return ret;
}

int ObAIFuncPromptObjectUtils::build_image_arg_object(ObIAllocator &allocator,
                                                      const ObString &arg_str,
                                                      const ImageArgType image_arg_type,
                                                      ObJsonObject *&image_obj)
{
  int ret = OB_SUCCESS;
  ObJsonObject *obj = nullptr;
  ObJsonString *type_json = nullptr;
  ObString image_type("image");
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, obj))) {
    LOG_WARN("fail to get image object", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, image_type, type_json))) {
    LOG_WARN("fail to get image type", K(ret));
  } else if (OB_FAIL(obj->add("type", type_json))) {
    LOG_WARN("fail to add image type", K(ret));
  } else if (image_arg_type == ImageArgType::URL) {
    ObJsonString *url_json = nullptr;
    ObString url = arg_str;
    if (!url.prefix_match_ci(HTTPS_PREFIX) && !url.prefix_match_ci(HTTP_PREFIX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid image url, should start with http or https", K(ret), K(url));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt image url should start with http or https");
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, url, url_json))) {
      LOG_WARN("fail to get image url", K(ret), K(url));
    } else if (OB_FAIL(obj->add("url", url_json))) {
      LOG_WARN("fail to add image url", K(ret));
    }
  } else if (image_arg_type == ImageArgType::BINARY) {
    ObAiFuncImageUtils::ObImageType image_type = ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN;
    ObString data_base64;
    ObJsonString *format_json = nullptr;
    ObJsonString *data_json = nullptr;
    if (OB_FAIL(ObAiFuncImageUtils::get_type_from_binary(allocator, arg_str, image_type))) {
      LOG_WARN("fail to get image type from binary", K(ret));
    } else {
      const char *type_str = ObAiFuncImageUtils::get_image_type_str(image_type);
      if (OB_ISNULL(type_str)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("fail to get image type string", K(ret), K(image_type));
      } else if (OB_FAIL(encode_base64(allocator, arg_str, data_base64))) {
        LOG_WARN("fail to base64 image binary", K(ret));
      } else {
        ObString format_str(type_str);
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, format_str, format_json))) {
          LOG_WARN("fail to get image format", K(ret));
        } else if (OB_FAIL(obj->add("format", format_json))) {
          LOG_WARN("fail to add image format", K(ret));
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, data_base64, data_json))) {
          LOG_WARN("fail to get image data", K(ret));
        } else if (OB_FAIL(obj->add("data", data_json))) {
          LOG_WARN("fail to add image data", K(ret));
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid image arg type enum", K(ret), K(static_cast<int8_t>(image_arg_type)));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt: invalid image argument type");
  }
  if (OB_SUCC(ret)) {
    image_obj = obj;
  }
  return ret;
}

int ObAIFuncPromptObjectUtils::validate_binary_image_sizes(ObJsonObject *prompt_object,
                                                            const int64_t max_image_size)
{
  int ret = OB_SUCCESS;
  ObJsonNode *args_node = nullptr;
  ObJsonArray *args_array = nullptr;
  if (max_image_size == 0) {
    // no limit, skip validation
  } else if (OB_ISNULL(prompt_object)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prompt_object is null", K(ret));
  } else if (OB_ISNULL(args_node = prompt_object->get_value(prompt_args_key))
             || ObJsonNodeType::J_ARRAY != args_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prompt args type is invalid", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, prompt args type is invalid");
  } else if (OB_FALSE_IT(args_array = static_cast<ObJsonArray *>(args_node))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < args_array->element_count(); ++i) {
      ObJsonNode *arg_node = args_array->get_value(i);
      if (OB_ISNULL(arg_node) || arg_node->json_type() != ObJsonNodeType::J_OBJECT) {
        continue; // text arg or unknown, skip
      }
      ObJsonObject *image_obj = static_cast<ObJsonObject *>(arg_node);
      if (OB_NOT_NULL(image_obj->get_value("url"))) {
        continue; // URL type: skip size check
      }
      ObJsonNode *data_node = image_obj->get_value("data");
      if (OB_ISNULL(data_node) || data_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("binary image arg missing data field", K(ret), K(i));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_complete, binary image arg is invalid");
      } else {
        const int64_t binary_len = static_cast<ObJsonString *>(data_node)->get_str().length();
        if (binary_len > max_image_size) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("binary image size exceeds max_image_size", K(ret), K(i), K(binary_len), K(max_image_size));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "image size exceeds model limit");
        }
      }
    }
  }
  return ret;
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
  ObJsonNode *template_node = NULL;
  ObJsonNode *args_node = NULL;
  ObJsonString *template_json_str = NULL;
  ObJsonArray *args_array = NULL;
  ObString template_str;
  ObString result_str;
  if (OB_ISNULL(prompt_object) ||
      OB_ISNULL(template_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key)) ||
      ObJsonNodeType::J_STRING != template_node->json_type() ||
      OB_ISNULL(args_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key)) ||
      ObJsonNodeType::J_ARRAY != args_node->json_type() ||
      OB_ISNULL(template_json_str = static_cast<ObJsonString *>(template_node)) ||
      OB_ISNULL(args_array = static_cast<ObJsonArray *>(args_node)) ||
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

#define OB_AI_MODEL_CONFIG_REGISTER(ret, manager, provider_literal, model_literal, batch_size, max_image_size) \
  do { \
    if (OB_SUCC(ret)) { \
      share::ObAIModelConfigItem item; \
      item.batch_size_ = (batch_size); \
      item.max_image_size_ = (max_image_size); \
      if (OB_FAIL((manager).set_config(ObString(provider_literal), \
                                       ObString(model_literal), \
                                       item, \
                                       true /* overwrite */))) { \
        LOG_WARN("failed to register default ai model config", K(ret), \
                 K(provider_literal), K(model_literal), K(item)); \
      } \
    } \
  } while (false)

ObAIModelConfigInfoManager::~ObAIModelConfigInfoManager()
{
  reset();
}

int ObAIModelConfigInfoManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = DEFAULT_BUCKET_NUM;
  if (ATOMIC_LOAD(&is_inited_)) {
    // already initialized
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (ATOMIC_LOAD(&is_inited_)) {
      // already initialized
    } else if (OB_FAIL(config_map_.create(bucket_num, "AIModelCfgMap", "AIModelCfgNode", OB_SERVER_TENANT_ID))) {
      LOG_WARN("failed to create ai model config map", K(ret), K(bucket_num));
    } else if (OB_FAIL(register_default_model_configs_())) {
      LOG_WARN("failed to register default model configs", K(ret));
      reset();
    } else {
      // Publish initialized state only after config_map_ is fully populated.
      ATOMIC_SET(&is_inited_, true);
    }
  }
  return ret;
}

void ObAIModelConfigInfoManager::reset()
{
  config_map_.destroy();
  allocator_.reset();
  ATOMIC_SET(&is_inited_, false);
}

int ObAIModelConfigInfoManager::set_config(const ObString &provider,
                                           const ObString &model_name,
                                           const share::ObAIModelConfigItem &item,
                                           const bool overwrite)
{
  int ret = OB_SUCCESS;
  ObAIModelConfigKey key;
  if (!config_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAIModelConfigInfoManager config map not created", K(ret));
  } else if (provider.empty() || model_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider or model_name is empty", K(ret), K(provider), K(model_name));
  } else if (OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_GENERAL_CI, provider, key.provider_, allocator_))) {
    LOG_WARN("failed to toupper provider", K(ret), K(provider));
  } else if (OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_GENERAL_CI, model_name, key.model_name_, allocator_))) {
    LOG_WARN("failed to toupper model_name", K(ret), K(model_name));
  } else if (OB_FAIL(config_map_.set_refactored(key, item, overwrite ? 1 : 0))) {
    LOG_WARN("failed to set model config item", K(ret), K(key), K(item), K(overwrite));
  }
  return ret;
}

int ObAIModelConfigInfoManager::get_model_config(const ObString &provider,
                                                 const ObString &request_model_name,
                                                 share::ObAIModelConfigItem &item) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_config(provider, request_model_name, item))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get model config", K(ret), K(provider), K(request_model_name));
    }
  }
  return ret;
}

int ObAIModelConfigInfoManager::get_config(const ObString &provider,
                                           const ObString &model_name,
                                           share::ObAIModelConfigItem &item) const
{
  int ret = OB_SUCCESS;
  // Thread-safe lazy initialization
  if (OB_FAIL(const_cast<ObAIModelConfigInfoManager *>(this)->init())) {
    LOG_WARN("failed to init ObAIModelConfigInfoManager", K(ret));
  } else if (provider.empty() || model_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider or model_name is empty", K(ret), K(provider), K(model_name));
  } else {
    ObAIModelConfigKey key;
    ObArenaAllocator allocator("AICfgKey");
    if (OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_GENERAL_CI, provider, key.provider_, allocator))) {
      LOG_WARN("failed to toupper provider", K(ret), K(provider));
    } else if (OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_GENERAL_CI, model_name, key.model_name_, allocator))) {
      LOG_WARN("failed to toupper model_name", K(ret), K(model_name));
    } else if (OB_FAIL(config_map_.get_refactored(key, item))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get model config", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObAIModelConfigInfoManager::register_default_model_configs_()
{
  int ret = OB_SUCCESS;
  if (!config_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAIModelConfigInfoManager config map not created", K(ret));
  } else if (OB_FAIL(register_aliyun_model_configs_())) {
    LOG_WARN("failed to register aliyun model configs", K(ret));
  }
  return ret;
}

int ObAIModelConfigInfoManager::register_aliyun_model_configs_()
{
  int ret = OB_SUCCESS;
  const int64_t MB = 1024L * 1024L;
  OB_AI_MODEL_CONFIG_REGISTER(ret, *this, ObAIFuncProviderUtils::DASHSCOPE, "qwen3-vl-embedding", 5, 5 * MB);
  OB_AI_MODEL_CONFIG_REGISTER(ret, *this, ObAIFuncProviderUtils::DASHSCOPE, "qwen2.5-vl-embedding", 1, 5 * MB);
  OB_AI_MODEL_CONFIG_REGISTER(ret, *this, ObAIFuncProviderUtils::DASHSCOPE, "tongyi-embedding-vision-plus", 16, 3 * MB);
  OB_AI_MODEL_CONFIG_REGISTER(ret, *this, ObAIFuncProviderUtils::DASHSCOPE, "tongyi-embedding-vision-flash", 16, 3 * MB);
  OB_AI_MODEL_CONFIG_REGISTER(ret, *this, ObAIFuncProviderUtils::DASHSCOPE, "multimodal-embedding-v1", 1, 3 * MB);
  return ret;
}

const ObString ObAIFuncModel::get_request_model_name()
{
  ObString request_model_name = info_.model_;
  if (!endpoint_info_.get_request_model_name().empty()) {
    request_model_name = endpoint_info_.get_request_model_name();
  }
  return request_model_name;
}

// Image magic number definitions
// JPEG: FF D8 FF
// PNG: 89 50 4E 47 0D 0A 1A 0A
// GIF: 47 49 46 38 (GIF8)
// WebP: 52 49 46 46 (RIFF) + 57 45 42 50 (WEBP) at offset 8
// BMP: 42 4D (BM)
// TIFF: 49 49 2A 00 (little endian) or 4D 4D 00 2A (big endian)
// ICO: 00 00 01 00 (reserved=0, type=1)
// DIB: same as BMP (starts with 42 4D)
// ICNS: 69 63 (icns magic)
// SGI: 01 DA (magic + type) or 01 52
static const uint8_t JPEG_MAGIC[] = {0xFF, 0xD8, 0xFF};
static const uint8_t PNG_MAGIC[] = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
static const uint8_t GIF_MAGIC[] = {0x47, 0x49, 0x46, 0x38}; // GIF8
static const uint8_t BMP_MAGIC[] = {0x42, 0x4D}; // BM
static const uint8_t WEBP_RIFF_MAGIC[] = {0x52, 0x49, 0x46, 0x46}; // RIFF
static const uint8_t WEBP_MAGIC[] = {0x57, 0x45, 0x42, 0x50}; // WEBP
static const uint8_t TIFF_LE_MAGIC[] = {0x49, 0x49, 0x2A, 0x00}; // little endian
static const uint8_t TIFF_BE_MAGIC[] = {0x4D, 0x4D, 0x00, 0x2A}; // big endian
static const uint8_t ICO_MAGIC[] = {0x00, 0x00, 0x01, 0x00}; // ICO
static const uint8_t ICNS_MAGIC[] = {0x69, 0x63, 0x6E, 0x73}; // icns
static const uint8_t SGI_MAGIC[] = {0x01, 0xDA}; // SGI RLE
static const uint8_t SGI_MAGIC_UNC[] = {0x01, 0x52}; // SGI uncompressed

// Static string array for image type to string conversion
static const char* IMAGE_TYPE_STRINGS[] = {
  "jpeg",   // IMAGE_TYPE_JPEG = 0
  "png",     // IMAGE_TYPE_PNG = 1
  "gif",    // IMAGE_TYPE_GIF = 2
  "webp",   // IMAGE_TYPE_WEBP = 3
  "bmp",    // IMAGE_TYPE_BMP = 4
  "tiff",   // IMAGE_TYPE_TIFF = 5
  "x-icon", // IMAGE_TYPE_ICO = 6
  "bmp",    // IMAGE_TYPE_DIB = 7
  "icns",   // IMAGE_TYPE_ICNS = 8
  "sgi"     // IMAGE_TYPE_SGI = 9
};
static_assert(sizeof(IMAGE_TYPE_STRINGS)/sizeof(IMAGE_TYPE_STRINGS[0]) == ObAiFuncImageUtils::IMAGE_TYPE_UNKNOWN,
              "IMAGE_TYPE_STRINGS size must match IMAGE_TYPE_UNKNOWN value");

// Maximum bytes needed to detect image type (WebP needs 12 bytes: 4 + 8)
static const int64_t MAX_DETECT_LENGTH = 64;

static bool match_magic(const uint8_t *data, int64_t data_len, const uint8_t *magic, int64_t magic_len)
{
  return data_len >= magic_len && MEMCMP(data, magic, magic_len) == 0;
}

int ObAiFuncImageUtils::get_type_from_binary(ObIAllocator &allocator, const ObString &binary_image_str, ObImageType &image_type)
{
  int ret = OB_SUCCESS;
  image_type = IMAGE_TYPE_UNKNOWN;
  if (binary_image_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("binary_image_str is empty", K(ret));
  } else {
    const uint8_t *data = reinterpret_cast<const uint8_t *>(binary_image_str.ptr());
    // Only detect using first MAX_DETECT_LENGTH bytes to avoid processing huge data
    int64_t data_len = binary_image_str.length() > MAX_DETECT_LENGTH ? MAX_DETECT_LENGTH : binary_image_str.length();

    // JPEG: FF D8 FF
    if (match_magic(data, data_len, JPEG_MAGIC, 3)) {
      image_type = IMAGE_TYPE_JPEG;
    // PNG: 89 50 4E 47 0D 0A 1A 0A
    } else if (match_magic(data, data_len, PNG_MAGIC, 8)) {
      image_type = IMAGE_TYPE_PNG;
    // GIF: 47 49 46 38
    } else if (match_magic(data, data_len, GIF_MAGIC, 4)) {
      image_type = IMAGE_TYPE_GIF;
    // BMP/DIB: 42 4D
    } else if (match_magic(data, data_len, BMP_MAGIC, 2)) {
      image_type = IMAGE_TYPE_BMP;
    // WebP: RIFF at offset 0, WEBP at offset 8
    } else if (match_magic(data, data_len, WEBP_RIFF_MAGIC, 4) &&
                match_magic(data + 8, data_len - 8, WEBP_MAGIC, 4)) {
      image_type = IMAGE_TYPE_WEBP;
    // TIFF little endian: 49 49 2A 00
    } else if (match_magic(data, data_len, TIFF_LE_MAGIC, 4)) {
      image_type = IMAGE_TYPE_TIFF;
    // TIFF big endian: 4D 4D 00 2A
    } else if (match_magic(data, data_len, TIFF_BE_MAGIC, 4)) {
      image_type = IMAGE_TYPE_TIFF;
    // ICO: 00 00 01 00
    } else if (match_magic(data, data_len, ICO_MAGIC, 4)) {
      image_type = IMAGE_TYPE_ICO;
    // ICNS: 69 63 6E 73
    } else if (match_magic(data, data_len, ICNS_MAGIC, 4)) {
      image_type = IMAGE_TYPE_ICNS;
    // SGI: 01 DA or 01 52
    } else if (match_magic(data, data_len, SGI_MAGIC, 2) ||
               match_magic(data, data_len, SGI_MAGIC_UNC, 2)) {
      image_type = IMAGE_TYPE_SGI;
    }

    if (image_type == IMAGE_TYPE_UNKNOWN) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unknown image type", K(ret), K(data_len));
    }
  }
  return ret;
}

const char* ObAiFuncImageUtils::get_image_type_str(ObImageType image_type)
{
  if (image_type < 0 || image_type >= IMAGE_TYPE_UNKNOWN) {
    return "unknown";
  }
  return IMAGE_TYPE_STRINGS[image_type];
}

int ObAiFuncImageUtils::get_base64_data_uri_from_binary(ObIAllocator &allocator, const ObString &binary_image_str, ObString &base64_data_uri)
{
  int ret = OB_SUCCESS;
  if (binary_image_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("binary_image_str is empty", K(ret));
  } else {
    ObImageType image_type = IMAGE_TYPE_UNKNOWN;
    if (OB_FAIL(get_type_from_binary(allocator, binary_image_str, image_type))) {
      LOG_WARN("fail to get image type from binary", K(ret));
    } else if (image_type == IMAGE_TYPE_UNKNOWN) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unknown image type", K(ret));
    } else {
      const char *type_str = get_image_type_str(image_type);
      if (OB_ISNULL(type_str)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to get image type string", K(ret));
      } else {
        ObString image_type_str(static_cast<int32_t>(strlen(type_str)), type_str);
        // Build data URI format: data:image/<type>;base64,<base64_encoded_data>
        int64_t encoded_len = ObBase64Encoder::needed_encoded_length(binary_image_str.length());
        const int64_t prefix_len = 5 + 6 + image_type_str.length() + 8; // "data:" + "image/" + type + ";base64,"
        int64_t data_uri_len = prefix_len + encoded_len;
        char *data_uri_buf = static_cast<char *>(allocator.alloc(data_uri_len));
        if (OB_ISNULL(data_uri_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for data uri buffer", K(ret), K(data_uri_len));
        } else {
          int64_t uri_pos = 0;
          MEMCPY(data_uri_buf + uri_pos, "data:image/", 11);
          uri_pos += 11;
          MEMCPY(data_uri_buf + uri_pos, image_type_str.ptr(), image_type_str.length());
          uri_pos += image_type_str.length();
          MEMCPY(data_uri_buf + uri_pos, ";base64,", 8);
          uri_pos += 8;
          int64_t base64_pos = 0;
          if (OB_FAIL(ObBase64Encoder::encode(reinterpret_cast<const uint8_t *>(binary_image_str.ptr()),
                                              binary_image_str.length(),
                                              data_uri_buf + uri_pos, encoded_len, base64_pos))) {
            LOG_WARN("fail to encode binary to base64", K(ret), K(binary_image_str.length()));
          } else {
            uri_pos += base64_pos;
            base64_data_uri.assign_ptr(data_uri_buf, static_cast<ObString::obstr_size_t>(uri_pos));
          }
        }
      }
    }
  }
  return ret;
}

// AIMD concurrency adjustment: additive increase on success, multiplicative decrease on failure
int64_t ObAIFuncBatchUtils::adjust_concurrency(int64_t current, int64_t min_concurrency,
                                               int64_t max_concurrency, bool all_success)
{
  int64_t new_concurrency = current;
  if (all_success) {
    // Additive increase: +1 on success
    new_concurrency = std::min(current + 1, max_concurrency);
  } else {
    // Multiplicative decrease: halve on failure
    new_concurrency = std::max(current / 2, min_concurrency);
  }
  return new_concurrency;
}

int ObAIFuncBatchUtils::flush_pending_batch(ObIAllocator &allocator,
                                            const sql::ObExpr &expr,
                                            sql::ObEvalCtx &ctx,
                                            ObAIFuncBatchState &pending,
                                            ObIVector *res_vec,
                                            ObBitVector &eval_flags,
                                            ParseBatchResponseFn parse_fn)
{
  int ret = OB_SUCCESS;
  if (pending.bodies_.empty()) {
    // Nothing to do
  } else if (OB_ISNULL(pending.endpoint_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending endpoint info is null", K(ret));
  } else if (pending.row_starts_.count() != pending.bodies_.count()
             || pending.row_lens_.count() != pending.bodies_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending row mapping count does not match request count", K(ret),
             K(pending.row_starts_.count()), K(pending.row_lens_.count()),
             K(pending.bodies_.count()));
  } else {
    const int64_t total_requests = pending.bodies_.count();

    const int64_t min_concurrency = pending.min_concurrency_;
    const int64_t max_concurrency = pending.max_concurrency_;
    int64_t current_concurrency = max_concurrency;

    // Initialize completion tracking arrays
    ObSEArray<bool, 64> completed;
    ObSEArray<ObJsonObject *, 64> responses;
    if (OB_FAIL(completed.prepare_allocate(total_requests))) {
      LOG_WARN("fail to prepare completed array", K(ret), K(total_requests));
    } else if (OB_FAIL(responses.prepare_allocate(total_requests))) {
      LOG_WARN("fail to prepare responses array", K(ret), K(total_requests));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < total_requests; ++i) {
      completed.at(i) = false;
      responses.at(i) = nullptr;
    }

    // Adaptive concurrency main loop: continue until all completed or error/timeout
    int64_t retry_count = 0;
    int64_t pending_count = total_requests;
    while (OB_SUCC(ret) && pending_count > 0 && !THIS_WORKER.is_timeout()) {
      // Collect indices of uncompleted requests, up to current_concurrency
      const int64_t batch_size = std::min(current_concurrency, pending_count);
      ObArray<ObJsonObject *> batch_bodies;
      ObSEArray<int64_t, 64> batch_indices;
      for (int64_t i = 0, collected = 0; OB_SUCC(ret) && i < total_requests && collected < batch_size; ++i) {
        if (!completed.at(i)) {
          if (OB_FAIL(batch_bodies.push_back(pending.bodies_.at(i)))) {
            LOG_WARN("fail to push back batch body", K(ret), K(i));
          } else if (OB_FAIL(batch_indices.push_back(i))) {
            LOG_WARN("fail to push back batch index", K(ret), K(i));
          } else {
            ++collected;
          }
        }
      }

      // Send batch request
      ObAIFuncClient ai_client;
      ObArray<ObAIBatchItemResult> results;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ai_client.send_post_batch_with_result(allocator,
                                                               pending.endpoint_info_->get_url(),
                                                               pending.headers_,
                                                               batch_bodies,
                                                               results))) {
        LOG_WARN("fail to send batch request", K(ret), K(batch_size));
      } else if (results.count() != batch_indices.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result count mismatch", K(ret), K(results.count()), K(batch_indices.count()));
      }

      // Process results and adjust state
      bool all_success = true;
      bool has_retryable = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
        const int64_t idx = batch_indices.at(i);
        const ObAIBatchItemResult &result = results.at(i);
        if (result.is_success()) {
          completed.at(idx) = true;
          responses.at(idx) = result.response_;
          --pending_count;
        } else if (ObAIFuncClient::is_retryable_status_code(result.http_code_)) {
          all_success = false;
          has_retryable = true;
          LOG_WARN("retryable HTTP error for request", K(idx), K(result.http_code_));
        } else if (0 == result.http_code_
                   && ObAIFuncClient::is_retryable_curl_code(result.curl_code_)) {
          all_success = false;
          has_retryable = true;
          LOG_WARN("retryable network error for request", K(idx), K(result.curl_code_));
        } else {
          ret = OB_CURL_ERROR;
          ObString response_str;
          if (result.response_ != nullptr &&
              OB_SUCCESS == ObAIFuncJsonUtils::print_json_to_str(allocator, result.response_, response_str)) {
            LOG_WARN("fatal HTTP error for request", K(ret), K(idx), K(result.http_code_), K(result.curl_code_), K(response_str));
            FORWARD_USER_ERROR_MSG(ret, "HTTP error %ld, curl code %d: %.*s", result.http_code_,
                                   static_cast<int>(result.curl_code_),
                                   static_cast<int>(response_str.length()), response_str.ptr());
          } else {
            LOG_WARN("fatal HTTP error for request", K(ret), K(idx), K(result.http_code_), K(result.curl_code_));
            FORWARD_USER_ERROR_MSG(ret, "HTTP error %ld, curl code %d", result.http_code_,
                                   static_cast<int>(result.curl_code_));
          }
        }
      }

      // Adjust concurrency and handle retry backoff
      if (OB_SUCC(ret)) {
        current_concurrency = adjust_concurrency(current_concurrency, min_concurrency,
                                                 max_concurrency, all_success);
        if (all_success) {
          retry_count = retry_count/2;
        } else if (has_retryable) {
          ++retry_count;
          const int64_t delay_ms = 1000 * (1 << std::min(retry_count, static_cast<int64_t>(2))) + ObRandom::rand(0, 1000);
          LOG_INFO("waiting before retry", K(delay_ms), K(retry_count), K(current_concurrency), K(min_concurrency), K(max_concurrency));
          ob_usleep(delay_ms * 1000);
        }
      }
    }

    // Check timeout after loop
    if (OB_SUCC(ret) && pending_count > 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("query timeout during batch processing", K(ret), K(pending_count));
    }

    // Parse all successful responses in order
    for (int64_t i = 0; OB_SUCC(ret) && i < total_requests; ++i) {
      const int64_t start = pending.row_starts_.at(i);
      const int64_t len = pending.row_lens_.at(i);
      if (OB_ISNULL(responses.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("response is null after main loop", K(ret), K(i));
      } else if (start < 0 || len < 0 || start + len > pending.row_indices_flat_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pending row mapping", K(ret), K(i), K(start), K(len));
      } else {
        ObArray<int64_t> row_indices;
        for (int64_t j = 0; OB_SUCC(ret) && j < len; ++j) {
          if (OB_FAIL(row_indices.push_back(pending.row_indices_flat_.at(start + j)))) {
            LOG_WARN("fail to push back row index", K(ret), K(i), K(j), K(start), K(len));
          }
        }
        if (OB_FAIL(ret)) {
          // Error already logged
        } else if (OB_FAIL(parse_fn(expr, ctx, allocator, responses.at(i), row_indices,
                                    *pending.endpoint_info_, res_vec))) {
          LOG_WARN("fail to parse batch response", K(ret), K(i));
        } else {
          for (int64_t j = 0; j < row_indices.count(); ++j) {
            eval_flags.set(row_indices.at(j));
          }
        }
      }
    }
  }
  return ret;
}

int ObAIFuncBatchUtils::init_pending_state(ObIAllocator &allocator,
                                           const ObString &model_id,
                                           const share::ObAiModelEndpointInfo *endpoint_info,
                                           ObAIFuncBatchState &pending,
                                           CheckModelTypeFn check_fn)
{
  int ret = OB_SUCCESS;
  pending.model_id_ = model_id;
  if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, pending.info_))) {
    LOG_WARN("fail to get ai func info", K(ret), K(model_id));
  } else if (OB_FAIL(check_fn(pending.info_))) {
    LOG_WARN("fail to check model type", K(ret), K(model_id));
  } else if (OB_ISNULL(endpoint_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("endpoint info is null", K(ret), K(model_id));
  } else {
    pending.endpoint_info_ = endpoint_info;
    // Get concurrency parameters from model config (non-fatal if lookup fails)
    {
      share::ObAIModelConfigInfo tmp_model_config;
      int tmp_ret = ObAIFuncUtils::get_model_config_info(allocator, model_id, tmp_model_config);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to get model config info, use default concurrency", K(tmp_ret), K(model_id));
      } else {
        pending.min_concurrency_ = tmp_model_config.get_min_concurrency();
        pending.max_concurrency_ = tmp_model_config.get_max_concurrency();
      }
    }
    if (OB_FAIL(ObAIFuncUtils::get_header(allocator, *pending.info_,
                                          *pending.endpoint_info_, pending.headers_))) {
      LOG_WARN("fail to get header", K(ret));
    } else {
      pending.initialized_ = true;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase