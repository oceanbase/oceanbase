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
#include "ob_ai_func_client.h"

namespace oceanbase
{
namespace common
{

ObAIFuncClient::ObAIFuncClient()
    : allocator_(nullptr), url_(nullptr), header_list_(nullptr),
      curlm_(nullptr), curl_(nullptr), curl_handles_(), response_buffers_()
{
  is_finished_.store(false);
  abs_timeout_ts_ = 0;
  max_retry_times_ = 3;  // default retry 3 times
  timeout_sec_ = 60; // default timeout 1 minute
}

void ObAIFuncClient::reset()
{
  if (url_ != nullptr && allocator_ != nullptr) {
    allocator_->free(url_);
    url_ = nullptr;
  } else if (url_!=nullptr && allocator_ == nullptr) {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("url_ is not null but allocator_ is null", K(ret));
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "can not free url_");
  }
  allocator_ = nullptr;
  if (OB_NOT_NULL(header_list_)) {
    curl_slist_free_all(header_list_);
    header_list_ = nullptr;
  }
  is_finished_.store(false);
  clean_up();
}

ObAIFuncClient::~ObAIFuncClient()
{
  if (OB_NOT_NULL(header_list_)) {
    curl_slist_free_all(header_list_);
    header_list_ = nullptr;
  }
  if (OB_NOT_NULL(curl_)) {
    curl_easy_cleanup(curl_);
    curl_ = nullptr;
  }
  if (url_ != nullptr && allocator_ != nullptr) {
    allocator_->free(url_);
    url_ = nullptr;
  }
  allocator_ = nullptr;
  clean_up();
  if (OB_NOT_NULL(curlm_)) {
    curl_multi_cleanup(curlm_);
    curlm_ = nullptr;
  }
}

int ObAIFuncClient::init(common::ObIAllocator &allocator, common::ObString &url, ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  int64_t remain_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : timeout_sec_ * 1000000;
  LOG_INFO("init ai func client", K(remain_timeout_us));
  if (OB_ISNULL(url) || headers.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for init", K(ret));
  } else {
    allocator_ = &allocator;
    ObString url_str;
    if (OB_FAIL(ob_write_string(*allocator_, url, url_str, true))) {
      LOG_WARN("failed to convert url to cstring", K(ret));
    } else {
      url_ = url_str.ptr();
    }
    timeout_sec_ = std::max(1L, remain_timeout_us / 1000000);
    abs_timeout_ts_ = remain_timeout_us + ObTimeUtility::current_time();
    if (OB_SUCC(ret)){
      const uint32_t num_headers = headers.count();
      for (uint32_t i = 0; OB_SUCC(ret) && i < num_headers; ++i) {
        ObString header_c_str;
        if (OB_FAIL(ob_write_string(*allocator_, headers.at(i), header_c_str, true))) {
          LOG_WARN("failed to convert header to cstring", K(ret), K(i));
        } else if (OB_ISNULL(header_list_ = curl_slist_append(header_list_, header_c_str.ptr()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to append header", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObAIFuncClient::error_handle(CURLcode res)
{
  int ret = OB_SUCCESS;
  switch (res) {
    case CURLE_URL_MALFORMAT:{
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("url malformat", K(res));
      char http_code_str[1024] = "error url format";
      FORWARD_USER_ERROR(ret, http_code_str);
      break;
    }
    default:{
      ret = OB_CURL_ERROR;
      LOG_WARN("curl error", K(res));
      break;
    }
  }
  return ret;
}

int ObAIFuncClient::send_post(ObJsonObject *data, ObJsonObject *&response)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for send_post,no data", K(ret));
  } else if (OB_ISNULL(curl_)) {
    if (OB_ISNULL(curl_ = curl_easy_init())) {
      ret = OB_INIT_FAIL;
      LOG_WARN("fail to init curl", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    CURLcode res;
    ObIJsonBase *j_tree = NULL;
    ObStringBuffer response_buf(allocator_);
    int64_t http_code = 0;
    if (OB_FAIL(init_easy_handle(curl_, data, response_buf))) {
      LOG_WARN("fail to init easy handle", K(ret));
    }
    // retry if need
    for (int64_t i = 0; OB_SUCC(ret) && i < max_retry_times_; ++i) {
      http_code = 0;
      if (CURLE_OK != (res = curl_easy_perform(curl_))) {
        LOG_WARN("perform curl failed", K(res));
      } else if (CURLE_OK != (res = curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code))) {
        LOG_WARN("curl get response code failed", K(res));
      }
      if (is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("timeout", K(ret));
      } else if (res != CURLE_OK) {
        // retry
      } else if (!is_retryable_status_code(http_code)) {
        break;
      } else {
        LOG_WARN("retryable http status code", K(http_code), K(i));
        int64_t delay_ms = 1000 * (1 << i) + rand() % 1000;
        ob_usleep(delay_ms * 1000);
        response_buf.reset();
      }
    }
    if (OB_SUCC(ret)) {
      if (res != CURLE_OK && OB_FAIL(error_handle(res))) {
        LOG_WARN("fail to handle error", K(ret), K(res));
      } else if ((http_code / 100) != 2) { // http status code 2xx means success
        ret = OB_CURL_ERROR;
        char http_code_str[1024];
        ObString err_msg = response_buf.string();
        snprintf(http_code_str, sizeof(http_code_str), "http status code: %ld, error message: %s", http_code, err_msg.ptr());
        ObString ob_http_code_str(http_code_str);
        LOG_WARN("unexpected http status code", K(ret), K(http_code), K(err_msg));
        FORWARD_USER_ERROR(ret, http_code_str);
      }
    }

    if (OB_SUCC(ret)) {
        if (OB_FAIL(ObJsonBaseFactory::get_json_base(
              allocator_, response_buf.string(), ObJsonInType::JSON_TREE,
              ObJsonInType::JSON_TREE, j_tree))) {
          ret = OB_ERR_INVALID_JSON_TEXT;
          LOG_WARN("fail to parse http_response", K(ret));
        } else {
          response = static_cast<ObJsonObject *>(j_tree);
        }
    }
  }
  return ret;
}

int ObAIFuncClient::send_post(common::ObIAllocator &allocator,
                              common::ObString &url,
                              ObArray<ObString> &headers,
                              ObJsonObject *data,
                              ObJsonObject *&response)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for send_post", K(ret));
  }
  if (OB_SUCC(ret)) {
    reset();
    if (OB_FAIL(init(allocator, url, headers))) {
      LOG_WARN("fail to init curl ai handle", K(ret));
    } else if (OB_FAIL(send_post(data, response))) {
      LOG_WARN("fail to send post", K(ret));
    }
  }
  return ret;
}

int ObAIFuncClient::send_post_batch(ObArray<ObJsonObject *> &data_array, ObArray<ObJsonObject *> &responses)
{
  int ret = OB_SUCCESS;
  if (data_array.empty() || OB_ISNULL(url_) || OB_ISNULL(header_list_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for send_post_batch", K(ret));
  } else if (OB_ISNULL(curlm_)) {
    if (OB_ISNULL(curlm_ = curl_multi_init())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to init curl multi", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t num_data = data_array.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < num_data; ++i) {
      ObJsonObject *data = data_array.at(i);
      CURL *curl_handle = curl_easy_init();
      ObStringBuffer* response_buf = OB_NEWx(ObStringBuffer, allocator_, allocator_);
      if (OB_FAIL(init_easy_handle(curl_handle, data, *response_buf))) {
        LOG_WARN("failed to init easy handle", K(ret), K(i));
      } else if (OB_FAIL(curl_handles_.push_back(curl_handle))) {
        LOG_WARN("failed to push back curl handle", K(ret), K(i));
      } else if (OB_FAIL(response_buffers_.push_back(response_buf))) {
        LOG_WARN("failed to push back response buffer", K(ret), K(i));
      } else {
        CURLMcode mc = curl_multi_add_handle(curlm_, curl_handle);
        if (mc != CURLM_OK) {
          ret = OB_CURL_ERROR;
          LOG_WARN("failed to add handle to multi", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int running_handles = 0;
      CURLMcode mc = curl_multi_perform(curlm_, &running_handles);
      if (mc != CURLM_OK) {
        ret = OB_CURL_ERROR;
        LOG_WARN("curl_multi_perform failed", K(ret), K(mc));
      } else {
        while (running_handles > 0 && OB_SUCC(ret)) {
          int numfds = 0;
          mc = curl_multi_wait(curlm_, nullptr, 0, 1000, &numfds);
          if (mc != CURLM_OK) {
            ret = OB_CURL_ERROR;
            LOG_WARN("curl_multi_wait failed", K(ret), K(mc));
            break;
          }
          mc = curl_multi_perform(curlm_, &running_handles);
          if (mc != CURLM_OK) {
            ret = OB_CURL_ERROR;
            LOG_WARN("curl_multi_perform failed", K(ret), K(mc));
            break;
          }
        }
        is_finished_.store(true);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_batch_result(responses))) {
        LOG_WARN("fail to get batch result", K(ret));
      }
    }
    clean_up();
  }
  return ret;
}

int ObAIFuncClient::send_post_batch(common::ObIAllocator &allocator,
                                    common::ObString &url,
                                    ObArray<ObString> &headers,
                                    ObArray<ObJsonObject *> &data_array,
                                    ObArray<ObJsonObject *> &responses)
{
  int ret = OB_SUCCESS;
  if (data_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for send_post_batch", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init(allocator, url, headers))) {
      LOG_WARN("fail to init curl ai handle", K(ret));
    }
    // retry if need
    for (int64_t i = 0; OB_SUCC(ret) && i < max_retry_times_; ++i) {
      ret = send_post_batch(data_array, responses);
      if (is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("timeout", K(ret));
      } else if (OB_CURL_ERROR == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("need retry", K(ret), K(i));
        int64_t delay_ms = 1000 * (1 << i) + rand() % 1000;
        ob_usleep(delay_ms * 1000);
      } else if (OB_SUCC(ret)) {
        break;
      }
    }
  }
  return ret;
}

int ObAIFuncClient::send_post_batch_no_wait(ObArray<ObJsonObject *> &data_array)
{
  int ret = OB_SUCCESS;
  if (data_array.empty() || OB_ISNULL(url_) || OB_ISNULL(header_list_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for send_post_batch_no_wait", K(ret));
  } else if (OB_ISNULL(curlm_)) {
    if (OB_ISNULL(curlm_ = curl_multi_init())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to init curl multi", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t num_data = data_array.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < num_data; ++i) {
      ObJsonObject *data_item = data_array.at(i);
      CURL *curl_handle = curl_easy_init();
      ObStringBuffer* response_buf = OB_NEWx(ObStringBuffer, allocator_, allocator_);
      if (OB_ISNULL(data_item)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid data item", K(ret), K(i));
      } else if (OB_ISNULL(curl_handle)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create curl handle", K(ret), K(i));
      } else if (OB_FAIL(init_easy_handle(curl_handle, data_item, *response_buf))) {
        LOG_WARN("failed to init easy handle", K(ret), K(i));
      } else if (OB_FAIL(curl_handles_.push_back(curl_handle))) {
        LOG_WARN("failed to push back curl handle", K(ret), K(i));
      } else if (OB_FAIL(response_buffers_.push_back(response_buf))) {
        LOG_WARN("failed to push back response buffer", K(ret), K(i));
      } else {
        CURLMcode mc = curl_multi_add_handle(curlm_, curl_handle);
        if (mc != CURLM_OK) {
          ret = OB_CURL_ERROR;
          LOG_WARN("failed to add handle to multi", K(ret), K(mc), K(i));
        }
      }
    }
    int running_handles = 0;
    CURLMcode mc = curl_multi_perform(curlm_, &running_handles);
    if (mc != CURLM_OK) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl_multi_perform failed", K(ret), K(mc));
    }
  }
  return ret;
}

bool ObAIFuncClient::check_batch_finished()
{
  int running_handles = 0;
  CURLMcode mc = curl_multi_perform(curlm_, &running_handles);
  if (running_handles == 0) {
    is_finished_.store(true);
  }
  return is_finished_.load();
}

int ObAIFuncClient::get_batch_result(ObArray<ObJsonObject *> &responses)
{
  int ret = OB_SUCCESS;
  if (is_finished_.load()) {
    CURLMsg *msg;
    int msgs_in_queue;
    while ((msg = curl_multi_info_read(curlm_, &msgs_in_queue))) {
      if (msg->msg == CURLMSG_DONE) {
        CURL *easy_handle = msg->easy_handle;
        CURLcode res = msg->data.result;
        if (res == CURLE_OK) {
          long response_code;
          curl_easy_getinfo(easy_handle, CURLINFO_RESPONSE_CODE, &response_code);
          if (response_code != 200) {
            ret = OB_CURL_ERROR;
            LOG_WARN("unexpected http status code", K(ret), K(response_code));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObIJsonBase *j_tree = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < response_buffers_.count(); ++i) {
      ObStringBuffer *response_buf = response_buffers_.at(i);
      ObString response_str;
      response_str.reset();
      if (OB_ISNULL(response_buf)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid response buffer", K(ret), K(i));
      } else if (OB_FAIL(ob_write_string(*allocator_, response_buf->string(), response_str))) {
        LOG_WARN("failed to write response string", K(ret), K(i));
      } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(
              allocator_, response_str, ObJsonInType::JSON_TREE,
              ObJsonInType::JSON_TREE, j_tree))) {
        ret = OB_ERR_INVALID_JSON_TEXT;
        LOG_WARN("fail to parse http_response", K(ret), K(response_str), K(i));
      } else if (OB_FAIL(responses.push_back(static_cast<ObJsonObject *>(j_tree)))) {
        LOG_WARN("failed to append response", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObAIFuncClient::init_easy_handle(CURL *curl, ObJsonObject *body, ObStringBuffer &response_buf)
{
  int ret = OB_SUCCESS;
  ObJsonBuffer j_buf(allocator_);
  if (OB_FAIL(body->print(j_buf, false))) {
    LOG_WARN("failed to print body", K(ret));
  } else if (OB_ISNULL(curl)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init curl", K(ret));
  }
  ObString body_str = j_buf.string();
  CURLcode res;
  const int64_t no_delay = 1;
  const int64_t no_signal = 1;
  if (OB_SUCC(ret)) {
    if (CURLE_OK != (res = curl_easy_setopt(curl, CURLOPT_URL, url_))) {
      LOG_WARN("set url failed", K(res));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ObAIFuncClient::write_callback))) {
      LOG_WARN("set write function failed", K(res));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_buf))) {
      LOG_WARN("set write data failed", K(ret));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, no_signal))) {
      LOG_WARN("set no signal failed", K(res), K(no_signal));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, no_delay))) {
      LOG_WARN("set no delay failed", K(res), K(no_delay));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 10000))) {
      LOG_WARN("set connection timeout ms failed", K(res));
    } else if (CURLE_OK != (res = curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_sec_))) {
      LOG_WARN("set timeout failed", K(res));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list_))) {
      LOG_WARN("set headers failed", K(res));
    } else if (CURLE_OK !=(res = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body_str.ptr()))) {
      LOG_WARN("set post failed", K(res));
    } else if (CURLE_OK != (res = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,body_str.length()))) {
      LOG_WARN("set post failed", K(res));
    } else if (CURLE_OK != (res = curl_easy_setopt(curl, CURLOPT_POST, 1))) {
      LOG_WARN("set post method failed", K(res));
    }
    if (CURLE_OK != res) {
      ret = OB_CURL_ERROR;
      LOG_WARN("fail to set curl options", K(res), K(res));
    }
  }
  return ret;
}

void ObAIFuncClient::clean_up()
{
  for (int64_t i = 0; i < curl_handles_.count(); ++i) {
    CURL *curl_handle = curl_handles_.at(i);
    if (curl_handle != nullptr) {
      curl_multi_remove_handle(curlm_, curl_handle);
      curl_easy_cleanup(curl_handle);
      curl_handles_.at(i) = nullptr;
    }
  }
  curl_handles_.reset();
  for (int64_t i = 0; i < response_buffers_.count(); ++i) {
    ObStringBuffer *response_buf = response_buffers_.at(i);
    if (response_buf != nullptr) {
      OB_DELETEx(ObStringBuffer, allocator_, response_buf);
      response_buffers_.at(i) = nullptr;
    }
  }
  response_buffers_.reset();
  is_finished_.store(false);
}

size_t ObAIFuncClient::write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
  int ret = OB_SUCCESS;
  size_t total_size = size * nmemb;
  if (OB_ISNULL(contents)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for curl_write_callback", K(ret));
  } else if (total_size > 0) {
    ObStringBuffer &result = *static_cast<ObStringBuffer *>(userp);
    result.reserve(total_size);
    if (OB_FAIL(result.append(static_cast<const char *>(contents), total_size, 0))) {
      LOG_WARN("failed to append to result", K(ret));
    }
  }
  return OB_SUCC(ret) ? total_size : 0;
}

bool ObAIFuncClient::is_retryable_status_code(int64_t http_code)
{
  return (http_code == 429 || http_code == 500 ||
         http_code == 502 || http_code == 503 || http_code == 504);
}

bool ObAIFuncClient::is_timeout()
{
  return ObTimeUtility::current_time() > abs_timeout_ts_;
}

} // namespace common
} // namespace oceanbase