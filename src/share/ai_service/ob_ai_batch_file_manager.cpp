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

#define USING_LOG_PREFIX SHARE

#include "share/ai_service/ob_ai_batch_file_manager.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/json/ob_json.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"

#include <curl/curl.h>

namespace oceanbase
{
namespace share
{

//=============================================== ObAiBatchSubmitResult ================================================

bool ObAiBatchSubmitResult::is_terminal() const
{
  return OB_AI_BATCH_FILE_STATUS_COMPLETED == status_ ||
         OB_AI_BATCH_FILE_STATUS_FAILED == status_ ||
         OB_AI_BATCH_FILE_STATUS_EXPIRED == status_ ||
         OB_AI_BATCH_FILE_STATUS_CANCELLED == status_;
}

//=============================================== ObAiBatchFileManager ================================================

ObAiBatchFileManager::ObAiBatchFileManager()
  : is_inited_(false),
    allocator_(nullptr),
    response_buffer_(nullptr),
    response_buffer_size_(0),
    response_buffer_capacity_(0),
    http_status_code_(0),
    local_allocator_("AiBatchFileMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObAiBatchFileManager::~ObAiBatchFileManager()
{
  reset();
}

int ObAiBatchFileManager::init(common::ObIAllocator &allocator,
                               const common::ObString &base_url,
                               const common::ObString &api_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiBatchFileManager already initialized", K(ret));
  } else if (OB_UNLIKELY(base_url.empty() || api_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for batch file manager", K(ret), K(base_url));
  } else {
    allocator_ = &allocator;

    if (OB_FAIL(ob_write_string(*allocator_, base_url, base_url_, true))) {
      LOG_WARN("failed to deep copy base_url", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, api_key, api_key_, true))) {
      LOG_WARN("failed to deep copy api_key", K(ret));
    } else {
      // Allocate initial response buffer using ob_malloc (not arena)
      // so it can be truly freed and reallocated on expansion
      response_buffer_capacity_ = 64 * 1024;  // 64KB initial
      response_buffer_ = static_cast<char*>(ob_malloc(response_buffer_capacity_, ObMemAttr(MTL_ID(), "AiBatchResp")));
      if (OB_ISNULL(response_buffer_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate response buffer", K(ret));
      } else {
        is_inited_ = true;
        LOG_INFO("ObAiBatchFileManager initialized", K(base_url));
      }
    }
  }
  return ret;
}

void ObAiBatchFileManager::reset()
{
  if (OB_NOT_NULL(response_buffer_)) {
    ob_free(response_buffer_);
    response_buffer_ = nullptr;
  }

  base_url_.reset();
  api_key_.reset();
  response_buffer_size_ = 0;
  response_buffer_capacity_ = 0;
  http_status_code_ = 0;
  allocator_ = nullptr;
  is_inited_ = false;

  local_allocator_.reset();
}

int ObAiBatchFileManager::upload_from_tmpfile(int64_t fd,
                                               int64_t file_size,
                                               uint64_t tenant_id,
                                               const common::ObString &file_name,
                                               const common::ObString &purpose,
                                               ObAiFileUploadResult &result)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiBatchFile"));
  result.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchFileManager not initialized", K(ret));
  } else if (OB_UNLIKELY(fd < 0 || file_size <= 0 || file_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(fd), K(file_size), K(file_name));
  } else {
    ObArenaAllocator tmp_alloc("AiBatchFile", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const int64_t url_buf_len = base_url_.length() + 6 + 1; // base_url + "/files" + '\0'
    char *url_buf = static_cast<char*>(tmp_alloc.alloc(url_buf_len));
    const int64_t purpose_buf_len = purpose.length() + 1; // purpose + '\0'
    char *purpose_buf = static_cast<char*>(tmp_alloc.alloc(purpose_buf_len));
    const int64_t file_name_buf_len = file_name.length() + 1; // file_name + '\0'
    char *file_name_buf = static_cast<char*>(tmp_alloc.alloc(file_name_buf_len));
    struct curl_slist *headers = nullptr;
    CURL *curl = nullptr;
    if (OB_ISNULL(url_buf) || OB_ISNULL(purpose_buf) || OB_ISNULL(file_name_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buffers for upload", K(ret));
    } else if (OB_FAIL(init_authenticated_curl_("/files", DEFAULT_HTTP_TIMEOUT_US,
                                                url_buf, url_buf_len, headers, curl))) {
      LOG_WARN("failed to init authenticated curl for upload", K(ret));
    } else {
      if (!purpose.empty()) {
        MEMCPY(purpose_buf, purpose.ptr(), purpose.length());
      }
      purpose_buf[purpose.length()] = '\0';
      MEMCPY(file_name_buf, file_name.ptr(), file_name.length());
      file_name_buf[file_name.length()] = '\0';

      // Create iterator for streaming read from TmpFileManager
      ObBatchFileJsonlIterator iter;
      if (OB_FAIL(iter.init(fd, 0, file_size, tenant_id))) {
        LOG_WARN("failed to init iterator for upload", K(ret), K(fd), K(file_size));
      } else {
        curl_mime *form = curl_mime_init(curl);
        if (OB_ISNULL(form)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("curl_mime_init returned null", K(ret));
        } else {
          curl_mimepart *field;

          // Add purpose field
          field = curl_mime_addpart(form);
          curl_mime_name(field, "purpose");
          curl_mime_data(field, purpose_buf, purpose.length());

          // Add file field with streaming callback
          field = curl_mime_addpart(form);
          curl_mime_name(field, "file");
          curl_mime_filename(field, file_name_buf);
          curl_mime_data_cb(field, file_size, curl_mime_read_callback_,
                            NULL /* seek */, NULL /* free */, &iter);

          curl_easy_setopt(curl, CURLOPT_MIMEPOST, form);

          response_buffer_size_ = 0;
          http_status_code_ = 0;
          curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback_);
          curl_easy_setopt(curl, CURLOPT_WRITEDATA, this);

          CURLcode curl_ret = curl_easy_perform(curl);
          if (curl_ret != CURLE_OK) {
            ret = OB_CURL_ERROR;
            LOG_WARN("curl upload from tmpfile failed", K(ret), K(curl_ret),
                     "error", curl_easy_strerror(curl_ret));
          } else {
            long http_code = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
            http_status_code_ = http_code;

            if (!is_success_http_code_(http_code, 200)) {
              ret = map_http_status_to_error_code(http_code);
              LOG_WARN("upload from tmpfile failed", K(ret), K(http_code),
                       "response", common::ObString(response_buffer_size_, response_buffer_));
            } else if (OB_FAIL(parse_upload_response_(response_buffer_, response_buffer_size_, result))) {
              LOG_WARN("failed to parse upload response", K(ret));
            }
          }
        }
        curl_mime_free(form);
      }
      if (OB_NOT_NULL(curl)) {
        curl_easy_cleanup(curl);
      }
      if (OB_NOT_NULL(headers)) {
        curl_slist_free_all(headers);
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(build_error_detail_(local_allocator_, ret, http_status_code_,
                                          ObString::make_string("internal error"),
                                          result.error_detail_))) {
      LOG_WARN("failed to build error_detail", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObAiBatchFileManager::download_to_tmpfile(const common::ObString &file_id,
                                               ObBatchFileJsonlWriter &writer,
                                               ObBatchFileDataSegment &segment)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiBatchFile"));
  segment.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchFileManager not initialized", K(ret));
  } else if (OB_UNLIKELY(file_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file_id", K(ret));
  } else if (OB_UNLIKELY(!writer.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("writer not initialized", K(ret));
  } else {
    ObArenaAllocator tmp_alloc("AiBatchFile", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const int64_t ep_buf_len = 7 + file_id.length() + 8 + 1; // "/files/" + id + "/content" + '\0'
    char *endpoint_buf = static_cast<char*>(tmp_alloc.alloc(ep_buf_len));
    char *url_buf = nullptr;
    int64_t url_buf_len = 0;
    struct curl_slist *headers = nullptr;
    CURL *curl = nullptr;
    if (OB_ISNULL(endpoint_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc endpoint_buf", K(ret));
    } else {
      int ep_written = snprintf(endpoint_buf, ep_buf_len, "/files/%.*s/content",
                                static_cast<int>(file_id.length()), file_id.ptr());
      if (OB_UNLIKELY(ep_written < 0 || ep_written >= static_cast<int>(ep_buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("endpoint snprintf failed", K(ret), K(file_id.length()));
      } else {
        url_buf_len = base_url_.length() + ep_written + 1; // base_url + endpoint + '\0'
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(url_buf = static_cast<char*>(tmp_alloc.alloc(url_buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc url_buf", K(ret));
    } else if (OB_FAIL(init_authenticated_curl_(endpoint_buf, DOWNLOAD_HTTP_TIMEOUT_US,
                                         url_buf, url_buf_len, headers, curl))) {
      LOG_WARN("failed to init authenticated curl for download", K(ret), K(file_id));
    } else {
      curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &writer);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_tmpfile_write_callback_);

      CURLcode curl_ret = curl_easy_perform(curl);
      if (curl_ret != CURLE_OK) {
        ret = OB_CURL_ERROR;
        LOG_WARN("curl download to tmpfile failed", K(ret), K(curl_ret),
                 "error", curl_easy_strerror(curl_ret));
      } else {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        if (http_code != 200) {
          ret = OB_RPC_POST_ERROR;
          LOG_WARN("download to tmpfile failed", K(ret), K(http_code));
        } else {
          // Finish writer: flush buffer + seal fd
          if (OB_FAIL(writer.finish(segment))) {
            LOG_WARN("failed to finish writer after download", K(ret));
          } else {
            LOG_INFO("[BATCH-FILE] downloaded result to tmpfile", K(segment));
          }
        }
      }
      if (OB_NOT_NULL(curl)) {
        curl_easy_cleanup(curl);
      }
      if (OB_NOT_NULL(headers)) {
        curl_slist_free_all(headers);
      }
    }
  }
  return ret;
}

size_t ObAiBatchFileManager::curl_mime_read_callback_(char *buffer, size_t size,
                                                       size_t nitems, void *arg)
{
  ObBatchFileJsonlIterator *iter = static_cast<ObBatchFileJsonlIterator *>(arg);
  if (OB_ISNULL(iter)) {
    return CURL_READFUNC_ABORT;
  }
  int64_t actual_size = 0;
  int64_t buf_size = static_cast<int64_t>(size * nitems);
  int ret = iter->read_chunk(buffer, buf_size, actual_size);
  if (OB_SUCCESS != ret) {
    LOG_WARN_RET(ret, "failed to read chunk from iterator", K(ret), K(buf_size));
    return CURL_READFUNC_ABORT;
  }
  return static_cast<size_t>(actual_size);
}

size_t ObAiBatchFileManager::curl_tmpfile_write_callback_(void *contents, size_t size,
                                                           size_t nmemb, void *userp)
{
  ObBatchFileJsonlWriter *writer = static_cast<ObBatchFileJsonlWriter *>(userp);
  if (OB_ISNULL(writer)) {
    return 0;
  }
  int64_t total_size = static_cast<int64_t>(size * nmemb);
  int ret = writer->write_chunk(static_cast<const char *>(contents), total_size);
  if (OB_SUCCESS != ret) {
    LOG_WARN_RET(ret, "failed to write chunk to tmpfile", K(ret), K(total_size));
    return 0;
  }
  return size * nmemb;
}

int ObAiBatchFileManager::submit_batch(const common::ObString &input_file_id,
                                        const common::ObString &endpoint,
                                        const common::ObString &completion_window,
                                        ObAiBatchSubmitResult &result)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiBatchFile"));
  result.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchFileManager not initialized", K(ret));
  } else if (OB_UNLIKELY(input_file_id.empty() || endpoint.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(input_file_id), K(endpoint));
  } else {
    // Inputs are normally controlled config strings (e.g. "/v1/embeddings", "24h",
    // a provider-issued file_id). Reject anything that would require JSON escaping
    // before splicing into the body via snprintf, so a corrupted or hostile value
    // cannot break the JSON or inject extra fields.
    bool args_safe = true;
    const common::ObString *checked[] = { &input_file_id, &endpoint, &completion_window };
    for (int64_t s = 0; args_safe && s < ARRAYSIZEOF(checked); ++s) {
      const common::ObString &str = *checked[s];
      for (int64_t i = 0; args_safe && i < str.length(); ++i) {
        const unsigned char c = static_cast<unsigned char>(str.ptr()[i]);
        if (c < 0x20 || c == 0x7F || c == '"' || c == '\\') {
          args_safe = false;
        }
      }
    }
    if (!args_safe) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("submit_batch input contains JSON-unsafe characters", K(ret),
               K(input_file_id), K(endpoint), K(completion_window));
    }
    // Build URL: /v1/batches
    ObArenaAllocator tmp_alloc("SubmitBody", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const int64_t url_buf_len = base_url_.length() + 8 + 1; // base_url + "/batches" + '\0'
    char *url_buf = static_cast<char*>(tmp_alloc.alloc(url_buf_len));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(url_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc url_buf", K(ret));
    } else if (OB_FAIL(build_url_("/batches", url_buf, url_buf_len))) {
      LOG_WARN("failed to build url", K(ret));
    } else {
      const int64_t body_buf_len = 128 + input_file_id.length() + endpoint.length()
                                   + completion_window.length();
      char *body_buf = static_cast<char*>(tmp_alloc.alloc(body_buf_len));
      if (OB_ISNULL(body_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for body buffer", K(ret));
      } else {
        int64_t pos = snprintf(body_buf, body_buf_len,
                       "{\"input_file_id\":\"%.*s\",\"endpoint\":\"%.*s\",\"completion_window\":\"%.*s\"}",
                       static_cast<int>(input_file_id.length()), input_file_id.ptr(),
                       static_cast<int>(endpoint.length()), endpoint.ptr(),
                       static_cast<int>(completion_window.length()), completion_window.ptr());
        if (OB_UNLIKELY(pos < 0 || pos >= body_buf_len)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("body too large", K(ret));
        } else if (OB_FAIL(execute_authenticated_request_(url_buf, "POST", body_buf, pos,
                                                          200, "submit batch", true))) {
          LOG_WARN("submit batch failed", K(ret));
        } else if (OB_FAIL(parse_batch_response_(response_buffer_, response_buffer_size_, result))) {
          LOG_WARN("failed to parse batch submit response", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(build_error_detail_(local_allocator_, ret, http_status_code_,
                                          ObString::make_string("internal error"),
                                          result.error_detail_))) {
      LOG_WARN("failed to build error_detail", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObAiBatchFileManager::poll_batch_status(const common::ObString &batch_id,
                                             ObAiBatchSubmitResult &result)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiBatchFile"));
  result.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchFileManager not initialized", K(ret));
  } else if (OB_UNLIKELY(batch_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch_id", K(ret));
  } else {
    // Build URL: /v1/batches/{batch_id}
    ObArenaAllocator tmp_alloc("AiBatchFile", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const int64_t ep_buf_len = 9 + batch_id.length() + 1; // "/batches/" + id + '\0'
    char *endpoint_buf = static_cast<char*>(tmp_alloc.alloc(ep_buf_len));
    if (OB_ISNULL(endpoint_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc endpoint_buf", K(ret));
    } else {
      int ep_written = snprintf(endpoint_buf, ep_buf_len, "/batches/%.*s",
                                static_cast<int>(batch_id.length()), batch_id.ptr());
      if (OB_UNLIKELY(ep_written < 0 || ep_written >= ep_buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("endpoint snprintf failed", K(ret), K(batch_id.length()));
      } else {
        const int64_t url_buf_len = base_url_.length() + ep_written + 1; // base_url + endpoint + '\0'
        char *url_buf = static_cast<char*>(tmp_alloc.alloc(url_buf_len));
        if (OB_ISNULL(url_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc url_buf", K(ret));
        } else if (OB_FAIL(build_url_(endpoint_buf, url_buf, url_buf_len))) {
          LOG_WARN("failed to build url", K(ret));
        } else if (OB_FAIL(execute_authenticated_request_(url_buf, "GET", nullptr, 0,
                                                          200, "poll batch status"))) {
          LOG_WARN("poll batch status failed", K(ret), K(batch_id));
        } else if (OB_FAIL(parse_batch_response_(response_buffer_, response_buffer_size_, result))) {
          LOG_WARN("failed to parse poll batch status response", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(build_error_detail_(local_allocator_, ret, http_status_code_,
                                          ObString::make_string("internal error"),
                                          result.error_detail_))) {
      LOG_WARN("failed to build error_detail", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObAiBatchFileManager::parse_jsonl_results_from_buffer(common::ObIAllocator &allocator,
                                                          const char *buffer,
                                                          int64_t buffer_size,
                                                          common::ObIArray<ObAiBatchLineResult> &results)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer) || buffer_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buffer), K(buffer_size));
  } else {
    int64_t line_start = 0;
    common::ObArenaAllocator json_alloc("JsonParse", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    for (int64_t i = 0; i <= buffer_size && OB_SUCC(ret); ++i) {
      if (i == buffer_size || buffer[i] == '\n') {
        if (i > line_start) {
          json_alloc.reuse();
          ObAiBatchLineResult line_result;
          if (OB_FAIL(parse_jsonl_line_(allocator, json_alloc, buffer + line_start, i - line_start, line_result))) {
            LOG_WARN("failed to parse jsonl line", K(ret), K(line_start));
          } else if (OB_FAIL(results.push_back(line_result))) {
            LOG_WARN("failed to add result", K(ret));
          }
        }
        line_start = i + 1;
      }
    }
  }
  return ret;
}

int ObAiBatchFileManager::cancel_batch(const common::ObString &batch_id)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiBatchFile"));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchFileManager not initialized", K(ret));
  } else if (OB_UNLIKELY(batch_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch_id", K(ret));
  } else {
    // Build URL: /v1/batches/{batch_id}/cancel
    ObArenaAllocator tmp_alloc("AiBatchFile", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const int64_t ep_buf_len = 9 + batch_id.length() + 7 + 1; // "/batches/" + id + "/cancel" + '\0'
    char *endpoint_buf = static_cast<char*>(tmp_alloc.alloc(ep_buf_len));
    if (OB_ISNULL(endpoint_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc endpoint_buf", K(ret));
    } else {
      int ep_written = snprintf(endpoint_buf, ep_buf_len, "/batches/%.*s/cancel",
                                static_cast<int>(batch_id.length()), batch_id.ptr());
      if (OB_UNLIKELY(ep_written < 0 || ep_written >= ep_buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("endpoint snprintf failed", K(ret), K(batch_id.length()));
      } else {
        const int64_t url_buf_len = base_url_.length() + ep_written + 1; // base_url + endpoint + '\0'
        char *url_buf = static_cast<char*>(tmp_alloc.alloc(url_buf_len));
        if (OB_ISNULL(url_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc url_buf", K(ret));
        } else if (OB_FAIL(build_url_(endpoint_buf, url_buf, url_buf_len))) {
          LOG_WARN("failed to build url", K(ret));
        } else if (OB_FAIL(execute_authenticated_request_(url_buf, "POST", nullptr, 0,
                                                          200, "cancel batch"))) {
          LOG_WARN("cancel batch request failed", K(ret), K(batch_id));
        }
      }
    }
  }
  return ret;
}

int ObAiBatchFileManager::delete_file(const common::ObString &file_id)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "AiBatchFile"));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchFileManager not initialized", K(ret));
  } else if (OB_UNLIKELY(file_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file_id", K(ret));
  } else {
    ObArenaAllocator tmp_alloc("AiBatchFile", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const int64_t ep_buf_len = 7 + file_id.length() + 1; // "/files/" + id + '\0'
    char *endpoint_buf = static_cast<char*>(tmp_alloc.alloc(ep_buf_len));
    if (OB_ISNULL(endpoint_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc endpoint_buf", K(ret));
    } else {
      int ep_written = snprintf(endpoint_buf, ep_buf_len, "/files/%.*s",
                                static_cast<int>(file_id.length()), file_id.ptr());
      if (OB_UNLIKELY(ep_written < 0 || ep_written >= ep_buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("endpoint snprintf failed", K(ret), K(file_id.length()));
      } else {
        const int64_t url_buf_len = base_url_.length() + ep_written + 1; // base_url + endpoint + '\0'
        char *url_buf = static_cast<char*>(tmp_alloc.alloc(url_buf_len));
        if (OB_ISNULL(url_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc url_buf", K(ret));
        } else if (OB_FAIL(build_url_(endpoint_buf, url_buf, url_buf_len))) {
          LOG_WARN("failed to build url", K(ret));
        } else if (OB_FAIL(execute_authenticated_request_(url_buf, "DELETE", nullptr, 0,
                                               200, "delete file"))) {
          if (OB_LIKELY(http_status_code_ == 404)) {
            ret = OB_SUCCESS;
            LOG_INFO("remote file already deleted", K(file_id));
          } else {
            LOG_WARN("delete file request failed", K(ret), K(file_id), K(http_status_code_));
          }
        }
      }
    }
  }
  return ret;
}

int ObAiBatchFileManager::execute_http_request_(const char *url,
                                                 const char *method,
                                                 const struct curl_slist *headers,
                                                 const char *body,
                                                 int64_t body_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(url) || OB_ISNULL(method)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(url), KP(method));
  } else {
    // Create a new curl handle for each request (thread-safe)
    CURL *curl = curl_easy_init();
    if (OB_ISNULL(curl)) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to init curl easy handle", K(ret));
    } else {
      // Reset response buffer
      response_buffer_size_ = 0;
      http_status_code_ = 0;

      curl_easy_setopt(curl, CURLOPT_URL, url);
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

      if (0 == strcmp(method, "POST")) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        if (OB_NOT_NULL(body) && body_len > 0) {
          curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
          curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body_len);
        }
      } else if (0 == strcmp(method, "GET")) {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
      } else if (0 == strcmp(method, "DELETE")) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
      }

      setup_curl_common_(curl, DEFAULT_HTTP_TIMEOUT_US);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback_);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, this);

      CURLcode curl_ret = curl_easy_perform(curl);
      if (curl_ret != CURLE_OK) {
        ret = OB_CURL_ERROR;
        LOG_WARN("curl request failed", K(ret), K(curl_ret), "error", curl_easy_strerror(curl_ret));
      } else {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        http_status_code_ = http_code;
      }

      // Clean up curl handle after each request
      curl_easy_cleanup(curl);
    }
  }
  return ret;
}

int ObAiBatchFileManager::execute_authenticated_request_(const char *url,
                                                         const char *method,
                                                         const char *body,
                                                         int64_t body_len,
                                                         int64_t expected_http_code,
                                                         const char *request_name,
                                                         bool add_json_content_type)
{
  int ret = OB_SUCCESS;
  struct curl_slist *headers = nullptr;
  if (add_json_content_type) {
    headers = curl_slist_append(nullptr, "Content-Type: application/json");
    if (OB_ISNULL(headers)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to append json content type header", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_auth_headers_(headers))) {
    LOG_WARN("failed to build auth headers", K(ret));
  } else if (OB_FAIL(execute_http_request_(url, method, headers, body, body_len))) {
    LOG_WARN("failed to execute http request", K(ret), K(request_name));
  } else if (!is_success_http_code_(http_status_code_, expected_http_code)) {
    ret = map_http_status_to_error_code(http_status_code_);
    LOG_WARN(request_name, K(ret), K_(http_status_code),
             "response", common::ObString(response_buffer_size_, response_buffer_));
  }
  if (OB_NOT_NULL(headers)) {
    curl_slist_free_all(headers);
  }
  return ret;
}

int ObAiBatchFileManager::init_authenticated_curl_(const char *endpoint,
                                                   int64_t timeout_us,
                                                   char *url_buf,
                                                   int64_t url_buf_len,
                                                   struct curl_slist *&headers,
                                                   CURL *&curl)
{
  int ret = OB_SUCCESS;
  headers = nullptr;
  curl = nullptr;
  if (OB_ISNULL(endpoint) || OB_ISNULL(url_buf) || url_buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(endpoint), KP(url_buf), K(url_buf_len));
  } else if (OB_FAIL(build_url_(endpoint, url_buf, url_buf_len))) {
    LOG_WARN("failed to build url", K(ret), K(endpoint));
  } else if (OB_FAIL(build_auth_headers_(headers))) {
    LOG_WARN("failed to build auth headers", K(ret));
  } else if (OB_ISNULL(curl = curl_easy_init())) {
    ret = OB_CURL_ERROR;
    LOG_WARN("failed to init curl easy handle", K(ret));
  } else {
    curl_easy_setopt(curl, CURLOPT_URL, url_buf);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    setup_curl_common_(curl, timeout_us);
  }
  return ret;
}

// Helpers for the response/JSONL parsers below. Each macro is a no-op when the key is missing
// or the value is the wrong JSON type, and short-circuits when `ret` is already non-OK. Each
// must be invoked from a scope that has `int ret` and the standard OB_FAIL/LOG_WARN macros.
#define BATCH_EXTRACT_JSON_STR(_obj, _key, _allocator, _dst)                                  \
  do {                                                                                         \
    if (OB_SUCC(ret)) {                                                                        \
      if (OB_NOT_NULL(_obj)) {                                                                  \
        common::ObJsonNode *_node = (_obj)->get_value(_key);                                   \
        if (OB_NOT_NULL(_node) && _node->json_type() == common::ObJsonNodeType::J_STRING) {    \
          common::ObJsonString *_str = static_cast<common::ObJsonString*>(_node);              \
          if (OB_FAIL(ob_write_string((_allocator), _str->value(), (_dst), true))) {           \
            LOG_WARN("failed to deep copy json string field", K(ret), "key", (_key));          \
          }                                                                                    \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
  } while (0)

#define BATCH_EXTRACT_JSON_INT(_obj, _key, _dst)                                              \
  do {                                                                                         \
    if (OB_SUCC(ret)) {                                                                        \
      if (OB_NOT_NULL(_obj)) {                                                                  \
        common::ObJsonNode *_node = (_obj)->get_value(_key);                                   \
        if (OB_NOT_NULL(_node) && _node->json_type() == common::ObJsonNodeType::J_INT) {       \
          (_dst) = static_cast<common::ObJsonInt*>(_node)->value();                            \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
  } while (0)

int ObAiBatchFileManager::parse_json_object_(const common::ObString &json_str,
                                              common::ObIAllocator &alloc,
                                              common::ObJsonObject *&out_obj)
{
  int ret = OB_SUCCESS;
  common::ObJsonNode *root = nullptr;
  if (OB_FAIL(common::ObJsonParser::get_tree(&alloc, json_str, root))) {
    LOG_WARN("failed to parse json response", K(ret));
  } else if (OB_ISNULL(root) || root->json_type() != common::ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid json response", K(ret));
  } else {
    out_obj = static_cast<common::ObJsonObject*>(root);
  }
  return ret;
}

int ObAiBatchFileManager::parse_upload_response_(const char *response,
                                                  int64_t response_len,
                                                  ObAiFileUploadResult &result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(response) || response_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    // Reset arena before each parse to bound memory (no unbounded growth across poll cycles).
    local_allocator_.reset();
    common::ObArenaAllocator json_alloc("JsonParse", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    common::ObString json_str(response_len, response);
    common::ObJsonObject *obj = nullptr;
    if (OB_FAIL(parse_json_object_(json_str, json_alloc, obj))) {
      LOG_WARN("failed to parse upload response", K(ret));
    } else {
      // Strings are deep-copied into local_allocator_ so they outlive json_alloc.
      BATCH_EXTRACT_JSON_STR(obj, "id",     local_allocator_, result.file_id_);
      BATCH_EXTRACT_JSON_STR(obj, "status", local_allocator_, result.status_);

      if (OB_SUCC(ret)) {
        common::ObJsonNode *error_node = obj->get_value("error");
        if (OB_NOT_NULL(error_node) && error_node->json_type() == common::ObJsonNodeType::J_OBJECT) {
          common::ObJsonObject *error_obj = static_cast<common::ObJsonObject*>(error_node);
          common::ObString error_msg;
          BATCH_EXTRACT_JSON_STR(error_obj, "message", local_allocator_, error_msg);
          if (OB_SUCC(ret)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(build_error_detail_(local_allocator_,
                                                OB_RPC_POST_ERROR, 0,
                                                error_msg,
                                                result.error_detail_))) {
              LOG_WARN("failed to build error_detail json", K(tmp_ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAiBatchFileManager::parse_batch_response_(const char *response,
                                                 int64_t response_len,
                                                 ObAiBatchSubmitResult &result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(response) || response_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    // Reset arena before each parse to bound memory (no unbounded growth across poll cycles).
    local_allocator_.reset();
    common::ObArenaAllocator json_alloc("JsonParse", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    common::ObString json_str(response_len, response);
    common::ObJsonObject *obj = nullptr;
    if (OB_FAIL(parse_json_object_(json_str, json_alloc, obj))) {
      LOG_WARN("failed to parse batch response", K(ret));
    } else {
      // Strings are deep-copied into local_allocator_ so they outlive json_alloc.
      BATCH_EXTRACT_JSON_STR(obj, "id",             local_allocator_, result.batch_id_);
      BATCH_EXTRACT_JSON_STR(obj, "input_file_id",  local_allocator_, result.input_file_id_);
      BATCH_EXTRACT_JSON_STR(obj, "output_file_id", local_allocator_, result.output_file_id_);
      BATCH_EXTRACT_JSON_STR(obj, "error_file_id",  local_allocator_, result.error_file_id_);

      // status is mapped to enum via str_to_batch_status, not directly stored.
      if (OB_SUCC(ret)) {
        common::ObJsonNode *status_node = obj->get_value("status");
        if (OB_NOT_NULL(status_node) && status_node->json_type() == common::ObJsonNodeType::J_STRING) {
          common::ObJsonString *str = static_cast<common::ObJsonString*>(status_node);
          result.status_ = ObAiBatchFileManagerUtils::str_to_batch_status(str->value());
        }
      }

      if (OB_SUCC(ret)) {
        common::ObJsonNode *counts_node = obj->get_value("request_counts");
        if (OB_NOT_NULL(counts_node) && counts_node->json_type() == common::ObJsonNodeType::J_OBJECT) {
          common::ObJsonObject *counts_obj = static_cast<common::ObJsonObject*>(counts_node);
          BATCH_EXTRACT_JSON_INT(counts_obj, "total",     result.request_counts_total_);
          BATCH_EXTRACT_JSON_INT(counts_obj, "completed", result.request_counts_completed_);
          BATCH_EXTRACT_JSON_INT(counts_obj, "failed",    result.request_counts_failed_);
        }
      }

      if (OB_SUCC(ret)) {
        common::ObJsonNode *errors_node = obj->get_value("errors");
        if (OB_NOT_NULL(errors_node) && errors_node->json_type() == common::ObJsonNodeType::J_OBJECT) {
          common::ObJsonObject *errors_obj = static_cast<common::ObJsonObject*>(errors_node);
          common::ObJsonNode *data_node = errors_obj->get_value("data");
          if (OB_NOT_NULL(data_node) && data_node->json_type() == common::ObJsonNodeType::J_ARRAY) {
            common::ObJsonArray *data_arr = static_cast<common::ObJsonArray*>(data_node);
            if (data_arr->element_count() > 0) {
              common::ObJsonNode *first_error = data_arr->get_value(0);
              if (OB_NOT_NULL(first_error) && first_error->json_type() == common::ObJsonNodeType::J_OBJECT) {
                common::ObJsonObject *err_obj = static_cast<common::ObJsonObject*>(first_error);
                common::ObString err_msg;
                BATCH_EXTRACT_JSON_STR(err_obj, "message", local_allocator_, err_msg);
                if (OB_SUCC(ret)) {
                  int tmp_ret = OB_SUCCESS;
                  if (OB_TMP_FAIL(build_error_detail_(local_allocator_,
                                                      OB_RPC_POST_ERROR, 0,
                                                      err_msg,
                                                      result.error_detail_))) {
                    LOG_WARN("failed to build error_detail json", K(tmp_ret));
                  }
                }
              }
            }
          }
        }
      }

      if (OB_AI_BATCH_FILE_STATUS_FAILED == result.status_ ||
          OB_AI_BATCH_FILE_STATUS_EXPIRED == result.status_ ||
          OB_AI_BATCH_FILE_STATUS_CANCELLED == result.status_) {
        LOG_WARN("[BATCH-FILE] batch terminal state",
                 K(result.batch_id_), K(result.status_),
                 K(result.input_file_id_), K(result.output_file_id_), K(result.error_file_id_),
                 K(result.request_counts_total_), K(result.request_counts_completed_),
                 K(result.request_counts_failed_), K(result.error_detail_));
      }
    }
  }
  return ret;
}

int ObAiBatchFileManager::parse_jsonl_line_(common::ObIAllocator &allocator,
                                             common::ObIAllocator &json_alloc,
                                             const char *line,
                                             int64_t line_len,
                                             ObAiBatchLineResult &result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(line) || line_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    // Expected format: {"id":"...","custom_id":"...","response":{"status_code":200,"body":{...}},"error":null}
    common::ObString json_str(line_len, line);
    common::ObJsonObject *obj = nullptr;
    if (OB_FAIL(parse_json_object_(json_str, json_alloc, obj))) {
      int64_t dump_len = MIN(line_len, 256);
      LOG_WARN("failed to parse json line", K(ret), K(line_len),
               "line_prefix", common::ObString(dump_len, line));
    } else {
      BATCH_EXTRACT_JSON_STR(obj, "custom_id", allocator, result.custom_id_);

      // Extract response
      if (OB_SUCC(ret)) {
        common::ObJsonNode *response_node = obj->get_value("response");
        if (OB_NOT_NULL(response_node) && response_node->json_type() == common::ObJsonNodeType::J_OBJECT) {
          common::ObJsonObject *resp_obj = static_cast<common::ObJsonObject*>(response_node);

          common::ObJsonNode *status_code_node = resp_obj->get_value("status_code");
          if (OB_NOT_NULL(status_code_node) && status_code_node->json_type() == common::ObJsonNodeType::J_INT) {
            result.response_status_ = static_cast<common::ObJsonInt*>(status_code_node)->value();
          }

          common::ObJsonNode *body_node = resp_obj->get_value("body");
          if (OB_NOT_NULL(body_node)) {
            // Serialize body to string using allocator
            common::ObJsonBuffer jbuf(&allocator);
            if (OB_FAIL(body_node->print(jbuf, true))) {
              LOG_WARN("failed to serialize response body", K(ret));
            } else {
              // Deep copy the serialized body string
              char *buf = static_cast<char*>(allocator.alloc(jbuf.length() + 1));
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to allocate response_body buffer", K(ret));
              } else {
                MEMCPY(buf, jbuf.ptr(), jbuf.length());
                buf[jbuf.length()] = '\0';
                result.response_body_.assign_ptr(buf, jbuf.length());
              }
            }
          }
        }
      }

      // Extract error - build error_detail JSON
      if (OB_SUCC(ret)) {
        common::ObJsonNode *error_node = obj->get_value("error");
        if (OB_NOT_NULL(error_node) && error_node->json_type() != common::ObJsonNodeType::J_NULL) {
          common::ObString err_msg;
          if (error_node->json_type() == common::ObJsonNodeType::J_OBJECT) {
            common::ObJsonObject *err_obj = static_cast<common::ObJsonObject*>(error_node);
            BATCH_EXTRACT_JSON_STR(err_obj, "message", allocator, err_msg);
          }
          if (OB_SUCC(ret)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(build_error_detail_(allocator,
                                                OB_RPC_POST_ERROR, 0,
                                                err_msg,
                                                result.error_detail_))) {
              LOG_WARN("failed to build error_detail json", K(tmp_ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

#undef BATCH_EXTRACT_JSON_STR
#undef BATCH_EXTRACT_JSON_INT

int ObAiBatchFileManager::build_error_detail_(common::ObIAllocator &allocator,
                                              int ob_error_code,
                                              int64_t http_code,
                                              const common::ObString &message,
                                              common::ObString &error_detail)
{
  static const char FALLBACK[] = "{\"ob_error_code\":-1,\"message\":\"build_error_detail failed\"}";
  int ret = OB_SUCCESS;
  char *err_buf = static_cast<char*>(allocator.alloc(OB_AI_MAX_ERROR_DETAIL_LENGTH));
  if (OB_ISNULL(err_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc error_detail buffer", K(ret));
    error_detail = ObString::make_string(FALLBACK);
  } else if (OB_FAIL(ObAiExecUtils::build_error_detail_json(ob_error_code,
                                                         http_code,
                                                         message,
                                                         err_buf,
                                                         OB_AI_MAX_ERROR_DETAIL_LENGTH))) {
    LOG_WARN("failed to build error_detail json", K(ret));
    error_detail = ObString::make_string(FALLBACK);
  } else {
    error_detail = ObString::make_string(err_buf);
  }
  return ret;
}

int ObAiBatchFileManager::build_url_(const char *endpoint, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(endpoint) || OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    int64_t pos = snprintf(buf, buf_len, "%.*s%s",
                          static_cast<int>(base_url_.length()), base_url_.ptr(),
                          endpoint);
    if (pos < 0 || pos >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("url too long", K(ret));
    }
  }
  return ret;
}

int ObAiBatchFileManager::build_auth_headers_(struct curl_slist *&headers)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc("AuthHdr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const int64_t buf_len = 22 + api_key_.length() + 1; // "Authorization: Bearer " + key + '\0'
  char *auth_header = static_cast<char*>(tmp_alloc.alloc(buf_len));
  if (OB_ISNULL(auth_header)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for auth header", K(ret));
  } else {
    int written = snprintf(auth_header, buf_len, "Authorization: Bearer %.*s",
                           static_cast<int>(api_key_.length()), api_key_.ptr());
    if (written < 0 || written >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("auth header snprintf overflow", K(ret), K(api_key_.length()));
    } else {
      headers = curl_slist_append(headers, auth_header);
      if (OB_ISNULL(headers)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to append auth header", K(ret));
      }
    }
  }
  return ret;
}

void ObAiBatchFileManager::setup_curl_common_(CURL *curl, int64_t timeout_us)
{
  if (OB_ISNULL(curl)) {
    return;
  }
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_us / 1000000);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
}

bool ObAiBatchFileManager::is_success_http_code_(int64_t actual, int64_t expected)
{
  return actual == expected || (expected == 200 && actual == 201);
}

size_t ObAiBatchFileManager::curl_write_callback_(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t total_size = size * nmemb;
  ObAiBatchFileManager *manager = static_cast<ObAiBatchFileManager*>(userp);

  if (OB_ISNULL(manager) || OB_ISNULL(contents)) {
    return 0;
  }

  static const int64_t MAX_BATCH_RESPONSE_SIZE = 256LL * 1024 * 1024;  // 256 MB
  if (manager->response_buffer_size_ + total_size > MAX_BATCH_RESPONSE_SIZE) {
    LOG_WARN_RET(OB_SIZE_OVERFLOW, "batch response exceeds max size limit",
                 K(manager->response_buffer_size_), K(total_size), K(MAX_BATCH_RESPONSE_SIZE));
    return 0;
  }
  // Check if we need to expand buffer
  bool need_expand =
      (manager->response_buffer_size_ + total_size > manager->response_buffer_capacity_);
  bool expand_ok = true;
  if (need_expand) {
    int64_t new_capacity = manager->response_buffer_capacity_ * 2;
    while (new_capacity < manager->response_buffer_size_ + total_size) {
      new_capacity *= 2;
    }
    if (new_capacity > MAX_BATCH_RESPONSE_SIZE) {
      new_capacity = MAX_BATCH_RESPONSE_SIZE;
    }
    char *new_buffer = static_cast<char*>(ob_malloc(new_capacity, ObMemAttr(MTL_ID(), "AiBatchResp")));
    if (OB_ISNULL(new_buffer)) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "failed to expand response buffer");
      expand_ok = false;
    } else {
      MEMCPY(new_buffer, manager->response_buffer_, manager->response_buffer_size_);
      ob_free(manager->response_buffer_);
      manager->response_buffer_ = new_buffer;
      manager->response_buffer_capacity_ = new_capacity;
    }
  }

  if (expand_ok) {
    MEMCPY(manager->response_buffer_ + manager->response_buffer_size_, contents, total_size);
    manager->response_buffer_size_ += total_size;
    return total_size;
  }
  return 0;
}

//=============================================== ObAiBatchFileManagerUtils ================================================

ObAiBatchFileStatus ObAiBatchFileManagerUtils::str_to_batch_status(const common::ObString &status_str)
{
  ObAiBatchFileStatus status = OB_AI_BATCH_FILE_STATUS_INVALID;

  if (status_str.empty()) {
    status = OB_AI_BATCH_FILE_STATUS_INVALID;
  } else if (0 == status_str.case_compare("validating") ||
             0 == status_str.case_compare("uploaded")) {
    status = OB_AI_BATCH_FILE_STATUS_UPLOADED;
  } else if (0 == status_str.case_compare("queued")) {
    status = OB_AI_BATCH_FILE_STATUS_QUEUED;
  } else if (0 == status_str.case_compare("in_progress")) {
    status = OB_AI_BATCH_FILE_STATUS_IN_PROGRESS;
  } else if (0 == status_str.case_compare("finalizing")) {
    status = OB_AI_BATCH_FILE_STATUS_FINALIZING;
  } else if (0 == status_str.case_compare("completed")) {
    status = OB_AI_BATCH_FILE_STATUS_COMPLETED;
  } else if (0 == status_str.case_compare("failed")) {
    status = OB_AI_BATCH_FILE_STATUS_FAILED;
  } else if (0 == status_str.case_compare("expired")) {
    status = OB_AI_BATCH_FILE_STATUS_EXPIRED;
  } else if (0 == status_str.case_compare("cancelling")) {
    status = OB_AI_BATCH_FILE_STATUS_IN_PROGRESS;
  } else if (0 == status_str.case_compare("cancelled")) {
    status = OB_AI_BATCH_FILE_STATUS_CANCELLED;
  }

  return status;
}

const char* ObAiBatchFileManagerUtils::batch_status_to_str(ObAiBatchFileStatus status)
{
  const char *str = "unknown";
  switch (status) {
    case OB_AI_BATCH_FILE_STATUS_INVALID:
      str = "invalid";
      break;
    case OB_AI_BATCH_FILE_STATUS_UPLOADING:
      str = "uploading";
      break;
    case OB_AI_BATCH_FILE_STATUS_UPLOADED:
      str = "uploaded";
      break;
    case OB_AI_BATCH_FILE_STATUS_SUBMITTING:
      str = "submitting";
      break;
    case OB_AI_BATCH_FILE_STATUS_QUEUED:
      str = "queued";
      break;
    case OB_AI_BATCH_FILE_STATUS_IN_PROGRESS:
      str = "in_progress";
      break;
    case OB_AI_BATCH_FILE_STATUS_FINALIZING:
      str = "finalizing";
      break;
    case OB_AI_BATCH_FILE_STATUS_COMPLETED:
      str = "completed";
      break;
    case OB_AI_BATCH_FILE_STATUS_FAILED:
      str = "failed";
      break;
    case OB_AI_BATCH_FILE_STATUS_EXPIRED:
      str = "expired";
      break;
    case OB_AI_BATCH_FILE_STATUS_CANCELLED:
      str = "cancelled";
      break;
    default:
      str = "unknown";
      break;
  }
  return str;
}

int64_t ObAiBatchFileManagerUtils::calculate_poll_interval(ObAiBatchFileStatus status)
{
  int64_t interval = ObAiBatchFileManager::DEFAULT_POLL_INTERVAL_US;

  switch (status) {
    case OB_AI_BATCH_FILE_STATUS_QUEUED:
      // Poll more frequently when queued
      interval = 30 * 1000 * 1000;  // 30 seconds
      break;
    case OB_AI_BATCH_FILE_STATUS_IN_PROGRESS:
      // Standard polling when in progress
      interval = 60 * 1000 * 1000;  // 60 seconds
      break;
    case OB_AI_BATCH_FILE_STATUS_FINALIZING:
      // Poll more frequently when finalizing
      interval = 10 * 1000 * 1000;  // 10 seconds
      break;
    default:
      interval = 60 * 1000 * 1000;  // 60 seconds default
      break;
  }
  return interval;
}


int ObAiBatchFileManager::map_http_status_to_error_code(const int64_t http_status)
{
  switch (http_status) {
    case 0:              return OB_TIMEOUT;
    case 400:            return OB_INVALID_ARGUMENT;
    case 401: case 403:  return OB_ERR_NO_PRIVILEGE;
    case 404:            return OB_ENTRY_NOT_EXIST;
    case 408:            return OB_TIMEOUT;
    case 413:            return OB_SIZE_OVERFLOW;
    case 429:            return OB_EAGAIN;
    default:
      if (http_status >= 500) return OB_RPC_POST_ERROR;
      return OB_INVALID_ARGUMENT;
  }
}

} // namespace share
} // namespace oceanbase