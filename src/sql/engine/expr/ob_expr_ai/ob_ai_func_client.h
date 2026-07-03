/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_AI_FUNC_CLIENT_H_
#define OB_AI_FUNC_CLIENT_H_

#include <atomic>
#include <curl/curl.h>
#include "ob_ai_func.h"

namespace oceanbase
{
namespace common
{

// Result structure for individual request in batch
struct ObAIBatchItemResult
{
  int64_t http_code_;           // HTTP status code, 0 means network failure
  ObJsonObject *response_;      // Response JSON (valid when success)
  CURLcode curl_code_;          // curl error code (valid when network failure)

  ObAIBatchItemResult()
      : http_code_(0), response_(nullptr), curl_code_(CURLE_OK) {}

  bool is_success() const { return http_code_ >= 200 && http_code_ < 300 && OB_NOT_NULL(response_); }

  TO_STRING_KV(K_(http_code), KP_(response), K_(curl_code));
};

class ObAIFuncClient: public ObAIFuncHandle
{
public:
  ObAIFuncClient();
  virtual ~ObAIFuncClient();
  int init(common::ObIAllocator &allocator, const ObString &url, ObArray<ObString> &headers);
  void clean_up();
  void reset();
  void set_timeout_sec(int64_t timeout_sec) { timeout_sec_ = timeout_sec; }
  // Check if HTTP status code is retryable (network failure, rate limited, or server error)
  static bool is_retryable_status_code(int64_t http_code);
  static bool is_retryable_curl_code(int curl_code);
  // ai function interface
  virtual int send_post(common::ObIAllocator &allocator,
                        const ObString &url,
                        ObArray<ObString> &headers,
                        ObJsonObject *data,
                        ObJsonObject *&response) override;
  virtual int send_post_batch(common::ObIAllocator &allocator,
                              const ObString &url,
                              ObArray<ObString> &headers,
                              ObArray<ObJsonObject *> &data_array,
                              ObArray<ObJsonObject *> &responses) override;
  // batch interface with individual result (for adaptive concurrency)
  int send_post_batch_with_result(common::ObIAllocator &allocator,
                                  const ObString &url,
                                  ObArray<ObString> &headers,
                                  ObArray<ObJsonObject *> &data_array,
                                  ObArray<ObAIBatchItemResult> &results);
  // embedding service interface
  int send_post_batch_no_wait(ObArray<ObJsonObject *> &data_array);
  bool check_batch_finished();
  int get_batch_result(ObArray<ObJsonObject *> &responses);
  int get_batch_result(ObArray<ObAIBatchItemResult> &results);
private:
  int error_handle(CURLcode res);
  int send_post(ObJsonObject *data, ObJsonObject *&response);
  int send_post_batch(ObArray<ObJsonObject *> &data_array, ObArray<ObJsonObject *> &responses);
  // Core batch send implementation, returns individual results
  int send_post_batch_internal(ObArray<ObJsonObject *> &data_array, ObArray<ObAIBatchItemResult> &results);
  int init_easy_handle(CURL *curl, ObJsonObject *data, ObStringBuffer &response_buf);
  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp);
  bool is_timeout();
private:
  static const int64_t CURL_MAX_TIMEOUT_SEC;
  common::ObIAllocator *allocator_;
  char *url_;
  struct curl_slist *header_list_;
  CURLM *curlm_;
  CURL *curl_;
  ObArray<CURL *> curl_handles_;
  ObArray<ObStringBuffer *> response_buffers_;
  // atomic boolean value, used to check if the batch task is finished
  std::atomic<bool> is_finished_;
  int64_t max_retry_times_;
  int64_t abs_timeout_ts_;
  int64_t timeout_sec_;
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncClient);
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_CLIENT_H_ */