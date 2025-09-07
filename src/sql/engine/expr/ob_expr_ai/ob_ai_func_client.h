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

#ifndef OB_AI_FUNC_CLIENT_H_
#define OB_AI_FUNC_CLIENT_H_

#include <atomic>
#include <curl/curl.h>
#include "ob_ai_func.h"

namespace oceanbase
{
namespace common
{
class ObAIFuncClient: public ObAIFuncHandle
{
public:
  ObAIFuncClient();
  virtual ~ObAIFuncClient();
  int init(common::ObIAllocator &allocator, ObString &url, ObArray<ObString> &headers);
  void clean_up();
  void reset();
  void set_timeout_sec(int64_t timeout_sec) { timeout_sec_ = timeout_sec; }
  // ai function interface
  virtual int send_post(common::ObIAllocator &allocator,
                        ObString &url,
                        ObArray<ObString> &headers,
                        ObJsonObject *data,
                        ObJsonObject *&response) override;
  virtual int send_post_batch(common::ObIAllocator &allocator,
                              ObString &url,
                              ObArray<ObString> &headers,
                              ObArray<ObJsonObject *> &data_array,
                              ObArray<ObJsonObject *> &responses) override;
  // embedding service interface
  int send_post_batch_no_wait(ObArray<ObJsonObject *> &data_array);
  bool check_batch_finished();
  int get_batch_result(ObArray<ObJsonObject *> &responses);
private:
  int error_handle(CURLcode res);
  int send_post(ObJsonObject *data, ObJsonObject *&response);
  int send_post_batch(ObArray<ObJsonObject *> &data_array, ObArray<ObJsonObject *> &responses);
  int init_easy_handle(CURL *curl, ObJsonObject *data, ObStringBuffer &response_buf);
  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp);
  bool is_retryable_status_code(int64_t http_code);
  bool is_timeout();
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