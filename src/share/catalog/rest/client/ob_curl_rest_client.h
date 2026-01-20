/**
* Copyright (c) 2023 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*/

#ifndef _SHARE_OB_CURL_REST_CLIENT_H
#define _SHARE_OB_CURL_REST_CLIENT_H

#include <curl/curl.h>
#include "lib/string/ob_string.h"
#include "share/catalog/rest/client/ob_base_rest_client.h"

namespace oceanbase
{
namespace share
{
class ObCurlRestClient : public ObBaseRestClient
{
public:
  ObCurlRestClient(common::ObIAllocator &allocator)
      : ObBaseRestClient(allocator), curl_(nullptr),
        header_list_(nullptr)
  {
  }

  ~ObCurlRestClient() {
    if (is_inited_) {
      destroy();
    }
  }

  virtual int execute(const ObRestHttpRequest &request, ObRestHttpResponse &response) override;

private:
  CURL *curl_;
  struct curl_slist *header_list_;

  virtual int do_init(const ObRestCatalogProperties &properties) override;
  virtual int do_destroy() override;
  virtual int do_reuse() override;
  int init_curl();
  void cleanup_curl();
  int set_curl_options(const ObRestHttpRequest &request, ObRestHttpResponse &response);
  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp);
};

}
}

#endif /* _SHARE_OB_CURL_REST_CLIENT_H */