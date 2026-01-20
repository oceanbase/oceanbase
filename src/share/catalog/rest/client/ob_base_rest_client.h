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

#ifndef _SHARE_OB_BASE_REST_CLIENT_H
#define _SHARE_OB_BASE_REST_CLIENT_H

#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashmap.h"
#include "share/catalog/rest/requests/ob_rest_http_request.h"
#include "share/catalog/rest/responses/ob_rest_http_response.h"
#include "share/catalog/ob_catalog_properties.h"

namespace oceanbase
{
namespace share
{

class ObBaseRestClient
{
public:
  explicit ObBaseRestClient(common::ObIAllocator &allocator)
      : is_inited_(false), uri_(),
        client_pool_(nullptr), allocator_(allocator)
  {
  }
  virtual ~ObBaseRestClient() = default;
  int init(const ObRestCatalogProperties &properties);
  int destroy();
  int reuse();
  void *get_client_pool() const { return client_pool_; }
  void set_client_pool(void *client_pool) { client_pool_ = client_pool; }
  virtual int execute(const ObRestHttpRequest &request, ObRestHttpResponse &response) = 0;
protected:
  bool is_inited_;
  ObString uri_;         // base uri
  void *client_pool_;
  common::ObIAllocator &allocator_;

private:
  virtual int do_init(const ObRestCatalogProperties &properties) = 0;
  virtual int do_destroy() = 0;
  virtual int do_reuse() = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseRestClient);
};

} // namespace share
} // namespace oceanbase

#endif