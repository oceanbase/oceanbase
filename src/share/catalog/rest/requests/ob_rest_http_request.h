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

#ifndef _SHARE_OB_HTTP_REQUEST_H
#define _SHARE_OB_HTTP_REQUEST_H

#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace share
{

enum ObRestHttpMethod
{
  HTTP_METHOD_INVALID = 0,
  HTTP_METHOD_HEAD,
  HTTP_METHOD_GET,
  HTTP_METHOD_POST,
  HTTP_METHOD_PUT,
  HTTP_METHOD_DELETE,
};

static constexpr const char *REST_HTTP_METHOD_NAMES[] = {
  "INVALID",
  "HEAD",
  "GET",
  "POST",
  "PUT",
  "DELETE",
};

class ObRestHttpRequest
{
public:
  explicit ObRestHttpRequest(common::ObIAllocator &allocator)
    : method_(HTTP_METHOD_INVALID), url_(),
      query_param_map_(), header_map_(),
      content_type_(), body_(),
      allocator_(allocator)
  {
  }
  ~ObRestHttpRequest();
  void set_method(ObRestHttpMethod method) { method_ = method; }
  ObRestHttpMethod get_method() const { return method_; }

  int set_url(const ObString &url) { return ob_write_string(allocator_, url, url_, true /*c_style*/); }
  const ObString &get_url() const { return url_; }
  int get_concat_url(ObString &concat_url) const;

  int add_query_param(const ObString &name, const ObString &value);
  int remove_query_param(const ObString &name);
  int get_query_param(const ObString &name, ObString &value) const;
  int get_query_param_count() const { return query_param_map_.size(); }
  int get_all_query_params(common::ObArray<common::ObString> &names,
                           common::ObArray<common::ObString> &values) const;
  int get_all_query_params(ObString &query_params) const;

  int add_header(const ObString &name, const ObString &value);
  int remove_header(const ObString &name);
  int get_header(const ObString &name, ObString &value) const;
  int get_header_count() const { return header_map_.size(); }
  int get_all_headers(common::ObArray<common::ObString> &names,
                      common::ObArray<common::ObString> &values) const;
  int get_all_headers(ObString &headers) const;
  int get_all_header_names(ObString &names) const;

  int set_content_type(const ObString &content_type) { return ob_write_string(allocator_, content_type, content_type_); }
  const ObString &get_content_type() const { return content_type_; }

  int set_body(const ObString &body) { return ob_write_string(allocator_, body, body_); }
  const ObString &get_body() const { return body_; }
  TO_STRING_KV(K_(method), K_(url),
               K_(content_type),
               K_(body));

private:
  ObRestHttpMethod method_;
  ObString url_;
  common::hash::ObHashMap<common::ObString, common::ObString> query_param_map_;
  common::hash::ObHashMap<common::ObString, common::ObString> header_map_;
  ObString content_type_;
  ObString body_;
  common::ObIAllocator &allocator_;
  static constexpr int64_t DEFAULT_MAP_BUCKET_NUM = 4;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRestHttpRequest);
};

// 后续实现POST类的API时可以继承此类, 并实现to_json_kv方法以填充body_

}
}

#endif /* _SHARE_OB_HTTP_REQUEST_H */