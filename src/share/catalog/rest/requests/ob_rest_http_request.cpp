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

#define USING_LOG_PREFIX SHARE
#include "share/catalog/rest/requests/ob_rest_http_request.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{


ObRestHttpRequest::~ObRestHttpRequest()
{
  if (query_param_map_.created()) {
    query_param_map_.destroy();
  }
  if (header_map_.created()) {
    header_map_.destroy();
  }
}

int ObRestHttpRequest::get_concat_url(ObString &concat_url) const
{
  int ret = OB_SUCCESS;
  concat_url.reset();
  ObSqlString tmp_url;
  if (OB_FAIL(tmp_url.append(url_))) {
    LOG_WARN("failed to append url", K(ret));
  } else if (query_param_map_.created()) {
    common::hash::ObHashMap<common::ObString, common::ObString>::const_iterator iter = query_param_map_.begin();
    for (; OB_SUCC(ret) && iter != query_param_map_.end(); ++iter) {
      if (iter == query_param_map_.begin() && OB_FAIL(tmp_url.append("?"))) {
        LOG_WARN("failed to append query param", K(ret));
      } else if (iter != query_param_map_.begin() && OB_FAIL(tmp_url.append("&"))) {
        LOG_WARN("failed to append query param", K(ret));
      } else if (OB_FAIL(tmp_url.append_fmt("%.*s=%.*s",
                                                iter->first.length(), iter->first.ptr(),
                                                iter->second.length(), iter->second.ptr()))) {
        LOG_WARN("failed to append query param", K(ret), K(iter->first), K(iter->second));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator_, tmp_url.string(), concat_url))) {
    LOG_WARN("failed to write concat url", K(ret));
  }
  return ret;
}

int ObRestHttpRequest::add_query_param(const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (!query_param_map_.created()
       && OB_FAIL(query_param_map_.create(DEFAULT_MAP_BUCKET_NUM,
                                          lib::ObLabel("QueryParam")))) {
    LOG_WARN("failed to create query param map", K(ret));
  } else if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query param name is empty", K(ret));
  } else if (OB_FAIL(query_param_map_.set_refactored(name, value, 1))) {
    LOG_WARN("failed to add/update query param", K(ret), K(name), K(value));
  }
  return ret;
}

int ObRestHttpRequest::remove_query_param(const ObString &name)
{
  int ret = OB_SUCCESS;
  if (!query_param_map_.created()) {
    // do nothing
  } else if (OB_FAIL(query_param_map_.erase_refactored(name))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to remove query param from map", K(ret), K(name));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRestHttpRequest::get_query_param(const ObString &name, ObString &value) const
{
  int ret = OB_SUCCESS;
  value.reset();
  if (!query_param_map_.created()) {
    // do nothing
  } else if (OB_FAIL(query_param_map_.get_refactored(name, value))) {
    LOG_WARN("failed to get query param", K(ret), K(name));
  }
  return ret;
}

int ObRestHttpRequest::get_all_query_params(common::ObArray<common::ObString> &names,
                                            common::ObArray<common::ObString> &values) const
{
  int ret = OB_SUCCESS;
  names.reset();
  values.reset();
  if (!query_param_map_.created()) {
    // do nothing
  } else {
    common::hash::ObHashMap<common::ObString, common::ObString>::const_iterator iter = query_param_map_.begin();
    for (; OB_SUCC(ret) && iter != query_param_map_.end(); ++iter) {
      if (OB_FAIL(names.push_back(iter->first))) {
        LOG_WARN("failed to add query param name", K(ret), K(iter->first));
      } else if (OB_FAIL(values.push_back(iter->second))) {
        LOG_WARN("failed to add query param value", K(ret), K(iter->second));
      }
    }
  }
  return ret;
}

int ObRestHttpRequest::get_all_query_params(ObString &query_params) const
{
  int ret = OB_SUCCESS;
  query_params.reset();
  if (!query_param_map_.created()) {
    // do nothing
  } else {
    ObSqlString query_param_str;
    common::hash::ObHashMap<common::ObString, common::ObString>::const_iterator iter = query_param_map_.begin();
    for (; OB_SUCC(ret) && iter != query_param_map_.end(); ++iter) {
      if (iter != query_param_map_.begin() && OB_FAIL(query_param_str.append(","))) {
        LOG_WARN("failed to append query param", K(ret));
      } else if (OB_FAIL(query_param_str.append_fmt("%.*s=%.*s",
                                             iter->first.length(), iter->first.ptr(),
                                             iter->second.length(), iter->second.ptr()))) {
        LOG_WARN("failed to append query param", K(ret), K(iter->first), K(iter->second));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator_, query_param_str.string(), query_params))) {
      LOG_WARN("failed to write query params", K(ret));
    }
  }
  return ret;
}

int ObRestHttpRequest::add_header(const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (!header_map_.created() && OB_FAIL(header_map_.create(DEFAULT_MAP_BUCKET_NUM,
                                                           lib::ObLabel("Header")))) {
    LOG_WARN("failed to create header map", K(ret));
  } else if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("header name is empty", K(ret));
  } else if (OB_FAIL(header_map_.set_refactored(name, value, 1))) {
    LOG_WARN("failed to add/update header", K(ret), K(name), K(value));
  }
  return ret;
}

int ObRestHttpRequest::remove_header(const ObString &name)
{
  int ret = OB_SUCCESS;
  if (!header_map_.created()) {
    // do nothing
  } else if (OB_FAIL(header_map_.erase_refactored(name))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to remove header from map", K(ret), K(name));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRestHttpRequest::get_header(const ObString &name, ObString &value) const
{
  int ret = OB_SUCCESS;
  value.reset();
  if (!header_map_.created()) {
    // do nothing
  } else if (OB_FAIL(header_map_.get_refactored(name, value))) {
    LOG_WARN("failed to get header", K(ret), K(name));
  }
  return ret;
}

int ObRestHttpRequest::get_all_headers(common::ObArray<common::ObString> &names,
                                       common::ObArray<common::ObString> &values) const
{
  int ret = OB_SUCCESS;
  names.reset();
  values.reset();
  if (!header_map_.created()) {
    // do nothing
  } else {
    common::hash::ObHashMap<common::ObString, common::ObString>::const_iterator iter = header_map_.begin();
    for (; OB_SUCC(ret) && iter != header_map_.end(); ++iter) {
      if (OB_FAIL(names.push_back(iter->first))) {
        LOG_WARN("failed to add header name", K(ret), K(iter->first));
      } else if (OB_FAIL(values.push_back(iter->second))) {
        LOG_WARN("failed to add header value", K(ret), K(iter->second));
      }
    }
  }
  return ret;
}

int ObRestHttpRequest::get_all_headers(ObString &headers) const
{
  int ret = OB_SUCCESS;
  headers.reset();
  if (!header_map_.created()) {
    // do nothing
  } else {
    ObSqlString header_str;
    common::hash::ObHashMap<common::ObString, common::ObString>::const_iterator iter = header_map_.begin();
    for (; OB_SUCC(ret) && iter != header_map_.end(); ++iter) {
      if (iter != header_map_.begin() && OB_FAIL(header_str.append(","))) {
        LOG_WARN("failed to append header", K(ret));
      } else if (OB_FAIL(header_str.append_fmt("%.*s:%.*s", iter->first.length(), iter->first.ptr(), iter->second.length(), iter->second.ptr()))) {
        LOG_WARN("failed to append header", K(ret), K(iter->first), K(iter->second));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator_, header_str.string(), headers))) {
      LOG_WARN("failed to write header", K(ret));
    }
  }
  return ret;
}

int ObRestHttpRequest::get_all_header_names(ObString &names) const
{
  int ret = OB_SUCCESS;
  names.reset();
  if (!header_map_.created()) {
    // do nothing
  } else {
    ObSqlString header_str;
    common::hash::ObHashMap<common::ObString, common::ObString>::const_iterator iter = header_map_.begin();
    for (; OB_SUCC(ret) && iter != header_map_.end(); ++iter) {
      if (iter != header_map_.begin() && OB_FAIL(header_str.append(";"))) {
        LOG_WARN("failed to append header", K(ret));
      } else if (OB_FAIL(header_str.append_fmt("%.*s", iter->first.length(), iter->first.ptr()))) {
        LOG_WARN("failed to append header", K(ret), K(iter->first));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator_, header_str.string(), names))) {
      LOG_WARN("failed to write header", K(ret));
    }
  }
  return ret;
}

}
}
