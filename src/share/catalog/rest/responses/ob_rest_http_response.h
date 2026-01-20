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

#ifndef _SHARE_OB_REST_HTTP_RESPONSE_H
#define _SHARE_OB_REST_HTTP_RESPONSE_H

#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"

namespace oceanbase
{
namespace share
{

class ObRestHttpResponse
{
public:
  explicit ObRestHttpResponse(ObIAllocator &allocator)
    : body_(), status_code_(0),
      content_length_(0), content_type_(),
      allocator_(allocator)
  {
  }
  virtual ~ObRestHttpResponse() = default;
  int64_t get_status_code() const { return status_code_; }
  int64_t get_content_length() const { return content_length_; }
  const ObString &get_content_type() const { return content_type_; }
  const ObString get_body() const { return body_.string(); }

  void set_status_code(int64_t status_code) { status_code_ = status_code; }
  void set_content_length(int64_t content_length) { content_length_ = content_length; }
  int set_content_type(const ObString &content_type) { return ob_write_string(allocator_, content_type, content_type_, true/*c_style*/); }
  int reset();
  virtual int do_reset();
  virtual int parse_from_response();
  TO_STRING_KV(K_(status_code),
               K_(content_length),
               K_(content_type),
               K_(body));
  ObSqlString body_;
private:
  int64_t status_code_;
  int64_t content_length_;
  ObString content_type_;
protected:
  ObIAllocator &allocator_;
  static constexpr int64_t DEFAULT_MAP_BUCKET_NUM = 4;
  static int parse_json_to_map(const ObIJsonBase *json_node,
                               common::hash::ObHashMap<ObString, ObString> &map,
                               ObIAllocator &allocator);
  static int parse_json_to_array(const ObIJsonBase *json_node,
                                 ObArray<ObString> &array,
                                 ObIAllocator &allocator);
  static int parse_json_to_string(const ObIJsonBase *json_node,
                                  ObString &string,
                                  ObIAllocator *allocator = nullptr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObRestHttpResponse);
};

class ObRestConfigResponse : public ObRestHttpResponse
{
public:
  explicit ObRestConfigResponse(ObIAllocator &allocator)
    : ObRestHttpResponse(allocator),
      default_(), override_(),
      endpoints_()
  {
  }
  virtual ~ObRestConfigResponse() {
    if (default_.created()) {
      default_.destroy();
    }
    if (override_.created()) {
      override_.destroy();
    }
  }
  virtual int parse_from_response() override;
  virtual int do_reset() override;
  common::hash::ObHashMap<ObString, ObString> default_;
  common::hash::ObHashMap<ObString, ObString> override_;
  ObArray<ObString> endpoints_;
private:
  static constexpr const char *DEFAULT_CONFIG_KEY = "default";
  static constexpr const char *OVERRIDE_CONFIG_KEY = "override";
  static constexpr const char *ENDPOINTS_KEY = "endpoints";
};

class ObRestListNamespacesResponse : public ObRestHttpResponse
{
public:
  explicit ObRestListNamespacesResponse(ObIAllocator &allocator)
    : ObRestHttpResponse(allocator),
      namespaces_()
  {
  }
  virtual ~ObRestListNamespacesResponse() = default;
  virtual int parse_from_response() override;
  virtual int do_reset() override;
  ObArray<ObString> namespaces_;
private:
  static constexpr const char *NAMESPACES_KEY = "namespaces";
};

class ObRestGetNamespaceResponse : public ObRestHttpResponse
{
public:
  explicit ObRestGetNamespaceResponse(ObIAllocator &allocator)
    : ObRestHttpResponse(allocator),
      database_name_(), properties_()
  {
  }
  virtual ~ObRestGetNamespaceResponse() {
    if (properties_.created()) {
      properties_.destroy();
    }
  }
  virtual int parse_from_response() override;
  virtual int do_reset() override;
  ObString database_name_;
  common::hash::ObHashMap<ObString, ObString> properties_;
private:
  static constexpr const char *DATABASE_KEY = "namespace";
  static constexpr const char *PROPERTIES_KEY = "properties";
};

class ObRestListTablesResponse : public ObRestHttpResponse
{
public:
  explicit ObRestListTablesResponse(ObIAllocator &allocator)
    : ObRestHttpResponse(allocator),
      tables_()
  {
  }
  virtual ~ObRestListTablesResponse() = default;
  virtual int parse_from_response() override;
  virtual int do_reset() override;
  ObArray<ObString> tables_;
private:
  static constexpr const char *IDENTIFIERS_KEY = "identifiers";
  static constexpr const char *NAMESPACE_KEY = "namespace";
  static constexpr const char *NAME_KEY = "name";
};

class ObRestLoadTableResponse : public ObRestHttpResponse
{
public:
  explicit ObRestLoadTableResponse(ObIAllocator &allocator)
    : ObRestHttpResponse(allocator),
      table_metadata_(allocator), metadata_location_(),
      accessid_(), accesskey_(), endpoint_(), region_()
  {
  }
  virtual ~ObRestLoadTableResponse()
  {
  }
  virtual int parse_from_response() override;
  virtual int do_reset() override;
  sql::iceberg::TableMetadata table_metadata_;
  ObString metadata_location_;
  ObString accessid_;
  ObString accesskey_;
  ObString endpoint_;
  ObString region_;

private:
  static constexpr const char *METADATA_LOCATION_KEY = "metadata-location";
  static constexpr const char *METADATA_KEY = "metadata";
  static constexpr const char *CONFIG_KEY = "config";
  static constexpr const char *STORAGE_CREDENTIALS_KEY = "storage-credentials";
  static constexpr const char *ACCESS_ID_KEY_SUFFIX = "access-key-id";
  static constexpr const char *SECRET_ACCESS_KEY_SUFFIX_1 = "secret-access-key";
  static constexpr const char *SECRET_ACCESS_KEY_SUFFIX_2 = "access-key-secret";
  static constexpr const char *ENDPOINT_KEY_SUFFIX = "endpoint";
  static constexpr const char *REGION_KEY_SUFFIX = "region";
};

}
}

#endif /* _SHARE_OB_REST_HTTP_RESPONSE_H */