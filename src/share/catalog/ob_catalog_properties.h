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

#include "common/object/ob_object.h"
#include "lib/json/ob_json.h"
#include "lib/string/ob_string.h"
#include "sql/parser/parse_node.h"

#ifndef _OB_CATALOG_PROPERTIES_H_
#define _OB_CATALOG_PROPERTIES_H_

namespace oceanbase
{
namespace share
{

class ObCatalogProperties
{
public:
  enum class CatalogType
  {
    INVALID_TYPE = -1,
    ODPS_TYPE,
    MAX_TYPE
  };
  ObCatalogProperties() : type_(CatalogType::INVALID_TYPE) {}
  ObCatalogProperties(CatalogType type) : type_(type) {}
  virtual ~ObCatalogProperties() {}
  int to_string_with_alloc(ObString &str, ObIAllocator &allocator) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int to_string(char *buf, const int64_t buf_len, int64_t &pos) const;
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
  virtual int load_from_string(const common::ObString &str, common::ObIAllocator &allocator) = 0;
  static int parse_catalog_type(const common::ObString &str, CatalogType &type);
  static int resolve_catalog_type(const ParseNode &node, CatalogType &type);
  virtual int resolve_catalog_properties(const ParseNode &node) = 0;
  int encrypt_str(common::ObString &src, common::ObString &dst, ObIAllocator &allocator);
  int decrypt_str(common::ObString &src, common::ObString &dst, ObIAllocator &allocator);
  virtual int encrypt(ObIAllocator &allocator) = 0;
  virtual int decrypt(ObIAllocator &allocator) = 0;

public:
  CatalogType type_;
  static const char *CATALOG_TYPE_STR[];
};

class ObODPSCatalogProperties : public ObCatalogProperties
{
public:
  enum class ObOdpsCatalogOptions
  {
    ACCESSTYPE = 0,
    ACCESSID,
    ACCESSKEY,
    STSTOKEN,
    ENDPOINT,
    TUNNEL_ENDPOINT,
    PROJECT_NAME,
    QUOTA_NAME,
    COMPRESSION_CODE,
    REGION,
    MAX_OPTIONS
  };
  ObODPSCatalogProperties() : ObCatalogProperties(CatalogType::ODPS_TYPE) {}
  virtual ~ObODPSCatalogProperties() {}
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int load_from_string(const common::ObString &str,
                               common::ObIAllocator &allocator) override;
  virtual int resolve_catalog_properties(const ParseNode &node) override;
  virtual int encrypt(ObIAllocator &allocator) override;
  virtual int decrypt(ObIAllocator &allocator) override;

public:
  static constexpr const char *OPTION_NAMES[] = {
      "ACCESSTYPE",
      "ACCESSID",
      "ACCESSKEY",
      "STSTOKEN",
      "ENDPOINT",
      "TUNNEL_ENDPOINT",
      "PROJECT_NAME",
      "QUOTA_NAME",
      "COMPRESSION_CODE",
      "REGION",
  };
  common::ObString access_type_;
  common::ObString access_id_;
  common::ObString access_key_;
  common::ObString sts_token_;
  common::ObString endpoint_;
  common::ObString tunnel_endpoint_;
  common::ObString project_;
  common::ObString quota_;
  common::ObString compression_code_;
  common::ObString region_;
};

} // namespace share
} // namespace oceanbase

#endif //_OB_CATALOG_PROPERTIES_H_