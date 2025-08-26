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

#ifndef _OB_OCEANBASE_SCHEMA_LOCATION_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_LOCATION_SCHEMA_STRUCT_H

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTenantLocationId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantLocationId() : ObTenantCommonSchemaId() {}
  ObTenantLocationId(const uint64_t tenant_id, const uint64_t location_id)
    : ObTenantCommonSchemaId(tenant_id, location_id) {}
};

class ObLocationSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObLocationSchema();
  explicit ObLocationSchema(common::ObIAllocator *allocator);
  virtual ~ObLocationSchema();

  explicit ObLocationSchema(const ObLocationSchema &other);
  ObLocationSchema &operator=(const ObLocationSchema &other);

  int assign(const ObLocationSchema &other);

  virtual bool is_valid() const;
  virtual void reset();

  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline void set_location_id(const uint64_t location_id) { location_id_ = location_id; }
  inline int set_location_name(const char *name) { return deep_copy_str(name, location_name_); }
  inline int set_location_name(const common::ObString &name) { return deep_copy_str(name, location_name_); }
  inline int set_location_url(const char *path) { return deep_copy_str(path, location_url_); }
  inline int set_location_url(const common::ObString &path) { return deep_copy_str(path, location_url_); }
  inline int set_location_access_info(const char *access_info) { return deep_copy_str(access_info, location_access_info_); }
  inline int set_location_access_info(const common::ObString &access_info) { return deep_copy_str(access_info, location_access_info_); }
  inline void set_name_case_mode(const common::ObNameCaseMode mode) {name_case_mode_ = mode; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_location_id() const { return location_id_; }
  inline const char *get_location_name() const { return extract_str(location_name_); }
  inline const common::ObString &get_location_name_str() const { return location_name_; }
  inline const char *get_location_url() const { return extract_str(location_url_); }
  inline const common::ObString &get_location_url_str() const { return location_url_; }
  inline const char *get_location_access_info() const { return extract_str(location_access_info_); }
  inline const common::ObString &get_location_access_info_str() const { return location_access_info_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }

  inline ObTenantLocationId get_tenant_location_id() const { return ObTenantLocationId(tenant_id_, location_id_); }

  TO_STRING_KV(K_(tenant_id), K_(location_id), K_(schema_version),
               K_(location_name), K_(location_url), K_(location_access_info));
private:
  uint64_t tenant_id_;
  uint64_t location_id_;
  int64_t schema_version_;
  common::ObString location_name_;
  common::ObString location_url_;
  common::ObString location_access_info_;
  common::ObNameCaseMode name_case_mode_;//default:OB_NAME_CASE_INVALID
};

}
}
}
#endif