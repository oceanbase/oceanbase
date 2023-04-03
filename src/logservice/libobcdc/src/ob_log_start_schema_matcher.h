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
 *
 * Data Start Schema Matcher
 */

#ifndef OCEANBASE_LIBOBCDC_START_SCHEMA_MATCHER_H__
#define OCEANBASE_LIBOBCDC_START_SCHEMA_MATCHER_H__

#include "lib/utility/ob_print_utils.h"          // TO_STRING_KV
#include "lib/container/ob_array.h"              // ObArray

namespace oceanbase
{
namespace libobcdc
{
class IObLogStartSchemaMatcher
{
public:
  virtual ~IObLogStartSchemaMatcher() {}

public:
  // match function
  virtual int match_data_start_schema_version(const uint64_t tenant_id,
      bool &match,
      int64_t &schema_version) = 0;
};


/*
 * Impl.
 *
 */
class ObLogStartSchemaMatcher : public IObLogStartSchemaMatcher
{
  const char* DEFAULT_START_SCHEMA_VERSION_STR = "|";
public:
  ObLogStartSchemaMatcher();
  virtual ~ObLogStartSchemaMatcher();

public:
  int init(const char *schema_version_str);
  int destroy();

  // Matches a tenant and returns the schema_version set according to the profile
  int match_data_start_schema_version(const uint64_t tenant_id, bool &match, int64_t &schema_version);

private:
  // Initialising the configuration according to the configuration file
  int set_pattern_(const char *schema_version_str);

  int build_tenant_schema_version_();

private:
  struct TenantSchema
  {
    uint64_t tenant_id_;
    int64_t schema_version_;

    void reset();

    TO_STRING_KV(K(tenant_id_), K(schema_version_));
  };
  typedef common::ObArray<TenantSchema> TenantSchemaArray;

private:
  char *buf_;
  int64_t buf_size_;

  TenantSchemaArray tenant_schema_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStartSchemaMatcher);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_START_SCHEMA_MATCHER_H__ */
