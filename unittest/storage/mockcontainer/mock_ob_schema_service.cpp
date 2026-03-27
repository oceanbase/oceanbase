/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "mock_ob_schema_service.h"

namespace oceanbase
{

using namespace common;
using namespace sql;

namespace share
{
namespace schema
{

int MockObSchemaService::init(const char *schema_file)
{
  int ret = OB_SUCCESS;

  if (NULL == schema_file) {
    STORAGE_LOG(ERROR, "invalid argument", "schema_file", OB_P(schema_file));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = restore_schema_.init())
      || OB_SUCCESS != (ret = restore_schema_.parse_from_file(schema_file, schema_guard_))) {
    STORAGE_LOG(ERROR, "fail to get schema manger", K(schema_file));
  } else {
    STORAGE_LOG(INFO, "MockObSchemaService init success", K(schema_file));
  }

  return ret;
}

const ObSchemaGetterGuard *MockObSchemaService::get_schema_guard(
    const int64_t version)
{
  UNUSED(version);
  return schema_guard_;
}

//int MockObSchemaService::release_schema(const ObSchemaManager *schema)
//{
//  UNUSED(schema);
//  return OB_SUCCESS;
//}
//
//const ObSchemaManager *MockObSchemaService::get_schema_manager_by_version(
//    const int64_t version,
//    const bool for_merge)
//{
//  UNUSED(version);
//  UNUSED(for_merge);
//  return manager_;
//}
//
//int64_t MockObSchemaService::get_latest_local_version(const bool core_schema_version) const
//{
//  UNUSED(core_schema_version);
//  int64_t version = 2;
//  return version;
//}
//
//int64_t MockObSchemaService::get_received_broadcast_version(const bool core_schema_version) const
//{
//  UNUSED(core_schema_version);
//  int64_t version = 2;
//  return version;
//}

} // schema
} // share
} // oceanbase
