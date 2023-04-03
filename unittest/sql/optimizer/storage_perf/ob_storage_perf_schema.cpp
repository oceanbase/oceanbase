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

#include "ob_storage_perf_schema.h"
#include "ob_storage_perf_config.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storageperf
{
int MySchemaService::init(const char *file_name)
{
  int ret = OB_SUCCESS;
  schema_guard_ = NULL;
  if (OB_SUCCESS != (ret = restore_schema_.init())
      || OB_SUCCESS != (ret = restore_schema_.parse_from_file(
          file_name, schema_guard_))) {
    STORAGE_LOG(ERROR, "fail to get schema manger");
  }
  return ret;
}

int MySchemaService::add_schema(const char *file_name)
{
  int ret = OB_SUCCESS;
  schema_guard_ = NULL;
  if (OB_SUCCESS != (ret = restore_schema_.parse_from_file(
          file_name, schema_guard_))) {
    STORAGE_LOG(ERROR, "failed to add schema");
  }
  return ret;
}
/*
int MySchemaService::get_all_schema(ObSchemaManager &out_schema, const int64_t frozen_version)
{
  UNUSED(frozen_version);
  return out_schema.assign(*manager_, true);
}

const ObSchemaManager *MySchemaService::get_user_schema_manager(
    const int64_t version)
{
  UNUSED(version);
  return manager_;
}

const ObSchemaManager *MySchemaService::get_schema_manager_by_version(
    const int64_t version,
    const bool for_merge)
{
  UNUSED(version);
  UNUSED(for_merge);
  return manager_;
}

int MySchemaService::release_schema(const ObSchemaManager *schema)
{
  UNUSED(schema);
  return OB_SUCCESS;
}
*/
}//end storageperf
}//end oceanbase
