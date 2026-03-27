/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_table_schema.h"
#include "plugin/external_table/ob_external_arrow_status.h"

namespace oceanbase {
namespace plugin {
namespace external {

constexpr const char *EXTERNAL_TABLE_NAME_KEY_NAME = "TableName";

const char *ObTableSchema::table_name() const
{
  const char *table_name = nullptr;
  const shared_ptr<const KeyValueMetadata>& metadata = schema_->metadata();
  int index = !metadata ? -1 : metadata->FindKey(EXTERNAL_TABLE_NAME_KEY_NAME);
  if (-1 == index) {
  } else {
    table_name = metadata->value(index).c_str();
  }
  return table_name;
}

} // namespace external
} // namespace plugin
} // namespace oceanbase
