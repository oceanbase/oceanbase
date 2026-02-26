/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
