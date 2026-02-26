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

#pragma once

#include <memory>

#include <apache-arrow/arrow/api.h>

#include "plugin/external_table/ob_external_arrow_status.h"

namespace oceanbase {
namespace plugin {
namespace external {

/// TODO add columns and other schema information
class ObTableSchema
{
public:
  ObTableSchema() {}
  explicit ObTableSchema(const std::shared_ptr<arrow::Schema> &schema) : schema_(schema)
  {}

  const char *table_name() const;

  int64_t to_string(char buf[], int64_t buf_len) const;

private:
  std::shared_ptr<arrow::Schema> schema_;
};

} // namespace external
} // namespace plugin
} // namespace oceanbase
