/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
