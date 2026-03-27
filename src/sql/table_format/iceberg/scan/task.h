/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef TASK_H
#define TASK_H

#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/spec.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class FileScanTask : public SpecWithAllocator
{
public:
  explicit FileScanTask(ObIAllocator &allocator);
  TO_STRING_EMPTY();
  int64_t start;
  int64_t length;
  ObString data_file_path;
  DataFileContent data_file_content;
  DataFileFormat data_file_format;
  ObArray<const ManifestEntry *> pos_delete_files;
  ObArray<const ManifestEntry *> eq_delete_files;
  ObArray<const ManifestEntry *> dv_files;
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // TASK_H
