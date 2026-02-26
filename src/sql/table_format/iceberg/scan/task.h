/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef TASK_H
#define TASK_H

#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
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
