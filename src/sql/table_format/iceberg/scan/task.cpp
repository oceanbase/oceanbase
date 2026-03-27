/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/scan/task.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

FileScanTask::FileScanTask(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      pos_delete_files(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      eq_delete_files(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      dv_files(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

} // namespace iceberg

} // namespace sql
} // namespace oceanbase
