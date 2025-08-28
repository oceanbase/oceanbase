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
