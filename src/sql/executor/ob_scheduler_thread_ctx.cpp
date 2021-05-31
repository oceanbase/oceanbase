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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_scheduler_thread_ctx.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

int ObSchedulerThreadCtx::add_last_failed_partition(const ObTaskInfo::ObRangeLocation& loc)
{
  int ret = OB_SUCCESS;
  ObTaskInfo::ObRangeLocation* buffer = NULL;
  if (OB_ISNULL(buffer = static_cast<ObTaskInfo::ObRangeLocation*>(
                    sche_allocator_.alloc(sizeof(ObTaskInfo::ObRangeLocation))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no more memory. allocate failed", K(ret));
  } else {
    ObTaskInfo::ObRangeLocation* new_loc = new (buffer) ObTaskInfo::ObRangeLocation(sche_allocator_);
    if (OB_FAIL(new_loc->assign(loc))) {
      LOG_WARN("copy range location failed", K(ret));
    } else if (OB_FAIL(last_failed_partitions_.push_back(new_loc))) {
      LOG_WARN("push range location failed", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
