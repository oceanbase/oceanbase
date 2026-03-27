/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_uniq_task_queue.h"
namespace oceanbase
{
namespace observer
{
void *ObHighPrioMemAllocator::alloc(const int64_t sz)
{
  void *mem = NULL;
  int ret = common::OB_SUCCESS;
  if (sz > 0) {
    mem = common::ob_malloc(sz, attr_);
    if (NULL == mem) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "alloc memory failed", K(ret), K(sz), K_(attr_.label));
    }
  }
  return mem;
}

void ObHighPrioMemAllocator::free(void *p)
{
  if (NULL != p) {
    common::ob_free(p);
    p = NULL;
  }
}
}
}
