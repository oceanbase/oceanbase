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
