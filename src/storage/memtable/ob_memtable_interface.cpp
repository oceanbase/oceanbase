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

#include "lib/allocator/ob_pcounter.h"

#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase
{
using namespace common;
namespace memtable
{

////////////////////////////////////////////////////////////////////////////////////////////////////

int64_t ObMemtableFactory::alloc_count_ = 0;
int64_t ObMemtableFactory::free_count_ = 0;

ObMemtableFactory::ObMemtableFactory()
{
}

ObMemtableFactory::~ObMemtableFactory()
{
}

ObMemtable *ObMemtableFactory::alloc(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_MEMTABLE_OBJECT, ObCtxIds::MEMSTORE_CTX_ID);
  ObMemtable *mt = NULL;
  void *mt_buffer = ob_malloc(sizeof(ObMemtable), memattr);
  if (NULL == mt_buffer) {
    TRANS_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc memory for memtable fail");
  } else if (NULL == (mt = new(mt_buffer) ObMemtable())) {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "construct object of memtable fail");
    ob_free(mt_buffer);
    mt_buffer = NULL;
  } else {
    PC_ADD(MT, 1);
    (void)ATOMIC_AAF(&alloc_count_, 1);
  }
  TRANS_LOG(INFO, "alloc memtable", KP(mt), K_(alloc_count), K_(free_count));
  return mt;
}

void ObMemtableFactory::free(ObMemtable *mt)
{
  if (NULL != mt) {
    mt->~ObMemtable();
    ob_free(mt);
    (void)ATOMIC_AAF(&free_count_, 1);
    TRANS_LOG(INFO, "free memtable", KP(mt), K_(alloc_count), K_(free_count));
    mt = NULL;
    PC_ADD(MT, -1);
  }
}

}
}
