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

#include "storage/ob_partition_component_factory.h"
#include "storage/memtable/ob_memtable.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/allocator/ob_pcounter.h"
#include "ls/ob_ls.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::memtable;
namespace storage
{

static const char *MEMORY_LABEL_COMPONENT_FACTORY = "PartitionComponentFactory";

ObMemtable *ObPartitionComponentFactory::get_memtable(const uint64_t tenant_id)
{
  ObMemtable* obj = ObMemtableFactory::alloc(tenant_id);
  if (NULL != obj) {
    PC_ADD(MT, 1);
  }
  return obj;
}

ObTransService *ObPartitionComponentFactory::get_trans_service()
{
  return OB_NEW(ObTransService, MEMORY_LABEL_COMPONENT_FACTORY);
}


ObLS *ObPartitionComponentFactory::get_ls(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_PARTITION, ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID);
  ObLS *obj = OB_NEW_ALIGN32(ObLS, memattr);
  if (!OB_ISNULL(obj)) {
    PC_ADD(PT, 1);
  }
  return obj;
}

void ObPartitionComponentFactory::free(ObIPartitionGroup *partition)
{
  UNUSEDx(partition);
}

void ObPartitionComponentFactory::free(memtable::ObMemtable *memtable)
{
  if (NULL != memtable) {
    PC_ADD(MT, -1);
    ObMemtableFactory::free(memtable);
    memtable = NULL;
  }
}

void ObPartitionComponentFactory::free(ObTransService *txs)
{
  OB_DELETE(ObTransService, MEMORY_LABEL_COMPONENT_FACTORY, txs);
  txs = NULL;
}

} // namespace storage
} // namespace oceanbase
