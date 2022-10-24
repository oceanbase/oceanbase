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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_COMPONENT_FACTORY_
#define OCEANBASE_STORAGE_OB_PARTITION_COMPONENT_FACTORY_

#include "lib/allocator/ob_malloc.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "storage/ob_i_partition_component_factory.h"
#include "storage/memtable/ob_memtable_interface.h"

namespace oceanbase
{
namespace storage
{
class ObSSStore;
class ObLS;

class ObPartitionComponentFactory: public ObIPartitionComponentFactory
{
public:
  // for log stream
  virtual ObLS *get_ls(const uint64_t tenant_id);
public:
  ObPartitionComponentFactory() {}
  virtual ~ObPartitionComponentFactory() {}

  virtual memtable::ObMemtable *get_memtable(const uint64_t tenant_id);
  virtual transaction::ObTransService *get_trans_service();

  virtual void free(ObIPartitionGroup *partition);
  virtual void free(memtable::ObMemtable *memtable);
  virtual void free(transaction::ObTransService *txs);


protected:
  template<class IT>
  void component_free(IT *component)
  {
    if (OB_LIKELY(NULL != component)) {
      op_free(component);
      component = NULL;
    }
  }
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_PARTITION_COMPONENT_FACTORY_
