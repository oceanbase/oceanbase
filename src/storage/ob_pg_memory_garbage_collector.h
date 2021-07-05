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

#ifndef OB_PG_MEMORY_GARBAGE_COLLECTOR_H_
#define OB_PG_MEMORY_GARBAGE_COLLECTOR_H_

#include "storage/ob_i_partition_group.h"

namespace oceanbase {
namespace storage {

class ObPGRecycleNode : public common::ObDLinkBase<ObPGRecycleNode> {
public:
  ObPGRecycleNode() : pg_(nullptr)
  {}
  virtual ~ObPGRecycleNode() = default;
  void set_pg(ObIPartitionGroup* pg)
  {
    pg_ = pg;
  }
  ObIPartitionGroup* get_pg()
  {
    return pg_;
  }

private:
  ObIPartitionGroup* pg_;
};

class ObPGMemoryGCTask : public common::ObTimerTask {
public:
  ObPGMemoryGCTask() = default;
  virtual ~ObPGMemoryGCTask() = default;
  virtual void runTimerTask() override;
};

class ObPGMemoryGarbageCollector {
public:
  ObPGMemoryGarbageCollector();
  ~ObPGMemoryGarbageCollector() = default;
  static ObPGMemoryGarbageCollector& get_instance();
  int init(ObIPartitionComponentFactory* cp_fty);
  void add_pg(ObIPartitionGroup* pg);
  int check_tenant_pg_exist(const uint64_t tenant_id, bool& exist);
  int recycle();
  void stop();
  void wait();
  void destroy();

private:
  static const int64_t GC_INTERVAL_US = 10 * 1000 * 1000L;
  common::ObDList<ObPGRecycleNode> pg_list_;
  common::ObArray<ObPGRecycleNode*> pg_node_pool_;
  common::ObArenaAllocator allocator_;
  common::TCRWLock lock_;
  common::ObTimer timer_;
  ObPGMemoryGCTask gc_task_;
  ObIPartitionComponentFactory* cp_fty_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_MEMORY_GARBAGE_COLLECTOR_H_
