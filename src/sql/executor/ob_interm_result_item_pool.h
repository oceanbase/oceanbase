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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_ITEM_POOL_
#define OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_ITEM_POOL_

#include "lib/allocator/ob_small_allocator.h"
#include "sql/executor/ob_interm_result_pool.h"

namespace oceanbase {
namespace sql {
class ObIIntermResultItem;
class ObIntermResultItem;
class ObDiskIntermResultItem;

class ObIntermResultItemPool {
public:
  static const int64_t MAX_INTERM_RESULT_ITEM_POOL_CAPACITY = ObIntermResultPool::INTERM_RESULT_CAPACITY;
  ObIntermResultItemPool();
  virtual ~ObIntermResultItemPool();

  static int build_instance();
  static ObIntermResultItemPool* get_instance();

  void reset();

  int alloc_mem_item(ObIntermResultItem*& item, const uint64_t tenant_id);
  int alloc_disk_item(ObDiskIntermResultItem*& item, const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
      const int64_t offset);

  int free(ObIIntermResultItem* item);

  inline static int64_t get_capacity()
  {
    return MAX_INTERM_RESULT_ITEM_POOL_CAPACITY;
  }

private:
  int init();
  inline common::ObSmallAllocator& get_allocator(const bool is_memory_item)
  {
    return is_memory_item ? mem_item_allocator_ : disk_item_allocator_;
  }

private:
  static ObIntermResultItemPool* instance_;

  bool inited_;

  common::ObSmallAllocator mem_item_allocator_;
  common::ObSmallAllocator disk_item_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIntermResultItemPool);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_ITEM_POOL_ */
