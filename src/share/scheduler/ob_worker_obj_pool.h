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

#ifndef SRC_SHARE_SCHEDULER_OB_WORKER_OBJ_POOL_H_
#define SRC_SHARE_SCHEDULER_OB_WORKER_OBJ_POOL_H_

#include "share/rc/ob_context.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
namespace oceanbase {
namespace share {
class ObDagWorkerNew;

class ObWorkerObjPool {
public:
  static ObWorkerObjPool& get_instance();
  int init(const int64_t total_mem_limit = TOTAL_LIMIT, const int64_t hold_mem_limit = HOLD_LIMIT,
      const int64_t page_size = PAGE_SIZE);
  void destroy();
  int create_worker();
  int release_worker();
  int adjust_worker_cnt();
  int get_worker_obj(ObDagWorkerNew*& worker);
  int release_worker_obj(ObDagWorkerNew*& worker);
  int set_init_worker_cnt_inc(int32_t worker_cnt_inc);
  int32_t get_init_worker_cnt() const
  {
    return init_worker_cnt_;
  }
  int32_t get_worker_cnt() const
  {
    return worker_cnt_;
  }

private:
  ObWorkerObjPool();   // prevent to new
  ~ObWorkerObjPool();  // prevent to delete
  ObWorkerObjPool(const ObWorkerObjPool&);
  ObWorkerObjPool& operator=(const ObWorkerObjPool&);
  typedef common::ObDList<ObDagWorkerNew> WorkerList;
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static constexpr double OBJ_RELEASE_PERCENT = 3;
  static constexpr double OBJ_UP_LIMIT_PERCENT = 4;
  static constexpr double OBJ_CREATE_PERCENT = 2;

private:
  bool is_inited_;
  lib::ObMutex lock_;
  int32_t init_worker_cnt_;
  int32_t worker_cnt_;
  WorkerList free_worker_obj_list_;
  common::ObConcurrentFIFOAllocator allocator_;
};

}  // namespace share
}  // namespace oceanbase

#endif
