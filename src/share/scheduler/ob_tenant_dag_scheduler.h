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

#ifndef SRC_SHARE_SCHEDULER_OB_TENANT_DAG_SCHEDULER_H_
#define SRC_SHARE_SCHEDULER_OB_TENANT_DAG_SCHEDULER_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_pool.h"

#include "share/scheduler/ob_tenant_thread_pool.h"
#include "share/scheduler/ob_dag_worker.h"

namespace oceanbase {
namespace share {

typedef common::ObCurTraceId::TraceId ObDagId;

class ObDagSchedulerNew;
class ObWorkerObjPool;
class ObIDagNew;
#ifndef NDEBUG
class GetScheduleInfo;  // for test
#endif

class ObDagSchedulerNew : public lib::ThreadPool {
public:
  static ObDagSchedulerNew& get_instance();
  int init(const int64_t schedule_period = SCHEDULER_PERIOD, const int64_t total_mem_limit = TOTAL_LIMIT,
      const int64_t hold_mem_limit = HOLD_LIMIT, const int64_t page_size = PAGE_SIZE);
  int add_tenant_thread_pool(const int64_t tenant_id);
  int del_tenant_thread_pool(const int64_t tenant_id);
  template <typename T>
  int alloc_dag(int64_t tenant_id, T*& dag);
  template <typename T>
  void free_dag(int64_t tenant_id, T*& dag);
  int set_tenant_setting(common::ObIArray<ObTenantSetting>& setting_list);
  int get_tenant_settings(common::ObIArray<ObTenantSetting>& setting_list);
  int get_tenant_setting_by_id(int64_t tenant_id, ObTenantSetting& setting_vec);
  int add_dag_in_tenant(int64_t tenant_id, ObIDagNew* dag);
  int64_t get_dag_count_in_tenant(int64_t tenant_id, int64_t type_id);
  void run1();
  void destroy();
  bool is_empty();
  int start_run();
#ifndef NDEBUG
  void set_sche_info(int64_t tenant_id, GetScheduleInfo* info);  // for test
#endif

private:
  friend class ObTenantThreadPool;
  typedef common::hash::ObHashMap<int64_t, ObTenantThreadPool*> TenantThreadPoolMap;
  typedef common::hash::ObHashMap<int64_t, ObTenantThreadPool*>::iterator TenantThreadPoolIterator;
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t SCHEDULER_PERIOD = 1000;  // 1 ms
  static const int64_t DEFAULT_BUCKET_NUM;

private:
  ObDagSchedulerNew();
  virtual ~ObDagSchedulerNew();
  int schedule_();
  void notify_();
  int add_types_(ObTenantThreadPool* thread_pool);
  int get_tenant_thread_pool_by_id_(int64_t tenant_id, ObTenantThreadPool*& tenant_thread_pool);

private:
  bool is_inited_;
  bool is_running_;
  int64_t schedule_period_;
  common::ObThreadCond scheduler_sync_;
  TenantThreadPoolMap tenant_thread_pool_map_;  // HashMap of tenant_id->ObTenantThreadPool *
};

template <typename T>
int ObDagSchedulerNew::alloc_dag(int64_t tenant_id, T*& dag)
{
  int ret = common::OB_SUCCESS;
  ObTenantThreadPool* pool = NULL;
  if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_id, pool))) {
    COMMON_LOG(WARN, "failed to call get_tenant_thread_pool_by_id", K(ret), K(tenant_id));
  } else if (pool->has_set_stop()) {
    ret = common::OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "tenant thread pool is deleted", K(ret), K(tenant_id));
  } else if (OB_FAIL(pool->alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to call ObTenantThreadPool::alloc_dag", K(ret));
  }
  return ret;
}
template <typename T>
void ObDagSchedulerNew::free_dag(int64_t tenant_id, T*& dag)
{
  int ret = common::OB_SUCCESS;
  ObTenantThreadPool* pool = NULL;
  if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_id, pool))) {
    COMMON_LOG(WARN, "failed to call get_tenant_thread_pool_by_id", K(ret), K(tenant_id));
  } else {
    pool->free_dag(dag);
  }
}

#ifndef NDEBUG
class GetScheduleInfo {
public:
  common::ObVector<int64_t> choose_type_id_list_;
  common::ObVector<int64_t> running_tasks_cnt_;

public:
  GetScheduleInfo();
  ~GetScheduleInfo();
  void destroy();
};
#endif

}  // namespace share
}  // namespace oceanbase

#endif /* SRC_SHARE_SCHEDULER_OB_TENANT_DAG_SCHEDULER_H_ */
