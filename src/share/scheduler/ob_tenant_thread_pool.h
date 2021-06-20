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

#ifndef SRC_SHARE_SCHEDULER_OB_TENANT_THREAD_POOL_H_
#define SRC_SHARE_SCHEDULER_OB_TENANT_THREAD_POOL_H_

#define USING_LOG_PREFIX COMMON
#include "share/ob_force_print_log.h"
#include "share/rc/ob_context.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_vector.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/thread/thread_pool.h"
#include "share/scheduler/ob_dag.h"
#include "share/scheduler/ob_dag_type.h"

namespace oceanbase {
namespace share {
class ObDagWorkerNew;
#ifndef NDEBUG
class GetScheduleInfo;  // for test
#endif

// descending sort list
template <typename T>
class ObSortList {
public:
  ObSortList()
  {}
  virtual ~ObSortList()
  {
    clear();
  }
  T* get_first()
  {
    return list_.get_first();
  }
  T* get_header()
  {
    return list_.get_header();
  }
  T* get_last()
  {
    return list_.get_last();
  }
  void reset()
  {
    list_.reset();
  }
  void clear()
  {
    list_.clear();
  }
  bool is_empty() const
  {
    return list_.is_empty();
  }
  int get_size() const
  {
    return list_.get_size();
  }
  common::ObDList<T>& get_list()
  {
    return list_;
  }
  bool find(T* item)
  {
    bool bret = false;
    if (*item >= *(list_.get_last())) {
      T* cur = list_.get_first();
      T* head = list_.get_header();
      while (NULL != cur && head != cur) {
        if (item->is_equal(*cur)) {  // determine if the pointer is equal
          bret = true;
          break;
        } else if (*item > *cur) {
          break;
        } else {  // *item < *cur
          cur = cur->get_next();
        }
      }
    }
    return bret;
  }
  bool insert(T* item)
  {
    bool bret = false;
    if (list_.increasing_add(item)) {
      bret = true;
    }
    return bret;
  }
  bool adjust(T* item)
  {
    bool bret = false;
    T* tmp = list_.remove(item);  // remove & insert at the right position
    if (NULL != tmp) {
      insert(tmp);
      bret = true;
    }
    return bret;
  }
  bool remove(T* item)
  {
    bool bret = false;
    if (NULL != item && find(item) && !OB_ISNULL(list_.remove(item))) {
      bret = true;
    }
    return bret;
  }
  int move(ObSortList& list)
  {
    return list_.move(list.get_list());
  }
  VIRTUAL_TO_STRING_KV(K_(list));

private:
  common::ObDList<T> list_;
};

class ObTenantThreadPool {
public:
  ObTenantThreadPool(int64_t tenant_id);
  virtual ~ObTenantThreadPool();
  int init(const int64_t hash_bucket_num = DEFAULT_BUCKET_NUM, const int64_t total_mem_limit = TOTAL_LIMIT,
      const int64_t hold_mem_limit = HOLD_LIMIT, const int64_t page_size = PAGE_SIZE);
  int add_dag(ObIDagNew* dag);
  template <typename T>
  int alloc_dag(T*& dag);
  template <typename T>
  void free_dag(T*& dag);
  void destroy();
  bool is_empty();
  int get_dag_count(const int64_t type_id, int64_t& dag_cnt);
  int set_tenant_setting(ObTenantSetting& tenant_setting);
  int get_tenant_setting(ObTenantSetting& tenant_setting);
  int schedule(int64_t schedule_period);
  void dump_dag_status();
  bool has_remain_task(const int64_t type_id);
  void set_stop()
  {
    is_stoped_ = true;
  }
  bool has_set_stop() const
  {
    return is_stoped_;
  }
  // call by scheduler
  void set_last_schedule_time(int64_t time)
  {
    last_schedule_time_ = time;
  }
  bool need_schedule(int64_t schedule_period);
  int finish_task(ObITaskNew* task, ObDagWorkerNew* worker);
  int return_worker(ObITaskNew* task);
#ifndef NDEBUG
  void set_sche_info(GetScheduleInfo* info)
  {
    schedule_info_ = info;
  }
#endif
  VIRTUAL_TO_STRING_KV(KP(this), K_(is_inited), K_(tenant_id), K_(max_thread_num));

private:
  friend class ObDagWorkerNew;
  friend class ObTaskNew;
  static const int32_t MAX_TYPE_CNT = 20;

private:
  class DynamicScore : public common::ObDLinkBase<DynamicScore> {
  public:
    int64_t type_id_;
    double d_score_;
    DynamicScore() : type_id_(0), d_score_(0)
    {}
    ~DynamicScore()
    {}
    bool operator>(const DynamicScore& other) const
    {
      return d_score_ > other.d_score_;
    }
    bool operator>=(const DynamicScore& other) const
    {
      return d_score_ >= other.d_score_;
    }
    bool operator<=(const DynamicScore& other) const
    {
      return d_score_ <= other.d_score_;
    }
    bool is_equal(const DynamicScore& other) const
    {
      return this == &other;
    }
  };
  struct TypeInfo {
    int64_t type_id_;
    int64_t up_limit_;
    int64_t score_;
    DynamicScore* dynamic_score_;

    TypeInfo() : type_id_(0), up_limit_(INT_MAX), score_(0), dynamic_score_(NULL)
    {}
    ~TypeInfo()
    {}
    VIRTUAL_TO_STRING_KV(KP(this), K_(type_id), K_(up_limit), K_(score));
  };
  typedef common::ObSEArray<TypeInfo*, MAX_TYPE_CNT> TypeInfoList;
  typedef common::ObSortedVector<ObITaskNew*> TaskList;
  typedef ObSortList<DynamicScore> DynamicScoreList;
  typedef common::ObSEArray<ObSortList<ObIDagNew>*, MAX_TYPE_CNT> ReadyDagList;
  typedef common::ObSEArray<TaskList*, MAX_TYPE_CNT> WaitingTaskList;
  typedef TaskList::iterator TaskListIterator;
  typedef common::hash::ObHashMap<const ObIDagNew*, ObIDagNew*, common::hash::NoPthreadDefendMode,
      common::hash::hash_func<const ObIDagNew*>, common::hash::equal_to<const ObIDagNew*> >
      DagMap;
  static const int64_t DUMP_DAG_STATUS_INTERVAL = 10 * 1000LL * 1000LL;
  static const int64_t DEFAULT_BUCKET_NUM = 100;
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t DAG_SIZE_LIMIT = 10 << 10;

private:
  int sys_task_start_(ObIDagNew* dag);
  void reset_dynamic_score_();
  void calc_penalty_();
  int reset_switch_flag_();
  int get_schedule_list_(TaskList* schedule_list);
  int execute_penalty_(int64_t type_id);
  int recover_penalty_(int64_t type_id);
  int find_in_task_list_(ObITaskNew* task, TaskList* task_list, TaskListIterator& iterator);
  int remove_from_task_list_(ObITaskNew* task, TaskList* task_list);
  int add_running_task_(ObITaskNew* task);
  int add_waiting_task_(ObITaskNew* task);
  int remove_waiting_task_(ObITaskNew* task);
  int remove_running_task_(ObITaskNew* task);
  int get_ready_task_(const int64_t type_id, const ObIDagNew::GetTaskFlag flag, ObITaskNew*& task);
  int try_switch_task_(ObITaskNew* task);
  int switch_task_(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag);
  void copy_task_list_(TaskList* listA, TaskList* listB);
  bool is_type_id_valid_(int64_t type_id) const;
  int assign_worker_(ObITaskNew* task);
  int remove_dag_(ObIDagNew* dag);
  int pause_task_(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag);
  int add_type_(int64_t type_id, int64_t default_score);
  int add_types_();
  int set_max_thread_num_(int32_t max_thread_num);
  int schedule_();
  int pause_task(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag);
  int switch_task(ObITaskNew* task, const ObITaskNew::SwitchTaskFlag flag);

private:
  bool is_inited_;
  bool is_stoped_;
  int64_t tenant_id_;
  int32_t max_thread_num_;  // Maximum thread parallelism of the tenant thread pool
  int64_t last_schedule_time_;
  TypeInfoList type_info_list_;  // Type basic info list
  TaskList running_tasks_;
  WaitingTaskList waiting_tasks_;
  DynamicScoreList dynamic_score_list_;
  DagMap dag_map_;
  ReadyDagList ready_dag_list_;
  double type_penalty_[ObDagTypeIds::TYPE_SETTING_END];
  int64_t dag_cnts_[ObDagTypeIds::TYPE_SETTING_END];
  obsys::CRWLock rwlock_;
  common::ObConcurrentFIFOAllocator allocator_;
#ifndef NDEBUG
  GetScheduleInfo* schedule_info_;
#endif
};

template <typename T>
int ObTenantThreadPool::alloc_dag(T*& dag)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  dag = NULL;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "scheduler is not init", K(ret));
  } else if (DAG_SIZE_LIMIT < sizeof(T)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Dag Object is too large", K(ret), K(sizeof(T)));
  } else {
    if (NULL == (buf = allocator_.alloc(sizeof(T)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc dag", K(ret));
    } else {
      ObIDagNew* new_dag = new (buf) T();
      COMMON_LOG(INFO, "alloc dag", K(new_dag));
      if (OB_FAIL(new_dag->init())) {
        COMMON_LOG(WARN, "failed to init allocator", K(ret));
      } else {
        dag = static_cast<T*>(new_dag);
      }
    }
  }
  return ret;
}

template <typename T>
void ObTenantThreadPool::free_dag(T*& dag)
{
  if (OB_NOT_NULL(dag)) {
    dag->~T();
    allocator_.free((void*)dag);
  }
}

}  // namespace share
}  // namespace oceanbase
#endif
