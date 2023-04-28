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

#ifndef OCEANBASE_SQL_OB_MONITOR_INFO_MANAGER_H
#define OCEANBASE_SQL_OB_MONITOR_INFO_MANAGER_H
#include "lib/task/ob_timer.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "sql/monitor/ob_monitor_info_elimination_task.h"
#include "observer/mysql/ob_mysql_request_manager.h"
namespace oceanbase
{
namespace sql
{
struct PlanKey
{
  uint64_t hash() const
  {
    uint64_t value = 0;
    value =  murmurhash(&plan_id_, sizeof(plan_id_), value);
    value =  murmurhash(&addr_, sizeof(addr_), value);
    return value;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const PlanKey &other) const { return (plan_id_ == other.plan_id_) && (addr_ == other.addr_); }
  int64_t plan_id_;
  common::ObAddr addr_;
};
class ObMonitorInfoManager
{
  friend class TestMonitorInfoManager_tets_duplicated_Test;
  struct ReclainCond {
  public:
    ReclainCond(int64_t curr_timestamp,
                int64_t max_remain_interval)
        : curr_timestamp_(curr_timestamp),
        max_remain_interval_(max_remain_interval)
    {}
    int64_t curr_timestamp_;
    int64_t max_remain_interval_;
    bool operator()(const PlanKey &plan_id, int64_t &execution_time)
    {
      UNUSED(plan_id);
      bool bret = false;
      if (curr_timestamp_ - execution_time < max_remain_interval_) {
      } else {
        bret = true;
      }
      return bret;
    }
  };
  typedef common::ObRaQueue::Ref Ref;
public:
  ObMonitorInfoManager();
  ~ObMonitorInfoManager();
  int init();
  void destroy();
  int get_by_request_id(int64_t request_id, int64_t &index, ObPhyPlanMonitorInfo *&plan_info, Ref* ref);
  int get_by_index(int64_t index, ObPhyPlanMonitorInfo *&plan_info, Ref* ref);
  int revert(Ref* req)
  {
    slow_query_queue_.revert(req);
    return common::OB_SUCCESS;
  }
  int add_monitor_info(ObPhyPlanMonitorInfo *plan_info);
  int alloc(int64_t request_id, ObPhyPlanMonitorInfo *&plan_info);
  int free(ObPhyPlanMonitorInfo *&plan_info);
  int64_t get_start_index() { return slow_query_queue_.get_pop_idx(); }
  int64_t get_size() { return slow_query_queue_.get_size(); }
  int64_t get_count() { return slow_query_queue_.get_size(); }
  int is_info_nearly_duplicated(const common::ObAddr &addr, int64_t plan_id, bool &is_duplicated);
  void print_memory_size()
  {
    SQL_MONITOR_LOG(INFO, "print memory size, ", K(get_start_index()),
                    K(get_size()),
                    K(allocator_. allocated()),
                    K(allocator_.hold()),
                    K(operator_info_size_));
  }
  int gc();
  static const int64_t OB_MAX_QUEUE_SIZE = 100000;
  static const int64_t MAX_MEMORY_SIZE = 500 * 1024 * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t OB_BATCH_GC_COUNT = 2000;
  static const int64_t OB_MAX_PUSH_INTERVAL = 10 * 1000 * 1000;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMonitorInfoManager);
  void clear_queue(int64_t limit);
  int reclain_map();
  void set_max_push_interval(int64_t time) { max_push_interval_ = time; }
private:
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObRaQueue slow_query_queue_;
  common::ObTimer timer_; //定时器，启动定时任务淘汰旧数据；
  ObMonitorInfoEliminationTask elimination_task_;
  common::ObLinearHashMap<PlanKey, int64_t> plan_execution_time_map_; //记录每个plan最近一次执行的时间戳
  int64_t max_push_interval_;
  int64_t memory_limit_;
  int64_t operator_info_size_; // ObPhyPlanMonitorInfo中operator_infos_的内存不在allocator中，单独统计
};

} //namespace sql
} //namespace oceanbase
#endif


