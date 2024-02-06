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

#ifndef __OB_SHARE_SQL_PLAN_MONITOR_NODE_LIST_H__
#define __OB_SHARE_SQL_PLAN_MONITOR_NODE_LIST_H__

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/profile/ob_trace_id.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "share/diagnosis/ob_sql_monitor_statname.h"
#include "observer/mysql/ob_ra_queue.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace sql
{

// 用于统计一段代码的执行时间
class TimingGuard
{
public:
  explicit TimingGuard(int64_t &v) :
      v_(v),
      begin_(common::ObTimeUtility::fast_current_time())
  {
  }
  ~TimingGuard()
  {
    v_ =  v_ + common::ObTimeUtility::fast_current_time() - begin_;
  }
private:
  int64_t &v_;
  int64_t begin_;
};

class ObMonitorNode final : public common::ObDLinkBase<ObMonitorNode>
{
  friend class ObPlanMonitorNodeList;
  typedef common::ObCurTraceId::TraceId TraceId;
public:
  ObMonitorNode() :
      tenant_id_(0),
      op_id_(0),
      plan_depth_(0),
      output_batches_(0),
      skipped_rows_count_(0),
      op_type_(PHY_INVALID),
      rt_node_id_(OB_INVALID_ID),
      open_time_(0),
      first_row_time_(0),
      last_row_time_(0),
      close_time_(0),
      rescan_times_(0),
      output_row_count_(0),
      db_time_(0),
      block_time_(0),
      memory_used_(0),
      disk_read_count_(0),
      otherstat_1_value_(0),
      otherstat_2_value_(0),
      otherstat_3_value_(0),
      otherstat_4_value_(0),
      otherstat_5_value_(0),
      otherstat_6_value_(0),
      otherstat_1_id_(0),
      otherstat_2_id_(0),
      otherstat_3_id_(0),
      otherstat_4_id_(0),
      otherstat_5_id_(0),
      otherstat_6_id_(0)
  {
    TraceId* trace_id = common::ObCurTraceId::get_trace_id();
    if (NULL != trace_id) {
      trace_id_ = *trace_id;
    }
    thread_id_ = GETTID();
  }
  explicit ObMonitorNode(const ObMonitorNode &that) = default;
  ~ObMonitorNode() = default;
  int assign(const ObMonitorNode &that)
  {
    *this = that;
    return common::OB_SUCCESS;
  }
  void set_operator_type(ObPhyOperatorType type) { op_type_ = type; }
  void set_operator_id(int64_t op_id) { op_id_ = op_id; }
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_plan_depth(int64_t plan_depth) { plan_depth_ = plan_depth; }
  void set_rt_node_id(int64_t id) { rt_node_id_ = id; }
  const char *get_operator_name() const { return get_phy_op_name(op_type_); }
  ObPhyOperatorType get_operator_type() const { return op_type_; }
  int64_t get_op_id() const { return op_id_; }
  int64_t get_tenant_id() const { return tenant_id_; }
  const TraceId& get_trace_id() const { return trace_id_; }
  int64_t get_thread_id() { return thread_id_; }
  int64_t get_rt_node_id() { return rt_node_id_;}
  int add_rt_monitor_node(ObMonitorNode *node);
  TO_STRING_KV(K_(tenant_id), K_(op_id), "op_name", get_operator_name(), K_(thread_id));
public:
  int64_t tenant_id_;
  int64_t op_id_;
  int64_t plan_depth_;
  int64_t output_batches_; // for batch
  int64_t skipped_rows_count_; // for batch
  ObPhyOperatorType op_type_;
private:
  int64_t thread_id_;
  TraceId trace_id_;
  int64_t rt_node_id_; // for real time sql plan monitor
public:
  // 每个算子都要记录的信息
  int64_t open_time_;
  int64_t first_row_time_;
  int64_t last_row_time_;
  int64_t close_time_;
  int64_t rescan_times_;
  int64_t output_row_count_;
  uint64_t db_time_; // rdtsc cpu cycles spend on this op, include cpu instructions & io
  uint64_t block_time_; // rdtsc cpu cycles wait for network, io etc
  int64_t memory_used_;
  int64_t disk_read_count_;
  // 各个算子特有的信息
  int64_t otherstat_1_value_;
  int64_t otherstat_2_value_;
  int64_t otherstat_3_value_;
  int64_t otherstat_4_value_;
  int64_t otherstat_5_value_;
  int64_t otherstat_6_value_;
  int16_t otherstat_1_id_;
  int16_t otherstat_2_id_;
  int16_t otherstat_3_id_;
  int16_t otherstat_4_id_;
  int16_t otherstat_5_id_;
  int16_t otherstat_6_id_;
};


class ObPlanMonitorNodeList;
class ObSqlPlanMonitorRecycleTask : public common::ObTimerTask
{
public:
  ObSqlPlanMonitorRecycleTask() : node_list_(nullptr) {};
  virtual ~ObSqlPlanMonitorRecycleTask() = default;
  void runTimerTask();
  int init(ObPlanMonitorNodeList *node_list);
private:
  ObPlanMonitorNodeList *node_list_;
};

class ObPlanMonitorNodeList
{
public:
  class ObMonitorNodeKey
  {
  public:
    uint64_t hash() const
    {
      uint64_t hash_ret = 0;
      hash_ret = common::murmurhash(&node_id_, sizeof(uint64_t), 0);
      return hash_ret;
    }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    inline bool operator==(const ObMonitorNodeKey &other) const
    {
      return node_id_ == other.node_id_;
    }
    int64_t node_id_;
    TO_STRING_KV(K_(node_id));
  };

  class ObMonitorNodeTraverseCall
  {
  public:
    ObMonitorNodeTraverseCall(common::ObIArray<ObMonitorNode> &node_array) :
        node_array_(node_array), ret_(OB_SUCCESS) {}
  int operator() (common::hash::HashMapPair<ObMonitorNodeKey,
      ObMonitorNode *> &entry);
    common::ObIArray<ObMonitorNode> &node_array_;
    int ret_;
  };
public:
  typedef hash::ObHashMap<ObMonitorNodeKey, ObMonitorNode *,
      hash::SpinReadWriteDefendMode> MonitorNodeMap;
  static const int64_t MONITOR_NODE_PAGE_SIZE = (1LL << 21) - (1LL << 13); // 2M - 8k
  static const int64_t EVICT_INTERVAL = 1000000; //1s
  static const char *MOD_LABEL;
  typedef common::ObRaQueue::Ref Ref;
public:
  ObPlanMonitorNodeList();
  ~ObPlanMonitorNodeList();
  static int mtl_init(ObPlanMonitorNodeList* &node_list);
  static void mtl_destroy(ObPlanMonitorNodeList* &node_list);
  int submit_node(ObMonitorNode &node);
  int64_t get_start_idx() const { return (int64_t)queue_.get_pop_idx(); }
  int64_t get_end_idx() const { return (int64_t)queue_.get_push_idx(); }
  int64_t get_size_used() { return (int64_t)(queue_.get_push_idx() - queue_.get_pop_idx()); }
  int64_t get_size() { return (int64_t)queue_.get_size(); }
  int get(const int64_t idx, void *&record, Ref* ref)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == (record = queue_.get(idx, ref))) {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }
  int revert(Ref* ref)
  {
    queue_.revert(ref);
    return common::OB_SUCCESS;
  }
  int recycle_old(int64_t limit)
  {
    void* req = NULL;
    int64_t count = 0;
    while(count++ < limit && NULL != (req = queue_.pop())) {
      free_mem(req);
    }
    return common::OB_SUCCESS;
  }
  int64_t get_recycle_count()
  {
    int64_t cnt = 0;
    if (get_size_used() > recycle_threshold_) {
      cnt = batch_release_;
    }
    return cnt;
  }
  void clear_queue()
  {
    (void)recycle_old(INT64_MAX);
  }
  void* alloc_mem(const int64_t size)
  {
    void * ret = allocator_.alloc(size);
    return ret;
  }
  void free_mem(void *ptr)
  {
    allocator_.free(ptr);
    ptr = NULL;
  }
  int register_monitor_node(ObMonitorNode &node);
  int revert_monitor_node(ObMonitorNode &node);
  int convert_node_map_2_array(common::ObIArray<ObMonitorNode> &array);
private:
  int init(uint64_t tenant_id, const int64_t tenant_mem_size);
  void destroy();
private:
  common::ObConcurrentFIFOAllocator allocator_;//alloc mem for string buf
  common::ObRaQueue queue_;
  MonitorNodeMap node_map_; // for real time sql plan monitor
  ObSqlPlanMonitorRecycleTask task_; // 定期回收 sql plan mon 内存
  bool inited_;
  bool destroyed_;
  uint64_t request_id_;
  int64_t recycle_threshold_; // begin to recycle node when reach threshold
  int64_t batch_release_; // release node in batch
  uint64_t tenant_id_;
  int tg_id_;
  int64_t rt_node_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanMonitorNodeList);
};

}
}
#endif
