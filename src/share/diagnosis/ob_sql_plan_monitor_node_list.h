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
#include "observer/mysql/ob_dl_queue.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace sql
{
class ObOperator;

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

class ObMonitorNode
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
      op_(nullptr),
      rt_node_id_(OB_INVALID_ID),
      open_time_(0),
      first_row_time_(0),
      last_row_time_(0),
      close_time_(0),
      rescan_times_(0),
      output_row_count_(0),
      db_time_(0),
      block_time_(0),
      disk_read_count_(0),
      otherstat_1_value_(0),
      otherstat_2_value_(0),
      otherstat_3_value_(0),
      otherstat_4_value_(0),
      otherstat_5_value_(0),
      otherstat_6_value_(0),
      otherstat_7_value_(0),
      otherstat_8_value_(0),
      otherstat_9_value_(0),
      otherstat_10_value_(0),
      otherstat_1_id_(0),
      otherstat_2_id_(0),
      otherstat_3_id_(0),
      otherstat_4_id_(0),
      otherstat_5_id_(0),
      otherstat_6_id_(0),
      otherstat_7_id_(0),
      otherstat_8_id_(0),
      otherstat_9_id_(0),
      otherstat_10_id_(0),
      enable_rich_format_(false),
      workarea_mem_(0),
      workarea_max_mem_(0),
      workarea_tempseg_(0),
      workarea_max_tempseg_(0),
      plan_hash_value_(common::OB_INVALID_ID)
  {
    TraceId* trace_id = common::ObCurTraceId::get_trace_id();
    if (NULL != trace_id) {
      trace_id_ = *trace_id;
    }
    thread_id_ = GETTID();
    sql_id_[0] = '\0';
  }
  explicit ObMonitorNode(const ObMonitorNode &that) = default;
  ~ObMonitorNode() = default;
  int assign(const ObMonitorNode &that)
  {
    *this = that;
    return common::OB_SUCCESS;
  }
  void set_op(ObOperator *op) { op_ = op; }
  void set_operator_type(ObPhyOperatorType type) { op_type_ = type; }
  void set_operator_id(int64_t op_id) { op_id_ = op_id; }
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_plan_depth(int64_t plan_depth) { plan_depth_ = plan_depth; }
  void set_rt_node_id(int64_t id) { rt_node_id_ = id; }
  const char *get_operator_name() const { return get_phy_op_name(op_type_, enable_rich_format_); }
  ObPhyOperatorType get_operator_type() const { return op_type_; }
  int64_t get_op_id() const { return op_id_; }
  int64_t get_tenant_id() const { return tenant_id_; }
  const TraceId& get_trace_id() const { return trace_id_; }
  int64_t get_thread_id() { return thread_id_; }
  int64_t get_rt_node_id() { return rt_node_id_;}
  void set_rich_format(bool v) { enable_rich_format_ = v; }
  void update_memory(int64_t delta_size);
  void update_tempseg(int64_t delta_size);
  uint64_t calc_db_time();
  void covert_to_static_node();
  int set_sql_id(const ObString &sql_id);
  void set_plan_hash_value(uint64_t plan_hash_value) { plan_hash_value_ = plan_hash_value; }
  TO_STRING_KV(K_(tenant_id), K_(op_id), "op_name", get_operator_name(), K_(thread_id));
public:
  int64_t tenant_id_;
  int64_t op_id_;
  int64_t plan_depth_;
  int64_t output_batches_; // for batch
  int64_t skipped_rows_count_; // for batch
  ObPhyOperatorType op_type_;
  ObOperator *op_;
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
  int64_t disk_read_count_;
  // 各个算子特有的信息
  int64_t otherstat_1_value_;
  int64_t otherstat_2_value_;
  int64_t otherstat_3_value_;
  int64_t otherstat_4_value_;
  int64_t otherstat_5_value_;
  int64_t otherstat_6_value_;
  int64_t otherstat_7_value_;
  int64_t otherstat_8_value_;
  int64_t otherstat_9_value_;
  int64_t otherstat_10_value_;
  int16_t otherstat_1_id_;
  int16_t otherstat_2_id_;
  int16_t otherstat_3_id_;
  int16_t otherstat_4_id_;
  int16_t otherstat_5_id_;
  int16_t otherstat_6_id_;
  int16_t otherstat_7_id_;
  int16_t otherstat_8_id_;
  int16_t otherstat_9_id_;
  int16_t otherstat_10_id_;
  bool enable_rich_format_;
  int64_t workarea_mem_;
  int64_t workarea_max_mem_;
  int64_t workarea_tempseg_;
  int64_t workarea_max_tempseg_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t plan_hash_value_;
};


class ObPlanMonitorNodeList;
class ObSqlPlanMonitorRecycleTask : public common::ObTimerTask
{
public:
  ObSqlPlanMonitorRecycleTask() : node_list_(nullptr) {};
  virtual ~ObSqlPlanMonitorRecycleTask() = default;
  void runTimerTask();
  int init(ObPlanMonitorNodeList *node_list, int64_t tenant_id);
private:
  ObPlanMonitorNodeList *node_list_;
  int64_t tenant_id_;
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
    int recursive_add_node_to_array(ObMonitorNode &node);
    common::ObIArray<ObMonitorNode> &node_array_;
    int ret_;
  };
public:
  typedef hash::ObHashMap<ObMonitorNodeKey, ObMonitorNode *,
      hash::SpinReadWriteDefendMode> MonitorNodeMap;
  static const int64_t MONITOR_NODE_PAGE_SIZE = (128LL << 10); // 128K
  static const int64_t EVICT_INTERVAL = 1000000; //1s
  static const char *MOD_LABEL;
  static const int64_t ROOT_QUEUE_SIZE = 4 * 1024;
  static const int64_t LEAF_QUEUE_SIZE = 8 * 1024;
  static const int64_t IDLE_LEAF_QUEUE_NUM = 2;
  static const int64_t DEFAULT_MAX_QUEUE_SIZE;
  static constexpr double RECYCLE_THRESHOLD_SCALE = 0.9;
  static constexpr double RECYCLE_CNT_SCALE = 0.05;
public:
  ObPlanMonitorNodeList();
  ~ObPlanMonitorNodeList();
  static int mtl_init(ObPlanMonitorNodeList* &node_list);
  static void mtl_destroy(ObPlanMonitorNodeList* &node_list);
  int submit_node(ObMonitorNode &node);
  int64_t get_start_idx() const { return (int64_t)queue_.get_start_idx(); }
  int64_t get_end_idx() { return (int64_t)queue_.get_end_idx(); }
  int64_t get_size_used() { return (int64_t)(queue_.get_end_idx() - queue_.get_start_idx()); }
  int get(const int64_t idx, void *&record, common::ObDlQueue::DlRef* ref)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == (record = queue_.get(idx, ref))) {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }
  int revert(common::ObDlQueue::DlRef* ref)
  {
    queue_.revert(ref);
    return common::OB_SUCCESS;
  }
  int recycle_old(int64_t limit);
  int64_t get_recycle_count();
  void clear_queue();
  void* alloc_mem(const int64_t size)
  {
    void * ret = allocator_.alloc(size);
    return ret;
  }
  void free_mem(void *ptr)
  {
    if (OB_NOT_NULL(ptr)) {
      allocator_.free(ptr);
      ptr = NULL;
    }
  }
  int register_monitor_node(ObMonitorNode &node);
  int revert_monitor_node(ObMonitorNode &node);
  int convert_node_map_2_array(common::ObIArray<ObMonitorNode> &array);
  int64_t get_queue_size() { return queue_size_; }
  void set_queue_size(int64_t queue_size) { queue_size_ = queue_size; }
  void freeCallback(void* ptr) { free_mem(ptr); }
  int prepare_new() { return queue_.prepare_alloc_queue(); }
private:
  int init(uint64_t tenant_id, const int64_t tenant_mem_size);
  void destroy();
  int release_record(int64_t release_cnt, bool is_destroyed = false);
private:
  common::ObConcurrentFIFOAllocator allocator_;//alloc mem for string buf
  common::ObDlQueue queue_;
  MonitorNodeMap node_map_; // for real time sql plan monitor
  ObSqlPlanMonitorRecycleTask task_; // 定期回收 sql plan mon 内存
  bool inited_;
  bool destroyed_;
  uint64_t request_id_;
  uint64_t tenant_id_;
  int tg_id_;
  int64_t rt_node_id_;
  int64_t queue_size_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanMonitorNodeList);
};

}
}
#endif
