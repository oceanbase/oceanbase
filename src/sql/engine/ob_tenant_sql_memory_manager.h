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

#ifndef OB_TENANT_SQL_MEMORY_MANAGER_H
#define OB_TENANT_SQL_MEMORY_MANAGER_H

#include "lib/list/ob_dlist.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_mutex.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {

enum ObSqlWorkAreaType
{
  HASH_WORK_AREA = 0,
  SORT_WORK_AREA = 1,
  MAX_TYPE
};

class ObSqlWorkAreaProfile : public common::ObDLinkBase<ObSqlWorkAreaProfile>
{
public:
  ObSqlWorkAreaProfile(ObSqlWorkAreaType type) :
    ObDLinkBase(),
    random_id_(0), type_(type), op_type_(PHY_INVALID), op_id_(UINT64_MAX), exec_ctx_(nullptr),
    min_size_(0), row_count_(0), input_size_(0), bucket_size_(0),
    chunk_size_(0), cache_size_(-1), one_pass_size_(0), expect_size_(OB_INVALID_ID),
    global_bound_size_(INT64_MAX), dop_(-1), plan_id_(-1), exec_id_(-1), sql_id_(),
    session_id_(-1), max_bound_(INT64_MAX), delta_size_(0), data_size_(0),
    max_mem_used_(0), mem_used_(0),
    pre_mem_used_(0), dumped_size_(0), data_ratio_(0.5), active_time_(0), number_pass_(0),
    calc_count_(0)
  {
    sql_id_[0] = '\0';
    ObRandom rand;
    random_id_ = rand.get();
  }

  OB_INLINE int64_t get_row_count() const { return row_count_; }
  OB_INLINE int64_t get_input_size() const { return input_size_; }
  OB_INLINE int64_t get_bucket_size() const { return bucket_size_; }

  OB_INLINE int64_t get_expect_size() const { return expect_size_; }
  OB_INLINE void set_expect_size(int64_t expect_size)
  {
    expect_size_ = expect_size;
  }

  OB_INLINE void set_basic_info(int64_t row_count, int64_t input_size, int64_t bucket_size)
  {
    const int64_t DEFAULT_INPUT_SIZE = 2 * 1024 * 1024;
    row_count_ = row_count;
    input_size_ = (input_size < 0) ? DEFAULT_INPUT_SIZE : input_size;
    bucket_size_ = bucket_size;
  }

  int set_exec_info(ObExecContext &exec_ctx);
  OB_INLINE void set_operator_type(ObPhyOperatorType op_type) { op_type_ = op_type; }
  OB_INLINE void set_operator_id(uint64_t op_id) { op_id_ = op_id; }
  OB_INLINE void set_exec_ctx(ObExecContext *exec_ctx) { exec_ctx_ = exec_ctx; }
  OB_INLINE ObExecContext* get_exec_ctx() const { return exec_ctx_; }
  bool has_exec_ctx() { return nullptr != exec_ctx_; }

  // one_pass_size = sqrt(cache_size * chunk_size)
  OB_INLINE void init(int64_t cache_size, int64_t chunk_size)
  {
    const int64_t DEFAULT_CACHE_SIZE = 2 * 1024 * 1024;
    if (cache_size < 0) {
      cache_size = DEFAULT_CACHE_SIZE;
    }
    chunk_size_ = chunk_size;
    one_pass_size_ = calc_one_pass_size(cache_size);
    cache_size_ = cache_size;
    min_size_ = MIN_BOUND_SIZE[type_];
  }

  OB_INLINE int64_t calc_one_pass_size(int64_t cache_size)
  {
    return sqrt((double)cache_size * chunk_size_) + 1;
  }

  OB_INLINE int64_t get_id() const { return random_id_; }
  OB_INLINE ObPhyOperatorType get_operator_type() const { return op_type_; }
  OB_INLINE int64_t get_operator_id() const { return op_id_; }
  OB_INLINE int64_t get_min_size() const { return min_size_; }
  OB_INLINE int64_t get_chunk_size() const { return chunk_size_; }
  OB_INLINE int64_t get_cache_size() const { return cache_size_; }
  OB_INLINE void set_cache_size(int64_t cache_size) { cache_size_ = cache_size; }
  OB_INLINE int64_t get_one_pass_size() const { return one_pass_size_; }
  OB_INLINE void set_one_pass_size(int64_t one_pass_size) { one_pass_size_ = one_pass_size; }
  OB_INLINE int64_t get_global_bound_size() const { return global_bound_size_; }
  OB_INLINE void set_global_bound_size(int64_t global_bound_size)
            { global_bound_size_ = global_bound_size; }
  OB_INLINE int64_t get_max_bound() const { return max_bound_; }
  OB_INLINE void set_max_bound(int64_t max_bound) { max_bound_ = max_bound; }

  OB_INLINE bool is_hash_join_wa() const { return ObSqlWorkAreaType::HASH_WORK_AREA == type_; }
  OB_INLINE bool is_sort_wa() const { return ObSqlWorkAreaType::SORT_WORK_AREA == type_; }
  OB_INLINE ObSqlWorkAreaType get_work_area_type() const { return type_; }
  OB_INLINE bool get_auto_policy() const { return OB_INVALID_ID != expect_size_; }

  OB_INLINE void set_active_time(int64_t active_time) { active_time_ = active_time; }
  OB_INLINE int64_t get_active_time() const { return active_time_; }
  OB_INLINE void set_number_pass(int32_t num_pass)
  {
    if (num_pass > number_pass_) {
      number_pass_ = num_pass;
    }
  }
  OB_INLINE int64_t get_number_pass() const { return number_pass_; }

  static bool auto_sql_memory_manager(ObSqlWorkAreaProfile &profile)
  {
    return MIN_BOUND_SIZE[profile.type_] < profile.cache_size_;
  }

  // for statistics
  int64_t get_delta_size() const { return delta_size_; }
  int64_t get_data_size() const { return data_size_; }
  int64_t get_max_mem_used() const { return max_mem_used_; }
  int64_t get_mem_used() const { return mem_used_; }
  int64_t get_dumped_size() const { return dumped_size_; }
  int64_t get_data_ratio() const { return data_ratio_; }
  OB_INLINE bool is_registered() const { return OB_NOT_NULL(get_next())
                                                || OB_NOT_NULL(get_prev()); }
  OB_INLINE int64_t get_calc_count() const { return calc_count_; }
  OB_INLINE void inc_calc_count() { ++calc_count_; }

  int64_t get_dop();
  uint64_t get_plan_id();
  uint64_t get_exec_id();
  const char* get_sql_id();
  uint64_t get_session_id();

  OB_INLINE bool need_profiled()
  {
    bool profiled = false;
    const char* sql_id = get_sql_id();
    if (OB_NOT_NULL(sql_id)) {
      profiled = ('\0' != sql_id[0]);
    }
    return profiled;
  }

  TO_STRING_KV(K_(random_id), K_(type), K_(op_id),
    K_(cache_size), K_(one_pass_size), K_(expect_size),
    K_(calc_count));

private:
  static const int64_t MIN_BOUND_SIZE[ObSqlWorkAreaType::MAX_TYPE];
  int64_t random_id_;
  ObSqlWorkAreaType type_;
  ObPhyOperatorType op_type_;
  uint64_t op_id_;
  ObExecContext *exec_ctx_;
  int64_t min_size_;
  int64_t row_count_;
  int64_t input_size_;
  int64_t bucket_size_;
  int64_t chunk_size_;
  int64_t cache_size_;
  int64_t one_pass_size_;
  int64_t expect_size_;
  int64_t global_bound_size_;
  int64_t dop_;
  int64_t plan_id_;
  int64_t exec_id_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  int64_t session_id_;
  // 取 min(cache_size, global_bound_size)
  // sort场景，在global_bound_size比较大情况下，sort理论上有data和extra内存，data应该是one-pass size
  // 也就是expect_size
  // 但总体内存应该是global_bound_size和cache size更小值，其实也可以用expect_size和global_bound_size取最大值
  int64_t max_bound_;
public:
  int64_t delta_size_;
  int64_t data_size_;
  int64_t max_mem_used_;
  int64_t mem_used_;
  int64_t pre_mem_used_;
  int64_t dumped_size_;
  double data_ratio_;
public:
  // some statistics
  int64_t active_time_;   // init: start_time, unregister:
  int64_t number_pass_;
  int64_t calc_count_;    // the times of calculate global bound
};

static constexpr const char *EXECUTION_OPTIMAL = "OPTIMAL";
static constexpr const char *EXECUTION_ONEPASS = "ONE PASS";
static constexpr const char *EXECUTION_MULTIPASSES = "MULTI-PASS";

static constexpr const char *EXECUTION_AUTO_POLICY = "AUTO";
static constexpr const char *EXECUTION_MANUAL_POLICY = "MANUAL";

class ObSqlWorkAreaStat
{
public:
  ObSqlWorkAreaStat():
    seqno_(INT64_MAX), workarea_key_(), op_type_(PHY_INVALID), est_cache_size_(0),
    est_one_pass_size_(0), last_memory_used_(0), last_execution_(0), last_degree_(0),
    total_executions_(0), optimal_executions_(0), onepass_executions_(0), multipass_executions_(0),
    active_avg_time_(0), max_temp_size_(0), last_temp_size_(0), is_auto_policy_(false)
  {}
public:
  // key: (sql_id, plan_id, operator_id) 可以确认相同执行的统计
  struct WorkareaKey {
    WorkareaKey(uint64_t plan_id, uint64_t operator_id) :
      plan_id_(plan_id), operator_id_(operator_id)
    {
      sql_id_[0] = '\0';
    }
    WorkareaKey() : plan_id_(UINT64_MAX), operator_id_(UINT64_MAX)
    {
      sql_id_[0] = '\0';
    }

    void set_sql_id(const char* sql_id)
    {
      if (nullptr != sql_id) {
        strncpy(sql_id_, sql_id, common::OB_MAX_SQL_ID_LENGTH);
        sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
      }
    }
    void set_plan_id(uint64_t plan_id) { plan_id_ = plan_id; }
    void set_operator_id(uint64_t op_id) { operator_id_ = op_id; }

    void assign(const WorkareaKey &other)
    {
      strncpy(sql_id_, other.sql_id_, common::OB_MAX_SQL_ID_LENGTH + 1);
      plan_id_ = other.plan_id_;
      operator_id_ = other.operator_id_;
    }
    WorkareaKey &operator=(const WorkareaKey &other)
    {
      assign(other);
      return *this;
    }
    int64_t hash() const
    {
      uint64_t val = common::murmurhash(&plan_id_, sizeof(plan_id_), 0);
      return common::murmurhash(&operator_id_, sizeof(operator_id_), val);
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }

    bool operator==(const WorkareaKey &other) const
    {
      return plan_id_ == other.plan_id_ && operator_id_ == other.operator_id_
          && 0 == MEMCMP(sql_id_, other.sql_id_, strlen(sql_id_));
    }
    TO_STRING_KV(K_(sql_id), K_(plan_id), K_(operator_id));
  public:
    char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];   // sql id
    uint64_t plan_id_;                                // plan id
    uint64_t operator_id_;                            // operator id
  }; // end WorkareaKey

  OB_INLINE void set_seqno(int64_t seqno) { seqno_ = seqno; }
  OB_INLINE int64_t get_seqno() { return seqno_; }
  OB_INLINE WorkareaKey get_workarea_key() const { return workarea_key_; }
  OB_INLINE const char* get_sql_id() const { return workarea_key_.sql_id_; }
  OB_INLINE uint64_t get_plan_id() const { return workarea_key_.plan_id_; }
  OB_INLINE uint64_t get_operator_id() const { return workarea_key_.operator_id_; }
  OB_INLINE ObPhyOperatorType get_op_type() const { return op_type_; }
  OB_INLINE int64_t get_est_cache_size() const { return est_cache_size_; }
  OB_INLINE int64_t get_est_one_pass_size() const { return est_one_pass_size_; }
  OB_INLINE int64_t get_last_memory_used() const { return last_memory_used_; }
  OB_INLINE int64_t get_last_execution() const { return last_execution_; }
  OB_INLINE int64_t get_last_degree() const { return last_degree_; }
  OB_INLINE int64_t get_total_executions() const { return total_executions_; }
  OB_INLINE int64_t get_optimal_executions() const { return optimal_executions_; }
  OB_INLINE int64_t get_onepass_executions() const { return onepass_executions_; }
  OB_INLINE int64_t get_multipass_executions() const { return multipass_executions_; }
  OB_INLINE int64_t get_active_avg_time() const { return active_avg_time_; }
  OB_INLINE int64_t get_max_temp_size() const { return max_temp_size_; }
  OB_INLINE int64_t get_last_temp_size() const { return last_temp_size_; }
  OB_INLINE bool get_auto_policy() const { return is_auto_policy_; }

  // reset
  void reset()
  {
    new (&workarea_key_) WorkareaKey();
  }
  // update
  OB_INLINE void increase_total_executions() { ATOMIC_AAF(&total_executions_, 1); }
  OB_INLINE void increase_optimal_executions() { ATOMIC_AAF(&optimal_executions_, 1); }
  OB_INLINE void increase_onepass_executions() { ATOMIC_AAF(&onepass_executions_, 1); }
  OB_INLINE void increase_multipass_executions() { ATOMIC_AAF(&multipass_executions_, 1); }

  TO_STRING_KV(K_(workarea_key), K_(op_type), K_(seqno));
public:
  int64_t seqno_;
  WorkareaKey workarea_key_;
  ObPhyOperatorType op_type_;     // operator类型
  int64_t est_cache_size_;        // 估计的全内存大小
  int64_t est_one_pass_size_;     // 估计的one pass大小
  int64_t last_memory_used_;      // 上次执行内存使用大小
  int64_t last_execution_;        // 上次执行的状态，是optimal、onepass还是multipass
  int64_t last_degree_;           // 上次执行dop
  int64_t total_executions_;      // 每个worker算执行一次
  int64_t optimal_executions_;    // 全内存执行次数
  int64_t onepass_executions_;    // onepass执行次数
  int64_t multipass_executions_;  // 多路执行次数
  int64_t active_avg_time_;       // 平均活跃时间
  int64_t max_temp_size_;         // 最大写临时文件大小
  int64_t last_temp_size_;        // 上次写临时文件大小
  bool is_auto_policy_;                 // 是否是auto还是manual，1: auto 0:manual
};

class ObSqlWorkareaProfileInfo
{
public:
  ObSqlWorkareaProfileInfo() :
    profile_(ObSqlWorkAreaType::MAX_TYPE), plan_id_(0), sql_exec_id_(0), session_id_(0)
  {
    sql_id_[0] = '\0';
  }

  void assign(const ObSqlWorkareaProfileInfo &other)
  {
    profile_ = other.profile_;
    strncpy(sql_id_, other.sql_id_, common::OB_MAX_SQL_ID_LENGTH + 1);
    plan_id_ = other.plan_id_;
    sql_exec_id_ = other.sql_exec_id_;
    session_id_ = other.session_id_;
  }

  ObSqlWorkareaProfileInfo &operator=(const ObSqlWorkareaProfileInfo &other)
  {
    assign(other);
    return *this;
  }

  void set_sql_id(const char* sql_id)
  {
    if (nullptr != sql_id) {
      strncpy(sql_id_, sql_id, common::OB_MAX_SQL_ID_LENGTH);
      sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    }
  }

  TO_STRING_KV(K_(sql_id), K_(plan_id), K_(sql_exec_id));
public:
  sql::ObSqlWorkAreaProfile profile_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t plan_id_;
  uint64_t sql_exec_id_;
  uint64_t session_id_;
};

class ObSqlWorkAreaIntervalStat
{
public:
  ObSqlWorkAreaIntervalStat() :
    total_hash_cnt_(0), total_hash_size_(0), total_sort_cnt_(0), total_sort_size_(0),
    total_sort_one_pass_size_(0), total_one_pass_cnt_(0), total_one_pass_size_(0)
  {}
public:
  void reset();
  int64_t get_total_hash_cnt() const { return total_hash_cnt_; }
  int64_t get_total_hash_size() const { return total_hash_size_; }
  int64_t get_total_sort_cnt() const { return total_sort_cnt_; }
  int64_t get_total_sort_size() const { return total_sort_size_; }
  int64_t get_total_sort_one_pass_size() const { return total_sort_one_pass_size_; }
  int64_t get_total_one_pass_cnt() const { return total_one_pass_cnt_; }
  int64_t get_total_one_pass_size() const { return total_one_pass_size_; }

  int analyze_profile(ObSqlWorkAreaProfile &profile, int64_t size, const int64_t one_pass_size,
    const int64_t max_size, bool is_one_pass = false);
private:
  int64_t total_hash_cnt_;
  int64_t total_hash_size_;
  int64_t total_sort_cnt_;
  int64_t total_sort_size_; // 主要用于sort的one pass内存
  int64_t total_sort_one_pass_size_;
  // 用来处理sort对应的one pass的统计，与total_sort_one_pass_size_区别在于当不能cache时，统计内存时
  // 需要减去前者（可以任务是所有sort对应的one pass大小）
  // 后者时one pass size在的某个interval中，当跨越间隔后，bound小于one pass size，前者是bound小于cache size
  // 所以一个interval有3个点：hash size、sort size、one_pass size
  // 这里其实可以把统计放到total_hash中统计，暂时实现是分开
  int64_t total_one_pass_cnt_;
  int64_t total_one_pass_size_;
};

class ObTenantSqlMemoryCallback : public ObSqlMemoryCallback
{
public:
  ObTenantSqlMemoryCallback() :
    total_alloc_size_(0), total_dump_size_(0)
  {}

public:
  virtual void alloc(int64_t size) override;
  virtual void free(int64_t size) override;
  virtual void dumped(int64_t size) override;

  void reset() { total_alloc_size_ = 0; total_dump_size_ = 0; }
  int64_t get_total_alloc_size() const { return total_alloc_size_; }
  int64_t get_total_dump_size() const { return total_dump_size_; }
private:
  int64_t total_alloc_size_;
  int64_t total_dump_size_;
};

class ObSqlWorkAreaInterval
{
public:
  ObSqlWorkAreaInterval(int64_t interval_idx, int64_t interval_cache_size) :
    interval_idx_(interval_idx), interval_cache_size_(interval_cache_size),
    mem_target_(-1), interval_stat_()
  {}

public:
  OB_INLINE int64_t get_interval_idx() const { return interval_idx_; }
  OB_INLINE int64_t get_interval_cache_size() const { return interval_cache_size_; }

  ObSqlWorkAreaIntervalStat &get_interval_stat() { return interval_stat_; }

  OB_INLINE int64_t get_mem_target() const { return mem_target_; }
  void set_mem_target(int64_t mem_target) { mem_target_ = mem_target; }
private:
  int64_t interval_idx_;
  int64_t interval_cache_size_; //当前下标对应的内存值
  int64_t mem_target_;
  ObSqlWorkAreaIntervalStat interval_stat_;
};

class ObWorkareaHistogram
{
public:
  ObWorkareaHistogram(int64_t low_optimal_size, int64_t high_optimal_size) :
    low_optimal_size_(low_optimal_size), high_optimal_size_(high_optimal_size),
    optimal_executions_(0), onepass_executions_(0), multipass_executions_(0), total_executions_(0)
  {}

  ObWorkareaHistogram() :
    low_optimal_size_(INT64_MAX), high_optimal_size_(INT64_MAX),
    optimal_executions_(0), onepass_executions_(0), multipass_executions_(0), total_executions_(0)
  {}

  OB_INLINE int64_t get_low_optimal_size() const { return low_optimal_size_; }
  OB_INLINE int64_t get_high_optimal_size() const { return high_optimal_size_; }
  OB_INLINE int64_t get_optimal_executions() const { return optimal_executions_; }
  OB_INLINE int64_t get_onepass_executions() const { return onepass_executions_; }
  OB_INLINE int64_t get_multipass_executions() const { return multipass_executions_; }
  OB_INLINE int64_t get_total_executions() const { return total_executions_; }

  OB_INLINE void increase_optimal_executions() { ATOMIC_AAF(&optimal_executions_, 1); }
  OB_INLINE void increase_onepass_executions() { ATOMIC_AAF(&onepass_executions_, 1); }
  OB_INLINE void increase_multipass_executions() { ATOMIC_AAF(&multipass_executions_, 1); }
  OB_INLINE void increase_total_executions() { ATOMIC_AAF(&total_executions_, 1); }

  TO_STRING_KV(K_(low_optimal_size), K_(high_optimal_size));
private:
  int64_t low_optimal_size_;
  int64_t high_optimal_size_;
  int64_t optimal_executions_;
  int64_t onepass_executions_;
  int64_t multipass_executions_;
  int64_t total_executions_;
};

class ObSqlMemoryList
{
public:
  ObSqlMemoryList(int64_t seqno) :
    seqno_(seqno), lock_(common::ObLatchIds::SQL_WA_PROFILE_LIST_LOCK)
  {}
  ~ObSqlMemoryList() { reset(); }

  void reset();
  int register_work_area_profile(ObSqlWorkAreaProfile &profile);
  int unregister_work_area_profile(ObSqlWorkAreaProfile &profile);

  common::ObDList<ObSqlWorkAreaProfile> &get_profile_list() { return profile_list_; }
  ObSpinLock &get_lock() { return lock_; }
  TO_STRING_KV(K_(seqno));
private:
  int64_t seqno_;
  ObSpinLock lock_;
  common::ObDList<ObSqlWorkAreaProfile> profile_list_;
};

class ObSqlWorkareaCurrentMemoryInfo
{
public:
  ObSqlWorkareaCurrentMemoryInfo() :
    enable_(false), max_workarea_size_(0), workarea_hold_size_(0), max_auto_workarea_size_(0),
    mem_target_(0), total_mem_used_(0), global_bound_size_(0), drift_size_(0),
    workarea_cnt_(0), manual_calc_cnt_(0)
  {}

  int64_t get_max_workarea_size() const { return max_workarea_size_; }
  int64_t get_workarea_hold_size() const { return workarea_hold_size_; }
  int64_t get_max_auto_workarea_size() const { return max_auto_workarea_size_; }
  int64_t get_mem_target() const { return mem_target_; }
  int64_t get_total_mem_used() const { return total_mem_used_; }
  int64_t get_global_bound_size() const { return global_bound_size_; }
  int64_t get_drift_size() const { return drift_size_; }
  int64_t get_workarea_cnt() const { return workarea_cnt_; }
  int64_t get_manual_calc_cnt() const { return manual_calc_cnt_; }

  bool is_valid() { return enable_; }
  bool enable_;
  int64_t max_workarea_size_;
  int64_t workarea_hold_size_;
  int64_t max_auto_workarea_size_;
  int64_t mem_target_;
  int64_t total_mem_used_;
  int64_t global_bound_size_;
  int64_t drift_size_;
  int64_t workarea_cnt_;
  int64_t manual_calc_cnt_;
};

class ObTenantSqlMemoryManager
{
private:
  static const int64_t MAX_WORKAREA_STAT_CNT = 1024;
public:
  class ObSqlWorkAreaCalcInfo
  {
  public:
    ObSqlWorkAreaCalcInfo() :
      wa_intervals_(nullptr), profile_cnt_(0), mem_target_(0), global_bound_size_(0),
      tmp_no_cache_cnt_(0), min_bound_size_(MIN_GLOBAL_BOUND_SIZE)
    {}
    ~ObSqlWorkAreaCalcInfo() = default;

    int init(ObIAllocator &allocator, ObSqlWorkAreaInterval *wa_intervals, int64_t interval_cnt);
    void destroy(common::ObIAllocator &allocator);

    int64_t get_global_bound_size() const { return global_bound_size_; }
    int64_t get_mem_target() const { return mem_target_; }

    int calculate_global_bound_size(
      const int64_t wa_max_memory_size,
      const int64_t total_memory_size,
      const int64_t profile_cnt,
      const bool auto_calc);
    ObSqlWorkAreaInterval *get_wa_intervals() { return wa_intervals_; }
private:
    int find_best_interval_index_by_mem_target(
      int64_t &interval_idx,
      const int64_t expect_mem_target,
      const int64_t total_memory_size);
    int calc_memory_target(int64_t idx, const int64_t pre_mem_target);
  private:
    ObSqlWorkAreaInterval *wa_intervals_;
    int64_t profile_cnt_;
    int64_t mem_target_;
    int64_t global_bound_size_;
    int64_t tmp_no_cache_cnt_;
    int64_t min_bound_size_;
  };
public:
  ObTenantSqlMemoryManager(int64_t tenant_id) :
    wa_intervals_(nullptr), min_bound_size_(0), tenant_id_(tenant_id),
    enable_auto_memory_mgr_(false), mutex_(common::ObLatchIds::SQL_MEMORY_MGR_MUTEX_LOCK), profile_lists_(nullptr),
    drift_size_(0), profile_cnt_(0), pre_profile_cnt_(0), global_bound_size_(0),
    mem_target_(0), max_workarea_size_(0), workarea_hold_size_(0), max_auto_workarea_size_(0),
    max_tenant_memory_size_(0),
    manual_calc_cnt_(0), wa_start_(0), wa_end_(0), wa_cnt_(0),
    lock_()
  {}
  ~ObTenantSqlMemoryManager() {}
public:
  static int mtl_init(ObTenantSqlMemoryManager *&sql_mem_mgr);
  static void mtl_destroy(ObTenantSqlMemoryManager *&sql_mem_mgr);

  int get_work_area_size(ObIAllocator *allocator, ObSqlWorkAreaProfile &profile);

  int register_work_area_profile(ObSqlWorkAreaProfile &profile);
  // 由外层逻辑更新profile的信息以及一些变化信息
  int update_work_area_profile(
    common::ObIAllocator *allocator,
    ObSqlWorkAreaProfile &profile,
    const int64_t delta_size);
  int unregister_work_area_profile(ObSqlWorkAreaProfile &profile);

  int calculate_global_bound_size_by_interval_info(
    common::ObIAllocator &allocator,
    const int64_t wa_max_memory_size,
    const bool auto_calc);
  int calculate_global_bound_size(common::ObIAllocator *allocator = nullptr, bool auto_calc = true);
  OB_INLINE int64_t get_global_bound_size() { return ATOMIC_LOAD(&global_bound_size_); }

  // OB_INLINE int64_t get_max_memory_work_area_size()
  // {
  //   int64_t percent_execpt_memstore = 100 - GCONF.memstore_limit_percentage;
  //   return lib::get_tenant_memory_limit(tenant_id_) * percent_execpt_memstore / 100;
  // }

  OB_INLINE bool enable_auto_memory_mgr() { return enable_auto_memory_mgr_; }

  ObTenantSqlMemoryCallback* get_sql_memory_callback() { return &sql_mem_callback_; }

  int get_workarea_stat(common::ObIArray<ObSqlWorkAreaStat> &wa_stats);
  int get_workarea_histogram(common::ObIArray<ObWorkareaHistogram> &wa_histograms);
  int get_all_active_workarea(common::ObIArray<ObSqlWorkareaProfileInfo> &wa_actives);
  int get_workarea_memory_info(ObSqlWorkareaCurrentMemoryInfo &memory_info);

  int64_t get_max_workarea_size() const { return max_workarea_size_; }
  int64_t get_workarea_hold_size() const { return workarea_hold_size_; }
  int64_t get_max_auto_workarea_size() const { return max_auto_workarea_size_; }
  int64_t get_mem_target() const { return mem_target_; }
  int64_t get_drift_size() const { return drift_size_; }
  int64_t get_workarea_count() const { return profile_cnt_; }
  int64_t get_manual_calc_count() const { return manual_calc_cnt_; }
  int64_t get_total_mem_used() const { return sql_mem_callback_.get_total_alloc_size(); }
private:
  OB_INLINE bool need_manual_calc_bound();
  OB_INLINE bool need_manual_by_drift();

  OB_INLINE void increase(int64_t size)
  { (ATOMIC_AAF(&drift_size_, size)); ATOMIC_INC(&profile_cnt_); }
  OB_INLINE void decrease(int64_t size)
  { (ATOMIC_SAF(&drift_size_, size)); ATOMIC_DEC(&profile_cnt_); }
  OB_INLINE int64_t get_drift_size() { return (ATOMIC_LOAD(&drift_size_)); }

  void reset();
  int try_push_profiles_work_area_size(int64_t global_bound_size);
  int calc_work_area_size_by_profile(int64_t global_bound_size, ObSqlWorkAreaProfile &profile);
  bool enable_auto_sql_memory_manager();
  int get_max_work_area_size(int64_t &max_wa_memory_size, const bool auto_calc);
  int find_interval_index(const int64_t cache_size, int64_t &idx, int64_t &out_cache_size);
  int count_profile_into_work_area_intervals(
    ObSqlWorkAreaInterval *wa_intervals,
    int64_t &total_memory_size,
    int64_t &cur_profile_cnt);

  bool is_wa_full() { return MAX_WORKAREA_STAT_CNT == wa_cnt_; }

  static int64_t get_hash_value(int64_t id)
  {
    uint64_t val = common::murmurhash(&id, sizeof(id), 0);
    return val % HASH_CNT;
  }
private:
  int fill_workarea_stat(
    ObSqlWorkAreaStat &wa_stat,
    ObSqlWorkAreaProfile &profile);
  int try_fill_workarea_stat(
    ObSqlWorkAreaStat::WorkareaKey &workarea_key,
    ObSqlWorkAreaProfile &profile,
    bool &need_insert);
  int collect_workarea_stat(ObSqlWorkAreaProfile &profile);
  int fill_workarea_histogram(ObSqlWorkAreaProfile &profile);
  int new_and_fill_workarea_stat(
    ObSqlWorkAreaStat::WorkareaKey &workarea_key,
    ObSqlWorkAreaProfile &profile);
private:
  //interval define
  // 1 区间间隔划分(单位为M)
  //   区间     -> 间隔大小 间隔个数
  //   [0,100] -> 1, 100个点
  //   [100, 500] -> 2， 200个点
  //   [500, 1000] -> 5， 100个点
  //   [1000, 5000] -> 10, 400个点
  //   [5000, 10000] -> 50, 100个点
  //   [10000, 100000] -> 900, 100个点
  //   [100000, 1000000] -> 9000, 100个点
  static const int64_t INTERVAL_NUM = 1100;
  static const int64_t LESS_THAN_100M_INTERVAL_SIZE = 1 * 1024 * 1024;
  static const int64_t LESS_THAN_100M_CNT = 100;
  static const int64_t LESS_THAN_500M_INTERVAL_SIZE = 2 * 1024 * 1024;
  static const int64_t LESS_THAN_500M_CNT = 300;
  static const int64_t LESS_THAN_1G_INTERVAL_SIZE = 5 * 1024 * 1024;
  static const int64_t LESS_THAN_1G_CNT = 400;
  static const int64_t LESS_THAN_5G_INTERVAL_SIZE = 10 * 1024 * 1024;
  static const int64_t LESS_THAN_5G_CNT = 800;
  static const int64_t LESS_THAN_10G_INTERVAL_SIZE = 50 * 1024 * 1024;
  static const int64_t LESS_THAN_10G_CNT = 900;
  static const int64_t LESS_THAN_100G_INTERVAL_SIZE = 900 * 1024 * 1024L;
  static const int64_t LESS_THAN_100G_CNT = 1000;
  static const int64_t LESS_THAN_1T_INTERVAL_SIZE = 9000 * 1024 * 1024L;
  static const int64_t LESS_THAN_1T_CNT = 1100;
  static const int64_t MAX_INTERVAL_SIZE = 1000 * 1000 * 1024L * 1024L;

  static const int64_t MIN_GLOBAL_BOUND_SIZE = 1 * 1024 * 1024;

  static const int64_t DRIFT_PERCENT = 10;
  static const int64_t DRIFT_CNT_PERCENT = 10;

  static const int64_t HASH_CNT = 256;

  ObTenantSqlMemoryCallback sql_mem_callback_;
  common::ObFIFOAllocator allocator_;
  ObSqlWorkAreaInterval *wa_intervals_;
  int64_t min_bound_size_;
  int64_t tenant_id_;

  bool enable_auto_memory_mgr_;
  bool pre_enable_auto_memory_mgr_;
  lib::ObMutex mutex_;
  ObSqlMemoryList *profile_lists_;
  int64_t drift_size_;
  int64_t profile_cnt_;
  int64_t pre_profile_cnt_;
  int64_t global_bound_size_;
  int64_t mem_target_;
  int64_t max_workarea_size_;
  int64_t workarea_hold_size_;
  int64_t max_auto_workarea_size_;
  int64_t max_tenant_memory_size_;

  // statistics
  int64_t manual_calc_cnt_;

  int64_t wa_start_;
  int64_t wa_end_;
  int64_t wa_cnt_;
  ObLatch lock_;
  hash::ObHashMap<ObSqlWorkAreaStat::WorkareaKey,
      ObSqlWorkAreaStat*, hash::NoPthreadDefendMode> wa_ht_;
  ObSEArray<ObSqlWorkAreaStat, MAX_WORKAREA_STAT_CNT> workarea_stats_;
  ObSEArray<ObWorkareaHistogram, INTERVAL_NUM> workarea_histograms_;
};


OB_INLINE bool ObTenantSqlMemoryManager::need_manual_by_drift()
{
  return (drift_size_ > 0 && mem_target_ * DRIFT_PERCENT / 100 < drift_size_)
      || (drift_size_ < 0 && mem_target_ * DRIFT_PERCENT / 100 < -drift_size_);
}

OB_INLINE bool ObTenantSqlMemoryManager::need_manual_calc_bound()
{
  bool manual_calc_bound = false;
  if (0 == global_bound_size_) {
    manual_calc_bound = true;
  } else {
    // [-10%, 10%]
    if (need_manual_by_drift()) {
      manual_calc_bound = true;
    } else {
      int64_t delta_cnt = pre_profile_cnt_ - profile_cnt_;
      if (delta_cnt > 0) {
        manual_calc_bound = profile_cnt_ * DRIFT_CNT_PERCENT / 100 < delta_cnt;
      } else {
        manual_calc_bound = profile_cnt_ * DRIFT_CNT_PERCENT / 100 < -delta_cnt;
      }
    }
  }
  return manual_calc_bound;
}

OB_INLINE void ObTenantSqlMemoryCallback::alloc(int64_t size)
{
  (ATOMIC_AAF(&total_alloc_size_, size));
}

OB_INLINE void ObTenantSqlMemoryCallback::free(int64_t size)
{
  (ATOMIC_SAF(&total_alloc_size_, size));
}

OB_INLINE void ObTenantSqlMemoryCallback::dumped(int64_t size)
{
  (ATOMIC_AAF(&total_dump_size_, size));
}

} // sql
} // oceanbase
#endif /* OB_DTL_FC_SERVER_H */
