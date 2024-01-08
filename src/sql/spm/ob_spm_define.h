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

#ifndef OCEANBASE_SQL_SPM_OB_SPM_DEFINE_H_
#define OCEANBASE_SQL_SPM_OB_SPM_DEFINE_H_
#include "sql/ob_sql_define.h"
#include "share/ob_define.h"
#include "sql/spm/ob_spm_struct.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"

namespace oceanbase
{
namespace sql
{

enum PlanBaselineFlag
{
  PLAN_BASELINE_ENABLED = 1,
  PLAN_BASELINE_ACCEPTED = 2,
  PLAN_BASELINE_FIXED = 4,
  PLAN_BASELINE_AUTOPURGE = 8,
  PLAN_BASELINE_REPRODUCED = 16
};

struct ObBaselineKey : public ObILibCacheKey
{
  ObBaselineKey()
  : ObILibCacheKey(ObLibCacheNameSpace::NS_SPM), 
    db_id_(common::OB_INVALID_ID),
    constructed_sql_(),
    sql_id_(),
    sql_cs_type_(common::ObCollationType::CS_TYPE_INVALID) {}
  ObBaselineKey(const ObBaselineKey &other)
  : ObILibCacheKey(ObLibCacheNameSpace::NS_SPM),
    db_id_(other.db_id_),
    constructed_sql_(other.constructed_sql_),
    sql_id_(other.sql_id_),
    sql_cs_type_(other.sql_cs_type_) {}
  ObBaselineKey(uint64_t db_id, const ObString &constructed_sql, const ObString &sql_id)
  : ObILibCacheKey(ObLibCacheNameSpace::NS_SPM),
    db_id_(db_id),
    constructed_sql_(constructed_sql),
    sql_id_(sql_id),
    sql_cs_type_(common::ObCollationType::CS_TYPE_INVALID) {}

  void reset();
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other) override;
  void destory(common::ObIAllocator &allocator);
  virtual uint64_t hash() const override;
  virtual bool is_equal(const ObILibCacheKey &other) const;
  
  TO_STRING_KV(K_(db_id),
               K_(constructed_sql),
               K_(sql_id),
               K_(namespace));

  uint64_t  db_id_;
  common::ObString constructed_sql_;    // Storing data only. not use in operator== and hash.
  common::ObString sql_id_;
  common::ObCollationType sql_cs_type_; // Storing data only. not use in operator== and hash.
};

class ObPlanBaselineItem : public ObILibCacheObject
{
public:
  explicit ObPlanBaselineItem(lib::MemoryContext &mem_context = CURRENT_CONTEXT)
  : ObILibCacheObject(ObLibCacheNameSpace::NS_SPM, mem_context),
    origin_sql_text_(),
    origin_(-1),
    db_version_(),
    last_executed_(OB_INVALID_ID),
    last_verified_(OB_INVALID_ID),
    plan_hash_value_(OB_INVALID_ID),
    plan_type_(OB_PHY_PLAN_UNINITIALIZED),
    // constraint
    outline_data_(),
    flags_(0),
    optimizer_cost_(UINT64_MAX),
    executions_(0),
    elapsed_time_(UINT64_MAX),
    cpu_time_( UINT64_MAX),
    need_evict_(false),
    need_sync_(false) {}

  virtual ~ObPlanBaselineItem() { destroy(); }
  
  void reset();
  void destroy();
  inline const char *extract_str(const ObString &str) const { return str.empty() ? "" : str.ptr(); }
  inline const char *get_origin_sql_text() const { return extract_str(origin_sql_text_); }
  inline const common::ObString &get_origin_sql_text_str() const { return origin_sql_text_; }
  int set_origin_sql_text(const char *sql_text)
  {
    return ob_write_string(allocator_, ObString(strlen(sql_text), sql_text), origin_sql_text_);
  }
  int set_origin_sql_text(const common::ObString &sql_text)
  {
    return ob_write_string(allocator_, sql_text, origin_sql_text_);
  }
  inline int64_t get_origin() const { return origin_; }
  inline void set_origin(const int64_t v) { origin_ = v; }
  inline common::ObString get_db_version() const { return db_version_; }
  int set_db_version(const char *version)
  {
    return ob_write_string(allocator_, ObString(strlen(version), version), db_version_);
  }
  int set_db_version(const common::ObString &version)
  {
    return ob_write_string(allocator_, version, db_version_);
  }
  inline uint64_t get_last_executed() const { return last_executed_; }
  inline void set_last_executed(const uint64_t v) { last_executed_ = v; }
  inline uint64_t get_last_verified() const { return last_verified_; }
  inline void set_last_verified(const uint64_t v) { last_verified_ = v; }
  inline uint64_t get_plan_hash_value() const { return plan_hash_value_; }
  inline void set_plan_hash_value(uint64_t v) { plan_hash_value_ = v;}
  inline ObPhyPlanType get_plan_type() const { return plan_type_; }
  inline void set_plan_type(const ObPhyPlanType v) { plan_type_ = v; }
  inline void set_plan_type(const int64_t v) { plan_type_ = static_cast<const ObPhyPlanType>(v); }
  inline const char *get_outline_data() const { return extract_str(outline_data_); }
  inline const common::ObString &get_outline_data_str() const { return outline_data_; }
  int set_outline_data(const char *outline_data)
  {
    return ob_write_string(allocator_, ObString(strlen(outline_data), outline_data), outline_data_);
  }
  int set_outline_data(const common::ObString &outline_data)
  {
    return ob_write_string(allocator_, outline_data, outline_data_);
  }
  inline int64_t get_flags() const { return flags_; }
  inline void set_flags(const int64_t v) { flags_ = v; }
  inline bool get_enabled() const { return flags_ & PlanBaselineFlag::PLAN_BASELINE_ENABLED; }
  inline void set_enabled() { flags_ |= PlanBaselineFlag::PLAN_BASELINE_ENABLED; }
  inline void unset_enabled() { flags_ &= ~PlanBaselineFlag::PLAN_BASELINE_ENABLED; }
  inline bool get_accepted() const { return flags_ & PlanBaselineFlag::PLAN_BASELINE_ACCEPTED; }
  inline void set_accepted() { flags_ |= PlanBaselineFlag::PLAN_BASELINE_ACCEPTED; }
  inline void unset_accepted() { flags_ &= ~PlanBaselineFlag::PLAN_BASELINE_ACCEPTED; }
  inline bool get_fixed() const { return flags_ & PlanBaselineFlag::PLAN_BASELINE_FIXED; }
  inline void set_fixed() { flags_ |= PlanBaselineFlag::PLAN_BASELINE_FIXED; }
  inline void unset_fixed() { flags_ &= ~PlanBaselineFlag::PLAN_BASELINE_FIXED; }
  inline bool get_reproduced() const { return flags_ & PlanBaselineFlag::PLAN_BASELINE_REPRODUCED; }
  inline void set_reproduced() { flags_ |= PlanBaselineFlag::PLAN_BASELINE_REPRODUCED; }
  inline void unset_reproduced() { flags_ &= ~PlanBaselineFlag::PLAN_BASELINE_REPRODUCED; }
  inline bool get_autopurge() const { return flags_ & PlanBaselineFlag::PLAN_BASELINE_AUTOPURGE; }
  inline void set_autopurge() { flags_ |= PlanBaselineFlag::PLAN_BASELINE_AUTOPURGE; }
  inline void unset_autopurge() { flags_ &= ~PlanBaselineFlag::PLAN_BASELINE_AUTOPURGE; }
  inline uint64_t get_optimizer_cost() const { return optimizer_cost_; }
  inline void set_optimizer_cost(const uint64_t v) { optimizer_cost_ = v; }
  inline int64_t get_executions() const { return executions_; }
  inline void set_executions(int64_t v) { executions_ = v; }
  inline int64_t get_elapsed_time() const { return elapsed_time_; }
  inline void set_elapsed_time(int64_t v) { elapsed_time_ = v; }
  inline int64_t get_cpu_time() const { return cpu_time_; }
  inline void set_cpu_time(int64_t v) { cpu_time_ = v; }
  inline bool get_need_evict() const { return need_evict_; }
  inline void set_need_evict(bool v) { need_evict_ = v; }
  inline bool get_need_sync() const { return need_sync_; }
  inline void set_need_sync(bool v) { need_sync_ = v; }
  
  int check_basic_constraint_match(bool& is_match);
  int merge_baseline_item(ObPlanBaselineItem& other);
  
  TO_STRING_KV(K_(origin), K_(db_version), K_(last_executed),
               K_(last_verified), K_(plan_hash_value), K_(plan_type), K_(outline_data),
               K_(flags), K_(optimizer_cost), K_(executions), K_(elapsed_time), K_(cpu_time));

public:
  common::ObString origin_sql_text_;  // origin sql text for baseline
  int64_t origin_;  // baseline source, 1 for AUTO-CAPTURE, 2 for MANUAL-LOAD
  common::ObString db_version_;  // database version when generate baseline
  uint64_t last_executed_;  // last time when baseline executed
  uint64_t last_verified_;  // last time when baseline verified
  uint64_t plan_hash_value_;
  ObPhyPlanType plan_type_;
  //constraint
  common::ObString outline_data_;  // Hint collection of fixed plan
  int64_t flags_;
  uint64_t optimizer_cost_;
  int64_t  executions_;       // The total number of executions in the evolution process
  int64_t  elapsed_time_;         // The total elapsed time consumed during the evolution process
  int64_t  cpu_time_;         // The total CPU time consumed during the evolution process
  // common::ObString hints_info_;
  // for lib cache management
  bool need_evict_;
  bool need_sync_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanBaselineItem);
};

class ObSpmSet : public ObILibCacheNode
{

public:
  ObSpmSet(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : ObILibCacheNode(lib_cache, mem_context),
      is_inited_(false),
      baseline_key_(),
      enabled_fixed_baseline_count_(0)
  {
  }
  virtual ~ObSpmSet()
  {
    destroy();
  };
  virtual int init(ObILibCacheCtx &ctx, const ObILibCacheObject *cache_obj) override;
  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj) override;
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj) override;
  void destroy();
  void set_baseline_key(ObBaselineKey &key) { baseline_key_ = key; }
  ObBaselineKey &get_baseline_key() { return baseline_key_; }

  TO_STRING_KV(K_(is_inited));
private:
  bool is_inited_;
  ObBaselineKey baseline_key_; //used for manager key memory
  common::ObSEArray<ObPlanBaselineItem*, 4> baseline_array_;
  common::ObSEArray<ObPlanBaselineItem*, 4> fixed_baseline_array_;
  int64_t enabled_fixed_baseline_count_;
};

struct BaselineCmp
{
  inline bool operator()(const ObPlanBaselineItem *left, const ObPlanBaselineItem *right)
  {
    bool bret = false;
    if (left != nullptr && right != nullptr) {
      bret = left->get_elapsed_time() < right->get_elapsed_time();
    }
    return bret;
  }
};

struct ObSpmCacheCtx : public ObILibCacheCtx
{
  ObSpmCacheCtx()
    : ObILibCacheCtx(),
      bl_key_(),
      handle_cache_mode_(MODE_INVALID),
      plan_hash_value_(0),
      offset_(-1),
      check_execute_status_(false),
      is_retry_for_spm_(false),
      capture_baseline_(false),
      new_plan_hash_(0),
      baseline_guard_(PLAN_BASELINE_HANDLE),
      spm_stat_(STAT_INVALID),
      cache_node_empty_(true),
      spm_force_disable_(false),
      has_fixed_plan_to_check_(false),
      evolution_plan_type_(OB_PHY_PLAN_UNINITIALIZED),
      select_plan_type_(INVALID_TYPE),
      cur_baseline_not_enable_(false),
      need_spm_timeout_(false),
      baseline_exec_time_(0),
      evolution_task_in_two_plan_set_(false)
  {}
  enum SpmMode {
    MODE_INVALID,
    // for get cache obj
    MODE_GET_NORMAL,
    MODE_GET_OFFSET,
    MODE_GET_FOR_UPDATE,
    MODE_GET_UNACCEPT_FOR_UPDATE,
    // for add cache obj
    MODE_ADD_FROM_SERVER,
    MODE_ADD_FROM_INNER_TABLE,
    MODE_ADD_FORCE,
    MODE_MAX
  };
  enum SpmStat {
    STAT_INVALID,
    STAT_ADD_EVOLUTION_PLAN,      // add evolution plan to plan cache evolution layer
    STAT_ADD_BASELINE_PLAN,       // add baseline plan to plan cache evolution layer
    STAT_ACCEPT_EVOLUTION_PLAN,   // accept evolution plan as baseline and move it from evolution layer to plan layer 
    STAT_ACCEPT_BASELINE_PLAN,    // move baseline plan from evolution layer to plan layer 
    STAT_FIRST_EXECUTE_PLAN,
    STAT_FALLBACK_EXECUTE_PLAN,
    STAT_MAX
  };
  enum SpmSelectPlanType
  {
    INVALID_TYPE,
    EVO_PLAN,
    BASELINE_PLAN,
    MAX_TYPE
  };
  void reset();
  void set_get_normal_mode(uint64_t v) { plan_hash_value_ = v; handle_cache_mode_ = MODE_GET_NORMAL; }
  void set_get_offset_mode() { handle_cache_mode_ = MODE_GET_OFFSET; }
  void set_get_for_update_mode(uint64_t v) { plan_hash_value_ = v; handle_cache_mode_ = MODE_GET_FOR_UPDATE; }
  void set_get_unaccept_for_update_mode() { handle_cache_mode_ = MODE_GET_UNACCEPT_FOR_UPDATE; }
  void set_add_from_server_mode() { handle_cache_mode_ = MODE_ADD_FROM_SERVER; }
  void set_add_from_inner_table_mode() { handle_cache_mode_ = MODE_ADD_FROM_INNER_TABLE; }
  void set_add_force_mode() { handle_cache_mode_ = MODE_ADD_FORCE; }
  bool force_get_evolution_plan()
  {
    return (STAT_ACCEPT_EVOLUTION_PLAN == spm_stat_) || (STAT_ACCEPT_BASELINE_PLAN == spm_stat_);
  }
  bool is_spm_in_process()
  {
    return is_retry_for_spm_ ||
           (spm_stat_ > STAT_INVALID && spm_stat_ < STAT_MAX);
  }
  
  ObBaselineKey bl_key_;
  SpmMode handle_cache_mode_;
  uint64_t plan_hash_value_;
  int64_t offset_;
  bool check_execute_status_;
  bool is_retry_for_spm_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  bool capture_baseline_;
  uint64_t new_plan_hash_;
  ObCacheObjGuard baseline_guard_;
  SpmStat spm_stat_;
  bool cache_node_empty_;
  bool spm_force_disable_;
  bool has_fixed_plan_to_check_;
  ObPhyPlanType evolution_plan_type_;
  SpmSelectPlanType select_plan_type_; // for retry
  bool cur_baseline_not_enable_;
  bool need_spm_timeout_;
  int64_t baseline_exec_time_;
  bool evolution_task_in_two_plan_set_;
};

struct EvolutionTaskResult
{
public:
  EvolutionTaskResult()
  : key_(),
    accept_new_plan_(false),
    new_plan_hash_(0),
    new_stat_()
  {}
  ~EvolutionTaskResult() {}
int deep_copy(common::ObIAllocator& allocator, const EvolutionTaskResult& other);
public:
  ObBaselineKey key_;
  bool accept_new_plan_;
  // old plan statistics
  common::ObSEArray<uint64_t, 4> old_plan_hash_array_;
  common::ObSEArray<ObEvolutionStat, 4> old_stat_array_;
  // new plan statistics
  uint64_t new_plan_hash_;
  ObEvolutionStat new_stat_;
};

struct EvoResultUpdateTask
{
public:
  EvoResultUpdateTask()
  : key_(),
    plan_hash_(),
    plan_stat_()
  {}
  ~EvoResultUpdateTask() {}
  int init_task(common::ObIAllocator& allocator, const EvolutionTaskResult& evo_task_result);
  TO_STRING_KV(K_(key), K_(plan_hash), K_(plan_stat));
public:
  ObBaselineKey key_;
  common::ObSEArray<uint64_t, 4> plan_hash_;
  common::ObSEArray<ObEvolutionStat, 4> plan_stat_;
};

} // namespace sql end
} // namespace oceanbase end

#endif