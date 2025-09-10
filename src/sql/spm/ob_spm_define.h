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
class ObSqlPlanSet;
struct ObSpmCacheCtx;

enum PlanBaselineFlag
{
  PLAN_BASELINE_ENABLED = 1,
  PLAN_BASELINE_ACCEPTED = 2,
  PLAN_BASELINE_FIXED = 4,
  PLAN_BASELINE_AUTOPURGE = 8,
  PLAN_BASELINE_REPRODUCED = 16
};

#define OB_SPM_MAX_ORDERED_BASELINE_RETRY_TIMES 20

struct ObBaselineKey : public ObILibCacheKey
{
  ObBaselineKey()
  : ObILibCacheKey(ObLibCacheNameSpace::NS_SPM), 
    db_id_(common::OB_INVALID_ID),
    constructed_sql_(),
    sql_id_(),
    format_sql_(),
    format_sql_id_(),
    sql_cs_type_(common::ObCollationType::CS_TYPE_INVALID) {}
  ObBaselineKey(const ObBaselineKey &other)
  : ObILibCacheKey(ObLibCacheNameSpace::NS_SPM),
    db_id_(other.db_id_),
    constructed_sql_(other.constructed_sql_),
    sql_id_(other.sql_id_),
    format_sql_(other.format_sql_),
    format_sql_id_(other.format_sql_id_),
    sql_cs_type_(other.sql_cs_type_) {}
  ObBaselineKey(uint64_t db_id, const ObString &constructed_sql,
                const ObString &sql_id, const ObString &format_sql_id,
                const ObString &format_sql)
  : ObILibCacheKey(ObLibCacheNameSpace::NS_SPM),
    db_id_(db_id),
    constructed_sql_(constructed_sql),
    sql_id_(sql_id),
    format_sql_(format_sql),
    format_sql_id_(format_sql_id),
    sql_cs_type_(common::ObCollationType::CS_TYPE_INVALID) {}

  void reset();
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other) override;
  void destory(common::ObIAllocator &allocator);
  virtual uint64_t hash() const override;
  virtual bool is_equal(const ObILibCacheKey &other) const;
  
  TO_STRING_KV(K_(db_id),
               K_(constructed_sql),
               K_(sql_id),
               K_(format_sql),
               K_(format_sql_id),
               K_(namespace));

  uint64_t  db_id_;
  common::ObString constructed_sql_;    // Storing data only. not use in operator== and hash.
  common::ObString sql_id_;
  common::ObString format_sql_;          // Storing data only. not use in operator== and hash.
  common::ObString format_sql_id_;      // Storing data only. not use in operator== and hash.
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
    last_verified_(0),
    gmt_modified_(OB_INVALID_ID),
    plan_hash_value_(OB_INVALID_ID),
    plan_type_(OB_PHY_PLAN_UNINITIALIZED),
    // constraint
    outline_data_(),
    flags_(0),
    optimizer_cost_(UINT64_MAX),
    executions_(0),
    elapsed_time_(UINT64_MAX),
    cpu_time_( UINT64_MAX),
    avg_cpu_time_(-1) {}

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
  inline uint64_t get_gmt_modified() const { return gmt_modified_; }
  inline void set_gmt_modified(const uint64_t v) { gmt_modified_ = v; }
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
  inline int64_t get_avg_cpu_time() const { return avg_cpu_time_; }
  inline void set_avg_cpu_time(int64_t v) { avg_cpu_time_ = v; }
  int calc_avg_cpu_time();
  
  int check_basic_constraint_match(bool& is_match);
  int merge_baseline_item(ObPlanBaselineItem& other);
  
  TO_STRING_KV(K_(origin), K_(db_version), K_(last_executed),
               K_(last_verified), K_(plan_hash_value), K_(plan_type), K_(outline_data),
               K_(flags), K_(optimizer_cost), K_(executions), K_(elapsed_time), K_(cpu_time),
               K_(avg_cpu_time));

public:
  common::ObString origin_sql_text_;  // origin sql text for baseline
  int64_t origin_;  // baseline source, 1 for AUTO-CAPTURE, 2 for MANUAL-LOAD
  common::ObString db_version_;  // database version when generate baseline
  uint64_t last_executed_;  // last time when baseline executed
  uint64_t last_verified_;  // last time when baseline verified
  uint64_t gmt_modified_;  // last time when baseline modified
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
  int64_t avg_cpu_time_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanBaselineItem);
};

struct BaselineSortInfo
{
  BaselineSortInfo()
  : plan_hash_(0),
    last_verified_(0),
    avg_cpu_time_(-1)
  {}

  BaselineSortInfo(const ObPlanBaselineItem &baseline)
    : plan_hash_(baseline.get_plan_hash_value()),
      last_verified_(baseline.get_last_verified()),
      avg_cpu_time_(baseline.get_avg_cpu_time())
  {}

  TO_STRING_KV(K_(plan_hash), K_(last_verified), K_(avg_cpu_time));

  uint64_t plan_hash_;
  uint64_t last_verified_;
  int64_t avg_cpu_time_;
};

struct BaselineLastVerifiedCmp
{
  inline bool operator()(const BaselineSortInfo left, const BaselineSortInfo right)
  {
    return left.last_verified_ > right.last_verified_;
  }
};

struct BaselineCpuTimeCmp
{
  inline bool operator()(const BaselineSortInfo left, const BaselineSortInfo right)
  {
    bool bret = false;
    if (left.avg_cpu_time_ < 0 || right.avg_cpu_time_ < 0) {
      bret = left.avg_cpu_time_ >= 0;
    } else {
      bret = left.avg_cpu_time_ < right.avg_cpu_time_;
    }
    return bret;
  }
};

class ObSpmSet : public ObILibCacheNode
{

public:
  ObSpmSet(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : ObILibCacheNode(lib_cache, mem_context),
      is_inited_(false),
      baseline_key_(),
      ref_lock_(common::ObLatchIds::SPM_SET_LOCK)
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
  bool check_has_accepted_baseline() const;
  static bool is_outline_data_str_truncated(const ObString &outline_data_str);
  static int get_plan_baseline(ObPlanCache *lib_cache,
                               ObSpmCacheCtx &spm_ctx,
                               ObBaselineKey *key,
                               ObCacheObjGuard &guard);
  static int add_plan_baseline(ObPlanCache *lib_cache,
                               ObSpmCacheCtx &spm_ctx,
                               ObBaselineKey *key,
                               ObPlanBaselineItem *baseline_item);

  TO_STRING_KV(K_(is_inited));

private:
  static ObPlanBaselineItem* get_target_baseline(const ObIArray<ObPlanBaselineItem*> &all_baselines,
                                                 const uint64_t target_plan_hash);
  static int append_baseline_hash(const ObIArray<BaselineSortInfo> &sort_infos,
                                  const uint64_t max_append_size,
                                  ObIArray<uint64_t> &ordered_plan_hash);
  static int init_get_offset_mode(const ObIArray<ObPlanBaselineItem*> &all_baselines,
                                  ObSpmCacheCtx& spm_ctx);
private:
  bool is_inited_;
  ObBaselineKey baseline_key_; //used for manager key memory
  common::ObSEArray<ObPlanBaselineItem*, 4> baseline_array_;
  common::SpinRWLock ref_lock_;
};

class ObEvoPlanGuard {
  public:
    ObEvoPlanGuard()
      : obj_guard_(MAX_HANDLE)
    {}
    ~ObEvoPlanGuard() { reset();  }
    void reset(const bool try_reset_evo_plan = false);
    void swap(ObCacheObjGuard& other)
    {
      reset();
      obj_guard_.swap(other);
    }
    const ObILibCacheObject* get_cache_obj() const  { return obj_guard_.get_cache_obj();  }
    TO_STRING_KV(K_(obj_guard));
  private:
    ObCacheObjGuard obj_guard_;
};

struct ObSpmCacheCtx : public ObILibCacheCtx
{
  ObSpmCacheCtx()
    : ObILibCacheCtx(),
      bl_key_(),
      handle_cache_mode_(MODE_INVALID),
      plan_hash_value_(0),
      offset_(OB_INVALID_INDEX),
      new_plan_hash_(0),
      baseline_guard_(PLAN_BASELINE_HANDLE),
      spm_stat_(STAT_INVALID),
      evolution_plan_type_(OB_PHY_PLAN_UNINITIALIZED),
      select_plan_type_(INVALID_TYPE),
      spm_plan_timeout_(0),
      flags_(0),
      spm_mode_(SPM_MODE_DISABLE),
      evo_plan_guard_()
  {
    cache_node_empty_ = true;
  }
  enum SpmMode {
    MODE_INVALID,
    // for get cache obj
    MODE_GET_NORMAL,
    MODE_GET_OFFSET,
    MODE_GET_UNACCEPT_FOR_UPDATE,
    MODE_MAX
  };
  enum SpmStat {
    STAT_INVALID,
    STAT_ADD_EVOLUTION_PLAN,      // add evolution plan to plan cache evolution layer
    STAT_ADD_BASELINE_PLAN,       // add baseline plan to plan cache evolution layer
    STAT_START_EVOLUTION,
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
  void set_get_unaccept_for_update_mode() { handle_cache_mode_ = MODE_GET_UNACCEPT_FOR_UPDATE; }
  bool force_get_evolution_plan()
  {
    return STAT_START_EVOLUTION == spm_stat_;
  }
  // query from ObMPQuery/ObMPStmtExecute is allowed. query from ObInnerSQLConnection is not supported
  // TODO: supported spm for query from ObInnerSQLConnection
  bool is_spm_supported() { return NULL != baseline_plan_hash_array_.get_allocator(); }

  ObBaselineKey bl_key_;
  SpmMode handle_cache_mode_;
  uint64_t plan_hash_value_;
  int64_t offset_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t new_plan_hash_;
  ObCacheObjGuard baseline_guard_;
  SpmStat spm_stat_;
  ObPhyPlanType evolution_plan_type_;
  SpmSelectPlanType select_plan_type_; // for retry
  int64_t spm_plan_timeout_;
  union { //FARM COMPAT WHITELIST
    uint64_t flags_;
    struct {
      uint64_t is_retry_for_spm_:               1;
      uint64_t cache_node_empty_:               1;
      uint64_t spm_force_disable_:              1;
      uint64_t need_spm_timeout_:               1;
      uint64_t evolution_task_in_two_plan_set_: 1;
      uint64_t evo_plan_is_baseline_:           1;
      uint64_t evo_plan_is_fixed_baseline_:     1;
      uint64_t evo_plan_added_:                 1;
      uint64_t finish_start_evolution_:         1;
      uint64_t has_concurrent_limited_:         1;
      uint64_t reserved_:                      54;
    };
  };
  int64_t spm_mode_;
  ObEvoPlanGuard evo_plan_guard_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> baseline_plan_hash_array_;
};

struct EvolutionTaskResult
{
public:
  EvolutionTaskResult()
  : key_(),
    accept_new_plan_(false),
    new_plan_hash_(0),
    new_stat_(),
    from_mock_task_(false),
    status_(0),
    start_time_(0),
    end_time_(0),
    update_baseline_outline_(false),
    type_(-1),
    records_(NULL),
    evo_plan_is_baseline_(false)
  {}
  ~EvolutionTaskResult() {}
  int deep_copy(common::ObIAllocator& allocator, const EvolutionTaskResult& other);
  bool need_record_evolution_result() const { return !update_baseline_outline_; }
  inline bool need_update_evo_plan_baseline() const { return (0 == type_ && accept_new_plan_) || type_ >= 3; }
  inline bool is_valid()  const { return old_plan_hash_array_.count() == old_stat_array_.count(); }
  inline bool is_self_evolution()  const { return  old_plan_hash_array_.empty(); }
  TO_STRING_KV(K_(key), K_(old_plan_hash_array), K_(old_stat_array), K_(new_plan_hash),
               K_(new_stat), K_(start_time), K_(end_time), K_(status), K_(type));
public:
  ObBaselineKey key_;
  bool accept_new_plan_;
  // old plan statistics
  common::ObSEArray<uint64_t, 4> old_plan_hash_array_;
  common::ObSEArray<ObEvolutionStat, 4> old_stat_array_;
  // new plan statistics
  uint64_t new_plan_hash_;
  ObEvolutionStat new_stat_;
  bool from_mock_task_;
  int64_t status_;
  int64_t start_time_;
  int64_t end_time_;
  bool update_baseline_outline_;
  int64_t type_;  //  0 OnlineEvolve, 1 FirstBaseline, 2 UnReproducible, 3 BaselineFirst, 4 BestBaselinem, 5 FixedBaseline
  ObEvolutionRecords *records_;
  bool evo_plan_is_baseline_;
};

} // namespace sql end
} // namespace oceanbase end

#endif
