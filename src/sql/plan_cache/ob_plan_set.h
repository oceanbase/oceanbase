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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_SET_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_SET_

#include "lib/net/ob_addr.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_2d_array.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_dist_plans.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/hash/ob_hashset.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_evolution_plan.h"
#endif

namespace test
{
class TestPlanSet_basic_Test;
}
namespace oceanbase
{
namespace share
{
namespace schema
{
  class ObSchemaGetterGuard;
}
}
namespace sql
{
class ObPlanCacheValue;
class ObPlanCache;
class ObPlanCacheObject;
class ObPhysicalPlan;
class ObExecContext;
struct ObSqlCtx;
struct ObPCUserVarMeta;

typedef ObFixedArray<ObPCUserVarMeta, common::ObIAllocator> UserSessionVarMetaArray;
typedef ObFixedArray<ObPCConstParamInfo, common::ObIAllocator> ConstParamConstraint;
typedef ObFixedArray<ObPCParamEqualInfo, common::ObIAllocator> EqualParamConstraint;
typedef ObDList<ObPreCalcExprConstraint> PreCalcExprConstraint;
typedef ObFixedArray<ObPCPrivInfo, common::ObIAllocator> PrivConstraint;

enum ObPlanSetType
{
  PST_SQL_CRSR = 0, //sql plan
  PST_PRCD = 1, //store procedure, store function, package
  PST_MAX
};

struct HashKey
{
  inline bool operator ==(const HashKey &other) const
  {
    bool bret = true;
    if (rowkey_.count() != other.rowkey_.count()) {
      bret = false;
    }
    for (int64_t i = 0; bret && i < rowkey_.count(); ++i) {
      if (rowkey_.at(i) != other.rowkey_.at(i)) {
        bret = false;
      }
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    for (int64_t i = 0; i < rowkey_.count(); ++i) {
      rowkey_.at(i).hash(hash_ret, hash_ret);
    }
    return hash_ret;
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  void reuse() { rowkey_.reuse(); }
  common::ObSEArray<ObObj, 4> rowkey_;
  TO_STRING_KV(K_(rowkey));
};

typedef common::hash::ObHashSet<HashKey, common::hash::NoPthreadDefendMode> UniqueHashSet;

struct ObPCUserVarMeta
{
public:
  ObPCUserVarMeta(): precision_(-1), obj_meta_() {}
  ObPCUserVarMeta(const ObSessionVariable &sess_var)
  {
    obj_meta_ = sess_var.meta_;
    if (sess_var.meta_.is_decimal_int()) {
      precision_ = wide::ObDecimalIntConstValue::get_max_precision_by_int_bytes(
        sess_var.value_.get_int_bytes());
    } else {
      precision_ = PRECISION_UNKNOWN_YET;
    }
  }
  ObPCUserVarMeta(const ObPCUserVarMeta &other)
  {
    obj_meta_ = other.obj_meta_;
    precision_ = other.precision_;
  }
  inline bool operator==(const ObPCUserVarMeta &other) const
  {
    bool ret = (obj_meta_ == other.obj_meta_);
    if (ret && obj_meta_.is_decimal_int()) {
      ret = (obj_meta_.get_scale() == other.obj_meta_.get_scale()
            && wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision_)
                 == wide::ObDecimalIntConstValue::get_int_bytes_by_precision(other.precision_));
    }
    return ret;
  }
  inline bool operator!=(const ObPCUserVarMeta &other) const
  {
    return !this->operator==(other);
  }
  TO_STRING_KV(K_(precision), K_(obj_meta));

private:
  ObPrecision precision_;
  ObObjMeta obj_meta_;
};

//ObPlanSet is a set of ObPhysicalPlans with same PlanMetaInfo
class ObPlanSet : public common::ObDLinkBase<ObPlanSet>
{
  friend struct ObPhyLocationGetter;
public:
  explicit ObPlanSet(ObPlanSetType type)
      : alloc_(common::ObNewModIds::OB_SQL_PLAN_CACHE),
        plan_cache_value_(NULL),
        type_(type),
        params_info_(ObWrapperAllocator(alloc_)),
        ref_lock_(common::ObLatchIds::PLAN_SET_LOCK),
        stmt_type_(stmt::T_NONE),
        fetch_cur_time_(false),
        is_ignore_stmt_(false),
        outline_param_idx_(common::OB_INVALID_INDEX),
        related_user_var_names_(alloc_),
        related_user_sess_var_metas_(alloc_),
        all_possible_const_param_constraints_(alloc_),
        all_plan_const_param_constraints_(alloc_),
        all_pre_calc_constraints_(),
        all_priv_constraints_(),
        need_match_all_params_(false),
        multi_stmt_rowkey_pos_(alloc_),
        pre_cal_expr_handler_(NULL),
        can_skip_params_match_(false),
        can_delay_init_datum_store_(false),
        res_map_rule_id_(common::OB_INVALID_ID),
        res_map_rule_param_idx_(common::OB_INVALID_INDEX),
        is_cli_return_rowid_(false)
  {}
  virtual ~ObPlanSet();

public:
  int match_params_info(const ParamStore *params,
                        ObPlanCacheCtx &pc_ctx,
                        int64_t outline_param_idx,
                        bool &is_same);
  int match_params_info(const common::Ob2DArray<ObParamInfo,
                        common::OB_MALLOC_BIG_BLOCK_SIZE,
                        common::ObWrapperAllocator, false> &infos,
                        int64_t outline_param_idx,
                        const ObPlanCacheCtx &pc_ctx,
                        bool &is_same);
  bool can_skip_params_match();
  bool can_delay_init_datum_store();
  int match_param_info(const ObParamInfo &param_info,
                       const ObObjParam &param,
                       bool &is_same,
                       bool is_sql_planset) const;
  int copy_param_flag_from_param_info(ParamStore *params);
  int match_param_bool_value(const ObParamInfo &param_info,
                             const ObObjParam &param,
                             bool &is_same) const;
  static int match_multi_stmt_info(const ParamStore &params,
                                   const common::ObIArray<int64_t> &multi_stmt_rowkey_pos,
                                   bool &is_match);
  ObPlanSetType get_type() { return type_; }
  void set_type(ObPlanSetType plan_set_type) { type_ = plan_set_type; }
  static ObPlanSetType get_plan_set_type_by_cache_obj_type(ObLibCacheNameSpace ns);
  void set_plan_cache_value(ObPlanCacheValue *pc_value) { plan_cache_value_ = pc_value; }
  ObPlanCacheValue *get_plan_cache_value() { return plan_cache_value_; }
  ObPlanCache *get_plan_cache() const;
  virtual int add_cache_obj(ObPlanCacheObject &cache_object,
                            ObPlanCacheCtx &pc_ctx,
                            int64_t ol_param_idx,
                            int &add_ret) = 0;
  virtual int select_plan(ObPlanCacheCtx &pc_ctx,
                          ObPlanCacheObject *&plan) = 0;
  virtual void remove_all_plan() = 0;
  virtual int64_t get_mem_size() = 0;
  virtual void reset();
  virtual int init_new_set(const ObPlanCacheCtx &pc_ctx,
                           const ObPlanCacheObject &cache_obj,
                           int64_t outline_param_idx,
                           common::ObIAllocator* pc_alloc_);
  virtual bool is_sql_planset() = 0;
/*  static int check_array_bind_same_bool_param(*/
               //const Ob2DArray<ObParamInfo,
                                 //OB_MALLOC_BIG_BLOCK_SIZE,
                                 //ObWrapperAllocator, false> &param_infos,
               //const ParamStore & param_store,
               /*bool &same_bool_param);*/
  inline bool is_multi_stmt_plan() const { return !multi_stmt_rowkey_pos_.empty(); }
  int remove_cache_obj_entry(const ObCacheObjID obj_id);

  bool get_can_skip_params_match() { return can_skip_params_match_; }
  bool get_can_delay_init_datum_store() { return can_delay_init_datum_store_; }

private:
  bool is_match_outline_param(int64_t param_idx)
  {
    return outline_param_idx_ == param_idx;
  }

  /**
   * @brief set const param constraints
   *
   */
  int set_const_param_constraint(common::ObIArray<ObPCConstParamInfo> &const_param_constraint,
                                 const bool is_all_constraint);

  int set_equal_param_constraint(common::ObIArray<ObPCParamEqualInfo> &equal_param_constraint);

  int set_pre_calc_constraint(common::ObDList<ObPreCalcExprConstraint> &pre_calc_cons);

  int set_priv_constraint(common::ObIArray<ObPCPrivInfo> &priv_constraint);

  int match_cons(const ObPlanCacheCtx &pc_ctx, bool &is_matched);
  /**
   * @brief Match const param constraint.
   * If all_plan_const_param_constraints_ is not empty, check wether the constraints is mached and return the result.
   * If all_plan_const_param_constraints_ is empty, but any of the constraints in all_possible_const_param_constraints_ is
   * matched, the is_matched is false (new plan shoule be generated).
   *
   * @param params Const Params about to match
   * @retval is_matched Matching result
   */
  int match_constraint(const ParamStore &params, bool &is_matched);

  int init_pre_calc_exprs(const ObPlanCacheObject &cache_obj, common::ObIAllocator* pc_alloc_);

  int pre_calc_exprs(ObExecContext &exec_ctx);

  int match_priv_cons(ObPlanCacheCtx &pc_ctx, bool &is_matched);
  static int check_vector_param_same_bool(const ObObjParam &param_obj,
                                         bool &first_val,
                                         bool &is_same);

  bool match_decint_precision(const ObParamInfo &param_info, ObPrecision other_prec) const;

  DISALLOW_COPY_AND_ASSIGN(ObPlanSet);
  friend class ::test::TestPlanSet_basic_Test;
protected:
  common::ObArenaAllocator alloc_;
  ObPlanCacheValue *plan_cache_value_;
  ObPlanSetType type_;
  common::Ob2DArray<ObParamInfo, common::OB_MALLOC_BIG_BLOCK_SIZE,
                    common::ObWrapperAllocator, false> params_info_;
  common::SpinRWLock ref_lock_;
  stmt::StmtType stmt_type_;
  bool fetch_cur_time_;
  bool is_ignore_stmt_;
  int64_t outline_param_idx_;
  // related user session var names
  common::ObFixedArray<common::ObString, common::ObIAllocator> related_user_var_names_;
  UserSessionVarMetaArray related_user_sess_var_metas_;
  ConstParamConstraint all_possible_const_param_constraints_;
  ConstParamConstraint all_plan_const_param_constraints_;
  EqualParamConstraint all_equal_param_constraints_;
  PreCalcExprConstraint all_pre_calc_constraints_;
  PrivConstraint all_priv_constraints_;
  //if true, check the datatypes of all params
  bool need_match_all_params_;
  // maintain the rowkey position for multi_stmt
  common::ObFixedArray<int64_t, common::ObIAllocator> multi_stmt_rowkey_pos_;
  // pre calculable expression list handler.
  PreCalcExprHandler* pre_cal_expr_handler_;
  bool can_skip_params_match_;
  bool can_delay_init_datum_store_;

public:
  //variables for resource map rule
  uint64_t res_map_rule_id_;
  int64_t res_map_rule_param_idx_;
  bool is_cli_return_rowid_;
};

class ObSqlPlanSet : public ObPlanSet
{
public:
  ObSqlPlanSet()
    : ObPlanSet(PST_SQL_CRSR),
      is_all_non_partition_(true),
      table_locations_(alloc_),
      array_binding_plan_(),
      local_plan_(NULL),
      remote_plan_(NULL),
      direct_local_plan_(NULL),
      dist_plans_(),
      need_try_plan_(0),
      has_duplicate_table_(false),
      //has_array_binding_(false),
      is_contain_virtual_table_(false),
#ifdef OB_BUILD_SPM
      enable_inner_part_parallel_exec_(false),
      is_spm_closed_(false)
#else
      enable_inner_part_parallel_exec_(false)
#endif
      {
      }

  virtual ~ObSqlPlanSet() {}
public:
  virtual int add_cache_obj(ObPlanCacheObject &cache_object,
                            ObPlanCacheCtx &pc_ctx,
                            int64_t ol_param_idx,
                            int &add_ret) override;
  virtual int select_plan(ObPlanCacheCtx &pc_ctx,
                          ObPlanCacheObject *&cache_obj) override;
  virtual void remove_all_plan() override;
  virtual int64_t get_mem_size() override;
  virtual void reset() override;
  virtual bool is_sql_planset() override;
  virtual int init_new_set(const ObPlanCacheCtx &pc_ctx,
                           const ObPlanCacheObject &cache_obj,
                           int64_t outline_param_idx,
                           common::ObIAllocator* pc_alloc_) override;
  // calculate phy_plan type:
  // @param [in]  phy_locations
  // @param [out] plan_type
  static int calc_phy_plan_type_v2(const common::ObIArray<ObCandiTableLoc> &candi_table_locs,
                                   const ObPlanCacheCtx &pc_ctx,
                                   ObPhyPlanType &plan_type);
  static int calc_phy_plan_type(const common::ObIArray<ObCandiTableLoc> &phy_locations,
                                ObPhyPlanType &plan_type);
  inline bool has_duplicate_table() const { return has_duplicate_table_; }
  //inline bool has_array_binding() const { return has_array_binding_; }
  inline bool enable_inner_part_parallel() const { return enable_inner_part_parallel_exec_; }
#ifdef OB_BUILD_SPM
  int add_evolution_plan_for_spm(ObPhysicalPlan *plan, ObPlanCacheCtx &ctx);
  int get_evolving_evolution_task(EvolutionPlanList &evo_task_list);
#endif
private:
  enum
  {
    TRY_PLAN_LATE_MAT = 1,
    TRY_PLAN_OR_EXPAND = 1 << 1,
    TRY_PLAN_UNCERTAIN = 1 << 2,
    TRY_PLAN_INDEX = 1 << 3,
  };
  int get_plan_normal(ObPlanCacheCtx &pc_ctx,
                      ObPhysicalPlan *&plan);

  int get_local_plan_direct(ObPlanCacheCtx &pc_ctx,
                            bool &is_direct_local_plan,
                            ObPhysicalPlan *&plan);

  int add_plan(ObPhysicalPlan &plan,
               ObPlanCacheCtx &pc_ctx,
               int64_t outline_param_idx);

  int add_physical_plan(const ObPhyPlanType plan_type,
                         ObPlanCacheCtx &pc_ctx,
                         ObPhysicalPlan &plan);

  int get_physical_plan(const ObPhyPlanType plan_type,
                        ObPlanCacheCtx &pc_ctx,
                        ObPhysicalPlan *&plan);

  int get_plan_special(ObPlanCacheCtx &pc_ctx,
                       ObPhysicalPlan *&plan);
#ifdef OB_BUILD_SPM
  int try_get_local_evolution_plan(ObPlanCacheCtx &pc_ctx,
                                   ObPhysicalPlan *&plan,
                                   bool &get_next);
  int try_get_dist_evolution_plan(ObPlanCacheCtx &pc_ctx,
                                  ObPhysicalPlan *&plan,
                                  bool &get_next);
  int try_get_evolution_plan(ObPlanCacheCtx &pc_ctx,
                             ObPhysicalPlan *&plan,
                             bool &get_next);
#endif
  int try_get_local_plan(ObPlanCacheCtx &pc_ctx,
                         ObPhysicalPlan *&plan,
                         bool &get_next);
  int try_get_remote_plan(ObPlanCacheCtx &pc_ctx,
                          ObPhysicalPlan *&plan,
                          bool &get_next);
  int try_get_dist_plan(ObPlanCacheCtx &pc_ctx,
                        ObPhysicalPlan *&plan);
  int get_phy_locations(const ObIArray<ObTableLocation> &table_locations,
                        ObPlanCacheCtx &pc_ctx,
                        ObIArray<ObCandiTableLoc> &candi_table_locs);

  int get_phy_locations(const ObTablePartitionInfoArray &partition_infos,
                        ObIArray<ObCandiTableLoc> &candi_table_locs);

  int set_concurrent_degree(int64_t outline_param_idx,
                            ObPhysicalPlan &plan);

  int get_plan_type(const ObIArray<ObTableLocation> &table_locations,
                    const bool is_contain_uncertain_op,
                    ObPlanCacheCtx &pc_ctx,
                    ObIArray<ObCandiTableLoc> &candi_table_locs,
                    ObPhyPlanType &plan_type);

  static int is_partition_in_same_server(const ObIArray<ObCandiTableLoc> &candi_table_locs,
                                         bool &is_same,
                                         ObAddr &first_addr);

  static int extend_param_store(const ParamStore &params,
                                common::ObIAllocator &allocator,
                                ObIArray<ParamStore *> &expanded_params);

  bool is_local_plan_opt_allowed(int last_retry_err);
private:
  bool is_all_non_partition_; //判断该plan对应的表是否均为非分区表
  TableLocationFixedArray table_locations_;
  //used for array binding, only local plan
  ObPhysicalPlan *array_binding_plan_;
  ObPhysicalPlan *local_plan_;
#ifdef OB_BUILD_SPM
  ObEvolutionPlan local_evolution_plan_;
  ObEvolutionPlan dist_evolution_plan_;
#endif
  ObPhysicalPlan *remote_plan_;
  // for directly get plan
  ObPhysicalPlan *direct_local_plan_;
  ObDistPlans dist_plans_;

  // 用于处理or expansion、晚期物化，全局索引等特殊场景
  // 以上的特殊场景的共同特点是plan_set缓存的table location和计划内的table location不一致，
  // 必须从计划内拿table location去计算物理分区地址
  int64_t need_try_plan_;
  //计划中是否含有复制表
  bool has_duplicate_table_;
  ObSEArray<int64_t, 4> part_param_idxs_;
  // 是否含有虚拟表，如果包含虚拟表，不做直接获取local计划的优化
  bool is_contain_virtual_table_;
  // px并行度是否大于1
  bool enable_inner_part_parallel_exec_;
#ifdef OB_BUILD_SPM
  bool is_spm_closed_;
#endif
};

inline ObPlanSetType ObPlanSet::get_plan_set_type_by_cache_obj_type(ObLibCacheNameSpace ns)
{
  ObPlanSetType ret = PST_MAX;
  switch (ns) {
    case ObLibCacheNameSpace::NS_CRSR:
      ret = PST_SQL_CRSR;
      break;
    case ObLibCacheNameSpace::NS_PRCR:
    case ObLibCacheNameSpace::NS_SFC:
    case ObLibCacheNameSpace::NS_PKG:
    case ObLibCacheNameSpace::NS_ANON:
      ret = PST_PRCD;
      break;
    default:
      break;
  }
  return ret;
}
}
}
#endif
