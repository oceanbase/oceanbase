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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_VALUE_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_VALUE_

#include "lib/lock/ob_spin_rwlock.h"
#include "common/object/ob_object.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/engine/ob_physical_plan.h"
namespace test
{
class TestPlanSet_basic_Test;
class TestPlanCacheValue_basic_Test;
}

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
namespace schema
{
  class ObSchemaGetterGuard;
}
} //namespace share end

namespace sql
{
class ObExecContext;
class ObPlanCache;
class ObPCVSet;
class ObPhysicalPlan;
class ObTablePartitionInfo;

enum ObPlanCacheItemStatus
{
  PLAN_INVALID,
  PLAN_EXPIRED,
  PLAN_VALID
};

struct PCVSchemaObj
{
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_id_;
  int64_t schema_version_;
  share::schema::ObSchemaType schema_type_;
  share::schema::ObTableType table_type_;
  common::ObString table_name_;
  bool is_tmp_table_;
  bool is_explicit_db_name_;
  common::ObIAllocator *inner_alloc_;

  PCVSchemaObj():
  tenant_id_(common::OB_INVALID_ID),
  database_id_(common::OB_INVALID_ID),
  schema_id_(common::OB_INVALID_ID),
  schema_version_(0),
  schema_type_(share::schema::OB_MAX_SCHEMA),
  table_type_(share::schema::MAX_TABLE_TYPE),
  is_tmp_table_(false),
  is_explicit_db_name_(false),
  inner_alloc_(nullptr) {}

  explicit PCVSchemaObj(ObIAllocator *alloc):
    tenant_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    schema_id_(common::OB_INVALID_ID),
    schema_version_(0),
    schema_type_(share::schema::OB_MAX_SCHEMA),
    table_type_(share::schema::MAX_TABLE_TYPE),
    is_tmp_table_(false),
    is_explicit_db_name_(false),
    inner_alloc_(alloc) {}

  int init(const share::schema::ObTableSchema *schema);
  int init_with_synonym(const ObSimpleSynonymSchema *schema);
  int init_with_version_obj(const share::schema::ObSchemaObjVersion &schema_obj_version);
  int init_without_copy_name(const share::schema::ObSimpleTableSchemaV2 *schema);
  void set_allocator(common::ObIAllocator *alloc)
  {
    inner_alloc_ = alloc;
  }

  bool compare_schema(const share::schema::ObTableSchema &schema) const
  {
    bool ret = false;
    ret = tenant_id_ == schema.get_tenant_id() &&
          database_id_ == schema.get_database_id() &&
          schema_id_ == schema.get_table_id() &&
          schema_version_ == schema.get_schema_version() &&
          table_type_ == schema.get_table_type();
    return ret;
  }

  bool match_compare(const PCVSchemaObj &other) const
  {
    bool ret = true;
    ret = tenant_id_ == other.tenant_id_
          && database_id_ == other.database_id_
          && table_type_ == other.table_type_;
    return ret;
  }

  bool operator==(const PCVSchemaObj &other) const;

  bool operator!=(const PCVSchemaObj &other) const
  {
    return !operator==(other);
  }

  void reset();
  ~PCVSchemaObj();

  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(schema_id),
               K_(schema_version),
               K_(schema_type),
               K_(table_type),
               K_(table_name),
               K_(is_tmp_table),
               K_(is_explicit_db_name));
};

class ObPlanCacheValue :public common::ObDLinkBase<ObPlanCacheValue>
{
public:
  static const int64_t MAX_SQL_LENGTH = 2048;
  static const int32_t MAX_PLAN_PER_SQL = 8;
  //存放影响plan的系统变量中需要deep_copy的obj的内存最大值
  const static int64_t OB_INFLUENCE_PLAN_SYS_VAR_LEN = 512;

public:
  ObPlanCacheValue();
  ~ObPlanCacheValue() { reset(); }
  int init(ObPCVSet *pcv_set, const ObILibCacheObject *cache_obj, ObPlanCacheCtx &pc_ctx);

  // alloc and free
  void reset();
  int32_t get_type() { return 0; }

  //通过匹配不同的sys_vars, db_id, not_params选择对应的plan cache value
  //检查是否包含临时表
  int match(ObPlanCacheCtx &pc_ctx,
            const common::ObIArray<PCVSchemaObj> &schema_array,
            bool &is_same);
  //将fast parser参数化的参数转化为ObObjParam
  static int resolver_params(ObPlanCacheCtx &pc_ctx,
                             stmt::StmtType stmt_type,
                             const ObIArray<ObCharsetType> &param_charset_type,
                             const ObBitSet<> &neg_param_index,
                             const ObBitSet<> &not_param_index,
                             const ObBitSet<> &must_be_positive_idx,
                             ObIArray<ObPCParam *> &raw_params,
                             ParamStore *obj_params);

  int resolve_multi_stmt_params(ObPlanCacheCtx &pc_ctx);

  int before_resolve_array_params(ObPlanCacheCtx &pc_ctx, int64_t query_num, int64_t param_num, ParamStore *&ab_params);

  static int check_multi_stmt_param_type(ObPlanCacheCtx &pc_ctx,
                                         const stmt::StmtType stmt_type,
                                         const ObIArray<ObCharsetType> &param_charset_type,
                                         const ObBitSet<> &neg_param_index_,
                                         const ObBitSet<> &not_param_index,
                                         const ObBitSet<> &must_be_positive_idx,
                                         ParamStore &param_store);

  static int resolve_insert_multi_values_param(ObPlanCacheCtx &pc_ctx,
                                               const stmt::StmtType stmt_type,
                                               const ObIArray<ObCharsetType> &param_charset_type,
                                               const ObBitSet<> &neg_param_index_,
                                               const ObBitSet<> &not_param_index,
                                               const ObBitSet<> &must_be_positive_idx,
                                               int64_t params_num,
                                               ParamStore &param_store);

  static int check_multi_stmt_not_param_value(
                                const ObIArray<ObFastParserResult> &multi_stmt_fp_results,
                                const ObIArray<NotParamInfo> &not_param_info,
                                bool &is_same);
  static int check_insert_multi_values_param(ObPlanCacheCtx &pc_ctx, bool &is_same);
  static int check_not_param_value(const ObIArray<ObPCParam *> &raw_params,
                                   const ObIArray<NotParamInfo> &not_param_info,
                                   bool &is_same);
  static int check_not_param_value(const ObFastParserResult &fp_result,
                                   const ObIArray<NotParamInfo> &not_param_info,
                                   bool &is_same);
  static int get_one_group_params(int64_t pos, const ParamStore &src_params, ParamStore &dst_params);

  int match_all_params_info(ObPlanSet *batch_plan_set,
                            ObPlanCacheCtx &pc_ctx,
                            int64_t outline_param_idx,
                            bool &is_same);
  // choose an appropriate physical plan
  int choose_plan(ObPlanCacheCtx &pc_ctx,
                  const common::ObIArray<PCVSchemaObj> &schema_array,
                  ObPlanCacheObject *&plan);
  // add a physical plan
  int add_plan(ObPlanCacheObject &cache_obj,
               const common::ObIArray<PCVSchemaObj> &schema_array,
               ObPlanCacheCtx &pc_ctx);

  int match_and_generate_ext_params(ObPlanSet *batch_plan_set,
                                    ObPlanCacheCtx &pc_ctx,
                                    int64_t outline_param_idx);

  //this sql contain can't be parameterized value
  const common::ObBitSet<> &get_not_param_index() const { return not_param_index_; }
  const common::ObBitSet<> &get_neg_param_index() const { return neg_param_index_; }
  // remove physical plan(pointer)
  //void remove_plan(ObExecContext &exec_context, ObPhysicalPlan &plan);
  // remove all reference count to phy plan
  int clear_plan_set();
  // get phy plan total mem size
  int64_t get_mem_size();
  int set_pcv_set(ObPCVSet *pcv_set);
  ObPCVSet *get_pcv_set() const { return pcv_set_; }
  //cut when length of stmt is more than MAX_SQL_LENGTH;
  //void set_use_global_location_cache(bool use_global_location_cache)
  //{
  //  use_global_location_cache_ = use_global_location_cache;
  //}
  //bool get_use_global_location_cache() { return use_global_location_cache_; }
  //StmtStat *get_stmt_stat() { return &stmt_stat_;}
  void set_tenant_schema_version(int64_t version) { tenant_schema_version_ = version; }
  void set_sys_schema_version(int64_t version) { sys_schema_version_ = version; }
  bool is_plan_fixed() { return outline_state_.is_plan_fixed_; }
  void set_outline_state(const ObOutlineState &outline_state)
  {
    outline_state_ = outline_state;
  }
  int set_outline_params_wrapper(const share::schema::ObOutlineParamsWrapper &params)
  {
    return outline_params_wrapper_.assign(params);
  }
  char *get_sql_id() {
    return sql_id_;
  }
  ObIAllocator *get_pc_alloc() const { return pc_alloc_; }
  ObIAllocator *get_pc_malloc() const { return pc_malloc_; }
  const share::schema::ObMaxConcurrentParam *get_outline_param(int64_t index) const;

  /**
   * @brief  get sessid
   *
   * @param void
   * @retval session id cached in this pcv
   */
  uint64_t get_sessid() const { return sessid_; }

  // get all dependency schemas, used for get plan
  int get_all_dep_schema(ObPlanCacheCtx &pc_ctx,
                         const uint64_t database_id,
                         int64_t &new_schema_version,
                         bool &need_check_schema,
                         common::ObIArray<PCVSchemaObj> &schema_array);

  // get all dependency schemas, used for add plan
  static int get_all_dep_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                                const DependenyTableStore &dep_schema_objs,
                                common::ObIArray<PCVSchemaObj> &schema_array);

  int lift_tenant_schema_version(int64_t new_schema_version);
  int check_contains_table(uint64_t db_id, common::ObString tab_name, bool &contains);
#ifdef OB_BUILD_SPM
  int get_evolving_evolution_task(EvolutionPlanList &evo_task_list);
#endif
private:
  //used for add plan
  //check table version, view table version, merged version
  int check_value_version_for_add(const ObPlanCacheObject &cache_obj,
                                  const common::ObIArray<PCVSchemaObj> &schema_array,
                                  bool &result);
  //used for get plan
  //check table version, view table version, merged version
  int check_value_version_for_get(share::schema::ObSchemaGetterGuard *schema_guard,
                                  bool need_check_schema,
                                  const common::ObIArray<PCVSchemaObj> &schema_array,
                                  const uint64_t tenant_id,
                                  bool &result);

  int get_outline_version(share::schema::ObSchemaGetterGuard &schema_guard,
                          const uint64_t tenant_id,
                          share::schema::ObSchemaObjVersion &local_outline_version);

  int get_outline_param_index(ObExecContext &exec_ctx, int64_t &param_idx) const;
  /**
   * @brief if there is a temporary table in dependency tables
   * @retval is_contain: true for containing temporary table
   */
  bool is_contain_tmp_tbl() const;

  /**
   * @brief if there is a synonym in dependency tables
   * @retval is_contain: true for containing synonym
   */
  bool is_contain_synonym() const;

  /**
   * @brief if there is a sys package/type in dependency tables
   * @retval is_contain: true for containing sys package/type
   */
  bool is_contain_sys_pl_object() const;

  /**
   * @brief get temp table names in dependency tables
   *
   * @retval tmp_tbl_names Temp table names
   */
  int get_tmp_depend_tbl_names(TmpTableNameArray &tmp_tbl_names);

  int create_new_plan_set(const ObPlanSetType plan_set_type,
                          ObPlanSet *&new_plan_set);
  void free_plan_set(ObPlanSet *plan_set);

  int set_stored_schema_objs(const DependenyTableStore &dep_table_store,
                             share::schema::ObSchemaGetterGuard *schema_guard);

  int check_dep_schema_version(const common::ObIArray<PCVSchemaObj> &schema_array,
                               const common::ObIArray<PCVSchemaObj *> &pcv_schema_objs,
                               bool &is_old_version);

  int match_dep_schema(const ObPlanCacheCtx &pc_ctx,
                       const common::ObIArray<PCVSchemaObj> &schema_array,
                       bool &is_same);

  // 检查pcv对象是否过期
  int check_value_version(bool need_check_schema,
                          const share::schema::ObSchemaObjVersion &outline_version,
                          const ObIArray<PCVSchemaObj> &schema_array,
                          bool &is_old_version);

  // 是否需要check version信息
  int need_check_schema_version(ObPlanCacheCtx &pc_ctx,
                                int64_t &new_schema_version,
                                bool &need_check);
  int assign_udr_infos(ObPlanCacheCtx &pc_ctx);
  void reset_tpl_sql_const_cons();
  int check_tpl_sql_const_cons(const ObFastParserResult &fp_result,
                               const TplSqlConstCons &tpl_cst_cons_list,
                               bool &is_same);
  int cmp_not_param_info(const NotParamInfoList &l_param_info_list,
                         const NotParamInfoList &r_param_info_list,
                         bool &is_equal);

  friend class ::test::TestPlanSet_basic_Test;
  friend class ::test::TestPlanCacheValue_basic_Test;
private:
  //***********  for match **************
  //记录不需要参数化的常量信息及常量为负数的信息
  common::ObSEArray<NotParamInfo, 4> not_param_info_;
  common::ObBitSet<> not_param_index_;
  // two param index are recorded in neg_param_index_;
  // 1. negative const, like select -1 from dual, index of -1 is recorded here
  // 2. const param that transformed from minus op, like select 1 - 2 from dual, index of '- 2' is recorded here
  common::ObBitSet<> neg_param_index_;

  //ps mode match will use variables below
  common::ObSEArray<PsNotParamInfo, 4> not_param_var_;

  common::ObSEArray<ObCharsetType, 4> param_charset_type_;
  ObSqlTraits sql_traits_;
  //*************************
  //not param回填后的sql序列化结果, 主要是用于outline 的signature
  common::ObString outline_signature_;
  common::ObString constructed_sql_;
  ObPCVSet *pcv_set_;
  common::ObIAllocator *pc_alloc_;
  common::ObIAllocator *pc_malloc_;
  //plan id
  int64_t last_plan_id_;
  // a list of plan sets with different param types combination
  common::ObDList<ObPlanSet> plan_sets_;
  //if there is no virtual table in ObPhysicalPlan, set true(default), or set false
  //bool use_global_location_cache_;
  int64_t tenant_schema_version_;
  int64_t sys_schema_version_;
  ObOutlineState outline_state_;
  share::schema::ObOutlineParamsWrapper outline_params_wrapper_;
  char sql_id_[OB_MAX_SQL_ID_LENGTH + 1];

  // session id for temporary table
  uint64_t sessid_;
  // sess_create_time_ for temporary table
  uint64_t sess_create_time_;
  // wether this pcv's plans contains sys table (oracle mode)
  bool contain_sys_name_table_;
#ifdef OB_BUILD_SPM
  bool is_spm_closed_;
#endif

  bool need_param_;
  //at present, if a SQL is in nested sql, it is forced to use DAS plan
  //if a non-DAS plan with the same text is used,
  //it may cause correctness problems.
  //Therefore, it is necessary to distinguish whether it is nested SQL or not.
  bool is_nested_sql_;
  bool is_batch_execute_;
  bool has_dynamic_values_table_;
  // only when not need to param, this feild will be used.
  common::ObString raw_sql_;
  common::ObFixedArray<PCVSchemaObj *, common::ObIAllocator> stored_schema_objs_;
  common::ObBitSet<> must_be_positive_idx_;
  stmt::StmtType stmt_type_;
  //***********  for user-defined rules **************
  DynamicParamInfoArray dynamic_param_list_;
  /**
   * call dbms_udr.create_rule('select ?, 1 from dual', 'select ? + 1, 1 from dual');
   * call dbms_udr.create_rule('select ?, 2 from dual', 'select ? + 2, 1 from dual');
   *
   * template SQL: select ?, ? from dual has the following two constant constraints:
   * tpl_sql_const_cons_ : {{idx:1, raw_text:"1"}, {idx:1, raw_text:"2"}}
   *
   * The following constraints are generated when executing a SQL that does not hit any of the rules:
   * SQL: select 4, 5 from dual;
   * not_param_info_ : {}
   * tpl_sql_const_cons_ : {{idx:1, raw_text:"1"}, {idx:1, raw_text:"2"}}
   *
   * When executing a SQL that hits a rule, the following constraints are generated:
   * SQL: select 4, 1 from dual;
   * not_param_info_ : {idx:1, raw_text:"1"}
   * tpl_sql_const_cons_ : {{idx:1, raw_text:"1"}, {idx:1, raw_text:"2"}}
   *
   * So the constant constraint matching rules are as follows:
   * 1、First match tpl_sql_const_cons_constraint list
   *   2、If it hits, use the hit rule to compare it with not_param_info_, if it is the same
   *     the match succeeds, otherwise it fails
   *   3、If there is no hit, match not_param_info_. if the match is successful
   *     the result is successful, otherwise it fails
   */
  TplSqlConstCons tpl_sql_const_cons_;
  //***********  end user-defined rules **************

  DISALLOW_COPY_AND_ASSIGN(ObPlanCacheValue);
};

}
}
#endif
