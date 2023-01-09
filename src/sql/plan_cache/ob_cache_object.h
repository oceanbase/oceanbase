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

#ifndef DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_H_
#define DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_H_
#include "share/ob_define.h"
#include "lib/container/ob_2d_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_dlist.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_fixed_array.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"

namespace test
{
class MockCacheObjectFactory;
}
namespace oceanbase
{
namespace common
{
struct ObExprCtx;
}  // namespace common
namespace sql
{
class ObSqlExpression;
struct ObPreCalcExprFrameInfo;
struct ObPreCalcExprConstraint;

struct ObOutlineState
{
  OB_UNIS_VERSION(1);
public:
  ObOutlineState() : outline_version_(), is_plan_fixed_(false) {}
  ~ObOutlineState(){}
  void reset()
  {
    outline_version_.reset();
    is_plan_fixed_ = false;
  }
  TO_STRING_KV(K(outline_version_), K(is_plan_fixed_));
  share::schema::ObSchemaObjVersion outline_version_;
  bool is_plan_fixed_;//whether the plan will be fixed with outline_content
};

struct AllocCacheObjInfo {
  uint64_t obj_id_;
  uint64_t tenant_id_;
  int64_t log_del_time_;
  int64_t real_del_time_;
  int64_t ref_count_;
  int64_t mem_used_;
  bool added_to_lc_;

  AllocCacheObjInfo(uint64_t obj_id, uint64_t tenant_id,
                    int64_t log_del_time, int64_t real_del_time,
                    int64_t ref_count, int64_t mem_used, bool added_to_pc)
      : obj_id_(obj_id),
        tenant_id_(tenant_id),
        log_del_time_(log_del_time),
        real_del_time_(real_del_time),
        ref_count_(ref_count),
        mem_used_(mem_used),
        added_to_lc_(added_to_pc) {}

  AllocCacheObjInfo()
      : obj_id_(common::OB_INVALID_ID),
        tenant_id_(common::OB_INVALID_ID),
        log_del_time_(INT64_MAX),
        real_del_time_(INT64_MAX),
        ref_count_(0),
        mem_used_(0),
        added_to_lc_(false) {}

  TO_STRING_KV(K_(obj_id), K_(tenant_id), K_(log_del_time),
               K_(real_del_time), K_(ref_count), K_(added_to_lc),
               K_(mem_used));
};

/**
 * @brief The PreCalcPolicy enum
 * PRE_CALC_DEFAULT:    add frame to phy plan, add result to datum store & param store
 * PRE_CALC_CHECK:      add nothing to phy plan / datum store & param store
 */
enum PreCalcPolicy {
  PRE_CALC_DEFAULT    = 0,
  PRE_CALC_CHECK      = 1 << 0,
};

struct ObParamInfo
{
  ObParamInfo()
  : scale_(0),
    type_(common::ObNullType),
    ext_real_type_(common::ObNullType),
    is_oracle_empty_string_(false),
    col_type_(common::CS_TYPE_INVALID)
  {}
  virtual ~ObParamInfo() {}
  void reset();

  TO_STRING_KV(K_(flag),
               K_(scale),
               K_(type),
               K_(ext_real_type),
               K_(is_oracle_empty_string),
               K_(col_type));

  static const int64_t MAX_STR_DES_LEN = 17;
  //存放是否需要check type和bool 值，以及期望的bool值
  common::ParamFlag flag_;
  common::ObScale scale_;
  common::ObObjType type_;
  common::ObObjType ext_real_type_;
  //处理Oracle模式空串在plan_cache中的匹配
  bool is_oracle_empty_string_;
  common::ObCollationType col_type_;

  OB_UNIS_VERSION_V(1);
};

class ObPlanCacheObject : public ObILibCacheObject
{
friend class ::test::MockCacheObjectFactory;
public:
  ObPlanCacheObject(ObLibCacheNameSpace ns, lib::MemoryContext &mem_context);
  virtual ~ObPlanCacheObject() {}

  inline int64_t get_dependency_table_size() const { return dependency_tables_.count(); }
  inline const DependenyTableStore &get_dependency_table() const { return dependency_tables_; }
  inline void set_sys_schema_version(int64_t schema_version) { sys_schema_version_ = schema_version; }
  inline void set_tenant_schema_version(int64_t schema_version) { tenant_schema_version_ = schema_version; }
  inline int64_t get_tenant_schema_version() const { return tenant_schema_version_; }
  inline int64_t get_sys_schema_version() const { return sys_schema_version_; }
  int init_dependency_table_store(int64_t dependency_table_cnt) { return dependency_tables_.init(dependency_table_cnt); }
  inline DependenyTableStore &get_dependency_table() { return dependency_tables_; }
  int get_audit_objects(common::ObIArray<share::schema::ObObjectStruct> &object_ids) const;
  bool has_sequence() const;
  int get_base_table_version(const uint64_t table_id, int64_t &table_version) const;
  inline ObOutlineState &get_outline_state() { return outline_state_; }
  inline const ObOutlineState &get_outline_state() const { return outline_state_; }
  inline void set_outline_state(const ObOutlineState &state) { outline_state_ = state; }
  int set_params_info(const ParamStore &params);
  const common::Ob2DArray<ObParamInfo,
                          common::OB_MALLOC_BIG_BLOCK_SIZE,
                          common::ObWrapperAllocator, false> &get_params_info() const { return params_info_; }
  inline void set_is_contain_virtual_table(bool is_contain_virtual_table) { is_contain_virtual_table_ = is_contain_virtual_table; }
  inline void set_is_contain_inner_table(bool is_contain_inner_table) { is_contain_inner_table_ = is_contain_inner_table; }
  inline bool is_contain_virtual_table() const { return is_contain_virtual_table_; }
  inline bool is_contain_inner_table() const { return is_contain_inner_table_; }
  virtual void inc_pre_expr_ref_count() {}
  virtual void dec_pre_expr_ref_count() {}
  virtual int64_t get_pre_expr_ref_count() const {return -1;}
  virtual void set_pre_calc_expr_handler(PreCalcExprHandler* handler) {UNUSED(handler);}
  virtual PreCalcExprHandler* get_pre_calc_expr_handler(){return NULL;}
  inline const common::ObDList<ObPreCalcExprFrameInfo> &get_pre_calc_frames() const
  {
    return pre_calc_frames_;
  }
  inline common::ObDList<ObPreCalcExprFrameInfo> &get_pre_calc_frames()
  {
    return pre_calc_frames_;
  }
  inline void set_fetch_cur_time(bool fetch_cur_time) { fetch_cur_time_ = fetch_cur_time; }
  inline bool get_fetch_cur_time() const { return fetch_cur_time_; }
  inline void set_ignore(bool ignore) { is_ignore_stmt_ = ignore; }
  inline bool is_ignore() const { return is_ignore_stmt_; }
  inline void set_stmt_type(stmt::StmtType stmt_type) { stmt_type_ = stmt_type; }
  inline stmt::StmtType get_stmt_type() const { return stmt_type_; }
  inline void set_need_param(bool need_param) { need_param_ = need_param; }
  inline bool need_param() const { return need_param_; }
  static int check_pre_calc_cons(const bool is_ignore_stmt,
                                 bool &is_match,
                                 ObPreCalcExprConstraint &pre_calc_con,
                                 ObExecContext &exec_ctx);

  static int pre_calculation(const bool is_ignore_stmt,
                             ObPreCalcExprFrameInfo &pre_calc_frame,
                             ObExecContext &exec_ctx,
                             const uint64_t calc_types = PRE_CALC_DEFAULT);

  virtual void reset();
  virtual void dump_deleted_log_info(const bool is_debug_log = true) const;
  virtual int check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add);
  static int type_to_name(const ObLibCacheNameSpace ns,
                          common::ObIAllocator &allocator,
                          common::ObString &type_name);
  VIRTUAL_TO_STRING_KV(K_(tenant_schema_version),
                       K_(sys_schema_version),
                       K_(dependency_tables),
                       K_(outline_state),
                       K_(params_info),
                       K_(is_contain_virtual_table),
                       K_(is_contain_inner_table),
                       K_(fetch_cur_time));
protected:
  static int construct_array_params(const ObSqlExpression &expr,
                                    common::ObIAllocator &allocator,
                                    common::ObExprCtx &expr_ctx,
                                    common::ObNewRow &row,
                                    common::ObObjParam &result);

protected:
  int64_t tenant_schema_version_;
  int64_t sys_schema_version_;
  DependenyTableStore dependency_tables_;
  //for outline use
  ObOutlineState outline_state_;//TODO:check this need to be
  // store the expressions need to be calculated when plan beginning
  common::ObDList<ObPreCalcExprFrameInfo> pre_calc_frames_;
  //存放参数化后的参数以及可计算表达式的结果值的信息
  common::Ob2DArray<ObParamInfo,
                    common::OB_MALLOC_BIG_BLOCK_SIZE,
                    common::ObWrapperAllocator, false> params_info_;
  bool is_contain_virtual_table_;//为虚拟表服务，如果判断出语句中涉及虚拟表
  bool is_contain_inner_table_;//为内部表服务，如果判断出语句中涉及内部表
  bool fetch_cur_time_;
  bool is_ignore_stmt_;
  stmt::StmtType stmt_type_;
  bool need_param_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_H_ */
