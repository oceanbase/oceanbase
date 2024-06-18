/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_PL_CACHE_OBJECT_H_
#define OCEANBASE_PL_CACHE_OBJECT_H_
#include "share/ob_define.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_cache_object.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_sql_expression.h"

namespace oceanbase
{


namespace pl
{

struct ObPlParamInfo : public sql::ObParamInfo
{
  ObPlParamInfo() :
    sql::ObParamInfo(),
    pl_type_(PL_INVALID_TYPE),
    udt_id_(OB_INVALID_ID)
  {}
  ~ObPlParamInfo() {}
  void reset()
  {
    ObParamInfo::reset();
    pl_type_ = PL_INVALID_TYPE;
    udt_id_ = OB_INVALID_ID;
  }

  TO_STRING_KV(K_(flag),
               K_(scale),
               K_(type),
               K_(ext_real_type),
               K_(is_oracle_empty_string),
               K_(col_type),
               K_(pl_type),
               K_(udt_id));

  uint8_t pl_type_;
  uint64_t udt_id_;

  OB_UNIS_VERSION_V(1);
};

enum ObPLCacheObjectType
{
  INVALID_PL_OBJECT_TYPE = 5, // start with OB_PHY_PLAN_UNCERTAIN, distict ObPhyPlanType
  STANDALONE_ROUTINE_TYPE,
  PACKAGE_TYPE,
  PACKAGE_BODY_TYPE,
  ANONYMOUS_BLOCK_TYPE,
  CALL_STMT_TYPE,
  MAX_TYPE_NUM
};

struct PLCacheObjStat
{
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  int64_t pl_schema_id_;
  uint64_t db_id_;
  common::ObString raw_sql_; // sql txt for anonymous block
  common::ObString name_;
  common::ObCollationType sql_cs_type_;
  int64_t gen_time_;
  int64_t last_active_time_;
  int64_t compile_time_; // pl object cost time of compile
  uint64_t hit_count_;
  ObPLCacheObjectType type_;
  int64_t elapsed_time_;          //执行时间rt
  int64_t execute_times_;        //SUCC下执行次数
  int64_t  slowest_exec_time_;    // execution slowest time
  uint64_t slowest_exec_usec_;    // execution slowest usec
  int64_t schema_version_;
  int64_t ps_stmt_id_;//prepare stmt id
  common::ObString sys_vars_str_;

  PLCacheObjStat()
    : pl_schema_id_(OB_INVALID_ID),
      db_id_(OB_INVALID_ID),
      raw_sql_(),
      name_(),
      sql_cs_type_(common::CS_TYPE_INVALID),
      gen_time_(0),
      last_active_time_(0),
      compile_time_(0),
      hit_count_(0),
      type_(ObPLCacheObjectType::INVALID_PL_OBJECT_TYPE),
      elapsed_time_(0),
      execute_times_(0),
      slowest_exec_time_(0),
      slowest_exec_usec_(0),
      schema_version_(OB_INVALID_ID),
      ps_stmt_id_(OB_INVALID_ID),
      sys_vars_str_()
  {
    sql_id_[0] = '\0';
  }

  inline bool is_updated() const
  {
    return last_active_time_ != 0;
  }

  void reset()
  {
    sql_id_[0] = '\0';
    pl_schema_id_ = OB_INVALID_ID;
    db_id_ = OB_INVALID_ID;
    sql_cs_type_ = common::CS_TYPE_INVALID;
    raw_sql_.reset();
    name_.reset();
    gen_time_ = 0;
    last_active_time_ = 0;
    hit_count_ = 0;
    type_ = ObPLCacheObjectType::INVALID_PL_OBJECT_TYPE;
    elapsed_time_ = 0;
    execute_times_ = 0;
    slowest_exec_time_ = 0;
    slowest_exec_usec_ = 0;
    schema_version_ = OB_INVALID_ID;
    ps_stmt_id_ = OB_INVALID_ID;
    sys_vars_str_.reset();
  }

  TO_STRING_KV(K_(pl_schema_id),
               K_(db_id),
               K_(gen_time),
               K_(last_active_time),
               K_(hit_count),
               K_(raw_sql),
               K_(name),
               K_(compile_time),
               K_(type),
               K_(schema_version),
               K_(sys_vars_str));
};

class ObPLCacheObject : public sql::ObILibCacheObject
{
public:
  ObPLCacheObject(sql::ObLibCacheNameSpace ns, lib::MemoryContext &mem_context)
  : ObILibCacheObject(ns, mem_context),
    tenant_schema_version_(OB_INVALID_VERSION),
    sys_schema_version_(OB_INVALID_VERSION),
    dependency_tables_(allocator_),
    params_info_( (ObWrapperAllocator(allocator_)) ),
    expr_factory_(allocator_),
    sql_expression_factory_(allocator_),
    expr_operator_factory_(allocator_),
    expressions_(allocator_),
    expr_op_size_(0),
    frame_info_(allocator_),
    stat_()
    {}

  virtual ~ObPLCacheObject() {}
  virtual void reset();
  inline bool is_call_stmt() const { return sql::ObLibCacheNameSpace::NS_CALLSTMT == ns_; }

  inline void set_sys_schema_version(int64_t schema_version) { sys_schema_version_ = schema_version; }
  inline void set_tenant_schema_version(int64_t schema_version) { tenant_schema_version_ = schema_version; }
  inline int64_t get_tenant_schema_version() const { return tenant_schema_version_; }
  inline int64_t get_sys_schema_version() const { return sys_schema_version_; }

  inline int64_t get_dependency_table_size() const { return dependency_tables_.count(); }
  inline const sql::DependenyTableStore &get_dependency_table() const { return dependency_tables_; }
  int init_dependency_table_store(int64_t dependency_table_cnt) { return dependency_tables_.init(dependency_table_cnt); }
  inline sql::DependenyTableStore &get_dependency_table() { return dependency_tables_; }
  int set_params_info(const ParamStore &params, bool is_anonymous = false);
  const common::Ob2DArray<ObPlParamInfo,
                          common::OB_MALLOC_BIG_BLOCK_SIZE,
                          common::ObWrapperAllocator, false> &get_params_info() const { return params_info_; }

  inline sql::ObRawExprFactory &get_expr_factory() { return expr_factory_; }
  inline sql::ObSqlExpressionFactory &get_sql_expression_factory() { return sql_expression_factory_; }
  inline sql::ObExprOperatorFactory &get_expr_operator_factory() { return expr_operator_factory_; }
  inline const common::ObIArray<sql::ObSqlExpression*> &get_expressions() const { return expressions_; }
  inline common::ObIArray<sql::ObSqlExpression*> &get_expressions() { return expressions_; }
  inline int set_expressions(common::ObIArray<sql::ObSqlExpression*> &exprs) { return expressions_.assign(exprs); }
  inline int64_t get_expr_op_size() const { return expr_op_size_; }
  inline void set_expr_op_size(int64_t size) { expr_op_size_ = size; }
  inline sql::ObExprFrameInfo &get_frame_info() { return frame_info_; }

  inline const PLCacheObjStat get_stat() const { return stat_; }
  inline PLCacheObjStat &get_stat_for_update() { return stat_; }

  virtual int update_cache_obj_stat(sql::ObILibCacheCtx &ctx);
  int update_execute_time(int64_t exec_time);

  TO_STRING_KV(K_(expr_op_size),
               K_(tenant_schema_version),
               K_(sys_schema_version),
               K_(dependency_tables));

protected:
  int64_t tenant_schema_version_;
  int64_t sys_schema_version_;
  sql::DependenyTableStore dependency_tables_;
  // stored args information after paramalization
  common::Ob2DArray<ObPlParamInfo,
                    common::OB_MALLOC_BIG_BLOCK_SIZE,
                    common::ObWrapperAllocator, false> params_info_;
  sql::ObRawExprFactory expr_factory_;
  sql::ObSqlExpressionFactory sql_expression_factory_;
  sql::ObExprOperatorFactory expr_operator_factory_;
  common::ObFixedArray<sql::ObSqlExpression*, common::ObIAllocator> expressions_;
  int64_t expr_op_size_;
  sql::ObExprFrameInfo frame_info_;
  // stat info
  PLCacheObjStat stat_;
};

} // namespace pl end
} // namespace oceanbase end

#endif