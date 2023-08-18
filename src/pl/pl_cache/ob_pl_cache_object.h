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
    frame_info_(allocator_)
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
  int set_params_info(const ParamStore &params);
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
};

} // namespace pl end
} // namespace oceanbase end

#endif