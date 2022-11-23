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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DO_ANONYMOUS_BLOCK_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DO_ANONYMOUS_BLOCK_STMT_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
namespace oceanbase
{
namespace sql
{

class ObAnonymousBlockStmt : public ObCMDStmt
{
public:
  explicit ObAnonymousBlockStmt(common::ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_ANONYMOUS_BLOCK),
      body_(NULL),
      sql_(),
      statement_id_(OB_INVALID_ID),
      is_prepare_protocol_(false),
      params_(NULL),
      out_idx_()
  {
  }
  ObAnonymousBlockStmt()
    : ObCMDStmt(stmt::T_ANONYMOUS_BLOCK),
      body_(NULL),
      sql_(),
      statement_id_(OB_INVALID_ID),
      is_prepare_protocol_(false),
      params_(NULL),
      out_idx_()
  {
  }
  virtual ~ObAnonymousBlockStmt() {}

  inline const ParseNode *get_body() const { return body_; }
  inline void set_body(ParseNode *body) { body_ = body; }

  inline const ObString &get_sql() const { return sql_;}
  inline void set_sql(const ObString &sql) { sql_ = sql; }

  inline uint64_t get_stmt_id() const { return statement_id_; }
  inline void set_stmt_id(uint64_t stmt_id) { statement_id_ = stmt_id; }
\
  inline bool is_prepare_protocol() const {return is_prepare_protocol_; }
  inline void set_prepare_protocol(bool is_prepare) { is_prepare_protocol_ = is_prepare; }
  inline void set_params(ParamStore *params) { params_ = params; }
  inline ParamStore *get_params() { return params_; }
  int add_param(const common::ObObjParam& param);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return ddl_arg_; }

  int resolve_inout_param(ParseNode &block_node, ObAnonymousBlockStmt &stmt);
  const ObBitSet<>& get_out_idx() const { return out_idx_; }
  ObBitSet<>& get_out_idx() { return out_idx_; }

  TO_STRING_KV(K_(stmt_type),
               K_(sql),
               K_(statement_id),
               K_(is_prepare_protocol));

private:
  ParseNode *body_;
  ObString sql_;
  uint64_t statement_id_;
  bool is_prepare_protocol_;
  ParamStore *params_;//for ps param
  obrpc::ObDDLArg ddl_arg_; // 用于返回exec_tenant_id_
  ObBitSet<> out_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObAnonymousBlockStmt);
};


}//namespace sql
}//namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DO_ANONYMOUS_BLOCK_STMT_H_ */
