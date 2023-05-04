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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_ROUTINE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_ROUTINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObRoutineParam;
}
}
namespace sql
{
class ObCreateRoutineResolver: public ObDDLResolver
{
public:
  static const int64_t ROUTINE_STANDALONE_OVERLOAD = 0;
  static const int64_t ROUTINE_STANDALONE_SUBPROGRAM_ID = 0;
  static const int64_t ROUTINE_RET_TYPE_POSITION = 0;
  static const int64_t ROUTINE_RET_TYPE_SEQUENCE = 1;
  explicit ObCreateRoutineResolver(ObResolverParams &params) : ObDDLResolver(params) {}

  int resolve(const ParseNode &parse_tree,
                     const ParseNode *sp_definer_node,
                     const ParseNode *name_node,
                     const ParseNode *body_node,
                     const ParseNode *ret_node,
                     const ParseNode *param_node,
                     const ParseNode *clause_list,
                     obrpc::ObCreateRoutineArg *crt_routine_arg);
  int resolve_impl(share::schema::ObRoutineType routine_type,
                   const ParseNode *sp_definer_node,
                   const ParseNode *name_node,
                   const ParseNode *body_node,
                   const ParseNode *ret_node,
                   const ParseNode *param_node,
                   const ParseNode *clause_list,
                   obrpc::ObCreateRoutineArg *crt_routine_arg,
                   bool is_udt_udf = false);
  
  virtual int resolve(const ParseNode &parse_tree);
  virtual int resolve_impl(const ParseNode &parse_tree, obrpc::ObCreateRoutineArg *crt_routine_arg) = 0;

private:
  int check_dup_routine_param(const common::ObIArray<share::schema::ObRoutineParam*> &params,
                              const common::ObString &param_name);
  int create_routine_arg(obrpc::ObCreateRoutineArg *&crt_routine_arg);
  int set_routine_info(const share::schema::ObRoutineType &type,
                       share::schema::ObRoutineInfo &routine_info,
                       bool is_udt_udf = false);
  int analyze_router_sql(obrpc::ObCreateRoutineArg *crt_routine_arg);
  int resolve_sp_definer(const ParseNode *parse_node, share::schema::ObRoutineInfo &routine_info);
  int resolve_sp_name(const ParseNode *parse_node, obrpc::ObCreateRoutineArg *crt_routine_arg);
  int resolve_sp_body(const ParseNode *parse_node, share::schema::ObRoutineInfo &routine_info);
  int resolve_ret_type(const ParseNode *ret_type_node, share::schema::ObRoutineInfo &func_info);
  int analyze_expr_type(ObRawExpr *&expr, share::schema::ObRoutineInfo &routine_info);
  int resolve_param_list(const ParseNode *param_list, share::schema::ObRoutineInfo &routine_info);
  int resolve_replace(const ParseNode *parse_node, obrpc::ObCreateRoutineArg *crt_routine_arg);
  int resolve_editionable(const ParseNode *parse_node, obrpc::ObCreateRoutineArg *crt_routine_arg);
  int resolve_param_type(const ParseNode *type_node, const common::ObString &param_name,
                         ObSQLSessionInfo &session_info, share::schema::ObRoutineParam &routine_param);
  int resolve_clause_list(const ParseNode *clause_list, share::schema::ObRoutineInfo &func_info);
  int set_routine_param(const ObIArray<pl::ObObjAccessIdx> &access_idxs,
                        share::schema::ObRoutineParam &routine_param);
  int resolve_aggregate_body(const ParseNode *parse_node, share::schema::ObRoutineInfo &routine_info);
};

class ObCreateProcedureResolver: public ObCreateRoutineResolver
{
public:
  explicit ObCreateProcedureResolver(ObResolverParams &params) : ObCreateRoutineResolver(params) {}
  virtual ~ObCreateProcedureResolver() {}

  virtual int resolve_impl(const ParseNode &parse_tree, obrpc::ObCreateRoutineArg *crt_routine_arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateProcedureResolver);
  // function members

private:
  // data members
};

class ObCreateFunctionResolver: public ObCreateRoutineResolver
{
public:
  explicit ObCreateFunctionResolver(ObResolverParams &params) : ObCreateRoutineResolver(params) {}
  virtual ~ObCreateFunctionResolver() { }

  virtual int resolve_impl(const ParseNode &parse_tree, obrpc::ObCreateRoutineArg *crt_routine_arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateFunctionResolver);
  // function members
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_ROUTINE_RESOLVER_H_ */
