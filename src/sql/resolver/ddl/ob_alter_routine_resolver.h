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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_ROUTINE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_ROUTINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_create_routine_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/system_variable/ob_sys_var_class_type.h"

namespace oceanbase
{
namespace sql
{
class ObAlterRoutineResolver: public ObDDLResolver
{
protected:
  explicit ObAlterRoutineResolver(ObResolverParams &params) : ObDDLResolver(params) {}

public:
  virtual int resolve(const ParseNode &parse_tree);

protected:
  int resolve_impl(
    obrpc::ObCreateRoutineArg &crt_routine_arg,
    const share::schema::ObRoutineInfo &routine_info, const ParseNode &alter_clause_node);
  int resolve_compile_clause(
    obrpc::ObCreateRoutineArg &crt_routine_arg,
    const share::schema::ObRoutineInfo &routine_info, const ParseNode &alter_clause_node);
  int resolve_compile_parameters(
    const ParseNode *compile_params_node,
    ObIArray<std::pair<share::ObSysVarClassType, common::ObObj> > &params);
  int resolve_compile_parameter(
    const ParseNode *ident, const ParseNode *value,
    ObIArray<std::pair<share::ObSysVarClassType, common::ObObj> > &params);
  int parse_routine(
    const ObString &source, const ParseNode *&parse_tree);
  int resolve_routine(
    obrpc::ObCreateRoutineArg &crt_routine_arg,
    const share::schema::ObRoutineInfo &routine_info,
    bool need_recreate, const ParseNode *source_tree);
  int resolve_clause_list(const ParseNode *node,
                          obrpc::ObCreateRoutineArg &crt_routine_arg);
  int register_debug_info(const share::schema::ObRoutineInfo &routine_info);

private:
  int mock_create_parse_node(const ParseNode *source_tree,
                             const bool need_recreate,
                             const bool is_noneditionable,
                             ParseNode *&crt_tree);
};

class ObAlterProcedureResolver: public ObAlterRoutineResolver
{
public:
  explicit ObAlterProcedureResolver(ObResolverParams &params) : ObAlterRoutineResolver(params) {}
  virtual ~ObAlterProcedureResolver() {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterProcedureResolver);
};

class ObAlterFunctionResolver: public ObAlterRoutineResolver
{
public:
  explicit ObAlterFunctionResolver(ObResolverParams &params) : ObAlterRoutineResolver(params) {}
  virtual ~ObAlterFunctionResolver() {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterFunctionResolver);
};

} // end namespace sql
} // end namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_ROUTINE_RESOLVER_H_ */
