/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PACKAGE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PACKAGE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
namespace oceanbase
{
namespace pl {
  class ObPLRoutineTable;
}
namespace sql
{
class ObCreatePackageResolver: public ObDDLResolver
{
public:
  static const int64_t CREATE_PACKAGE_NODE_CHILD_COUNT = 1;
  static const int64_t PACKAGE_BLOCK_NODE_CHILD_COUNT = 4;
  static const int64_t NO_OVERLOAD_IDX = 0;
  static const int64_t OVERLOAD_START_IDX = 1;

  explicit ObCreatePackageResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObCreatePackageResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
  static int resolve_invoke_accessible(const ParseNode *package_clause_node,
                                       bool &is_invoker_right,
                                       bool &has_accessible_by);
  static int resolve_functions_spec(const share::schema::ObPackageInfo &package_info,
                                    ObIArray<share::schema::ObRoutineInfo> &routine_list,
                                    const pl::ObPLRoutineTable &routine_table,
  share::schema::ObRoutineType routine_type = share::schema::ObRoutineType::ROUTINE_PACKAGE_TYPE);
  static int check_overload_out_argument(const pl::ObPLRoutineTable &routine_table, int64_t idx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePackageResolver);
};

class ObCreatePackageBodyResolver: public ObDDLResolver
{
public:
  static const int64_t CREATE_PACKAGE_BODY_NODE_CHILD_COUNT = 1;
  static const int64_t PACKAGE_BODY_BLOCK_NODE_CHILD_COUNT = 4;

  explicit ObCreatePackageBodyResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObCreatePackageBodyResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
  static int update_routine_route_sql(ObIAllocator &allocator,
                                      const ObSQLSessionInfo &session_info,
                                      ObIArray<ObRoutineInfo> &public_routine_list,
                                      const pl::ObPLRoutineTable &spec_routine_table,
                                      const pl::ObPLRoutineTable &body_routine_table,
                                      ObIArray<const ObRoutineInfo *> &routine_infos);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePackageBodyResolver);
};
} //namespace sql
} //namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PACKAGE_RESOLVER_H_ */
