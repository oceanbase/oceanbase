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

#ifndef OCEANBASE_SRC_PL_OB_PL_ROUTER_H_
#define OCEANBASE_SRC_PL_OB_PL_ROUTER_H_

#include "ob_pl_stmt.h"

namespace oceanbase {
namespace pl {

class ObPLRouter
{
public:
  ObPLRouter(const share::schema::ObRoutineInfo &routine_info,
             sql::ObSQLSessionInfo &session_info,
             share::schema::ObSchemaGetterGuard &schema_guard,
             common::ObMySQLProxy &sql_proxy)
    : routine_info_(routine_info),
      session_info_(session_info),
      schema_guard_(schema_guard),
      sql_proxy_(sql_proxy),
      inner_allocator_(ObModIds::OB_PL_TEMP, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      expr_factory_(inner_allocator_) {}
  virtual ~ObPLRouter() {}

  int analyze(ObString &route_sql, common::ObIArray<share::schema::ObDependencyInfo> &dep_infos, ObRoutineInfo &routine_info);
  int simple_resolve(ObPLFunctionAST &func_ast);
  static int analyze_stmt(const ObPLStmt *stmt, ObString &route_sql);

private:
  static int check_route_sql(const ObPLSql *pl_sql, ObString &route_sql);
  static int check_error_in_resolve(int code);
private:
  const share::schema::ObRoutineInfo &routine_info_;
  sql::ObSQLSessionInfo &session_info_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  common::ObMySQLProxy &sql_proxy_;
  ObArenaAllocator inner_allocator_;
  sql::ObRawExprFactory expr_factory_;
};


}
};


#endif /* OCEANBASE_SRC_PL_OB_PL_ROUTER_H_ */
