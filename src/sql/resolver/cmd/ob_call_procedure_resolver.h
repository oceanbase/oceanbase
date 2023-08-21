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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_RESOLVER_H_

#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "pl/pl_cache/ob_pl_cache.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObRoutineInfo;
}
}
namespace sql
{
class ObCallProcedureStmt;
class ObCallProcedureInfo;
class ObCallProcedureResolver: public ObCMDResolver
{
public:
  explicit ObCallProcedureResolver(ObResolverParams &params) : ObCMDResolver(params) {}
  virtual ~ObCallProcedureResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_cparams(const ParseNode* params_node,
                      const share::schema::ObRoutineInfo *routien_info,
                      ObCallProcedureInfo *call_proc_info,
                      ObIArray<ObRawExpr*> &params);
  int resolve_cparam_without_assign(const ParseNode *param_node,
                      const int64_t position,
                      common::ObIArray<ObRawExpr*> &params);
  int resolve_cparam_with_assign(const ParseNode *param_node,
                      const share::schema::ObRoutineInfo *routine_info,
                      common::ObIArray<ObRawExpr*> &params);
  int resolve_param_exprs(const ParseNode *params_node,
                      ObIArray<ObRawExpr*> &expr_params);
  int check_param_expr_legal(ObRawExpr *param);
  int find_call_proc_info(ObCallProcedureStmt &stmt);
  int add_call_proc_info(ObCallProcedureInfo *call_info);
  int generate_pl_cache_ctx(pl::ObPLCacheCtx &pc_ctx);
  int resolve_dblink_routine_name(const ParseNode &access_node,
                                  const ParseNode &dblink_node,
                                  ObString &dblink_name,
                                  ObString &db_name,
                                  ObString &pkg_name,
                                  ObString &sp_name);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCallProcedureResolver);
  // function members

private:
  // data members

};

} // end namespace sql
} // end namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_RESOLVER_H_ */
