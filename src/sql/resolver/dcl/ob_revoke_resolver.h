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

#ifndef OCEANBASE_SQL_RESOVLER_DCL_OB_REVOKE_RESOLVER_
#define OCEANBASE_SQL_RESOVLER_DCL_OB_REVOKE_RESOLVER_
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObRevokeResolver: public ObDCLResolver
{
private:
  int resolve_revoke_role_inner(
      const ParseNode *revoke_role,
      ObRevokeStmt *revoke_stmt);
  int resolve_revoke_sysprivs_inner(
      const ParseNode *revoke_role,
      ObRevokeStmt *revoke_stmt);
  int resolve_mysql(const ParseNode &parse_tree);
  int resolve_ora(const ParseNode &parse_tree);
  int resolve_priv_set_ora(
      const ParseNode *privs_node,
      share::schema::ObPrivLevel grant_level,
      ObPrivSet &priv_set,
      share::ObRawObjPrivArray &obj_priv_array,
      bool &revoke_all_ora);
  static int trans_ora_sys_priv_to_obj(ParseNode *priv_type_node);
  int resolve_revoke_role_and_sysprivs_inner(const ParseNode *node,
                                             ObRevokeStmt *revoke_stmt);
  int resolve_revoke_obj_priv_inner(const ParseNode *node,
                                    ObRevokeStmt *revoke_stmt);

public:
  explicit ObRevokeResolver(ObResolverParams &params);
  virtual ~ObRevokeResolver();

  virtual int resolve(const ParseNode &parse_tree);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRevokeResolver);
};

} // end namespace sql
} // end namespace oceanbase
#endif
