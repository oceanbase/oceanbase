/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALTER_USER_PROFILE_RESOLVER_H
#define OB_ALTER_USER_PROFILE_RESOLVER_H

#include "sql/resolver/dcl/ob_alter_user_profile_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUserProfileResolver: public ObDCLResolver
{
public:
  explicit ObAlterUserProfileResolver(ObResolverParams &params);
  virtual ~ObAlterUserProfileResolver();
  virtual int resolve(const ParseNode &parse_tree);
  
private:
  int resolve_set_role(const ParseNode &parse_tree);
  int resolve_default_role(const ParseNode &parse_tree);
  int resolve_default_role_clause(
      const ParseNode *parse_tree, 
      obrpc::ObAlterUserProfileArg &arg,
      const ObIArray<uint64_t> &role_id_array,
      bool for_default_role_stmt);
  int resolve_role_list(
      const ParseNode *role_list, 
      obrpc::ObAlterUserProfileArg &arg,
      const ObIArray<uint64_t> &role_id_array,
      bool for_default_role_stmt);
  int check_role_password(
      const uint64_t tenant_id,
      const ObIArray<uint64_t> &role_id_array,
      const common::hash::ObHashMap<uint64_t, ObString> &roleid_pwd_map,
      bool has_all,
      bool has_except);
  int check_passwd(const ObString &pwd, const ObString &saved_pwd);
  int check_role_password_all(const uint64_t tenant_id,
                              const ObIArray<uint64_t> &role_id_array);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserProfileResolver);
};

} // end namespace sql
} // end namespace oceanbase



#endif // OB_ALTER_USER_PROFILE_RESOLVER_H
