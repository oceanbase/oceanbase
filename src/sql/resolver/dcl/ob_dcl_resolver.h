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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_DCL_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_DCL_RESOLVER_
#include "sql/resolver/ob_stmt_resolver.h"
#include "share/schema/ob_table_schema.h"
namespace oceanbase
{
namespace sql
{
class ObDCLResolver : public ObStmtResolver
{
public:
  explicit ObDCLResolver(ObResolverParams &params) :
      ObStmtResolver(params)
  {
  }
  virtual ~ObDCLResolver()
  {
  }
  static int mask_password_for_passwd_node(ObIAllocator *allocator,
                                           const common::ObString &src,
                                           const ParseNode *passwd_node,
                                           common::ObString &masked_sql,
                                           bool skip_enclosed_char = false);
  int resolve_user_list_node(ParseNode *user_node,
                             ParseNode *top_node,
                             common::ObString &user_name,
                             common::ObString &host_name);

  int resolve_user_host(const ParseNode *user_pass,
                        common::ObString &user_name,
                        common::ObString &host_name);

protected:
  int check_and_convert_name(common::ObString &db, common::ObString &table);
  int check_password_strength(common::ObString &password);
  int check_user_name(common::ObString &password, const common::ObString &user_name);
  int check_oracle_password_strength(int64_t tenant_id,
                                     int64_t profile_id,
                                     common::ObString &password, 
                                     common::ObString &user_name);
  int check_dcl_on_inner_user(const ObItemType &type,
                              const uint64_t &session_user_id,
                              const ObString &user_name,
                              const ObString &host_name);
  int check_dcl_on_inner_user(const ObItemType &type,
                              const uint64_t &session_user_id,
                              const uint64_t &user_id);

  static int mask_password_for_single_user(ObIAllocator *allocator,
                                           const common::ObString &src,
                                           const ParseNode *user_node,
                                           int64_t pwd_idx,
                                           common::ObString &masked_sql);
  static int mask_password_for_users(ObIAllocator *allocator,
                                     const common::ObString &src,
                                     const ParseNode *users,
                                     int64_t pwd_idx,
                                     common::ObString &masked_sql);
  enum ObPasswordPolicy {LOW = 0, MEDIUM};
  static const char password_mask_ = '*';
private:
  DISALLOW_COPY_AND_ASSIGN(ObDCLResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_DCL_RESOLVER_
