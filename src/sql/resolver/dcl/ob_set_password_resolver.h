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

#ifndef OCEANBASE_SQL_RESOVLER_DCL_OB_SET_PASSWORD_RESOLVER_
#define OCEANBASE_SQL_RESOVLER_DCL_OB_SET_PASSWORD_RESOLVER_
#include "sql/resolver/dcl/ob_set_password_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
namespace oceanbase {
namespace sql {
class ObSetPasswordResolver : public ObDCLResolver {
public:
  explicit ObSetPasswordResolver(ObResolverParams& params);
  virtual ~ObSetPasswordResolver();

  virtual int resolve(const ParseNode& parse_tree);

  static bool is_hex_literal(const common::ObString& str);

  static bool is_valid_mysql41_passwd(const common::ObString& str);

private:
  int resolve_oracle_password_strength(
      common::ObString& user_name, common::ObString& hostname, common::ObString& password);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSetPasswordResolver);
};

}  // end namespace sql
}  // end namespace oceanbase
#endif
