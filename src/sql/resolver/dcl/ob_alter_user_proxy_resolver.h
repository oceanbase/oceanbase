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

#ifndef OB_ALTER_USER_PROXY_RESOLVER_H
#define OB_ALTER_USER_PROXY_RESOLVER_H

#include "sql/resolver/dcl/ob_alter_user_proxy_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUserProxyResolver: public ObDCLResolver
{
public:
  explicit ObAlterUserProxyResolver(ObResolverParams &params);
  virtual ~ObAlterUserProxyResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserProxyResolver);
};

} // end namespace sql
} // end namespace oceanbase



#endif // OB_ALTER_USER_PROFILE_RESOLVER_H
