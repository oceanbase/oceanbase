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

#ifndef _OB_CREATE_PROFILE_RESOLVER_H
#define _OB_CREATE_PROFILE_RESOLVER_H 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
namespace oceanbase
{
namespace sql
{


class ObUserProfileResolver: public ObDDLResolver
{
public:
  explicit ObUserProfileResolver(ObResolverParams &params);
  virtual ~ObUserProfileResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int fill_arg(int64_t type, ObObj &value, obrpc::ObProfileDDLArg &arg);
  int resolver_password_verify_function(const ParseNode *node, obrpc::ObProfileDDLArg &arg);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUserProfileResolver);
};
}//namespace sql
}//namespace oceanbase
#endif
