/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
