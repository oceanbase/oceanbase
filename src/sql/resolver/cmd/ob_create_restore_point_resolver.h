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

#ifndef OCEANBASE_CREATE_RESTORE_POINT_RESOLVER_
#define OCEANBASE_CREATE_RESTORE_POINT_RESOLVER_ 1

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateRestorePointResolver: public ObSystemCmdResolver
{
public:
  explicit ObCreateRestorePointResolver(ObResolverParams &params);
  virtual ~ObCreateRestorePointResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateRestorePointResolver);
  // function members

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_CREATE_RESTORE_POINT_RESOLVER_ */
