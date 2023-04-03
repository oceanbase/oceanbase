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

#ifndef _OB_SET_NAMES_RESOLVER_H
#define _OB_SET_NAMES_RESOLVER_H
#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
// resolver for both SET NAMES and SET CHARSET
class ObSetNamesResolver : public ObCMDResolver
{
public:
  explicit ObSetNamesResolver(ObResolverParams &params);
  virtual ~ObSetNamesResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetNamesResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OB_SET_NAMES_RESOLVER_H
