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

#ifndef _OB_DROP_EVENT_RESOLVER_H
#define _OB_DROP_EVENT_RESOLVER_H 1

#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/cmd/ob_drop_event_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropEventResolver: public ObCMDResolver
{
public:
  explicit ObDropEventResolver(ObResolverParams &params);
  virtual ~ObDropEventResolver() = default;

  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int OB_EVENT_NAME_MAX_LENGTH = 128;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropEventResolver);
};
}//namespace sql
}//namespace oceanbase
#endif // _OB_DROP_EVENT_RESOLVER_H
