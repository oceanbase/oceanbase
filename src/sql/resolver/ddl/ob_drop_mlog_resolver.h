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

#ifndef OB_DROP_MLOG_RESOLVER_H_
#define OB_DROP_MLOG_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropMLogResolver : public ObDDLResolver
{
public:
  explicit ObDropMLogResolver(ObResolverParams &params);
  virtual ~ObDropMLogResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  enum ParameterEnum {
    ENUM_TABLE_NAME = 0,
    ENUM_TOTAL_COUNT
  };
  DISALLOW_COPY_AND_ASSIGN(ObDropMLogResolver);
};
} // sql
} // oceanbase

#endif // OB_DROP_MLOG_RESOLVER_H_