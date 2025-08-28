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

#ifndef SQL_RESOLVER_CMD_OB_TRIGGER_STORAGE_CACHE_RESOLVER_H
#define SQL_RESOLVER_CMD_OB_TRIGGER_STORAGE_CACHE_RESOLVER_H

#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
  
class ObTriggerStorageCacheResolver : public ObCMDResolver
{
public:
  explicit ObTriggerStorageCacheResolver(ObResolverParams &params);
  virtual ~ObTriggerStorageCacheResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTriggerStorageCacheResolver);
};
} // end namespace sql
} // end namespace oceanbase

#endif /*_OB_TENANT_SNAPSHOT_RESOLVER_H*/
