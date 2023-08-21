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

#ifndef SRC_PL_OB_PL_PACKAGE_GUARD_H_
#define SRC_PL_OB_PL_PACKAGE_GUARD_H_

#include "sql/plan_cache/ob_cache_object_factory.h"
#include "pl/dblink/ob_pl_dblink_guard.h"

namespace oceanbase
{

namespace pl
{
class ObPLPackageGuard
{
public:
  ObPLPackageGuard(uint64_t tenant_id) : alloc_(), dblink_guard_(alloc_)
  {
    lib::ObMemAttr attr;
    attr.label_ = "PLPKGGuard";
    attr.tenant_id_ = tenant_id;
    attr.ctx_id_ = common::ObCtxIds::EXECUTE_CTX_ID;
    alloc_.set_attr(attr);
  }
  virtual ~ObPLPackageGuard();

  int init();
  inline bool is_inited() { return map_.created(); }
  inline int put(uint64_t package_id, sql::ObCacheObjGuard *package)
  {
    return map_.set_refactored(package_id, package);
  }
  inline int get(uint64_t package_id, sql::ObCacheObjGuard *&package)
  {
    return map_.get_refactored(package_id, package);
  }
  common::ObArenaAllocator alloc_;
  ObPLDbLinkGuard dblink_guard_;
private:
  common::hash::ObHashMap<uint64_t, sql::ObCacheObjGuard*> map_;
};

}
}
#endif
