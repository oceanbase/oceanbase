
/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_PL_CACHE_MGR_H_
#define OCEANBASE_PL_CACHE_MGR_H_
#include "share/ob_define.h"
#include "ob_pl_cache.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"

namespace oceanbase
{

namespace sql
{
  class ObPlanCache;
  class ObKVEntryTraverseOp;
}

namespace pl
{

struct ObGetPLKVEntryOp : public sql::ObKVEntryTraverseOp
{
  explicit ObGetPLKVEntryOp(LCKeyValueArray *key_val_list,
                            const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = OB_SUCCESS;
    is_match = false;
    if (ObLibCacheNameSpace::NS_PRCR == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_SFC == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_ANON == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_PKG == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_CALLSTMT == entry.first->namespace_) {
      is_match = true;
    }
    return ret;
  }
};

class ObPLCacheMgr
{
public:
  ObPLCacheMgr() {}

  ~ObPLCacheMgr() {}

  static int add_pl_cache(ObPlanCache *lib_cache, ObILibCacheObject *pl_object, ObPLCacheCtx &pc_ctx);
  static int get_pl_cache(ObPlanCache *lib_cache,
                        ObCacheObjGuard& guard, ObPLCacheCtx &pc_ctx);

  static int cache_evict_all_pl(ObPlanCache *lib_cache);

private:
  static int add_pl_object(ObPlanCache *lib_cache,
                                  ObILibCacheCtx &ctx,
                                  ObILibCacheObject *cache_obj);
  static int get_pl_object(ObPlanCache *lib_cache,
                    ObILibCacheCtx &pc_ctx, ObCacheObjGuard& guard);

  DISALLOW_COPY_AND_ASSIGN(ObPLCacheMgr);
};

} // namespace pl end
} // namespace oceanbase end

#endif
