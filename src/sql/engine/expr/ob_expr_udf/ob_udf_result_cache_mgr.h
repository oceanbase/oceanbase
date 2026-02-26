
/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_UDF_RESULT_CACHE_MGR_H_
#define OCEANBASE_UDF_RESULT_CACHE_MGR_H_
#include "share/ob_define.h"
#include "ob_udf_result_cache.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"

namespace oceanbase
{

namespace pl
{

static constexpr int64_t RESULT_CACHE_SYS_VAR_COUNT = 14;
static constexpr share::ObSysVarClassType InfluenceMap[RESULT_CACHE_SYS_VAR_COUNT + 1] = {
  share::SYS_VAR_CHARACTER_SET_RESULTS,
  share::SYS_VAR_NCHARACTER_SET_CONNECTION,
  share::SYS_VAR_TIME_ZONE,
  share::SYS_VAR_ERROR_ON_OVERLAP_TIME,
  share::SYS_VAR_BLOCK_ENCRYPTION_MODE,
  share::SYS_VAR_NLS_DATE_FORMAT,
  share::SYS_VAR_NLS_TIMESTAMP_FORMAT,
  share::SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
  share::SYS_VAR_NLS_NUMERIC_CHARACTERS,
  share::SYS_VAR_NLS_CURRENCY,
  share::SYS_VAR_NLS_ISO_CURRENCY,
  share::SYS_VAR_NLS_DUAL_CURRENCY,
  share::SYS_VAR_PLSQL_CCFLAGS,
  share::SYS_VAR_OB_COMPATIBILITY_VERSION,
  share::SYS_VAR_INVALID
};

struct ObGetResultCacheKVEntryOp : public sql::ObKVEntryTraverseOp
{
  explicit ObGetResultCacheKVEntryOp(LCKeyValueArray *key_val_list,
                                      const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = OB_SUCCESS;
    is_match = false;
    if (ObLibCacheNameSpace::NS_UDF_RESULT_CACHE == entry.first->namespace_) {
      is_match = true;
    }
    return ret;
  }
};

class ObPLUDFResultCacheMgr
{
public:

  ObPLUDFResultCacheMgr() {}

  ~ObPLUDFResultCacheMgr() {}

  static int add_udf_result_cache(ObPlanCache *lib_cache, ObILibCacheObject *pl_object, ObPLUDFResultCacheCtx &pc_ctx);
  static int get_udf_result_cache(ObPlanCache *lib_cache,
                        ObCacheObjGuard& guard, ObPLUDFResultCacheCtx &pc_ctx);

  static int cache_evict_all_obj(ObPlanCache *lib_cache);
  static int result_cache_evict(ObPlanCache *lib_cache);

private:
  static int add_udf_result_object(ObPlanCache *lib_cache,
                                  ObILibCacheCtx &ctx,
                                  ObILibCacheObject *cache_obj);
  static int get_udf_result_object(ObPlanCache *lib_cache,
                    ObILibCacheCtx &pc_ctx, ObCacheObjGuard& guard);

  DISALLOW_COPY_AND_ASSIGN(ObPLUDFResultCacheMgr);
};

} // namespace pl end
} // namespace oceanbase end

#endif
