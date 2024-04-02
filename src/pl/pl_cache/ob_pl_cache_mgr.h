
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

struct ObGetPLKVEntryBySchemaIdOp : public ObKVEntryTraverseOp
{
  explicit ObGetPLKVEntryBySchemaIdOp(uint64_t db_id,
                                 uint64_t schema_id,
                                 LCKeyValueArray *key_val_list,
                                 const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle),
      db_id_(db_id),
      schema_id_(schema_id)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = OB_SUCCESS;
    is_match = false;
    if (ObLibCacheNameSpace::NS_PRCR == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_SFC == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_PKG == entry.first->namespace_) {
      ObPLObjectKey *key = static_cast<ObPLObjectKey*>(entry.first);
      if (db_id_ != common::OB_INVALID_ID && db_id_ != key->db_id_) {
        // skip entry that has non-matched db_id
      } else if (schema_id_ != key->key_id_) {
        // skip entry which is not same schema id
      } else {
        is_match = true;
      }
    }
    return ret;
  }

  uint64_t db_id_;
  uint64_t schema_id_;
};

struct ObGetPLKVEntryBySQLIDOp : public ObKVEntryTraverseOp
{
  explicit ObGetPLKVEntryBySQLIDOp(uint64_t db_id,
                                 common::ObString sql_id,
                                 LCKeyValueArray *key_val_list,
                                 const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle),
      db_id_(db_id),
      sql_id_(sql_id)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = OB_SUCCESS;
    is_match = false;
    if (ObLibCacheNameSpace::NS_ANON == entry.first->namespace_ ||
        ObLibCacheNameSpace::NS_CALLSTMT == entry.first->namespace_) {
      ObPLObjectKey *key = static_cast<ObPLObjectKey*>(entry.first);
      ObPLObjectSet *node = static_cast<ObPLObjectSet*>(entry.second);
      if (db_id_ != common::OB_INVALID_ID && db_id_ != key->db_id_) {
        // skip entry that has non-matched db_id
      } else if (sql_id_ != node->get_sql_id()) {
        // skip entry which not contains same sql_id
      } else {
        is_match = true;
      }
    }
    return ret;
  }
  bool contain_sql_id(common::ObIArray<common::ObString> &sql_ids)
  {
    bool contains = false;
    for (int64_t i = 0; !contains && i < sql_ids.count(); i++) {
      if (sql_ids.at(i) == sql_id_) {
        contains = true;
      }
    }
    return contains;
  }

  uint64_t db_id_;
  common::ObString sql_id_;
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
  template<typename GETPLKVEntryOp, typename EvictAttr>
  static int cache_evict_pl_cache_single(ObPlanCache *lib_cache, uint64_t db_id, EvictAttr &attr);

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
