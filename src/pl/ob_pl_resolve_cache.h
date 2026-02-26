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

#ifndef OCEANBASE_PL_RESOLVE_CACHE_H_
#define OCEANBASE_PL_RESOLVE_CACHE_H_
#include "share/ob_define.h"
#include "ob_pl_stmt.h"

namespace oceanbase
{

namespace pl
{

enum ObPLResolveCacheType
{
  INVALID_CACHE_TYPE,
  CACHE_RECORD_TYPE,
  CACHE_VIEW_DEPENDENCY_TYPE,
  CACHE_EXTERNAL_SYMBOL_TYPE,
  MAX_CACHE_TYPE
};

class ObPLResolveCacheKey
{
public:
  ObPLResolveCacheKey() :
    type_(INVALID_CACHE_TYPE),
    parent_id_(OB_INVALID_ID),
    name_()
  {}

  int hash(uint64_t &hash_val) const;
  bool operator==(const ObPLResolveCacheKey &other) const;

  TO_STRING_KV(K_(type), K_(parent_id), K_(name));

  ObPLResolveCacheType type_;
  uint64_t parent_id_;
  ObString name_;
};

struct ObPLResolveCacheCtx
{
public:
  ObPLResolveCacheCtx() :
    key_()
  {}

  ObPLResolveCacheKey key_;
};

class ObPLResolveCacheValue
{
public:
  ObPLResolveCacheValue() :
    type_value_(-1),
    parent_id_value_(OB_INVALID_ID),
    var_idx_value_(OB_INVALID_INDEX),
    data_type_value_(),
    record_type_value_(nullptr),
    dependency_objects_()
  {}

  int64_t type_value_;
  uint64_t parent_id_value_;
  int64_t var_idx_value_;
  ObPLDataType data_type_value_;
  ObRecordType *record_type_value_;
  ObArray<ObSchemaObjVersion> dependency_objects_;
};


class ObPLResolveCache
{
public:
  typedef common::hash::ObHashMap<ObPLResolveCacheKey, ObPLResolveCacheValue, common::hash::NoPthreadDefendMode> ObPLResolveCacheMap;

  ObPLResolveCache(common::ObIAllocator &allocator) :
    alloc_(allocator),
    map_()
  {}

  ~ObPLResolveCache()
  {
    if (map_.created()) {
      map_.destroy();
    }
  }

  int init();

  int construct_key(ObPLResolveCacheType type,
                    uint64_t parent_id,
                    const ObString &name,
                    ObPLResolveCacheKey &hash_key);

  int gen_resolve_cache_value(int64_t type,
                              uint64_t parent_id,
                              int64_t var_idx,
                              const ObPLDataType &data_type,
                              const ObRecordType *record_type,
                              const ObIArray<ObSchemaObjVersion> *dep,
                              ObPLResolveCacheValue &value);

  int find_hash_map(ObPLResolveCacheKey &key, ObPLResolveCacheValue &value, bool &is_find);
  int add_hash_map(ObPLResolveCacheKey &key, ObPLResolveCacheValue &value);

  static int gen_reoslve_cache(ObIAllocator &alloc, ObPLResolveCache *&resove_cache);

  common::ObIAllocator &alloc_;

private:
  ObPLResolveCacheMap map_;
};

} // namespace pl end
} // namespace oceanbase end

#endif