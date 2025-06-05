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

#define USING_LOG_PREFIX PL

#include "ob_pl_resolve_cache.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{

namespace pl
{

int ObPLResolveCacheKey::hash(uint64_t &hash_val) const
{
  hash_val = murmurhash(&type_, sizeof(uint64_t), 0);
  hash_val = murmurhash(&parent_id_, sizeof(uint64_t), hash_val);
  hash_val = name_.hash(hash_val);
  return OB_SUCCESS;
}

bool ObPLResolveCacheKey::operator==(const ObPLResolveCacheKey &other) const
{
  bool cmp_ret = type_ == other.type_ &&
                 parent_id_ == other.parent_id_ &&
                 name_ == other.name_;
  return cmp_ret;
}

int ObPLResolveCache::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(map_.create(64, "RESOLVE_CACHE", ObModIds::OB_HASH_NODE, MTL_ID()))) {
    LOG_WARN("fail to create hash map", K(ret));
  }

  return ret;
}

int ObPLResolveCache::construct_key(ObPLResolveCacheType type,
                                    uint64_t parent_id,
                                    const ObString &name,
                                    ObPLResolveCacheKey &hash_key)
{
  int ret = OB_SUCCESS;

  hash_key.type_ = type;
  hash_key.parent_id_ = parent_id;
  OZ (common::ob_write_string(alloc_, name, hash_key.name_));

  return ret;
}

int ObPLResolveCache::gen_resolve_cache_value(int64_t type,
                                              uint64_t parent_id,
                                              int64_t var_idx,
                                              const ObPLDataType &data_type,
                                              const ObRecordType *record_type,
                                              const ObIArray<ObSchemaObjVersion> *dep,
                                              ObPLResolveCacheValue &value)
{
  int ret = OB_SUCCESS;

  value.type_value_ = type;
  value.parent_id_value_ = parent_id;
  value.var_idx_value_ = var_idx;
  OZ (value.data_type_value_.deep_copy(alloc_, data_type));
  OX (value.record_type_value_ = const_cast<ObRecordType *>(record_type));
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(dep) && i < dep->count(); ++i) {
    OZ (value.dependency_objects_.push_back(dep->at(i)));
  }

  return ret;
}

int ObPLResolveCache::find_hash_map(ObPLResolveCacheKey &key, ObPLResolveCacheValue &value, bool &is_find)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(map_.get_refactored(key, value))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to find in hash map", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    is_find = true;
  }

  return ret;
}

int ObPLResolveCache::add_hash_map(ObPLResolveCacheKey &key, ObPLResolveCacheValue &value)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(map_.set_refactored(key, value))) {
    LOG_WARN("fail to add value to hashmap", K(ret));
  }

  return ret;
}

int ObPLResolveCache::gen_reoslve_cache(ObIAllocator &alloc, ObPLResolveCache *&resove_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(resove_cache =
      reinterpret_cast<ObPLResolveCache*>(alloc.alloc(sizeof(ObPLResolveCache))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for resolve cache!", K(ret));
  } else {
    resove_cache = new(resove_cache)ObPLResolveCache(alloc);
    if (OB_FAIL(resove_cache->init())) {
      LOG_WARN("fail to init resolve cache", K(ret));
    }
  }
  return ret;
}

}
}