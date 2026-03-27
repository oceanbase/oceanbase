/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_TTL_FILTER_INFO_KV_CACHE_H_
#define OB_STORAGE_TTL_FILTER_INFO_KV_CACHE_H_

#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/truncate_info/ob_truncate_info_kv_cache.h"

namespace oceanbase
{
namespace storage
{

using ObTTLFilterInfoCacheKey = ObMDSInfoCacheKeyImpl<MDSInfoType::TTL_FILTER_INFO>;
using ObTTLFilterInfoCacheValue = ObMDSArrayCacheValue<ObTTLFilterInfo>;
using ObTTLFilterInfoCacheValueHandle = ObMDSArrayValueHandle<ObTTLFilterInfo>;
using ObTTLFilterInfoKVCache = ObMDSInfoKVCache<ObTTLFilterInfoCacheKey, ObTTLFilterInfo>;
using ObTTLFilterInfoKVCacheUtil = ObMDSInfoKVCacheUtil<ObTTLFilterInfoCacheKey, ObTTLFilterInfo>;

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TTL_FILTER_INFO_KV_CACHE_H_