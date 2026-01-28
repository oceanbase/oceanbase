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