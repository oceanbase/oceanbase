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

#include "share/stat/ob_column_stat.h"
#include "share/stat/ob_column_stat_cache.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
namespace common {

ObColumnStatCache::ObColumnStatCache()
{}

ObColumnStatCache::~ObColumnStatCache()
{}

int ObColumnStatCache::get_row(const ObColumnStat::Key& key, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(get(key, handle.cache_value_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to get key from row cache. ", K(key), K(ret));
    }
    EVENT_INC(ObStatEventIds::USER_TAB_COL_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::USER_TAB_COL_STAT_CACHE_HIT);
  }
  return ret;
}

int ObColumnStatCache::put_row(const ObColumnStat::Key& key, const ObColumnStat& value)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    COMMON_LOG(WARN, "put value in cache failed.", K(key), K(value), K(ret));
  }
  return ret;
}

int ObColumnStatCache::put_and_fetch_row(
    const ObColumnStat::Key& key, const ObColumnStat& value, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put_and_fetch(key, value, handle.cache_value_, handle.handle_, true /*overwrite*/))) {
    COMMON_LOG(WARN, "Fail to put kvpair to cache.", K(ret));
  }
  return ret;
}

ObColumnStatValueHandle::ObColumnStatValueHandle() : cache_value_(NULL), cache_(NULL)
{}

ObColumnStatValueHandle::ObColumnStatValueHandle(const ObColumnStatValueHandle& other)
    : cache_value_(NULL), cache_(NULL)
{
  if (this != &other) {
    *this = other;
  }
}

ObColumnStatValueHandle::~ObColumnStatValueHandle()
{
  cache_value_ = NULL;
  cache_ = NULL;
}

}  // end of namespace common
}  // end of namespace oceanbase
