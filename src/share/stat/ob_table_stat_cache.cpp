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

#include "ob_table_stat.h"
#include "ob_table_stat_cache.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
namespace common {

ObTableStatCache::ObTableStatCache()
{}

ObTableStatCache::~ObTableStatCache()
{}

int ObTableStatCache::get_row(const ObTableStat::Key& key, ObTableStatValueHandle& handle)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid partition key.", K(ret), K(key));
  } else if (OB_FAIL(get(key, handle.cache_value_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to get key from cache.", K(ret), K(key));
    }
    EVENT_INC(ObStatEventIds::USER_TABLE_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::USER_TABLE_STAT_CACHE_HIT);
  }
  return ret;
}

int ObTableStatCache::put_row(const ObTableStat::Key& key, const ObTableStat& value)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid partition key.", K(ret), K(key));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    COMMON_LOG(WARN, "Fail to put value to cache.", K(ret));
  }
  return ret;
}

int ObTableStatCache::put_and_fetch_row(
    const ObTableStat::Key& key, const ObTableStat& value, ObTableStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid partition key.", K(ret), K(key));
  } else if (OB_FAIL(put_and_fetch(key, value, handle.cache_value_, handle.handle_, true /*overwrite*/))) {
    COMMON_LOG(WARN, "Fail to put kvpair to cache.", K(ret), K(key));
  }
  return ret;
}

ObTableStatValueHandle::ObTableStatValueHandle() : cache_value_(NULL), cache_(NULL), handle_()
{}

ObTableStatValueHandle::ObTableStatValueHandle(const ObTableStatValueHandle& other)
    : cache_value_(NULL), cache_(NULL), handle_()
{
  if (this != &other) {
    this->cache_value_ = other.cache_value_;
    this->cache_ = other.cache_;
    this->handle_ = other.handle_;
  }
}

ObTableStatValueHandle::~ObTableStatValueHandle()
{
  cache_value_ = NULL;
  cache_ = NULL;
}

void ObTableStatValueHandle::reset()
{
  cache_value_ = NULL;
  cache_ = NULL;
  handle_.reset();
}
}  // namespace common
}  // namespace oceanbase
