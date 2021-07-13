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

#ifndef _OB_COLUMN_STAT_CACHE_H_
#define _OB_COLUMN_STAT_CACHE_H_

#include "lib/allocator/ob_allocator.h"
#include "share/stat/ob_column_stat.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase {
namespace common {
class ObColumnStat;
class ObColumnStatValueHandle;

class ObColumnStatCache : public common::ObKVCache<ObColumnStat::Key, ObColumnStat> {
public:
  ObColumnStatCache();
  ~ObColumnStatCache();

  int get_row(const ObColumnStat::Key& key, ObColumnStatValueHandle& handle);
  int put_row(const ObColumnStat::Key& key, const ObColumnStat& value);
  int put_and_fetch_row(const ObColumnStat::Key& key, const ObColumnStat& value, ObColumnStatValueHandle& handle);
};

class ObColumnStatValueHandle {
public:
  friend class ObColumnStatCache;
  ObColumnStatValueHandle();
  ObColumnStatValueHandle(const ObColumnStatValueHandle& other);
  virtual ~ObColumnStatValueHandle();
  void reset()
  {
    ObColumnStatValueHandle tmp_handle;
    *this = tmp_handle;
  }
  const ObColumnStat* cache_value_;
  TO_STRING_KV(K(cache_value_));

private:
  ObColumnStatCache* cache_;
  ObKVCacheHandle handle_;
};

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_COLUMN_STAT_CACHE_H_ */
