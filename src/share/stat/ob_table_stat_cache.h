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

#ifndef _OB_TABLE_STAT_CACHE_H_
#define _OB_TABLE_STAT_CACHE_H_

#include "ob_table_stat.h"
#include "lib/allocator/ob_allocator.h"
#include "common/ob_partition_key.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase {
namespace common {
class ObTableStat;
class ObTableStatValueHandle;

class ObTableStatCache : public common::ObKVCache<ObTableStat::Key, ObTableStat> {
public:
  ObTableStatCache();
  ~ObTableStatCache();

  int get_row(const ObTableStat::Key& key, ObTableStatValueHandle& handle);
  int put_row(const ObTableStat::Key& key, const ObTableStat& value);
  int put_and_fetch_row(const ObTableStat::Key& key, const ObTableStat& value, ObTableStatValueHandle& handle);
};

struct ObTableStatValueHandle {
  const ObTableStat* cache_value_;
  ObTableStatCache* cache_;
  ObKVCacheHandle handle_;

  ObTableStatValueHandle();
  ObTableStatValueHandle(const ObTableStatValueHandle& other);
  virtual ~ObTableStatValueHandle();
  void reset();
  TO_STRING_KV(K(cache_value_));
};
} /* namespace common */
} /* namespace oceanbase */

#endif /* _OB_TABLE_STAT_CACHE_H_ */
