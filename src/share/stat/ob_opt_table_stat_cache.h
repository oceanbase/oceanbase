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

#ifndef _OB_OPT_TABLE_STAT_CACHE_H_
#define _OB_OPT_TABLE_STAT_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/stat/ob_opt_table_stat.h"

namespace oceanbase {
namespace common {

struct ObOptTableStatHandle;

class ObOptTableStatCache : public common::ObKVCache<ObOptTableStat::Key, ObOptTableStat>
{
public:
  int get_value(const ObOptTableStat::Key &key, ObOptTableStatHandle &handle);
  int put_value(const ObOptTableStat::Key &key, const ObOptTableStat &value);
  int put_and_fetch_value(const ObOptTableStat::Key &key,
                          const ObOptTableStat &value,
                          ObOptTableStatHandle &handle);
};

struct ObOptTableStatHandle
{
  const ObOptTableStat *stat_;
  ObOptTableStatCache *cache_;
  ObKVCacheHandle handle_;

  ObOptTableStatHandle()
    : stat_(nullptr), cache_(nullptr), handle_() {}
  ObOptTableStatHandle(const ObOptTableStatHandle &other)
  {
    if (this != &other) {
      *this = other;
    }
  }
  ~ObOptTableStatHandle()
  {
    stat_ = nullptr;
    cache_ = nullptr;
  }
  void reset()
  {
    stat_ = nullptr;
    cache_ = nullptr;
    handle_.reset();
  }
  TO_STRING_KV(K(stat_));
};

}
}



#endif /* _OB_OPT_TABLE_STAT_CACHE_H_ */
