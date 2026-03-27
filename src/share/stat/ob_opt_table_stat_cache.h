/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  ~ObOptTableStatHandle()
  {
    stat_ = nullptr;
    cache_ = nullptr;
  }
  void move_from(ObOptTableStatHandle &other)
  {
    stat_ = other.stat_;
    cache_ = other.cache_;
    handle_.move_from(other.handle_);
    other.reset();
  }
  int assign(const ObOptTableStatHandle &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "fail to assign kv cache handle", K(ret));
      reset();
    } else {
      this->stat_ = other.stat_;
      this->cache_ = other.cache_;
    }
    return ret;
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
