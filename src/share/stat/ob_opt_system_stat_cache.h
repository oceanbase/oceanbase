/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_OPT_SYSTEM_STAT_CACHE_H_
#define _OB_OPT_SYSTEM_STAT_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/stat/ob_opt_system_stat.h"

namespace oceanbase {
namespace common {

struct ObOptSystemStatHandle;

class ObOptSystemStatCache : public common::ObKVCache<ObOptSystemStat::Key, ObOptSystemStat>
{
public:
  int get_value(const ObOptSystemStat::Key &key, ObOptSystemStatHandle &handle);
  int put_value(const ObOptSystemStat::Key &key, const ObOptSystemStat &value);
  int put_and_fetch_value(const ObOptSystemStat::Key &key,
                          const ObOptSystemStat &value,
                          ObOptSystemStatHandle &handle);
};

struct ObOptSystemStatHandle
{
  const ObOptSystemStat *stat_;
  ObOptSystemStatCache *cache_;
  ObKVCacheHandle handle_;

  ObOptSystemStatHandle()
    : stat_(nullptr), cache_(nullptr), handle_() {}
  ~ObOptSystemStatHandle()
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



#endif /* _OB_OPT_SYSTEM_STAT_CACHE_H_ */
