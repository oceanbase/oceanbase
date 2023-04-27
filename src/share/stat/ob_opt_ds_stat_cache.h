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

#ifndef _OB_OPT_DS_STAT_CACHE_H_
#define _OB_OPT_DS_STAT_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/stat/ob_opt_ds_stat.h"

namespace oceanbase {
namespace common {

struct ObOptDSStatHandle;

class ObOptDSStatCache : public common::ObKVCache<ObOptDSStat::Key, ObOptDSStat>
{
public:
  int get_value(const ObOptDSStat::Key &key, ObOptDSStatHandle &handle);
  int put_value(const ObOptDSStat::Key &key, const ObOptDSStat &value);
  int put_and_fetch_value(const ObOptDSStat::Key &key,
                          const ObOptDSStat &value,
                          ObOptDSStatHandle &handle);
};

struct ObOptDSStatHandle
{
  const ObOptDSStat *stat_;
  ObOptDSStatCache *cache_;
  ObKVCacheHandle handle_;

  ObOptDSStatHandle()
    : stat_(nullptr), cache_(nullptr), handle_() {}
  ObOptDSStatHandle(const ObOptDSStatHandle &other)
  {
    if (this != &other) {
      *this = other;
    }
  }
  ~ObOptDSStatHandle()
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



#endif /* _OB_OPT_DS_STAT_CACHE_H_ */
