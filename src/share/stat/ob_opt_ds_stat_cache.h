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
  ~ObOptDSStatHandle()
  {
    stat_ = nullptr;
    cache_ = nullptr;
  }
  void move_from(ObOptDSStatHandle &other)
  {
    stat_ = other.stat_;
    cache_ = other.cache_;
    handle_.move_from(other.handle_);
    other.reset();
  }
  int assign(const ObOptDSStatHandle& other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "Fail to assign", K(ret));
      this->reset();
    } else {
      stat_ = other.stat_;
      cache_ = other.cache_;
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



#endif /* _OB_OPT_DS_STAT_CACHE_H_ */
