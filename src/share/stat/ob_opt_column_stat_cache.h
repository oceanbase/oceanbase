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

#ifndef _OB_OPT_COLUMN_STAT_CACHE_H_
#define _OB_OPT_COLUMN_STAT_CACHE_H_

#include "lib/allocator/ob_allocator.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase
{
namespace common
{
class ObOptColumnStatHandle;
class ObOptColumnStatCache : public common::ObKVCache<ObOptColumnStat::Key, ObOptColumnStat>
{
public:
  ObOptColumnStatCache() {}
  ~ObOptColumnStatCache() {}

  /**
   * @param[out] handle  A handle object holding the column statistics.
   *                     the real data is stored in handle.stat_
   */
  int get_row(const ObOptColumnStat::Key &key, ObOptColumnStatHandle &handle);
  int put_row(const ObOptColumnStat::Key &key, const ObOptColumnStat &value);
  int put_and_fetch_row(const ObOptColumnStat::Key &key,
                        const ObOptColumnStat &value,
                        ObOptColumnStatHandle &handle);
};


/**
 * This class is used to hold a Column Statistics object(ObOptColumnStat).
 *
 * An instance of this class keeps a pointer to a Column Statistics object
 * stored in Statistics Cache and prevent it from being released.
 * As long as the instance is alive, the pointer is always valid
 * (even if the object is removed from the cache).
 *
 */
class ObOptColumnStatHandle
{
public:
  friend class ObOptColumnStatCache;
  ObOptColumnStatHandle() : stat_(nullptr), cache_(nullptr) {}
  ~ObOptColumnStatHandle() { stat_ = nullptr; cache_ = nullptr; }
  int assign(const ObOptColumnStatHandle& other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "fail to assign handle");
      this->stat_ = nullptr;
      this->cache_ = nullptr;
    } else {
      this->stat_ = other.stat_;
      this->cache_ = other.cache_;
    }
    return ret;
  }
  void move_from(ObOptColumnStatHandle& other)
  {
    this->stat_ = other.stat_;
    this->cache_ = other.cache_;
    this->handle_.move_from(other.handle_);
    other.reset();
  }
  void reset() { stat_ = nullptr; cache_ = nullptr; handle_.reset(); }
  const ObOptColumnStat *stat_;
  TO_STRING_KV(K(stat_));
private:
  ObOptColumnStatCache *cache_;
  ObKVCacheHandle handle_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_COLUMN_STAT_CACHE_H_ */
