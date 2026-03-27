/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_OPT_EXTERNAL_TABLE_STAT_CACHE_H_
#define _OB_OPT_EXTERNAL_TABLE_STAT_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/stat/ob_opt_external_table_stat.h"

namespace oceanbase {
namespace share {

struct ObOptExternalTableStatHandle;

class ObOptExternalTableStatCache
    : public common::ObKVCache<ObOptExternalTableStat::Key,
                               ObOptExternalTableStat> {
public:
  int get_value(const ObOptExternalTableStat::Key &key,
                ObOptExternalTableStatHandle &handle);
  int put_value(const ObOptExternalTableStat::Key &key,
                const ObOptExternalTableStat &value);
  int put_and_fetch_value(const ObOptExternalTableStat::Key &key,
                          const ObOptExternalTableStat &value,
                          ObOptExternalTableStatHandle &handle);
};

struct ObOptExternalTableStatHandle {
  const ObOptExternalTableStat *stat_;
  ObOptExternalTableStatCache *cache_;
  common::ObKVCacheHandle handle_;

  ObOptExternalTableStatHandle() : stat_(nullptr), cache_(nullptr), handle_() {}
  ~ObOptExternalTableStatHandle() {
    stat_ = nullptr;
    cache_ = nullptr;
  }
  void move_from(ObOptExternalTableStatHandle &other) {
    stat_ = other.stat_;
    cache_ = other.cache_;
    handle_.move_from(other.handle_);
    other.reset();
  }
  int assign(const ObOptExternalTableStatHandle &other) {
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
  void reset() {
    stat_ = nullptr;
    cache_ = nullptr;
    handle_.reset();
  }
  TO_STRING_KV(K(stat_));
};

} // namespace share
} // namespace oceanbase

#endif /* _OB_OPT_EXTERNAL_TABLE_STAT_CACHE_H_ */