/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_TABLET_LS_MAP
#define OCEANBASE_SHARE_OB_TABLET_LS_MAP

#include "share/location_cache/ob_location_struct.h" // ObTabletLSKey, ObTabletLSCache

namespace oceanbase
{
namespace common
{
class ObQSyncLock;
}
namespace share
{
class ObTabletLSService;

class ObTabletLSMap
{
public:
  ObTabletLSMap()
      : is_inited_(false),
        size_(0),
        ls_buckets_(nullptr),
        buckets_lock_(nullptr)
  {
    destroy();
  }
  ~ObTabletLSMap() { destroy(); }
  void destroy();
  int init();
  int update(const ObTabletLSCache &tablet_ls_cache);
  int update_limit_by_threshold(
      const int64_t threshold,
      const ObTabletLSKey &key,
      const ObTabletLSCache &tablet_ls_cache);
  int get(const ObTabletLSKey &key, ObTabletLSCache &tablet_ls_cache);
  int get_all(common::ObIArray<ObTabletLSCache> &cache_array);
  int del(const ObTabletLSKey &key);
  int64_t size() const { return size_; }

private:
  void try_update_access_ts_(ObTabletLSCache *cache_ptr);

private:
  static const int64_t MAX_ACCESS_TIME_UPDATE_THRESHOLD = 10000000; // 10s
  static const int64_t BUCKETS_CNT = 1 << 16;

private:
  bool is_inited_;
  int64_t size_;
  ObTabletLSCache **ls_buckets_;
  common::ObQSyncLock *buckets_lock_;
};

} // end namespace share
} // end namespace oceanbase
#endif
