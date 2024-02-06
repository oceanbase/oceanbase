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

#ifndef OCEANBASE_SHARE_OB_TABLET_AUTOINCREMENT_SERVICE_H_
#define OCEANBASE_SHARE_OB_TABLET_AUTOINCREMENT_SERVICE_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/ob_small_allocator.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace share
{
struct ObTabletCacheNode
{
public:
  ObTabletCacheNode() : cache_start_(0), cache_end_(0) {}

  void reset() { cache_start_ = 0; cache_end_ = 0; }
  bool is_valid() { return cache_end_ != 0; }

  TO_STRING_KV(K_(cache_start),
               K_(cache_end));
public:
  uint64_t cache_start_;
  uint64_t cache_end_;
};

class ObTabletAutoincMgr: public common::LinkHashValue<ObTabletAutoincKey>
{
public:
  ObTabletAutoincMgr()
    : mutex_(ObLatchIds::TABLET_AUTO_INCREMENT_MGR_LOCK),
      tablet_id_(),
      next_value_(1),
      last_refresh_ts_(common::ObTimeUtility::current_time()),
      cache_size_(DEFAULT_TABLET_INCREMENT_CACHE_SIZE),
      is_inited_(false)
  {}
  virtual ~ObTabletAutoincMgr()
  {
    destroy();
  }

  int init(const common::ObTabletID &tablet_id, const int64_t cache_size);
  void reset();
  int clear();
  int fetch_interval(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval);
  int fetch_interval_without_cache(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval);
  void destroy() {}

  TO_STRING_KV(K_(tablet_id),
               K_(next_value),
               K_(last_refresh_ts),
               K_(cache_size),
               K_(curr_node),
               K_(prefetch_node),
               K_(is_inited));
private:
  int set_interval(const ObTabletAutoincParam &param, ObTabletCacheInterval &interval);
  int fetch_new_range(const ObTabletAutoincParam &param,
                      const common::ObTabletID &tablet_id,
                      ObTabletCacheNode &node);
  bool prefetch_condition()
  {
    return !prefetch_node_.is_valid() &&
        (next_value_ - curr_node_.cache_start_) * PREFETCH_THRESHOLD > curr_node_.cache_end_ - curr_node_.cache_start_;
  }
  bool is_retryable(int ret)
  {
    return OB_NOT_MASTER == ret || OB_NOT_INIT == ret || OB_TIMEOUT == ret || OB_EAGAIN == ret || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret;
  }
private:
  static const int64_t PREFETCH_THRESHOLD = 4;
  static const int64_t RETRY_INTERVAL = 100 * 1000L; // 100ms
  lib::ObMutex mutex_;
  common::ObTabletID tablet_id_;
  uint64_t next_value_;
  int64_t  last_refresh_ts_; // use this to determine active tablet
  int64_t cache_size_;
  ObTabletCacheNode curr_node_;
  ObTabletCacheNode prefetch_node_;
  bool is_inited_;
};

class ObTabletAutoincrementService
{
public:
  static ObTabletAutoincrementService &get_instance();
  int init();
  void destroy();
  int get_tablet_cache_interval(const uint64_t tenant_id,
                                ObTabletCacheInterval &interval);
  int get_autoinc_seq(const uint64_t tenant_id, const common::ObTabletID &tablet_id, uint64_t &autoinc_seq);
  int clear_tablet_autoinc_cache(const uint64_t tenant_id, const common::ObTabletID &tablet_id);
private:
  int acquire_mgr(const uint64_t tenant_id, const common::ObTabletID &tablet_id, const int64_t init_cache_size, ObTabletAutoincMgr *&autoinc_mgr);
  void release_mgr(ObTabletAutoincMgr *autoinc_mgr);

  ObTabletAutoincrementService();
  ~ObTabletAutoincrementService();

private:
  typedef common::ObLinkHashMap<ObTabletAutoincKey, ObTabletAutoincMgr> TabletAutoincMgrMap;
  const static int INIT_NODE_MUTEX_NUM = 10243L;
  bool is_inited_;
  common::ObSmallAllocator node_allocator_;
  TabletAutoincMgrMap tablet_autoinc_mgr_map_;
  lib::ObMutex init_node_mutexs_[INIT_NODE_MUTEX_NUM];
};


} // end namespace share
} // end namespace oceanbase
#endif
