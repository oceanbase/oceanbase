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

#ifndef OCEANBASE_SHARE_OB_RL_STRUCT_H
#define OCEANBASE_SHARE_OB_RL_STRUCT_H

#include <atomic>
#include "lib/ob_define.h"
#include "lib/lock/ob_futex.h"
#include "lib/net/ob_addr.h"
#include "common/ob_region.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace share {

class SingleWaitCond
{
public:
  SingleWaitCond(): n_waiters_(0), futex_() {}
  ~SingleWaitCond() {}
  void signal();
  bool wait(int64_t timeout);
private:
  int32_t n_waiters_;
  lib::ObFutex futex_;
};

typedef common::ObSEArray<common::ObAddr, 256> ObServerSEArray;

struct ObServer2RegionMap
{
public:
  ObServer2RegionMap() {};
  ObServer2RegionMap(common::ObAddr &addr, char *region);
  common::ObAddr addr_;
  common::ObRegion region_;
  DECLARE_TO_STRING;
};
typedef common::ObSEArray<ObServer2RegionMap, 256>  ObServer2RegionMapSEArray;

struct ObServerBW
{
public:
  ObServerBW() {}
  ObServerBW(common::ObAddr &addr, int64_t max_bw, int64_t cur_bw);
  common::ObAddr addr_;
  int64_t max_bw_;
  int64_t cur_bw_;
  int pending_;
  DECLARE_TO_STRING;
};
typedef common::ObSEArray<ObServerBW, 64>  ObServerBwSEArray;

struct ObRegionBW
{
  OB_UNIS_VERSION(1);

public:
  ObRegionBW() {}
  ObRegionBW(const common::ObRegion &region, int64_t bw, int64_t max_bw): region_(region), bw_(bw), max_bw_(max_bw) {}
  ~ObRegionBW() {}
  common::ObRegion region_;
  int64_t bw_;
  int64_t max_bw_;
  DECLARE_TO_STRING;
};
typedef common::ObSEArray<ObRegionBW, 16> ObRegionBwSEArray;

struct ObRegionBwStat
{
public:
  ObRegionBwStat() {};
  ObRegionBwStat(char *region, int64_t max_bw_);
  ObRegionBwStat(const ObRegionBwStat& copy);
  ~ObRegionBwStat() {}
  ObRegionBwStat& operator=(const ObRegionBwStat&);
  int assign(const ObRegionBwStat &other);

public:
  common::ObRegion region_;
  int64_t max_bw_;
  int64_t local_server_max_bw_;
  int64_t local_server_cur_bw_;
  int64_t need_balance_;
  ObServerBwSEArray server_bw_list_;
  common::ObLatch server_bw_list_spinlock_;
  DECLARE_TO_STRING;
};
typedef common::ObSEArray<ObRegionBwStat, 16>  ObRegionBwStatSEArray;

} // namespace share
} // namespace oceanbase

#endif
