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

#include "lib/oblog/ob_log_module.h"
#include "ob_rl_struct.h"

namespace oceanbase {
namespace share {
DEF_TO_STRING(ObServer2RegionMap)
{
  int64_t pos = 0;
  J_KV("server", addr_, "region", region_);
  return pos;
}

ObServer2RegionMap::ObServer2RegionMap(common::ObAddr &addr, char *region)
{
  if (OB_ISNULL(region)) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", KP(region));
  } else {
    addr_ = addr;
    region_.assign(region);
  }
}

DEF_TO_STRING(ObServerBW)
{
  int64_t pos = 0;
  J_KV("addr", addr_, "max_bw", max_bw_, "cur_bw", cur_bw_);
  return pos;
}

ObServerBW::ObServerBW(common::ObAddr &addr, int64_t max_bw, int64_t cur_bw)
{
  addr_   = addr;
  max_bw_ = max_bw;
  cur_bw_ = cur_bw;
}

OB_SERIALIZE_MEMBER(ObRegionBW, region_, bw_, max_bw_);

DEF_TO_STRING(ObRegionBW)
{
  int64_t pos = 0;
  J_KV("region", region_, "bw", bw_);
  return pos;
}

DEF_TO_STRING(ObRegionBwStat)
{
  int64_t pos = 0;
  J_KV("region", region_, "max_bw", max_bw_);
  return pos;
}

ObRegionBwStat::ObRegionBwStat(char *region, int64_t max_bw)
{
  if (OB_ISNULL(region)) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", KP(region));
  } else {
    max_bw_ = max_bw;
    region_.assign(region);
  }
}

/*
 * Copy contructor.
 */
ObRegionBwStat::ObRegionBwStat(const ObRegionBwStat &copy)
{
  region_.assign(copy.region_.ptr());
  max_bw_ = copy.max_bw_;
}

ObRegionBwStat& ObRegionBwStat::operator=(const ObRegionBwStat &other)
{
  if (this != &other) {
    region_.assign(other.region_.ptr());
    max_bw_ = other.max_bw_;
  }
  return *this;
}

int ObRegionBwStat::assign(const ObRegionBwStat &other)
{
  int ret = common::OB_SUCCESS;
  if (this != &other) {
    region_.assign(other.region_.ptr());
    max_bw_ = other.max_bw_;
  }
  return ret;
}

bool SingleWaitCond::wait(int64_t timeout)
{
  bool ready = ATOMIC_LOAD(&futex_.val());
  if (!ready) {
    ATOMIC_FAA(&n_waiters_, 1);
    futex_.wait(0, timeout);
    ATOMIC_FAA(&n_waiters_, -1);
  } else {
    ATOMIC_STORE(&futex_.val(), 0);
  }
  return ready;
}

void SingleWaitCond::signal()
{
  auto &ready = futex_.val();
  if (!ATOMIC_LOAD(&ready) && ATOMIC_BCAS(&ready, 0, 1)) {
    if (ATOMIC_LOAD(&n_waiters_) > 0) {
      futex_.wake(1);
    }
  }
}

}
}
