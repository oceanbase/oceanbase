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

#include "ob_gts_define.h"
#include "ob_gts_local_cache.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace common;

namespace transaction
{

void ObGTSLocalCache::reset()
{
  srr_.reset();
  gts_ = 0;
  latest_srr_.reset();
  receive_gts_ts_.reset();
}

//Due to network and other factors, it is impossible to guarantee that srr and gts maintain partial order,
//so the logic of this method:
//srr and gts and the locally saved values are all taken to the maximum value for storage
int ObGTSLocalCache::update_gts(const MonotonicTs srr,
                                const int64_t gts,
                                const MonotonicTs receive_gts_ts,
                                bool &update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)
      || gts >= INT64_MAX/2) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts));
  } else {
    //The update sequence must be receive_gts_ts first, then gts, then srr
    (void)atomic_update(&receive_gts_ts_.mts_, receive_gts_ts.mts_);
    (void)atomic_update(&gts_, gts);
    update = atomic_update(&srr_.mts_, srr.mts_);
  }

  return ret;
}

// While updating srr and gts, it is also necessary to check whether gts crosses the barrier ts
int ObGTSLocalCache::update_gts_and_check_barrier(const MonotonicTs srr,
                                                  const int64_t gts,
                                                  const MonotonicTs receive_gts_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)
      || gts >= INT64_MAX/2) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts));
  } else {
    //The order of update must be receive_gts_ts first, then gts, then srr
    (void)atomic_update(&receive_gts_ts_.mts_, receive_gts_ts.mts_);
    (void)atomic_update(&gts_, gts);
    (void)atomic_update(&srr_.mts_, srr.mts_);
  }

  return ret;
}

int ObGTSLocalCache::update_gts(const int64_t gts, bool &update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(gts < 0) || gts >= INT64_MAX/2) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts));
  } else {
    update = atomic_update(&gts_, gts);
  }

  return ret;
}

int ObGTSLocalCache::get_gts(int64_t &gts) const
{
  int ret = OB_SUCCESS;
  const int64_t tmp_gts = ATOMIC_LOAD(&gts_);
  if (OB_UNLIKELY(0 == tmp_gts)) {
    ret = OB_EAGAIN;
  } else {
    //Here should not add 1
    gts = tmp_gts;
  }
  return ret;
}

int ObGTSLocalCache::get_gts(const MonotonicTs stc,
                             int64_t &gts,
                             MonotonicTs &receive_gts_ts,
                             bool &need_send_rpc) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!stc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(stc));
  } else {
    const int64_t srr = ATOMIC_LOAD(&srr_.mts_);
    // Must get gts first, then receive_gts_ts
    const int64_t tmp_gts = ATOMIC_LOAD(&gts_);
    const int64_t tmp_receive_gts_ts = ATOMIC_LOAD(&receive_gts_ts_.mts_);
    if (0 == tmp_gts) {
      ret = OB_EAGAIN;
      need_send_rpc = true;
    } else if (stc.mts_ > srr) {
      ret = OB_EAGAIN;
      need_send_rpc = (stc.mts_ > ATOMIC_LOAD(&latest_srr_.mts_));
    } else {
      //Here should not add 1
      gts = tmp_gts;
      receive_gts_ts = MonotonicTs(tmp_receive_gts_ts);
      need_send_rpc = false;
    }
  }

  return ret;
}

int ObGTSLocalCache::get_srr_and_gts_safe(MonotonicTs &srr,
                                          int64_t &gts,
                                          MonotonicTs &receive_gts_ts) const
{
  // must set srr before gts
  srr.mts_ = ATOMIC_LOAD(&srr_.mts_);
  //Here should not add 1
  gts = ATOMIC_LOAD(&gts_);
  receive_gts_ts.mts_ = ATOMIC_LOAD(&receive_gts_ts_.mts_);
  return OB_SUCCESS;
}

int ObGTSLocalCache::update_latest_srr(const MonotonicTs latest_srr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!latest_srr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(latest_srr));
  } else {
    (void)atomic_update(&latest_srr_.mts_, latest_srr.mts_);
  }

  return ret;
}

} // transaction
} // oceanbase
