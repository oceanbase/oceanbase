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

#ifndef OCEANBASE_TRANSACTION_OB_GTS_LOCAL_CACHE_
#define OCEANBASE_TRANSACTION_OB_GTS_LOCAL_CACHE_

#include "ob_gts_define.h"
#include "ob_gts_task_queue.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase {
namespace transaction {

class ObGTSLocalCache {
public:
  ObGTSLocalCache()
  {
    reset();
  }
  ~ObGTSLocalCache()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int update_gts(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts, bool& update);
  int update_gts_and_check_barrier(
      const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts, bool& is_cross_barrier);
  int update_gts(const int64_t gts, bool& update);
  int update_local_trans_version(const int64_t version, bool& update);
  int get_gts(int64_t& gts) const;
  MonotonicTs get_latest_srr() const
  {
    return MonotonicTs(ATOMIC_LOAD(&latest_srr_.mts_));
  }
  MonotonicTs get_srr() const
  {
    return MonotonicTs(ATOMIC_LOAD(&srr_.mts_));
  }
  int get_gts(const MonotonicTs stc, int64_t& gts, MonotonicTs& receive_gts_ts, bool& need_send_rpc) const;
  int get_local_trans_version(int64_t& local_trans_version) const;
  int get_local_trans_version(
      const MonotonicTs stc, int64_t& local_trans_version, MonotonicTs& receive_gts_ts, bool& need_send_rpc) const;
  int get_srr_and_gts_safe(
      MonotonicTs& srr, int64_t& gts, int64_t& local_trans_version, MonotonicTs& receive_gts_ts) const;
  int get_srr_and_gts_safe(MonotonicTs& srr, int64_t& gts, MonotonicTs& receive_gts_ts) const;
  int update_latest_srr(const MonotonicTs latest_srr);
  int update_base_ts(const int64_t base_ts);

  TO_STRING_KV(K_(srr), K_(gts), K_(local_trans_version), K_(barrier_ts), K_(latest_srr));

private:
  // send rpc request timestamp
  MonotonicTs srr_;
  // The latest local gts value is always less than or equal to the gts leader
  int64_t gts_;
  // Update timing:
  // 1. During the gts background refresh process, gts_ needs to be updated, and local_trans_version_ needs to be
  // updated at the same time
  // 2. After transaction elr, push up local_trans_version, and then wait for gts to push commit version
  // 3. In the process of start_task, local_trans_version_ is pushed up, but gts cache cannot be pushed
  int64_t local_trans_version_;
  // next timestamp
  int64_t barrier_ts_;
  MonotonicTs latest_srr_;
  // receive gts
  MonotonicTs receive_gts_ts_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_RANSACTION_OB_GTS_LOCAL_CACHE_
