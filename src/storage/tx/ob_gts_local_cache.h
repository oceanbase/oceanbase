/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_GTS_LOCAL_CACHE_
#define OCEANBASE_TRANSACTION_OB_GTS_LOCAL_CACHE_

#include "ob_gts_define.h"
#include "ob_gts_task_queue.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace transaction
{

class ObGTSLocalCache
{
public:
  ObGTSLocalCache() { reset(); }
  ~ObGTSLocalCache() { destroy(); }
  void reset();
  void destroy() { reset(); }
  int update_gts(const MonotonicTs srr,
                 const int64_t gts,
                 const MonotonicTs receive_gts_ts,
                 bool &update);
  int update_gts_and_check_barrier(const MonotonicTs srr,
                                   const int64_t gts,
                                   const MonotonicTs receive_gts_ts);
  int update_gts(const int64_t gts, bool &update);
  int get_gts(int64_t &gts) const;
  MonotonicTs get_latest_srr() const { return MonotonicTs(ATOMIC_LOAD(&latest_srr_.mts_)); }
  MonotonicTs get_srr() const { return MonotonicTs(ATOMIC_LOAD(&srr_.mts_)); }
  int get_gts(const MonotonicTs stc, int64_t &gts, MonotonicTs &receive_gts_ts, bool &need_send_rpc) const;
  int get_srr_and_gts_safe(MonotonicTs &srr, int64_t &gts, MonotonicTs &receive_gts_ts) const;
  int update_latest_srr(const MonotonicTs latest_srr);
  bool no_rpc_on_road() const { return ATOMIC_LOAD(&latest_srr_.mts_) == ATOMIC_LOAD(&srr_.mts_); }

  TO_STRING_KV(K_(srr), K_(gts), K_(latest_srr));
private:
  // send rpc request timestamp
  MonotonicTs srr_;
  // The latest local gts value is always less than or equal to the gts leader
  int64_t gts_;
  MonotonicTs latest_srr_;
  // receive gts
  MonotonicTs receive_gts_ts_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_RANSACTION_OB_GTS_LOCAL_CACHE_
