/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_FETCHER_SWITCH_INFO_H_
#define OCEANBASE_LOG_FETCHER_SWITCH_INFO_H_

#include "logservice/common_util/ob_log_ls_define.h"

namespace oceanbase
{
namespace logfetcher
{
// Switch fetch log stream definition
enum KickOutReason
{
  NONE                         = -1,

  FETCH_LOG_FAIL_ON_RPC        = 1,   // Rpc failure
  FETCH_LOG_FAIL_ON_SERVER     = 2,   // Server failure
  FETCH_LOG_FAIL_IN_DIRECT_MODE = 3,  // Fetch log in direct mode failure

  MISSING_LOG_FETCH_FAIL       = 4,   // Fetch missing redo log failure

  // Feedback
  LAGGED_FOLLOWER              = 5,   // Follow lag behind
  LOG_NOT_IN_THIS_SERVER       = 6,   // This log is not served in the server
  LS_OFFLINED                  = 7,   // LS offline

  // Progress timeout, long time to fetch logs
  PROGRESS_TIMEOUT                      = 8,   // Partition fetch log timeout

  NEED_SWITCH_SERVER           = 9,  // There is a higher priority server that actively switch
  DISCARDED                    = 10,  // Partition is discard

  // Feedback
  ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF        = 11,  //same as ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF
};
const char *print_switch_reason(const KickOutReason reason);

struct KickOutInfo
{
  logservice::TenantLSID tls_id_;
  KickOutReason kick_out_reason_;

  KickOutInfo() : tls_id_(), kick_out_reason_(NONE) {}
  KickOutInfo(const logservice::TenantLSID &tls_id, KickOutReason kick_out_reason) :
    tls_id_(tls_id),
    kick_out_reason_(kick_out_reason)
  {}

  void reset(const logservice::TenantLSID &tls_id, const KickOutReason kick_out_reason)
  {
    tls_id_ = tls_id;
    kick_out_reason_ = kick_out_reason;
  }

  bool need_kick_out() const { return NONE != kick_out_reason_; }

  uint64_t hash() const
  {
    return tls_id_.hash();
  }

  bool operator == (const KickOutInfo &task) const
  {
    return tls_id_ == task.tls_id_;
  }

  TO_STRING_KV(
      K_(tls_id),
      "switch_reason", print_switch_reason(kick_out_reason_));
};

} // namespace logfetcher
} // namespace oceanbase

#endif
