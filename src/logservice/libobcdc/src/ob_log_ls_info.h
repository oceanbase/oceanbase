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

#ifndef OCEANBASE_OB_LOG_LS_INFO_H_
#define OCEANBASE_OB_LOG_LS_INFO_H_

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/container/ob_array.h"             // ObArray

#include "ob_easy_hazard_map.h"                 // ObEasyHazardMap
#include "logservice/logfetcher/ob_log_part_serve_info.h" // logfetcher::PartServeInfo
#include "logservice/common_util/ob_log_ls_define.h" // logservice::TenantLSID

#define LS_STAT(level, ls_info, fmt, arg...) \
    do { \
      if (NULL != ls_info) { \
        _OBLOG_LOG(level, "[STAT] [LS_INFO] " fmt " TENANT=%lu LS=%s IS_SYS=%d STATE=%s TRANS_COUNT=%ld " \
            "START_TSTAMP=%s START_FROM_CREATE=%d", \
            ##arg, \
            ls_info->tls_id_.get_tenant_id(), \
            to_cstring(ls_info->tls_id_), \
            ls_info->tls_id_.is_sys_log_stream(), \
            ls_info->print_state(), \
            ls_info->ctx_.sv_.trans_count_, \
            NTS_TO_STR(ls_info->serve_info_.start_serve_timestamp_), \
            ls_info->serve_info_.start_serve_from_create_); \
      } \
    } while (0)

#define LS_ISTAT(ls_info, fmt, arg...) LS_STAT(INFO, ls_info, fmt, ##arg)
#define LS_DSTAT(ls_info, fmt, arg...) LS_STAT(DEBUG, ls_info, fmt, ##arg)

namespace oceanbase
{
namespace libobcdc
{
struct ObLogLSInfo
{
  // PART_STATE_NORMAL: normal state
  // 1. Enter NORMAL state immediately after the partition is added
  // 2. accepting new transaction writes, transaction count allowed to rise
  //
  // PART_STATE_OFFLINE: Offline state
  // 1. If a partition is deleted by a schema, e.g. drop table, drop partition, etc., it enters the OFFLINE state
  // 2. If the partition receives a split completion log and all previous data has been output, it enters the OFFLINE state
  // 3. if the partition receives the OFFLINE log and all previous data has been output, it enters the OFFLINE state
  // 4. This state no longer accepts new transaction writes and the number of transactions no longer changes
  enum
  {
    PART_STATE_INVALID = 0,               // Invalid state (uninitialized state)
    PART_STATE_NORMAL = 1,                // normal service state
    PART_STATE_OFFLINE = 3,               // offline state
    PART_STATE_NOT_SERVED = 4             // Partition not serviced state
  };

  // Joint variables to facilitate atomic operations
  union Ctx
  {
    struct
    {
      int64_t state_:8;         // The lower 8 bits are status variables
      int64_t trans_count_:56;  // The high 56 bits are the number of transactions
    } sv_;

    int64_t iv_;                // Full 64-bit values
  } ctx_;

  logservice::TenantLSID  tls_id_;
  logfetcher::PartServeInfo serve_info_;

  ObLogLSInfo() { reset(); }
  ~ObLogLSInfo() { reset(); }

  void reset();
  const char *print_state() const;
  bool operator<(const ObLogLSInfo &other) const;

  /// Initialize
  /// Set from INVALID state to PART_STATE_NORMAL state if serviced
  /// If not in service, set from INVALID state to PART_STATE_NOT_SERVED state
  int init(const logservice::TenantLSID &tls_id,
      const bool start_serve_from_create,   // Whether to start the service from the creation of a partition
      const int64_t start_tstamp,
      const bool is_served);

  /// Goes to the offline state
  /// Returns true if this is the state transition achieved by this operation, otherwise false
  ///
  /// @param [out] end_trans_count The number of transactions at the end, valid when the return value is true
  bool offline(int64_t &end_trans_count);

  /// Increase the number of transactions when the status is SERVING
  void inc_trans_count_on_serving(bool &is_serving);

  /// Decrement the number of transactions and return whether the partition needs to be deleted
  int dec_trans_count(bool &need_remove);

  bool is_invalid() const { return PART_STATE_INVALID == ctx_.sv_.state_; }
  bool is_serving() const { return is_serving_state_(ctx_.sv_.state_); }
  bool is_offline() const { return PART_STATE_OFFLINE == ctx_.sv_.state_; }
  bool is_not_serving() const { return PART_STATE_NOT_SERVED == ctx_.sv_.state_; }
  int64_t get_trans_count() const { return ctx_.sv_.trans_count_; }

  bool is_serving_state_(const int64_t state) const
  {
    // To facilitate the extension of more states in the future, there may be more than one NORMAL state in service
    return PART_STATE_NORMAL == state;
  }

  TO_STRING_KV("state", print_state(),
      "trans_count", ctx_.sv_.trans_count_,
      K_(tls_id), K_(serve_info));
};

// Print LS information by tenant
struct LSInfoPrinter
{
  uint64_t tenant_id_;
  int64_t serving_ls_count_;
  int64_t offline_ls_count_;
  int64_t not_served_ls_count_;

  explicit LSInfoPrinter(const uint64_t tenant_id) :
      tenant_id_(tenant_id),
      serving_ls_count_(0),
      offline_ls_count_(0),
      not_served_ls_count_(0)
  {}
  bool operator()(const logservice::TenantLSID &tls_id, ObLogLSInfo *ls_info);
};

struct LSInfoScannerByTenant
{
  uint64_t tenant_id_;
  common::ObArray<logservice::TenantLSID> ls_array_;

  explicit LSInfoScannerByTenant(const uint64_t tenant_id) :
      tenant_id_(tenant_id),
      ls_array_()
  {}
  bool operator()(const logservice::TenantLSID &tls_id, ObLogLSInfo *ls_info);
};

typedef ObEasyHazardMap<logservice::TenantLSID, ObLogLSInfo> LSInfoMap;

} // namespace libobcdc
} // namespace oceanbase

#endif
