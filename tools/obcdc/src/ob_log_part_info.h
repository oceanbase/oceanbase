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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_INFO_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_INFO_H_

#include "common/ob_partition_key.h"            // ObPartitionKey
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/container/ob_array.h"             // ObArray

#include "ob_easy_hazard_map.h"                 // ObEasyHazardMap
#include "ob_log_part_serve_info.h"             // PartServeInfo

#define PART_STAT(level, part_info, fmt, arg...) \
    do { \
      if (NULL != part_info) { \
        _OBLOG_LOG(level, "[STAT] [PART_INFO] " fmt " TENANT=%lu PART=%s IS_PG=%d STATE=%s TRANS_COUNT=%ld " \
            "START_TSTAMP=%s START_FROM_CREATE=%d", \
            ##arg, \
            part_info->pkey_.get_tenant_id(), \
            to_cstring(part_info->pkey_), \
            part_info->pkey_.is_pg(), \
            part_info->print_state(), part_info->ctx_.sv_.trans_count_, \
            TS_TO_STR(part_info->serve_info_.start_serve_timestamp_), \
            part_info->serve_info_.start_serve_from_create_); \
      } \
    } while (0)

#define PART_ISTAT(part_info, fmt, arg...) PART_STAT(INFO, part_info, fmt, ##arg)
#define PART_DSTAT(part_info, fmt, arg...) PART_STAT(DEBUG, part_info, fmt, ##arg)

#define REVERT_PART_INFO(info, ret) \
    do { \
      if (NULL != info && NULL != map_) { \
        int revert_ret = map_->revert(info); \
        if (OB_SUCCESS != revert_ret) { \
          LOG_ERROR("revert PartInfo fail", K(revert_ret), K(info)); \
          ret = OB_SUCCESS == ret ? revert_ret : ret; \
        } else { \
          info = NULL; \
        } \
      } \
    } while (0)


namespace oceanbase
{
namespace liboblog
{

struct ObLogPartInfo
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

  common::ObPartitionKey  pkey_;
  PartServeInfo           serve_info_;

  ObLogPartInfo() { reset(); }
  ~ObLogPartInfo() { reset(); }

  void reset();
  const char *print_state() const;
  bool operator<(const ObLogPartInfo &other) const;

  /// Initialize
  /// Set from INVALID state to PART_STATE_NORMAL state if serviced
  /// If not in service, set from INVALID state to PART_STATE_NOT_SERVED state
  int init(const common::ObPartitionKey &pkey,
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
      K_(pkey), K_(serve_info));
};

// Print partition information by tenant
struct PartInfoPrinter
{
  uint64_t tenant_id_;
  int64_t serving_part_count_;
  int64_t offline_part_count_;
  int64_t not_served_part_count_;

  explicit PartInfoPrinter(const uint64_t tenant_id) :
      tenant_id_(tenant_id),
      serving_part_count_(0),
      offline_part_count_(0),
      not_served_part_count_(0)
  {}
  bool operator()(const common::ObPartitionKey &pkey, ObLogPartInfo *part_info);
};

struct PartInfoScannerByTenant
{
  uint64_t tenant_id_;
  common::ObArray<common::ObPartitionKey> pkey_array_;

  explicit PartInfoScannerByTenant(const uint64_t tenant_id) :
      tenant_id_(tenant_id),
      pkey_array_()
  {}
  bool operator()(const common::ObPartitionKey &pkey, ObLogPartInfo *part_info);
};

struct PartInfoScannerByTableID
{
  uint64_t table_id_;
  common::ObArray<common::ObPartitionKey> pkey_array_;

  explicit PartInfoScannerByTableID(const uint64_t table_id);
  bool operator()(const common::ObPartitionKey &pkey, ObLogPartInfo *part_info);
};

typedef ObEasyHazardMap<common::ObPartitionKey, ObLogPartInfo> PartInfoMap;
}
}

#endif
