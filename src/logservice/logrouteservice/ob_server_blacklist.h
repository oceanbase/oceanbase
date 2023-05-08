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

#ifndef OCEANBASE_OB_SERVER_BLACKLIST_H_
#define OCEANBASE_OB_SERVER_BLACKLIST_H_

#include "lib/net/ob_addr.h"                  // ObAddr
#include "logservice/common_util/ob_log_time_utils.h"  // get_timestamp
#include "ob_log_route_key.h"                 // ObLSRouterKey

namespace oceanbase
{
namespace logservice
{
class BlackList
{
public:
  BlackList();
  virtual ~BlackList();

public:
  void reset();
  int64_t count() const;

  /// Add server to blacklist.
  /// Notes:
  /// When adding a server, there is no need to check for duplicate servers, because each time a server is served
  /// for the next log for a partition iteration, it will filter out any servers that are on the blacklist
  /// at that time, so it is unlikely that there is a server that is duplicated on the blacklist
  ////
  ///
  /// @param [in] svr                 blacklisted server
  /// @param [in] svr_service_time    the total time of the current partition of the server service
  /// @param [in] survival_time       survival_time
  ///
  /// @retval OB_SUCCESS              Success: add svr to blacklist
  /// @retval Other error codes       Fail
  int add(const common::ObAddr &svr,
      const int64_t svr_service_time,
      const int64_t blacklist_survival_time_upper_limit_min,
      const int64_t blacklist_survival_time_penalty_period_min,
      int64_t &survival_time);
  /// Find out if the server is on the blacklist
  ///
  /// @retval true    exists
  /// @retval false   does not exist
  bool exist(const common::ObAddr &svr) const;
  /// Whitewash: Iterate through the blacklist to whitewash
  ///
  /// There are two calls to the whitewash function.
  /// 1. before the server that serves the next log for each iteration of the partition
  /// 2. periodically before checking for the presence of a higher level server for the partition
  ///
  /// @param [out] wash_svr_array The server to be cleared
  ///
  /// @retval OB_SUCCESS          Success
  /// @retval Other error codes   Fail
  int do_white_washing();
  /// Clean up your history periodically
  /// 1. Delete history records that have not been update for a period of time
  /// 2. Delete history records whose survial_time has reached the upper threshold
  ////
  /// @param [out] clear_svr_array  The server to be cleared
  ///
  /// @retval OB_SUCCESS            Clear success
  //// @retval Other error codes    Fail
  int clear_overdue_history(const ObLSRouterKey &key,
      const int64_t blacklist_history_overdue_time);

  TO_STRING_KV(K_(bl_svr_items),
      "bl_svr_num", bl_svr_items_.count(),
      K_(history_svr_items),
      "history_svr_num", history_svr_items_.count());

private:
  struct BLSvrItem;
  // Determine the surival time for the server to be blacklisted based on the history
  int handle_based_on_history_(
      const int64_t blacklist_survival_time_upper_limit_min,
      const int64_t blacklist_survival_time_penalty_period_min,
      const int64_t svr_service_time,
      BLSvrItem &item);
  // Find the history, determine if the server exists, if so svr_index return the svr index position, otherwise return -1
  bool exist_in_history_(const common::ObAddr &svr, int64_t &svr_index) const;

private:
  static const int64_t UPDATE_SURVIVAL_TIME_MUTIPLE = 2;

  struct BLSvrItem
  {
    void reset(const common::ObAddr &svr,
      const int64_t survival_time,
      const int64_t timestamp)
    {
      svr_ = svr;
      survival_time_ = survival_time;
      access_timestamp_ = timestamp;
    }

    common::ObAddr svr_;
    int64_t survival_time_;    // lifetime of server in blacklist
    int64_t access_timestamp_; // time when server was added into blacklist

    TO_STRING_KV(K_(svr),
                 K_(survival_time),
                 "access_timestamp", TS_TO_STR(access_timestamp_));
  };
  static const int64_t DEFAULT_SERVER_NUM         = 8;
  static const int64_t DEFAULT_SERVER_HISTORY_NUM = 8;
  typedef common::ObSEArray<BLSvrItem, DEFAULT_SERVER_NUM> BLSvrArray;
  typedef common::ObSEArray<BLSvrItem, DEFAULT_SERVER_HISTORY_NUM> SvrHistoryArray;

private:
  BLSvrArray bl_svr_items_;
  SvrHistoryArray history_svr_items_;
};

} // namespace logservice
} // namespace oceanbase

#endif

