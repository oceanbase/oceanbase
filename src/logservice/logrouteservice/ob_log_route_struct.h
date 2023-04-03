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

#ifndef OCEANBASE_LOG_ROUTE_STRUCT_H_
#define OCEANBASE_LOG_ROUTE_STRUCT_H_

#include "ob_log_route_key.h"     // ObLSRouterKey
#include "ob_ls_server_list.h"    // LSSvrList
#include "ob_server_blacklist.h"  // BlackList

namespace oceanbase
{
namespace logservice
{
class ObLSRouterValue
{
public:
  ObLSRouterValue();
  ~ObLSRouterValue();
  void reset()
  {
    ls_svr_list_.reset();
    blacklist_.reset();
  }

public:
  int next_server(
      const ObLSRouterKey &router_key,
      const palf::LSN &next_lsn,
      common::ObAddr &addr);

  int get_leader(
      const ObLSRouterKey &router_key,
      common::ObAddr &leader);

  bool need_switch_server(
      const ObLSRouterKey &router_key,
      const palf::LSN &next_lsn,
      const common::ObAddr &cur_svr);

  int add_into_blacklist(
      ObLSRouterKey &router_key,
      const common::ObAddr &svr,
      const int64_t svr_service_time,
      const int64_t blacklist_survival_time_sec,
      const int64_t blacklist_survival_time_upper_limit_min,
      const int64_t blacklist_survival_time_penalty_period_min,
      const int64_t blacklist_history_overdue_time,
      const int64_t blacklist_history_clear_interval,
      int64_t &survival_time);

  int get_server_array_for_locate_start_lsn(ObIArray<common::ObAddr> &svr_array);

  common::ObByteLock &get_lock() { return lock_; }
  LSSvrList &get_ls_svr_list() { return ls_svr_list_; }

  int64_t get_server_count() const { return ls_svr_list_.count(); }

  TO_STRING_KV(K_(ls_svr_list),
      K_(blacklist));

private:
  common::ObByteLock lock_;
  LSSvrList ls_svr_list_;
  BlackList blacklist_;
};

// Router Asynchronous Task
struct ObLSRouterAsynTask
{
  ObLSRouterAsynTask() { reset(); }
  ~ObLSRouterAsynTask() { reset(); }

  void reset()
  {
    router_key_.reset();
  }

  ObLSRouterKey router_key_;

  TO_STRING_KV(K_(router_key));
};

} // namespace logservice
} // namespace oceanbase

#endif

