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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_route_struct.h"
#include "lib/ob_define.h"    // OB_INVALID_CLUSTER_ID, OB_INVALID_TENANT_ID
#include "logservice/common_util/ob_log_time_utils.h"

namespace oceanbase
{
namespace logservice
{
ObLSRouterValue::ObLSRouterValue() :
    lock_(),
    ls_svr_list_(),
    blacklist_()
{

}

ObLSRouterValue::~ObLSRouterValue()
{
  reset();
}

int ObLSRouterValue::next_server(const ObLSRouterKey &router_key,
    const palf::LSN &next_lsn,
    common::ObAddr &svr)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  // Note: The blacklist must be cleansed before the next available server can be retrieved from the server list.
  if (OB_FAIL(blacklist_.do_white_washing())) {
    LOG_ERROR("blacklist do while washing fail", KR(ret), K(blacklist_));
  } else if (OB_FAIL(ls_svr_list_.next_server(next_lsn, blacklist_, svr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("LSSvrList next_server failed", KR(ret), K(router_key), K(svr), K(blacklist_));
    }
  } else {}

  return ret;
}

int ObLSRouterValue::get_leader(
    const ObLSRouterKey &router_key,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (OB_FAIL(ls_svr_list_.get_leader(leader))) {
    if (OB_NOT_MASTER != ret) {
      LOG_WARN("LSSvrList get_leader failed", KR(ret), K(router_key), K(leader));
    }
  } else {}

  return ret;
}

bool ObLSRouterValue::need_switch_server(
    const ObLSRouterKey &router_key,
    const palf::LSN &next_lsn,
    const common::ObAddr &cur_svr)
{
  bool bool_ret = false;

  bool_ret = ls_svr_list_.need_switch_server(router_key, next_lsn, blacklist_, cur_svr);

  return bool_ret;
}

int ObLSRouterValue::add_into_blacklist(
    ObLSRouterKey &router_key,
    const common::ObAddr &svr,
    const int64_t svr_service_time,
    const int64_t blacklist_survival_time_sec,
    const int64_t blacklist_survival_time_upper_limit_min,
    const int64_t blacklist_survival_time_penalty_period_min,
    const int64_t blacklist_history_overdue_time,
    const int64_t blacklist_history_clear_interval,
    int64_t &survival_time)
{
  int ret = OB_SUCCESS;
  survival_time = blacklist_survival_time_sec;

  // Cyclical cleaning blacklist history
  if (REACH_TIME_INTERVAL_THREAD_LOCAL(blacklist_history_clear_interval)) {
    if (OB_FAIL(blacklist_.clear_overdue_history(router_key, blacklist_history_overdue_time))) {
      LOG_WARN("blacklist_ clear_overdue_history failed", KR(ret), K(router_key));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(blacklist_.add(svr, svr_service_time, blacklist_survival_time_upper_limit_min,
            blacklist_survival_time_penalty_period_min, survival_time))) {
      LOG_ERROR("blacklist add error", KR(ret), K(router_key), K(svr),
          "svr_service_time", TVAL_TO_STR(svr_service_time),
          "survival_time", TVAL_TO_STR(survival_time));
    } else {
      LOG_INFO("[STAT] [BLACK_LIST] [ADD]", K(router_key), K(svr),
          "svr_service_time", TVAL_TO_STR(svr_service_time),
          "survival_time", TVAL_TO_STR(survival_time),
          "blacklist_cnt", blacklist_.count(), K_(blacklist));
    }
  }

  return ret;
}

int ObLSRouterValue::get_server_array_for_locate_start_lsn(ObIArray<common::ObAddr> &svr_array)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (OB_FAIL(ls_svr_list_.get_server_array_for_locate_start_lsn(svr_array))) {
    LOG_WARN("LSSvrList get_server_array_for_locate_start_lsn failed", KR(ret));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

