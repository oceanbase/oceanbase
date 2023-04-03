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

#define USING_LOG_PREFIX OBLOG

#include "ob_server_blacklist.h"
#include "share/ob_errno.h"                   // KR

namespace oceanbase
{
namespace logservice
{
BlackList::BlackList() :
    bl_svr_items_(ObModIds::OB_LOG_PART_SVR_LIST_BLACK_LIST, OB_MALLOC_NORMAL_BLOCK_SIZE),
    history_svr_items_(ObModIds::OB_LOG_PART_SVR_LIST_HISTORY_LIST, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
  reset();
}

BlackList::~BlackList()
{
  reset();
}

void BlackList::reset()
{
  bl_svr_items_.reset();
  history_svr_items_.reset();
}

int64_t BlackList::count() const
{
  return bl_svr_items_.count();
}

int BlackList::add(const common::ObAddr &svr,
    const int64_t svr_service_time,
    const int64_t blacklist_survival_time_upper_limit_min,
    const int64_t blacklist_survival_time_penalty_period_min,
    int64_t &survival_time)
{
  int ret = OB_SUCCESS;
  BLSvrItem bl_svr_item;
  bl_svr_item.reset(svr, survival_time, get_timestamp());

  if (OB_FAIL(handle_based_on_history_(blacklist_survival_time_upper_limit_min,
        blacklist_survival_time_penalty_period_min,
        svr_service_time, bl_svr_item))) {
    LOG_ERROR("handle based svr history fail", KR(ret), K(svr_service_time), K(bl_svr_item), K(bl_svr_items_));
  } else if (OB_FAIL(bl_svr_items_.push_back(bl_svr_item))) {
    LOG_ERROR("push_back balcklist item fail", KR(ret), K(bl_svr_item), K(bl_svr_items_));
  } else {
    // succ
    survival_time = bl_svr_item.survival_time_;
  }

  return ret;
}

int BlackList::handle_based_on_history_(
    const int64_t blacklist_survival_time_upper_limit,
    const int64_t blacklist_survival_time_penalty_period,
    const int64_t svr_service_time,
    BLSvrItem &item)
{
  int ret = OB_SUCCESS;
  int64_t found_svr_index = -1;

  if (!exist_in_history_(item.svr_, found_svr_index)) {
    // History not found, add to history
    if (OB_FAIL(history_svr_items_.push_back(item))) {
      LOG_ERROR("push_back balcklist history fail", KR(ret), K(item), K(history_svr_items_));
    }
  } else {
    // Find history, decide surival time based on history records
    BLSvrItem &history_item = history_svr_items_.at(found_svr_index);
    int64_t history_survival_time = history_item.survival_time_;

    // The LS has been in service with the server for too short a time, and when it is added to the blacklist again, the time the svr has been in the blacklist is doubled
    if (svr_service_time < blacklist_survival_time_penalty_period) {
      if (history_survival_time >= blacklist_survival_time_upper_limit) {
        // Start again after one cycle, without updating the survival time
      } else {
        item.survival_time_ = std::max(item.survival_time_,
            std::min(UPDATE_SURVIVAL_TIME_MUTIPLE * history_survival_time, blacklist_survival_time_upper_limit));
      }
    } else {
      // do nothing
    }
    // update history records
    history_item.reset(item.svr_, item.survival_time_, item.access_timestamp_);
  }

  return ret;
}

bool BlackList::exist_in_history_(const common::ObAddr &svr, int64_t &svr_index) const
{
  int ret = OB_SUCCESS;
  bool found_svr = false;
  svr_index = -1;

  // lookup history records
  for (int64_t idx = 0; OB_SUCCESS == ret && !found_svr && idx < history_svr_items_.count(); ++idx) {
    const BLSvrItem &history_item = history_svr_items_.at(idx);
    if (svr == history_item.svr_) {
      found_svr = true;
      svr_index = idx;
    }
  }

  return found_svr;
}

bool BlackList::exist(const common::ObAddr &svr) const
{
  int ret = OB_SUCCESS;
  bool svr_existed = false;

  for (int64_t idx = 0; OB_SUCCESS == ret && !svr_existed && idx < bl_svr_items_.count(); ++idx) {
    svr_existed = (svr == bl_svr_items_.at(idx).svr_);
  }

  return svr_existed;
}

int BlackList::do_white_washing()
{
  int ret = OB_SUCCESS;
  BLSvrArray wash_svr_array;
  wash_svr_array.reset();
  int64_t current_time = get_timestamp();

  // Iterate through the server blacklist in reverse order, removing the servers that should be whitewashed
  for (int64_t svr_idx = bl_svr_items_.count() - 1; OB_SUCCESS == ret && svr_idx >= 0; --svr_idx) {
    BLSvrItem &item = bl_svr_items_.at(svr_idx);
    if ((current_time - item.access_timestamp_) >= item.survival_time_) {
      // Current svr can be whitewashed
      if (OB_FAIL(wash_svr_array.push_back(item))) {
        LOG_ERROR("wash svr array push back fail", KR(ret), K(svr_idx), K(item));
      } else if (OB_FAIL(bl_svr_items_.remove(svr_idx))) {
        LOG_ERROR("remove svr from blacklist fail", KR(ret), K(svr_idx), K(item));
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (wash_svr_array.count() > 0) {
      LOG_INFO("[STAT] [BLACK_LIST] [WASH]",
          "wash_svr_cnt", wash_svr_array.count(), K(wash_svr_array));
    }
  }

  return ret;
}

int BlackList::clear_overdue_history(const ObLSRouterKey &key,
    const int64_t blacklist_history_overdue_time)
{
  int ret = OB_SUCCESS;
  SvrHistoryArray clear_svr_array;
  clear_svr_array.reset();
  int64_t current_time = get_timestamp();

  // Iterate through the history in reverse order and delete
  for (int64_t svr_idx = history_svr_items_.count() - 1; OB_SUCCESS == ret && svr_idx >= 0; --svr_idx) {
    BLSvrItem &item = history_svr_items_.at(svr_idx);
    if ((current_time - item.access_timestamp_) >= blacklist_history_overdue_time) {
      if(OB_FAIL(clear_svr_array.push_back(item))) {
        LOG_ERROR("clear svr array push back fail", KR(ret), K(svr_idx), K(item));
      } else if (OB_FAIL(history_svr_items_.remove(svr_idx))) {
        LOG_ERROR("remove svr from blacklist history fail", KR(ret), K(svr_idx), K(item));
      } else {
        // do nothing
      }
    } else {
      // do nothing
    }
  }

  if (clear_svr_array.count() > 0) {
    // Print the number of blacklisted servers and the servers
    LOG_INFO("STAT" "[BLACK_LIST] [CLEAR]", KR(ret), K(key),
        "clear_svr_cnt", clear_svr_array.count(), K(clear_svr_array));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

