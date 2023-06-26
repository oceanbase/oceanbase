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

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "ob_ls_server_list.h"
#include "lib/container/ob_se_array_iterator.h"  // begin/end
#include <algorithm>                             // std::sort

using namespace oceanbase::common;

namespace oceanbase
{
namespace logservice
{
LSSvrList::LSSvrList() : svr_items_(ObModIds::OB_LOG_PART_SVR_LIST, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
  reset();
}

LSSvrList::~LSSvrList()
{
  reset();
}

LSSvrList& LSSvrList::operator=(const LSSvrList &other)
{
  next_svr_index_ = other.next_svr_index_;
  svr_items_ = other.svr_items_;

  return *this;
}

void LSSvrList::reset()
{
  next_svr_index_ = 0;
  svr_items_.reset();
}

int LSSvrList::add_server_or_update(const common::ObAddr &svr,
    const palf::LSN &start_lsn,
    const palf::LSN &end_lsn,
    const RegionPriority region_prio,
    const bool is_leader)
{
  int ret = OB_SUCCESS;
  bool found_svr = false;
  // TODO support query meta table
  const bool is_located_in_meta_table = true;
  // Default is F replica type
  ReplicaPriority replica_prio = REPLICA_PRIORITY_FULL;

  for (int64_t index = 0; ! found_svr && OB_SUCC(ret) && index < svr_items_.count(); index++) {
    SvrItem &svr_item = svr_items_.at(index);

    if (svr_item.svr_ == svr) {
      found_svr = true;
      // update is_located_in_meta_table/replica_prior if server exist
      svr_item.update(start_lsn, end_lsn, is_located_in_meta_table, region_prio, replica_prio, is_leader);
    }
  }

  if (OB_SUCC(ret) && ! found_svr) {
    SvrItem svr_item;
    svr_item.reset(
        svr,
        start_lsn,
        end_lsn,
        is_located_in_meta_table,
        region_prio,
        replica_prio,
        is_leader);

    if (OB_FAIL(svr_items_.push_back(svr_item))) {
      LOG_ERROR("push_back svr item fail", KR(ret), K(svr_item), K(svr_items_));
    } else {
      // succ
      LOG_INFO("add_server_or_update", K_(next_svr_index), K_(svr_items));
    }
  }

  return ret;
}

int LSSvrList::next_server(const palf::LSN &next_lsn,
    const BlackList &blacklist,
    common::ObAddr &svr)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("next_server-debug start", KR(ret), K(next_lsn), K(svr_items_));

  if (svr_items_.count() <= 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_server_based_on_blacklist_(next_lsn, blacklist, svr))) {
    if (OB_ITER_END != ret) {
      LOG_ERROR("get_next_server_based_on_blacklist_ fail", KR(ret), K(next_lsn), K(svr));
    }
  }

  LOG_DEBUG("next_server-debug end", KR(ret), K(next_lsn), K(svr_items_));
  return ret;
}

int LSSvrList::get_server_array_for_locate_start_lsn(ObIArray<common::ObAddr> &svr_list) const
{
  int ret = OB_SUCCESS;

  // Get the temporary SvrItemArray before getting the server list, then sort by policy
  SvrItemArray sort_svr_items;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < svr_items_.count(); ++idx) {
    const SvrItem &svr_item = svr_items_.at(idx);
    if (OB_FAIL(sort_svr_items.push_back(svr_item))) {
      LOG_ERROR("sort_svr_items push_back fail", KR(ret), K(idx), K(svr_item));
    } else {
      // succ
    }
  }

  // The returned svr list is returned according to the following policy: server service log time in descending order (i.e. new server first, old server second)
  if (OB_SUCC(ret)) {
    sort_by_priority_for_locate_start_log_id_(sort_svr_items);
  }

  // Finally, the list of ordered servers is returned
  for (int64_t idx = 0; OB_SUCC(ret) && idx < sort_svr_items.count(); ++idx) {
    if (OB_FAIL(svr_list.push_back(sort_svr_items.at(idx).svr_))) {
      LOG_ERROR("StartLogIdLocateReq::SvrList push_back fail", KR(ret), K(idx),
          K(svr_list));
    } else {
      // succ
    }
  }

  return ret;
}

void LSSvrList::sort_by_priority_for_locate_start_log_id_(SvrItemArray &svr_items) const
{
  std::sort(svr_items.begin(), svr_items.end(), LocateStartLogIdCompare());
}

void LSSvrList::sort_by_priority()
{
  std::sort(svr_items_.begin(), svr_items_.end(), SvrItemCompare());
}

int LSSvrList::filter_by_svr_blacklist(const ObLogSvrBlacklist &svr_blacklist,
    common::ObIArray<common::ObAddr> &remove_svrs)
{
  int ret = OB_SUCCESS;
  bool has_done = false;
  const int64_t svr_blacklist_cnt = svr_blacklist.count();
  int64_t svr_remove_cnt = 0;

  // 1. Iterate through the blacklist of servers in reverse order, eliminating svr's that are in the blacklist, since they are already sorted by priority, so here the low priority servers are removed in reverse order
  // 2. keep at least one server
  for (int64_t svr_idx = svr_items_.count() - 1; OB_SUCC(ret) && ! has_done && svr_idx >= 0; --svr_idx) {
    const ObAddr &svr = svr_items_.at(svr_idx).svr_;
    const int64_t svr_count = svr_items_.count();

    if (1 == svr_count) {
      // Retain, do not dispose
      has_done = true;
    } else if (svr_remove_cnt >= svr_blacklist_cnt) {
      // Based on the svr blacklist, the server list has been cleaned up
      has_done = true;
    } else {
      // won't request log from svr in blacklist
      if (svr_blacklist.is_exist(svr)) {
        if (OB_FAIL(remove_svrs.push_back(svr))) {
          LOG_ERROR("remove_svrs push_back fail", KR(ret), K(svr));
        } else if (OB_FAIL(svr_items_.remove(svr_idx))) {
          LOG_ERROR("remove svr item fail", KR(ret), K(svr_idx), K(svr), K(svr_items_));
        } else {
          // succ
          ++svr_remove_cnt;
        }
      } else {
        // do nothing
      }
    }
  } // for

  return ret;
}

int LSSvrList::get_leader(ObAddr &addr)
{
  int ret = OB_SUCCESS;
  bool found_svr = false;

  for (int64_t idx = 0; ! found_svr && OB_SUCC(ret) && idx < svr_items_.count(); idx++) {
    SvrItem &svr_item = svr_items_.at(idx);

    if (true == svr_item.is_leader_) {
      found_svr = true;
      addr = svr_item.svr_;
    }
  } // for

  if (! found_svr) {
    ret = OB_NOT_MASTER;
  }

  return ret;
}

bool LSSvrList::need_switch_server(const ObLSRouterKey &key,
    const palf::LSN &next_lsn,
    BlackList &blacklist,
    const common::ObAddr &cur_svr)
{
  int ret = OB_SUCCESS;
  bool svr_found = false;
  int64_t avail_svr_count = svr_items_.count();

  // Note: The blacklist must be white-washed before the lookup, to ensure that higher priority servers can be detected
  // (in the blacklist, but the whitewash condition is already met)
  if (OB_FAIL(blacklist.do_white_washing())) {
    LOG_ERROR("blacklist do while washing fail", KR(ret));
  } else {
    for (int64_t svr_idx = 0; OB_SUCC(ret) && ! svr_found && svr_idx < avail_svr_count; ++svr_idx) {
      bool is_log_served = false;
      bool is_svr_invalid = false;
      SvrItem &svr_item = svr_items_.at(svr_idx);

      // Switch the Server scenario and consider that the Server is always serving
      svr_item.check_and_update_serve_info(true/*is_always_serving*/, next_lsn, is_log_served, is_svr_invalid);

      LOG_TRACE("need_switch_server", K(key), K(next_lsn), K(cur_svr), K(svr_item), K(is_log_served), K(is_svr_invalid));

      if (is_log_served && !is_svr_invalid  && !blacklist.exist(svr_item.svr_)) {
        if (cur_svr == svr_item.svr_) {
          // End of lookup, no higher priority svr, do not need to switch server actively
          break;
        } else {
          svr_found = true;

          // Check the priority of the found svr and the current svr, if they are the same do not switch, otherwise do
          if (OB_FAIL(check_found_svr_priority_(key, svr_idx, avail_svr_count, cur_svr, svr_found))) {
            LOG_ERROR("check_found_svr_priority_ fail", KR(ret), K(key), K(svr_idx), K(avail_svr_count),
                K(cur_svr), K(svr_found), K(svr_items_));
          } else if (! svr_found) {
            // End of search with equal priority
            break;
          } else {
            // There is a higher priority svr, and set next_svr_index_to 0 to ensure that the dispatch cut starts at the beginning of the server list
            next_svr_index_ = 0;
          }
        }
      }
    }
  }

  return svr_found;
}

int LSSvrList::check_found_svr_priority_(
    const ObLSRouterKey &key,
    const int64_t found_svr_idx,
    const int64_t avail_svr_count,
    const common::ObAddr &cur_svr,
    bool &need_switch)
{
  int ret = OB_SUCCESS;
  int64_t cur_svr_idx = -1;
  const SvrItem &found_svr_item = svr_items_.at(found_svr_idx);

  // Get the SvrItem where the current fetch log stream svr is located
  for (int64_t svr_idx = found_svr_idx + 1; OB_SUCC(ret) && (-1 == cur_svr_idx) && svr_idx < avail_svr_count;
      ++svr_idx) {
    const ObAddr &svr = svr_items_.at(svr_idx).svr_;

    if (cur_svr == svr) {
      // Find update idx
      cur_svr_idx = svr_idx;
    }
  } // for

  if (OB_SUCC(ret)) {
    if (cur_svr_idx != -1) {
      SvrItem &cur_svr_item = svr_items_.at(cur_svr_idx);

      // The svr found has the same priority as the current svr, there is no need to switch servers at this point
      if (cur_svr_item.is_priority_equal(found_svr_item)) {
        need_switch = false;
      } else {
        need_switch = true;
      }
    } else {
      // Current server not found, switch
      need_switch = true;
    }

    LOG_INFO("[CHECK_NEED_SWITCH_SERVER] find different server", K(key), K(need_switch),
        K(cur_svr_idx), K(found_svr_idx),
        "cur_svr_item", (-1 == cur_svr_idx) ? to_cstring(cur_svr) : to_cstring(svr_items_.at(cur_svr_idx)),
        K(found_svr_item));
  }

  return ret;
}

int LSSvrList::get_next_server_based_on_blacklist_(const palf::LSN &next_lsn,
    const BlackList &blacklist,
    common::ObAddr &svr)
{
  int ret = OB_SUCCESS;
  bool svr_found = false;
  int64_t avail_svr_count = svr_items_.count();
  LOG_DEBUG("next_server-debug 1", K(next_lsn), K(svr_items_));

  // Iterate through all servers and find the server that serves the target log
  for (int64_t index = 0; OB_SUCC(ret) && ! svr_found && index < avail_svr_count; index++) {
    if (svr_items_.count() <= 0) {
      break;
    } else {
      bool is_log_served = false;
      bool is_svr_invalid = false;
      // Automatically modify next_svr_index_ to move to the next server
      int64_t svr_idx = next_svr_index_++ % svr_items_.count();
      SvrItem &svr_item = svr_items_.at(svr_idx);
      svr_item.check_and_update_serve_info(false/*is_always_serving*/,next_lsn, is_log_served, is_svr_invalid);

      LOG_DEBUG("next_server-debug 2", K(next_lsn), K(svr_items_), K(is_log_served));

      // server does not serve the target log
      if (! is_log_served) {
        if (is_svr_invalid) {
          // server is invalid, remove server from the array
          if (OB_FAIL(svr_items_.remove(svr_idx))) {
            LOG_ERROR("remove svr item fail", KR(ret), K(svr_idx), K(svr_items_));
          } else {
            // next_svr_index_ does not change as elements in the array are deleted
            next_svr_index_--;
          }
        } else {
          // server is valid, move to the next server
        }
      } else {
        if (blacklist.exist(svr_item.svr_)) {
          // Filtering on blacklisted servers
        } else {
          svr_found = true;
          svr = svr_item.svr_;
        }
      }
    }
  }

  if (OB_SUCC(ret) && ! svr_found) {
    ret = OB_ITER_END;
  }

  return ret;
}

/////////////////////////////////// LSNRange ///////////////////////////////
int64_t LSSvrList::LSNRange::to_string(char *buffer, int64_t length) const
{
  int64_t pos = 0;
  (void)databuff_printf(buffer, length, pos, "LSN:{%lu, %lu}",
      start_lsn_.val_, end_lsn_.val_);

  return pos;
}

/////////////////////////////////// SvrItem ///////////////////////////////

void LSSvrList::SvrItem::reset()
{
  svr_.reset();
  log_ranges_.reset();
  is_located_in_meta_table_ = false;
  region_prio_ = REGION_PRIORITY_UNKNOWN;
  replica_prio_ = REPLICA_PRIORITY_UNKNOWN;
  is_leader_ = false;
}

void LSSvrList::SvrItem::reset(const common::ObAddr &svr,
    const palf::LSN &start_lsn,
    const palf::LSN &end_lsn,
    const bool is_located_in_meta_table,
    const RegionPriority region_prio,
    const ReplicaPriority replica_prio,
    const bool is_leader)
{
  svr_ = svr;
  log_ranges_.reset(start_lsn, end_lsn);
  // Initialization priority
  is_located_in_meta_table_ = is_located_in_meta_table;
  region_prio_ = region_prio;
  replica_prio_ = replica_prio;
  is_leader_ = is_leader;
}

void LSSvrList::SvrItem::update(const palf::LSN &start_lsn,
    const palf::LSN &end_lsn,
    const bool is_located_in_meta_table,
    const RegionPriority region_prio,
    const ReplicaPriority replica_prio,
    const bool is_leader)
{
  log_ranges_.reset(start_lsn, end_lsn);
  is_located_in_meta_table_ = is_located_in_meta_table;
  region_prio_ = region_prio;
  replica_prio_ = replica_prio;
  is_leader_ = is_leader;
}

void LSSvrList::SvrItem::check_and_update_serve_info(
    const bool is_always_serving,
    const palf::LSN &lsn,
    bool &is_log_served,
    bool &is_server_invalid)
{
  is_log_served = false;
  is_server_invalid = false;

  is_log_served = log_ranges_.is_log_served(lsn);

  if (! is_log_served) {
    is_server_invalid = true;
  }

  if (is_always_serving) {
    is_log_served = true;
    is_server_invalid = false;
  }
}

bool LSSvrList::SvrItem::is_priority_equal(const SvrItem &svr_item) const
{
  bool bool_ret = false;

  bool_ret = (is_located_in_meta_table_ == svr_item.is_located_in_meta_table_)
    && (region_prio_ == svr_item.region_prio_)
    && (replica_prio_ == svr_item.replica_prio_)
    && (is_leader_ == svr_item.is_leader_);

  return bool_ret;
}

int64_t LSSvrList::SvrItem::to_string(char *buffer, int64_t length) const
{
  int64_t pos = 0;
  (void)databuff_printf(buffer, length, pos, "{server:%s, ranges:[%s], ",
      to_cstring(svr_), to_cstring(log_ranges_));

  (void)databuff_printf(buffer, length, pos,
      "priority:[is_meta_table:%d region:%s, replica:%s, is_leader:%d]}",
      is_located_in_meta_table_,
      print_region_priority(region_prio_),
      print_replica_priority(replica_prio_),
      is_leader_);

  return pos;
}

}
}
