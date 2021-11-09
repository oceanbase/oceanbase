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

#include "ob_log_part_svr_list.h"
#include <algorithm>                             // std::sort

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
PartSvrList::PartSvrList() : svr_items_(ObModIds::OB_LOG_PART_SVR_LIST, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
  reset();
}

PartSvrList::~PartSvrList()
{
  reset();
}

void PartSvrList::reset()
{
  next_svr_index_ = 0;
  svr_items_.reset();
}

int PartSvrList::add_server_or_update(const common::ObAddr &svr,
    const uint64_t start_log_id,
    const uint64_t end_log_id,
    const bool is_located_in_meta_table,
    const RegionPriority region_prio,
    const ReplicaPriority replica_prio,
    const bool is_leader,
    const bool is_log_range_valid /* = true */)
{
  int ret = OB_SUCCESS;
  bool found_svr = false;

  for (int64_t index = 0; ! found_svr && OB_SUCCESS == ret && index < svr_items_.count(); index++) {
    SvrItem &svr_item = svr_items_.at(index);
    if (svr_item.svr_ == svr) {
      found_svr = true;
      // Updateis_located_in_meta_table, replica_prio, leader info if server exist
      svr_item.reset(is_located_in_meta_table, replica_prio, is_leader);

      // Update if log range is valid
      if (is_log_range_valid) {
        if (OB_FAIL(svr_item.add_range(start_log_id, end_log_id))) {
          LOG_ERROR("server item add range fail", KR(ret), K(start_log_id),
              K(end_log_id), K(svr_item));
        }
      }
    }
  }

  if (OB_SUCCESS == ret && ! found_svr) {
    SvrItem svr_item;
    svr_item.reset(
        svr,
        start_log_id,
        end_log_id,
        is_located_in_meta_table,
        region_prio,
        replica_prio,
        is_leader);

    if (OB_FAIL(svr_items_.push_back(svr_item))) {
      LOG_ERROR("push_back svr item fail", KR(ret), K(svr_item), K(svr_items_));
    } else {
      // succ
    }
  }

  return ret;
}

bool PartSvrList::exist(const common::ObAddr &svr, int64_t &svr_index) const
{
  bool svr_existed = false;

  int64_t index = 0;
  for (; !svr_existed && index < svr_items_.count(); index++) {
    svr_existed = (svr_items_.at(index).svr_ == svr);
  }

  if (svr_existed) {
    svr_index = index - 1;
  } else {
    svr_index = -1;
  }

  return svr_existed;
}

int PartSvrList::next_server(const uint64_t next_log_id,
    const IBlackList &blacklist,
    common::ObAddr &svr)
{
  int ret = OB_SUCCESS;

  if (svr_items_.count() <= 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_server_based_on_blacklist_(next_log_id, blacklist, svr))) {
    if (OB_ITER_END != ret) {
      LOG_ERROR("get_next_server_based_on_blacklist_ fail", KR(ret), K(next_log_id));
    }
  }

  return ret;
}

int PartSvrList::get_server_array_for_locate_start_log_id(StartLogIdLocateReq::SvrList &svr_list) const
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
    // Generate a SvrItem
    StartLogIdLocateReq::SvrItem start_log_id_svr_item;
    start_log_id_svr_item.reset(sort_svr_items.at(idx).svr_);

    if (OB_FAIL(svr_list.push_back(start_log_id_svr_item))) {
      LOG_ERROR("StartLogIdLocateReq::SvrList push_back fail", KR(ret), K(idx),
          K(start_log_id_svr_item), K(svr_list));
    } else {
      // succ
    }
  }

  return ret;
}

void PartSvrList::sort_by_priority_for_locate_start_log_id_(SvrItemArray &svr_items) const
{
  std::sort(svr_items.begin(), svr_items.end(), LocateStartLogIdCompare());
}

void PartSvrList::sort_by_priority()
{
  std::sort(svr_items_.begin(), svr_items_.end(), SvrItemCompare());
}

int PartSvrList::filter_by_svr_blacklist(const ObLogSvrBlacklist &svr_blacklist,
    common::ObArray<common::ObAddr> &remove_svrs)
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

bool PartSvrList::need_switch_server(const uint64_t next_log_id,
    IBlackList &blacklist,
    const common::ObPartitionKey &pkey,
    const common::ObAddr &cur_svr)
{
  int ret = OB_SUCCESS;
  bool svr_found = false;
  int64_t avail_svr_count = svr_items_.count();

  IBlackList::BLSvrArray wash_svr_array;
  wash_svr_array.reset();

  // Note: The blacklist must be white-washed before the lookup, to ensure that higher priority servers can be detected
  // (in the blacklist, but the whitewash condition is already met)
  if (OB_FAIL(blacklist.do_white_washing(wash_svr_array))) {
    LOG_ERROR("blacklist do while washing fail", KR(ret), K(pkey));
  } else {
    if (wash_svr_array.count() > 0) {
      LOG_INFO("[STAT] [BLACK_LIST] [WASH]", KR(ret), K(pkey),
          "wash_svr_cnt", wash_svr_array.count(), K(wash_svr_array));

    }

    for (int64_t svr_idx = 0; OB_SUCC(ret) && ! svr_found && svr_idx < avail_svr_count; ++svr_idx) {
      bool is_log_served = false;
      bool is_svr_invalid = false;
      SvrItem &svr_item = svr_items_.at(svr_idx);

      svr_item.check_and_update_serve_info(next_log_id, is_log_served, is_svr_invalid);

      if (is_log_served && !is_svr_invalid && !blacklist.exist(svr_item.svr_)) {
        if (cur_svr == svr_item.svr_) {
          // End of lookup, no higher priority svr, no active flow cut required
          break;
        } else {
          svr_found = true;

          // Check the priority of the found svr and the current svr, if they are the same do not switch, otherwise do
          if (OB_FAIL(check_found_svr_priority_(pkey, svr_idx, avail_svr_count, cur_svr, svr_found))) {
            LOG_ERROR("check_found_svr_priority_ fail", KR(ret), K(pkey), K(svr_idx), K(avail_svr_count),
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

int PartSvrList::check_found_svr_priority_(const common::ObPartitionKey &pkey,
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

    LOG_INFO("[CHECK_NEED_SWITCH_SERVER] find different server", K(pkey), K(need_switch),
        K(cur_svr_idx), K(found_svr_idx),
        "cur_svr_item", (-1 == cur_svr_idx) ? to_cstring(cur_svr) : to_cstring(svr_items_.at(cur_svr_idx)),
        K(found_svr_item));
  }

  return ret;
}

int PartSvrList::get_next_server_based_on_blacklist_(const uint64_t next_log_id,
    const IBlackList &blacklist,
    common::ObAddr &svr)
{
  int ret = OB_SUCCESS;
  bool svr_found = false;
  int64_t avail_svr_count = svr_items_.count();

  // Iterate through all servers and find the server that serves the target log
  for (int64_t index = 0; OB_SUCCESS == ret && ! svr_found && index < avail_svr_count; index++) {
    if (svr_items_.count() <= 0) {
      break;
    } else {
      bool is_log_served = false;
      bool is_svr_invalid = false;
      // Automatically modify next_svr_index_ to move to the next server
      int64_t svr_idx = next_svr_index_++ % svr_items_.count();
      SvrItem &svr_item = svr_items_.at(svr_idx);

      svr_item.check_and_update_serve_info(next_log_id, is_log_served, is_svr_invalid);

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
          if (OB_SUCCESS == ret) {
            svr_found = true;
            svr = svr_item.svr_;
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret && ! svr_found) {
    ret = OB_ITER_END;
  }

  return ret;
}

/////////////////////////////////// SvrItem ///////////////////////////////

void PartSvrList::SvrItem::reset()
{
  svr_.reset();
  range_num_ = 0;
  (void)memset(log_ranges_, 0, sizeof(log_ranges_));
  is_located_in_meta_table_ = false;
  region_prio_ = REGION_PRIORITY_UNKNOWN;
  replica_prio_ = REPLICA_PRIORITY_UNKNOWN;
  is_leader_ = false;
}

void PartSvrList::SvrItem::reset(const bool is_located_in_meta_table,
    const ReplicaPriority replica_prio,
    const bool is_leader)
{
  is_located_in_meta_table_ = is_located_in_meta_table;
  replica_prio_ = replica_prio;
  is_leader_ = is_leader;
}

void PartSvrList::SvrItem::reset(const common::ObAddr &svr,
    const uint64_t start_log_id,
    const uint64_t end_log_id,
    const bool is_located_in_meta_table,
    const RegionPriority region_prio,
    const ReplicaPriority replica_prio,
    const bool is_leader)
{
  svr_ = svr;
  // Initialise a log range by default
  range_num_ = 1;
  log_ranges_[0].reset(start_log_id, end_log_id);
  // Initialization priority
  is_located_in_meta_table_ = is_located_in_meta_table;
  region_prio_ = region_prio;
  replica_prio_ = replica_prio;
  is_leader_ = is_leader;
}

int PartSvrList::SvrItem::find_pos_and_merge_(const uint64_t start_log_id,
    const uint64_t end_log_id,
    bool &merged,
    int64_t &target_index)
{
  int ret = OB_SUCCESS;
  int64_t merge_start = -1;
  int64_t merge_end = -1;
  uint64_t merge_start_log_id = start_log_id;
  uint64_t merge_end_log_id = end_log_id;

  merged = false;
  target_index = 0;

  for (target_index = 0; target_index < range_num_; target_index++) {
    LogIdRange &range = log_ranges_[target_index];

    // Find the insertion position
    if (merge_end_log_id < range.start_log_id_) {
      break;
    }
    // Find segments that can be merged
    else if (merge_start_log_id <= range.end_log_id_) {
      merge_start_log_id = std::min(merge_start_log_id, range.start_log_id_);
      merge_end_log_id = std::max(merge_end_log_id, range.end_log_id_);

      merge_end = target_index;
      if (-1 == merge_start) {
        merge_start = target_index;
      }

      merged = true;

			range.reset(merge_start_log_id, merge_end_log_id);
    }
    // Skip the current block
    else {
    }
  }

  // If a merge has occurred, collate the merged array
  if (merged) {
    if (OB_UNLIKELY(merge_start < 0) || OB_UNLIKELY(merge_end < 0)) {
      LOG_ERROR("merge_start or merge_end is invalid", K(merge_start), K(merge_end));
      ret = OB_ERR_UNEXPECTED;
    } else {
      int64_t merge_delta = merge_end - merge_start;

      if (merge_delta > 0) {
        // Reorganise the array to cover the excess elements after the merge
        for (int64_t old_idx = merge_end, new_idx = merge_start;
            old_idx < range_num_;
            old_idx++, new_idx++) {
          log_ranges_[new_idx] = log_ranges_[old_idx];
        }
      }
			range_num_ -= merge_delta;
    }
  }

  return ret;
}

int PartSvrList::SvrItem::insert_range_(const uint64_t start_log_id,
    const uint64_t end_log_id,
    const int64_t target_insert_index)
{
  int ret = OB_SUCCESS;
  int64_t target_index = target_insert_index;

  if (OB_UNLIKELY(target_insert_index < 0)) {
    LOG_ERROR("invalid index", K(target_insert_index));
    ret = OB_INVALID_ARGUMENT;
  }
  // The array is full, perform a manual merge: merge with the target_insert_index range
  else if (range_num_ >= MAX_RANGE_NUM) {
    if (target_index >= range_num_) {
      // If larger than all segments, merge with the last element
      target_index = range_num_ - 1;
    }

    log_ranges_[target_index].start_log_id_ =
        std::min(log_ranges_[target_index].start_log_id_, start_log_id);

    log_ranges_[target_index].end_log_id_ =
        std::max(log_ranges_[target_index].end_log_id_, end_log_id);
  }
  // Performing ordered insertion operations
  else {
    // Move subsequent elements backwards
    for (int64_t index = range_num_ - 1; index >= target_index; index--) {
      log_ranges_[index + 1] = log_ranges_[index];
    }

    log_ranges_[target_index].start_log_id_ = start_log_id;
    log_ranges_[target_index].end_log_id_ = end_log_id;
		range_num_++;
  }
  return ret;
}

int PartSvrList::SvrItem::add_range(const uint64_t start_log_id, const uint64_t end_log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(range_num_ < 0)) {
    LOG_ERROR("range num is invalid", K(range_num_));
    ret = OB_INVALID_ERROR;
  } else if (OB_UNLIKELY(OB_INVALID_ID == start_log_id)) {
    LOG_ERROR("invalid argument", K(start_log_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t target_index = 0;
    bool merged = false;

    // Find the insert and try to perform a merge operation
    if (OB_FAIL(find_pos_and_merge_(start_log_id, end_log_id, merged, target_index))) {
      LOG_ERROR("find_pos_and_merge_ fail", KR(ret), K(start_log_id), K(end_log_id));
    } else if (merged) {
      // Merging is equivalent to inserting a range, so there is no need to perform the operation again
    }
    // If there is no merge, the elements need to be inserted; if the array is full, then perform a manual merge
    else if (OB_FAIL(insert_range_(start_log_id, end_log_id, target_index))) {
      LOG_ERROR("insert_range_ fail", KR(ret), K(start_log_id), K(end_log_id));
    } else {
      // Insert range successfully
    }
  }
  return ret;
}

void PartSvrList::SvrItem::check_and_update_serve_info(const uint64_t log_id,
    bool &is_log_served,
    bool &is_server_invalid)
{
  is_log_served = false;
  is_server_invalid = false;

  int64_t target_index = 0;
  for (target_index = 0; target_index < range_num_; target_index++) {
    is_log_served = log_ranges_[target_index].is_log_served(log_id);

    if (is_log_served) {
      break;
    }
    // log id at lower range limit, exit loop directly
    else if (log_ranges_[target_index].is_lower_bound(log_id)) {
      break;
    }
  }

  // No valid range found, then the server is no longer valid
  if (target_index >= range_num_) {
    range_num_ = 0;
    is_server_invalid = true;
  }
  // Delete all ranges smaller than log_id
  else if (target_index > 0) {
    int64_t delta = target_index;
    for (int64_t index = target_index; index < range_num_; index++) {
      log_ranges_[index - delta] = log_ranges_[index];
    }

    range_num_ -= target_index;
  }
}

bool PartSvrList::SvrItem::is_priority_equal(const SvrItem &svr_item) const
{
  bool bool_ret = false;

  bool_ret = (is_located_in_meta_table_ == svr_item.is_located_in_meta_table_)
    && (region_prio_ == svr_item.region_prio_)
    && (replica_prio_ == svr_item.replica_prio_)
    && (is_leader_ == svr_item.is_leader_);

  return bool_ret;
}

int64_t PartSvrList::SvrItem::to_string(char *buffer, int64_t length) const
{
  int64_t pos = 0;
  (void)databuff_printf(buffer, length, pos, "{server:%s, range_num:%ld, ranges:[",
      to_cstring(svr_), range_num_);

  for (int64_t index = 0; index < range_num_; index++) {
    (void)databuff_printf(buffer, length, pos, "%s", to_cstring(log_ranges_[index]));
    if (index < range_num_ - 1) {
      (void)databuff_printf(buffer, length, pos, ", ");
    }
  }

  (void)databuff_printf(buffer, length, pos, "], ");
  (void)databuff_printf(buffer, length, pos,
      "priority:[is_meta_table_record:%d region:%s, replica:%s, is_leader:%d]}",
      is_located_in_meta_table_,
      print_region_priority(region_prio_),
      print_replica_priority(replica_prio_),
      is_leader_);

  return pos;
}

////////////////////////////////////////////////////////////////////////////////

int64_t BlackList::g_blacklist_survival_time_upper_limit =
	      ObLogConfig::default_blacklist_survival_time_upper_limit_min * _MIN_;
int64_t BlackList::g_blacklist_survival_time_penalty_period =
        ObLogConfig::default_blacklist_survival_time_penalty_period_min * _MIN_;
int64_t BlackList::g_blacklist_history_overdue_time =
        ObLogConfig::default_blacklist_history_overdue_time_min * _MIN_;

BlackList::BlackList() : bl_svr_items_(ObModIds::OB_LOG_PART_SVR_LIST_BLACK_LIST, OB_MALLOC_NORMAL_BLOCK_SIZE),
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
    int64_t &survival_time)
{
  int ret = OB_SUCCESS;
  BLSvrItem bl_svr_item;
  bl_svr_item.reset(svr, survival_time, get_timestamp());

  if (handle_based_on_history_(svr_service_time, bl_svr_item)) {
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
    int64_t blacklist_survival_time_upper_limit = ATOMIC_LOAD(&g_blacklist_survival_time_upper_limit);
    int64_t blacklist_survival_time_penalty_period = ATOMIC_LOAD(&g_blacklist_survival_time_penalty_period);

    BLSvrItem &history_item = history_svr_items_.at(found_svr_index);
    int64_t history_survival_time = history_item.survival_time_;

    // The partition has been in service with the server for too short a time, and when it is added to the blacklist again, the time the svr has been in the blacklist is doubled
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

int BlackList::do_white_washing(BLSvrArray &wash_svr_array)
{
  int ret = OB_SUCCESS;
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

  return ret;
}

int BlackList::clear_overdue_history(SvrHistoryArray &clear_svr_array)
{
  int ret = OB_SUCCESS;
  clear_svr_array.reset();
  int64_t current_time = get_timestamp();
  int64_t blacklist_history_overdue_time = ATOMIC_LOAD(&g_blacklist_history_overdue_time);

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

  return ret;
}

void BlackList::configure(const ObLogConfig & config)
{
	int64_t blacklist_survival_time_upper_limit_min = config.blacklist_survival_time_upper_limit_min;
  ATOMIC_STORE(&g_blacklist_survival_time_upper_limit, blacklist_survival_time_upper_limit_min * _MIN_);
  int64_t blacklist_survival_time_penalty_period_min = config.default_blacklist_survival_time_penalty_period_min;
  ATOMIC_STORE(&g_blacklist_survival_time_penalty_period, blacklist_survival_time_penalty_period_min * _MIN_);
  int64_t blacklist_history_overdue_time_min = config.blacklist_history_overdue_time_min;
  ATOMIC_STORE(&g_blacklist_history_overdue_time, blacklist_history_overdue_time_min * _MIN_);

  LOG_INFO("[CONFIG]", K(blacklist_survival_time_upper_limit_min));
  LOG_INFO("[CONFIG]", K(blacklist_survival_time_penalty_period_min));
  LOG_INFO("[CONFIG]", K(blacklist_history_overdue_time_min));
}

}
}
