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

#ifndef OCEANBASE_OB_LS_SERVER_LIST_H_
#define OCEANBASE_OB_LS_SERVER_LIST_H_

#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/utility/ob_print_utils.h"       // TO_STRING_KV
#include "lib/container/ob_se_array.h"        // ObSEArray

#include "ob_server_priority.h"               // RegionPriority, ReplicaPriority
#include "ob_external_server_blacklist.h"     // ObLogSvrBlacklist
#include "ob_log_all_svr_cache.h"             // ObLogAllSvrCache
#include "ob_server_blacklist.h"              // BlackList
#include "ob_log_route_key.h"                 // ObLSRouterKey

namespace oceanbase
{
namespace logservice
{
///////////////////////////////////////////// LSSvrList //////////////////////////////////////////////
// List of servers describing the range of logs serving the LS
class LSSvrList
{
public:
  LSSvrList();
  virtual ~LSSvrList();
  LSSvrList &operator=(const LSSvrList &other);

public:
  static const int64_t DEFAULT_SERVER_NUM = 16;
  void reset();

  // Add a server
  // When the server exist, update the corresponding information
  int add_server_or_update(const common::ObAddr &svr,
      const palf::LSN &start_lsn,
      const palf::LSN &end_lsn,
      const RegionPriority region_prio,
      const bool is_leader);

  // Iterate over the server that serves the next LSN
  // 1. return OB_ITER_END when the list of servers has been traversed or when no servers are available
  // 2. After returning OB_ITER_END, the history is cleared and the server can be iterated over normally next time
  //
  // @param [in] next_lsn         Next LSN
  // @param [in] blacklist        The LS blacklist
  // @param [out] svr             the server to return to
  //
  // @retval OB_SUCCESS           Success, found the server that served the nextL LSN
  // @retval OB_ITER_END          There is no server that serving the next LSN
  // @retval Other return values  Failed
  int next_server(const palf::LSN &next_lsn,
      const BlackList &blacklist,
      common::ObAddr &svr);

  // Detect whether the current LS needs to switch the server
  //
  // @param [in] next_lsn         next LSN
  // @param [in] blacklist        The LS blacklist
  // @param [in] cur_svr          LS located at current fetch log stream - target server
  //
  // @retval true                 requires switch stream, i.e. a higher priority server exists
  // @retval false                no stream switch required
  bool need_switch_server(const ObLSRouterKey &key,
      const palf::LSN &next_lsn,
      BlackList &blacklist,
      const common::ObAddr &cur_svr);

  int get_server_array_for_locate_start_lsn(ObIArray<common::ObAddr> &svr_list) const;

  // Return the number of available servers
  int64_t count() const { return svr_items_.count(); }

  // Sorting the server list by priority
  void sort_by_priority();

  // Server-based blacklist filtering
  int filter_by_svr_blacklist(const ObLogSvrBlacklist &svr_blacklist,
      common::ObIArray<common::ObAddr> &remove_svrs);

  // Try to get leader, if has not leader, return OB_NOT_MASTER
  int get_leader(ObAddr &addr);

private:
  struct SvrItem;
  typedef common::ObSEArray<SvrItem, DEFAULT_SERVER_NUM> SvrItemArray;

private:
  void sort_by_priority_for_locate_start_log_id_(SvrItemArray &svr_items) const;
  int get_next_server_based_on_blacklist_(const palf::LSN &next_lsn,
      const BlackList &blacklist,
      common::ObAddr &svr);
  int check_found_svr_priority_(
      const ObLSRouterKey &key,
      const int64_t found_svr_idx,
      const int64_t avail_svr_count,
      const common::ObAddr &cur_svr,
      bool &need_switch);

private:
  // Log serve range
  // Note: the interval is "left open, right closed"
  struct LSNRange
  {
    palf::LSN start_lsn_;
    palf::LSN end_lsn_;

    void reset()
    {
      start_lsn_.reset();
      end_lsn_.reset();
    }

    void reset(const palf::LSN &start_lsn, const palf::LSN &end_lsn)
    {
      start_lsn_ = start_lsn;
      end_lsn_ = end_lsn;
    }

    bool is_log_served(const palf::LSN &lsn)
    {
      // when lsn < start_lsn_, it doesn't mean that the log not exists in the server,
      // because the log could be archived, and the logroute service doesn't know whether
      // archive is on or log exists in archive, so the rpc would be lauched.
      // In this perspective, the log is served, because we need to lauch rpc.
      return lsn < end_lsn_;
    }

    bool is_lower_bound(const palf::LSN &lsn)
    {
      return lsn < start_lsn_;
    }

    int64_t to_string(char *buffer, int64_t length) const;
  };

  // Description of server information for a single service log
  struct SvrItem
  {
    common::ObAddr  svr_;
    LSNRange        log_ranges_;

    // Definition of priority
    bool            is_located_in_meta_table_;   // Is it in the meta table
    RegionPriority  region_prio_;                // region priority definition
    ReplicaPriority replica_prio_;               // replica type priority definition
    bool            is_leader_;                  // server is leader or not

    void reset();

    void reset(const common::ObAddr &svr,
        const palf::LSN &start_lsn,
        const palf::LSN &end_lsn,
        const bool is_located_in_meta_table,
        const RegionPriority region_prio,
        const ReplicaPriority replica_prio,
        const bool is_leader);

    // TODO support query meta table
    // The current LS get server list by query __all_virtual_palf_stat before querying the meta table
    // After querying the meta table, this interface is called to update the relevant information:
    // 1. Whether it is in the meta table
    // 2. The replicas type
    void update(const palf::LSN &start_lsn,
        const palf::LSN &end_lsn,
        const bool is_located_in_meta_table,
        const RegionPriority region_prio,
        const ReplicaPriority replica_prio,
        const bool is_leader);

    bool is_valid() const { return svr_.is_valid(); }

    // 1. check if a log is serviced
    // 2. Update service information: as log IDs are incremental, expired logs can be removed from the service range
    //
    // @param [in]  lsn                 target lsn
    // @param [out] is_log_served       whether serve target log id
    // @param [out] is_server_invalid   whether the server is no longer valid (no more valid ranges)
    void check_and_update_serve_info(
        const bool is_always_serving,
        const palf::LSN &lsn,
        bool &is_log_served,
        bool &is_server_invalid);

    bool is_priority_equal(const SvrItem &svr_item) const;

    int64_t to_string(char *buffer, int64_t length) const;
  };

  class SvrItemCompare
  {
    // 1. prioritize the server in the meta table to synchronize logs
    // 2. sort by region priority from largest to smallest
    // 3. prioritize by replica type from largest to smallest
    // 4. prioritize synchronizing from followers, followed by leaders
    //
    // Note: The lower the region and replica_type values, the higher the priority
  public:
    bool operator() (const SvrItem &a, const SvrItem &b)
    {
      bool bool_ret = false;

      if (a.is_located_in_meta_table_ != b.is_located_in_meta_table_) {
        bool_ret = static_cast<int8_t>(a.is_located_in_meta_table_) > static_cast<int8_t>(b.is_located_in_meta_table_);
      } else if (a.region_prio_ != b.region_prio_) {
        bool_ret = a.region_prio_ < b.region_prio_;
      } else if (a.replica_prio_ != b.replica_prio_) {
        bool_ret = a.replica_prio_ < b.replica_prio_;
      } else {
        bool_ret = static_cast<int8_t>(a.is_leader_) < static_cast<int8_t>(b.is_leader_);
      }

      return bool_ret;
    }
  };

  class LocateStartLogIdCompare
  {
    // 1. Prioritize the leader
    // 2. prioritize the records in the meta table
    // 3. Next to the remaining records
    //
    // No more sorting by record creation time from oldest to youngest (i.e. new server records are first and old server records are second.
    // When partitioning the start_log_id, you need to prioritize the new server to prevent the start_log_id from going back too much), because the meta table records
    // are the most recent, and locating them in the leader will block out some of the OB issues, such as locating the start log id incorrectly and being too large
  public:
    bool operator() (const SvrItem &a, const SvrItem &b)
    {
      bool bool_ret = false;

      if (a.is_leader_ != b.is_leader_) {
        bool_ret = static_cast<int8_t>(a.is_leader_) > static_cast<int8_t>(b.is_leader_);
      } else {
        bool_ret = static_cast<int8_t>(a.is_located_in_meta_table_) > static_cast<int8_t>(b.is_located_in_meta_table_);
      }

      return bool_ret;
    }
  };

public:
  TO_STRING_KV(K_(next_svr_index),
      "svr_num", svr_items_.count(),
      K_(svr_items));

  // Internal member variables
private:
  int64_t       next_svr_index_;   // Index of the next server item
  SvrItemArray  svr_items_;        // server list
};
}
} // namespace oceanbase

#endif

