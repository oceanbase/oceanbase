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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_SVR_LIST_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_SVR_LIST_H__

#include "lib/net/ob_addr.h"                  // ObAddr
#include "share/ob_define.h"                  // OB_INVALID_ID, ObReplicaType
#include "lib/utility/ob_print_utils.h"       // TO_STRING_KV
#include "lib/container/ob_se_array.h"        // ObSEArray
#include "lib/hash/ob_ext_iter_hashset.h"     // ObExtIterHashSet
#include "lib/allocator/page_arena.h"         // ObArenaAllocator

#include "ob_log_utils.h"                     // get_timestamp
#include "ob_log_server_priority.h"           // RegionPriority, ReplicaPriority
#include "ob_log_config.h"                    // ObLogConfig
#include "ob_log_svr_blacklist.h"             // ObLogSvrBlacklist
#include "ob_log_start_log_id_locator.h"      // StartLogIdLocateReq

namespace oceanbase
{
namespace liboblog
{
// server黑名单
class IBlackList
{
public:
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
  static const int64_t DEFAULT_SERVER_NUM         = 16;
  static const int64_t DEFAULT_SERVER_HISTORY_NUM = 16;
  typedef common::ObSEArray<BLSvrItem, DEFAULT_SERVER_NUM> BLSvrArray;
  typedef common::ObSEArray<BLSvrItem, DEFAULT_SERVER_HISTORY_NUM> SvrHistoryArray;

public:
  virtual ~IBlackList() {}

public:
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
  virtual int add(const common::ObAddr &svr,
      const int64_t svr_service_time,
      int64_t &survival_time) = 0;

  /// Find out if the server is on the blacklist
  ///
  /// @retval true    exists
  /// @retval false   does not exist
  virtual bool exist(const common::ObAddr &svr) const = 0;

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
  virtual int do_white_washing(BLSvrArray &wash_svr_array) = 0;

  /// Clean up your history periodically
  /// 1. Delete history records that have not been updated for a period of time
  /// 2. Delete history records whose survial_time has reached the upper threshold
  ////
  /// @param [out] clear_svr_array  The server to be cleared
  ///
  /// @retval OB_SUCCESS            Clear success
  //// @retval Other error codes    Fail
  virtual int clear_overdue_history(SvrHistoryArray &clear_svr_array) = 0;

};


// IPartSvrList
//
// List of servers describing the range of logs serving the partition
class IPartSvrList
{
public:
  static const int64_t DEFAULT_SERVER_NUM = 16;

public:
  virtual ~IPartSvrList() {}

public:
  virtual void reset() = 0;

  // Add a server
  // When the server does not exist, update the corresponding information
  //
  // @note is_log_range_valid indicates whether the log range passed in is valid
  virtual int add_server_or_update(const common::ObAddr &svr,
      const uint64_t start_log_id,
      const uint64_t end_log_id,
      const bool is_located_in_meta_table,
      const RegionPriority region_prio,
      const ReplicaPriority replica_prio,
      const bool is_leader,
      const bool is_log_range_valid = true) = 0;

  // Iterate over the server that serves the next log
  // 1. return OB_ITER_END when the list of servers has been traversed or when no servers are available
  // 2. After returning OB_ITER_END, the history is cleared and the server can be iterated over normally next time
  //
  // @param [in] next_log_id      Next log ID
  // @param [in] blacklist        Partition blacklist
  // @param [out] svr             the server to return to
  //
  // @retval OB_SUCCESS           Success, found the server that served the next log
  // @retval OB_ITER_END          server that did not serve the next log
  // @retval Other return values  Failed
  virtual int next_server(const uint64_t next_log_id,
      const IBlackList &blacklist,
      common::ObAddr &svr) = 0;


  // detect whether the current partition task needs to switch the stream
  //
  // @param [in] next_log_id      next log ID
  // @param [in] blacklist        The partition blacklist
  // @param [in] pkey             partition
  // @param [in] cur_svr          partition task located at current fetch log stream - target server
  //
  // @retval true                 requires switch stream, i.e. a higher priority server exists
  // @retval false                no stream switch required
  virtual bool need_switch_server(const uint64_t next_log_id,
      IBlackList &blacklist,
      const common::ObPartitionKey &pkey,
      const common::ObAddr &cur_svr) = 0;

  /// whether server exists, if so svr_index returns the svr index position, otherwise -1
  virtual bool exist(const common::ObAddr &svr, int64_t &svr_index) const = 0;

  virtual int get_server_array_for_locate_start_log_id(StartLogIdLocateReq::SvrList &svr_list) const = 0;

  // Return the number of available servers
  virtual int64_t count() const = 0;

  // Sorting the server list by priority
  virtual void sort_by_priority() = 0;

  // Server-based blacklist filtering
  virtual int filter_by_svr_blacklist(const ObLogSvrBlacklist &svr_blacklist,
      common::ObArray<common::ObAddr> &remove_svrs) = 0;

  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
};

///////////////////////////////////////////// PartSvrList //////////////////////////////////////////////

class PartSvrList : public IPartSvrList
{
public:
  PartSvrList();
  virtual ~PartSvrList();

public:
  void reset();
  int add_server_or_update(const common::ObAddr &svr,
      const uint64_t start_log_id,
      const uint64_t end_log_id,
      const bool is_located_in_meta_table,
      const RegionPriority region_prio,
      const ReplicaPriority replica_prio,
      const bool is_leader,
      const bool is_log_range_valid = true);
  int next_server(const uint64_t next_log_id,
      const IBlackList &blacklist,
      common::ObAddr &svr);
  bool need_switch_server(const uint64_t next_log_id,
      IBlackList &blacklist,
      const common::ObPartitionKey &pkey,
      const common::ObAddr &cur_svr);
  bool exist(const common::ObAddr &svr, int64_t &svr_index) const;
  int get_server_array_for_locate_start_log_id(StartLogIdLocateReq::SvrList &svr_list) const;
  int64_t count() const { return svr_items_.count(); }
  void sort_by_priority();
  int filter_by_svr_blacklist(const ObLogSvrBlacklist &svr_blacklist,
      common::ObArray<common::ObAddr> &remove_svrs);

private:
  struct SvrItem;
  typedef common::ObSEArray<SvrItem, DEFAULT_SERVER_NUM> SvrItemArray;

private:
  void sort_by_priority_for_locate_start_log_id_(SvrItemArray &svr_items) const;
  int get_next_server_based_on_blacklist_(const uint64_t next_log_id,
      const IBlackList &blacklist,
      common::ObAddr &svr);
  int check_found_svr_priority_(const common::ObPartitionKey &pkey,
      const int64_t found_svr_idx,
      const int64_t avail_svr_count,
      const common::ObAddr &cur_svr,
      bool &need_switch);

private:
  // Log serve range
  // Note: the interval is "left open, right closed"
  struct LogIdRange
  {
    uint64_t start_log_id_;
    uint64_t end_log_id_;

    void reset()
    {
      start_log_id_ = common::OB_INVALID_ID;
      end_log_id_ = common::OB_INVALID_ID;
    }

    void reset(const uint64_t start_log_id, const uint64_t end_log_id)
    {
      start_log_id_ = start_log_id;
      end_log_id_ = end_log_id;
    }

    bool is_log_served(const int64_t log_id)
    {
      return (log_id > start_log_id_) && (log_id <= end_log_id_);
    }

    bool is_lower_bound(const int64_t log_id)
    {
      return log_id <= start_log_id_;
    }

    TO_STRING_KV(K_(start_log_id), K_(end_log_id));
  };

  // Description of server information for a single service log
  // Each server may serve multiple segments of logs, only a fixed number of segments are logged here, the extra segments are randomly merged with other segments
  //
  // For ease of implementation, log segments are sorted by end_log_id and start_log_id incrementally, with no overlap between adjacent segments
  struct SvrItem
  {
    static const int64_t MAX_RANGE_NUM = 4;

    common::ObAddr  svr_;
    int64_t         range_num_;
    LogIdRange      log_ranges_[MAX_RANGE_NUM];

    // Definition of priority
    bool            is_located_in_meta_table_;   // Is it in the meta table
    RegionPriority  region_prio_;                // region priority definition
    ReplicaPriority replica_prio_;               // replica type priority definition
    bool            is_leader_;                  // server is leader or not

    void reset();
    // The current partitioned query server list is __clog_histore_info_v2 before querying the meta table
    // After querying the meta table, this interface is called to update the relevant information: whether it is in the meta table, the copy type and whether it is the leader
    void reset(const bool is_located_in_meta_table,
        const ReplicaPriority replica_prio,
        const bool is_leader);

    void reset(const common::ObAddr &svr,
        const uint64_t start_log_id,
        const uint64_t end_log_id,
        const bool is_located_in_meta_table,
        const RegionPriority region_prio,
        const ReplicaPriority replica_prio,
        const bool is_leader);

    bool is_valid() const { return svr_.is_valid(); }

    // add a log range
    int add_range(const uint64_t start_log_id, const uint64_t end_log_id);

    // 1. check if a log is serviced
    // 2. Update service information: as log IDs are incremental, expired logs can be removed from the service range
    //
    // @param [in] log_id               target log ID
    // @param [out] is_log_served       whether serve target log id
    // @param [out] is_server_invalid   whether the server is no longer valid (no more valid ranges)
    void check_and_update_serve_info(const uint64_t log_id,
        bool &is_log_served,
        bool &is_server_invalid);

    bool is_priority_equal(const SvrItem &svr_item) const;

    int64_t to_string(char *buffer, int64_t length) const;

  private:
    int insert_range_(const uint64_t start_log_id,
        const uint64_t end_log_id,
        const int64_t target_insert_index);

    int find_pos_and_merge_(const uint64_t start_log_id,
        const uint64_t end_log_id,
        bool &merged,
        int64_t &target_index);
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

private:
  DISALLOW_COPY_AND_ASSIGN(PartSvrList);
};

class BlackList : public IBlackList
{
  // Class global variables
public:
  static int64_t g_blacklist_survival_time_upper_limit;
  static int64_t g_blacklist_survival_time_penalty_period;
  static int64_t g_blacklist_history_overdue_time;

public:
  static const int64_t UPDATE_SURVIVAL_TIME_MUTIPLE = 2;

public:
  BlackList();
  virtual ~BlackList();

public:
  void reset();
  int64_t count() const;
  int add(const common::ObAddr &svr,
      const int64_t svr_service_time,
      int64_t &survival_time);
  bool exist(const common::ObAddr &svr) const;
  int do_white_washing(BLSvrArray &wash_svr_array);
  int clear_overdue_history(SvrHistoryArray &clear_svr_array);

public:
  static void configure(const ObLogConfig & config);

public:
  TO_STRING_KV(K_(bl_svr_items),
      "bl_svr_num", bl_svr_items_.count(),
      K_(history_svr_items),
      "history_svr_num", history_svr_items_.count());

private:
  // Determine the surival time for the server to be blacklisted based on the history
  int handle_based_on_history_(const int64_t svr_service_time, BLSvrItem &item);
  // Find the history, determine if the server exists, if so svr_index return the svr index position, otherwise return -1
  bool exist_in_history_(const common::ObAddr &svr, int64_t &svr_index) const;

private:
  BLSvrArray bl_svr_items_;
  SvrHistoryArray history_svr_items_;
};


}
}

#endif
