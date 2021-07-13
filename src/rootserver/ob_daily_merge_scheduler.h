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

#ifndef OCEANBASE_ROOTSERVER_OB_DAILY_MERGE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_DAILY_MERGE_SCHEDULER_H_

#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_mutex.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_daily_merge_sequence_generator.h"
#include "share/ob_zone_info.h"
#include "ob_leader_coordinator.h"
#include "ob_partition_table_util.h"
#include "ob_thread_idling.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
#include "share/ob_sstable_checksum_iterator.h"
#include "rootserver/ob_root_balancer.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
};

namespace share {
class ObGlobalInfo;
}

namespace rootserver {
class ObZoneManager;
class ObDDLService;
class ObFreezeInfoManager;

class ObIZoneOp {
public:
  enum Type {
    SetZoneMergingType = 1,
    SetFinishZoneMergeType = 2,
    SetZoneMergeTimeOutType = 3,
    MAXType,
  };
  virtual ~ObIZoneOp()
  {}
  virtual int do_op() = 0;
  virtual bool is_done() const = 0;
  virtual const common::ObZone& get_zone() const = 0;
  virtual Type get_type() const = 0;
  VIRTUAL_TO_STRING_KV("OpType", "ObIZoneOp");
};

class ObSetZoneMergingOp : public ObIZoneOp {
public:
  ObSetZoneMergingOp(ObZoneManager& zone_mgr, const common::ObZone& zone);
  virtual ~ObSetZoneMergingOp()
  {}
  virtual int do_op();
  virtual bool is_done() const
  {
    return is_done_;
  }
  virtual const common::ObZone& get_zone() const
  {
    return zone_;
  }
  virtual Type get_type() const
  {
    return SetZoneMergingType;
  }

  VIRTUAL_TO_STRING_KV("OpType", "ObSetZoneMergingOp", K_(zone), K_(is_done));

private:
  ObZoneManager& zone_mgr_;
  common::ObZone zone_;
  bool is_done_;
  DISALLOW_COPY_AND_ASSIGN(ObSetZoneMergingOp);
};

class ObSetFinishZoneMergeOp : public ObIZoneOp {
public:
  ObSetFinishZoneMergeOp(ObZoneManager& zone_mgr, const common::ObZone& zone, const int64_t last_merged_version,
      const int64_t all_merged_version);

  virtual ~ObSetFinishZoneMergeOp()
  {}
  virtual int do_op();
  virtual bool is_done() const
  {
    return is_done_;
  }
  virtual const common::ObZone& get_zone() const
  {
    return zone_;
  }
  virtual Type get_type() const
  {
    return SetFinishZoneMergeType;
  }

  VIRTUAL_TO_STRING_KV("OpType", "ObSetFinishZoneMergeOp", K_(zone), K_(is_done));

private:
  ObZoneManager& zone_mgr_;
  common::ObZone zone_;
  int64_t last_merged_version_;
  int64_t all_merged_version_;
  bool is_done_;
  DISALLOW_COPY_AND_ASSIGN(ObSetFinishZoneMergeOp);
};

class ObSetZoneMergeTimeOutOp : public ObIZoneOp {
public:
  ObSetZoneMergeTimeOutOp(ObZoneManager& zone_mgr, const common::ObZone& zone, const int64_t version);
  virtual ~ObSetZoneMergeTimeOutOp()
  {}
  virtual int do_op();
  virtual bool is_done() const
  {
    return is_done_;
  }
  virtual const common::ObZone& get_zone() const
  {
    return zone_;
  }
  virtual Type get_type() const
  {
    return SetZoneMergeTimeOutType;
  }

  VIRTUAL_TO_STRING_KV("OpType", "ObSetZoneMergeTimeOutOp", K_(zone), K_(is_done));

private:
  ObZoneManager& zone_mgr_;
  common::ObZone zone_;
  bool is_done_;
  int64_t version_;
  DISALLOW_COPY_AND_ASSIGN(ObSetZoneMergeTimeOutOp);
};

class ObSwitchLeaderMgr {
public:
  ObSwitchLeaderMgr();
  virtual ~ObSwitchLeaderMgr();

  void reset();
  void reset_op_array();
  int init(ObILeaderCoordinator& leader_coordinator, ObZoneManager& zone_mgr, common::ObServerConfig& config);
  // schedule warmup, leader-switch and other op. TODO: need clean up warmup flag time after switchleader
  int do_work();
  int start_smooth_coordinate();
  int check_all_op_finish(bool& is_finish);  // cleanup warmup flag when all op finish
  int is_last_switch_turn_succ(bool& is_succ);
  int start_switch_leader_without_warm_up();

  // add op only when warmup, cleanup warmup start time after op added
  int add_set_zone_merging_op(
      const common::ObZone& zone, const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array);
  int add_finish_zone_merge_op(const common::ObZone& zone, const int64_t last_merge_version,
      const int64_t all_merged_version, const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array);
  int add_set_zone_merge_time_out_op(const common::ObZone& zone, const int64_t version,
      const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array);
  int is_zone_merged_or_timeout(const common::ObZone& zone, bool& result);

  int check_switch_leader(const common::ObIArray<common::ObZone>& zone, common::ObIArray<bool>& is_succ);
  int wake_up_leader_coordinator();
  void set_merger_warm_up_duration_time(const int64_t merger_warm_up_duration_time);
  void set_merge_status(bool is_in_merging);

private:
  int can_add_new_op(const common::ObZone& zone, bool& can);
  int check_doing_warm_up(bool& is_doing);
  bool need_warm_up();
  bool need_smooth_coordinate(const ObIZoneOp& zone_op);
  int start_warm_up(const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array, const common::ObZone& zone);

private:
  bool is_inited_;
  ObILeaderCoordinator* leader_coordinator_;
  ObZoneManager* zone_mgr_;
  common::ObServerConfig* config_;
  common::ObArray<ObIZoneOp*> op_array_;
  common::ObArenaAllocator allocator_;
  int64_t merger_warm_up_duration_time_;
  DISALLOW_COPY_AND_ASSIGN(ObSwitchLeaderMgr);
};

class ObDailyMergeIdling : public ObThreadIdling {
public:
  explicit ObDailyMergeIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual int64_t get_idle_interval_us();
};

class ObPartitionChecksumChecker {
public:
  friend class TestChecksumChecker_test_checksum_error_Test;
  ObPartitionChecksumChecker()
      : sql_proxy_(NULL),
        remote_sql_proxy_(NULL),
        zone_manager_(NULL),
        freeze_info_manager_(),
        merge_error_cb_(NULL),
        last_check_time_(0),
        in_merging_(false)
  {}
  virtual ~ObPartitionChecksumChecker()
  {}
  int init(common::ObMySQLProxy* sql_proxy, common::ObISQLClient* remote_sql_proxy, ObZoneManager& zone_manager,
      ObFreezeInfoManager& freeze_info_manager, share::ObIMergeErrorCb* merge_error_cb);
  int check();
  void set_merging_status(bool in_merging)
  {
    in_merging_ = in_merging;
  }

  static const int64_t MIN_CHECK_INTERVAL = 10 * 1000 * 1000LL;

private:
  void check_partition_checksum();
  int check_global_index_column_checksum();

private:
  share::ObSSTableDataChecksumIterator local_data_checksum_iter_;
  share::ObSSTableDataChecksumIterator remote_data_checksum_iter_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObISQLClient* remote_sql_proxy_;
  ObZoneManager* zone_manager_;
  ObFreezeInfoManager* freeze_info_manager_;
  share::ObIMergeErrorCb* merge_error_cb_;
  int64_t last_check_time_;
  bool in_merging_;
};

// Schedule daily merge (merge dynamic data to base line data).
// Running in a single thread.
class ObDailyMergeScheduler : public ObRsReentrantThread {
public:
  friend class TestDailyMergerScheduler_double_check_Test;
  const static int64_t DEFAULT_ZONE_COUNT = 5;
  const static int64_t DEFAULT_IDLE_DURATION = 10 * 1000L * 1000L;
  typedef common::ObArray<share::ObZoneInfo> ObZoneInfoArray;
  typedef common::ObSEArray<common::ObZone, DEFAULT_ZONE_COUNT> ObZoneArray;

  ObDailyMergeScheduler();
  virtual ~ObDailyMergeScheduler();

  int init(ObZoneManager& zone_mgr, common::ObServerConfig& config, const common::ObZone& self_zone,
      ObILeaderCoordinator& leader_coordinator, ObPartitionTableUtil& pt_util, ObDDLService& ddl_service,
      ObServerManager& server_mgr, share::ObPartitionTableOperator& pt,
      share::ObRemotePartitionTableOperator& remote_pt, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObMySQLProxy& sql_proxy, ObFreezeInfoManager& freeze_info_manager, ObRootBalancer& root_balancer);
  void set_checksum_checker(ObPartitionChecksumChecker& checker)
  {
    checksum_checker_ = &checker;
  }
  virtual void run3() override;
  void wakeup();
  int reset_merger_warm_up_duration_time(const int64_t merger_warm_up_duration_time);

  void stop();
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

  virtual int manual_start_merge(const common::ObZone& zone);

  int64_t get_schedule_interval() const;

private:
  // return OB_CANCELED if stopped.
  int idle();

  // check schedule algrithm is normal
  int double_check(ObZoneArray& to_merge_zone);
  // force schedule when algrithm has problems
  int force_schedule_zone(ObZoneArray& to_merge);
  // all zone's all partition merged
  int is_all_partition_merged(const int64_t global_broadcast_version, const ObZoneInfoArray& infos, bool& merged);

  int limit_merge_concurrency(ObZoneArray& ready_zone, ObZoneArray& to_merge);
  // need to check and update local index status if %local_index_built is false
  int main_round();

  int schedule_daily_merge();
  int schedule_stagger_merge();

  int arrange_execute_order(ObZoneArray& order);

  void check_merge_timeout();
  int check_merge_list(ObZoneArray& merge_list);
  int next_merge_zones(ObZoneArray& to_merge);
  int generate_next_zones_by_merge_list(const ObZoneArray& merge_list, ObZoneArray& to_merge);
  int generate_next_zones_by_conflict_pair(ObZoneArray& to_merge);
  int calc_merge_info(int64_t& in_merge_zone_count, int64_t& merged_count);
  int calc_suspend_zone_count(int64_t& suspend_zone_count);
  // switch away leader form %to_merge && start merge
  int process_zones(const ObZoneArray& to_merge);
  int set_zone_merging(const common::ObZone& zone);
  int set_zone_merging_while_need_switch_leader(const common::ObZone& zone);
  int cancel_zone_merging(const common::ObZone& zone);
  int start_zones_merge(const ObZoneArray& to_merge);
  int get_merge_list(ObZoneArray& merge_list);
  // check and update merge status
  int update_merge_status();
  bool need_check_switch_leader();
  bool need_switch_leader();
  int check_switch_result(const common::ObZone& zone, bool& is_succ);

  int try_update_global_merged_version();

  // int backup_leader_pos();
  // int try_restore_leader_pos();
  int try_start_smooth_coordinate();
  const int64_t SWITCH_LEADER_CHECK_INTERVAL = 2 * 60 * 1000 * 1000;

private:
  bool inited_;
  ObZoneManager* zone_mgr_;
  common::ObServerConfig* config_;
  common::ObZone self_;
  ObPartitionTableUtil* pt_util_;
  mutable ObDailyMergeIdling idling_;
  // mutex to protect manual start merge and auto started merge.
  lib::ObMutex mutex_;
  ObDDLService* ddl_service_;
  // bool leader_pos_backuped_;
  ObSwitchLeaderMgr switch_leader_mgr_;
  int64_t merger_warm_up_duration_time_;
  ObPartitionTableUtil::ObLeaderInfoArray leader_info_array_;
  ObPartitionChecksumChecker* checksum_checker_;
  ObDailyMergeSequenceGenerator execute_order_generator_;
  common::ObMySQLProxy* sql_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  share::ObRemotePartitionTableOperator* remote_pt_operator_;
  int64_t switch_leader_check_interval_;
  int64_t last_start_merge_time_;
  ObFreezeInfoManager* freeze_info_manager_;
  ObRootBalancer* root_balancer_;
  DISALLOW_COPY_AND_ASSIGN(ObDailyMergeScheduler);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_DAILY_MERGE_SCHEDULER_H_
