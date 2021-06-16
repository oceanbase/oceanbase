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

#ifndef OCEANBASE_ROOTSERVER_OB_LEADER_COORDINATOR_H_
#define OCEANBASE_ROOTSERVER_OB_LEADER_COORDINATOR_H_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_latch.h"
#include "lib/list/ob_dlink_node.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "common/ob_zone.h"
#include "common/ob_unit_info.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_primary_zone_util.h"
#include "share/ob_leader_election_waiter.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_balance_group_container.h"

namespace oceanbase {

namespace common {
class ObServerConfig;
}

namespace obrpc {
class ObSrvRpcProxy;
class ObAdminSwitchReplicaRole;
}  // namespace obrpc

namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
class ObSimpleTableSchema;
}  // namespace schema
}  // namespace share

namespace rootserver {
class ObServerManager;
class ObZoneManager;
class ObRebalanceTaskMgr;
class ObUnitManager;
class ObDailyMergeScheduler;

namespace balancer {
class ILeaderBalancerIndexBuilder;
struct HashIndexMapItem;
class HashIndexCollection;
class ITenantStatFinder;
}  // namespace balancer

class ObRandomZoneSelector {
public:
  const static int64_t DEFAULT_ZONE_CNT = 7;
  int init(ObZoneManager& zone_mgr);
  int update_score(const uint64_t tg_id, const int64_t partition_id);
  // return OB_ENTRY_NOT_EXIST for zone not exist.
  int get_zone_score(const common::ObZone& zone, int64_t& score) const;
  bool is_inited() const
  {
    return !zones_.empty();
  }

private:
  common::ObSEArray<std::pair<common::ObZone, int64_t>, DEFAULT_ZONE_CNT> zones_;
};

class ObILeaderCoordinator {
public:
  struct ServerLeaderCount {
    ServerLeaderCount() : server_(), count_(0)
    {}
    void reset()
    {
      server_.reset();
      count_ = 0;
    }
    bool operator<(const ServerLeaderCount& count) const
    {
      return this->server_ < count.server_;
    }

    common::ObAddr server_;
    int64_t count_;
    TO_STRING_KV(K_(server), K_(count));
  };
  typedef common::ObArray<ServerLeaderCount> ServerLeaderStat;
  typedef common::ObArray<common::ObZone> ObZoneList;

  ObILeaderCoordinator()
  {}
  virtual ~ObILeaderCoordinator()
  {}

  virtual int coordinate() = 0;
  virtual void set_merge_status(bool is_in_merging) = 0;

  virtual int start_smooth_coordinate() = 0;
  virtual int is_doing_smooth_coordinate(bool& is_doing) = 0;
  virtual int is_last_switch_turn_succ(bool& is_succ) = 0;
  virtual int check_daily_merge_switch_leader(
      const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results) = 0;

  // signal leader coordinator to start working
  virtual void signal() = 0;
  // get leader count of servers
  virtual int get_leader_stat(ServerLeaderStat& leader_stat) = 0;

  virtual common::ObLatch& get_lock() = 0;
  // Switch leader lock, used to prevent leader switching while major freeze.
  virtual common::ObLatch& get_switch_leader_lock() = 0;

  virtual obrpc::ObSrvRpcProxy& get_rpc_proxy() = 0;
};

class ObServerSwitchLeaderInfoStat {
public:
  static const int64_t ALL_SWITCH_PERCENT = 100;
  static const int64_t MIN_SWITCH_TIMEOUT = 5 * 60 * 1000 * 1000;  // 5m
  static const int64_t MIN_SWITCH_INTERVAL = 1 * 1000 * 1000;      // 1s
  static const int64_t TIMES_IN_SMOOTH_SWITCH = 1;
  static const int64_t SMOOTH_SWITCH_LEADER_CNT_LIMIT = 1024;

  struct ServerSwitchLeaderInfo {
    ServerSwitchLeaderInfo();
    ~ServerSwitchLeaderInfo()
    {}
    void reset();

    common::ObAddr server_;
    int64_t new_leader_count_;  // new switch leader this turn
    int64_t leader_count_;      // count from meta_table, used for the leader_count on __all_virtual_server_stat
    int64_t partition_count_;   // fetch from observer
    bool is_switch_limited_;    // switch limit for this server in this round

    TO_STRING_KV(K_(server), K_(new_leader_count), K_(leader_count), K_(partition_count), K_(is_switch_limited));
  };

  typedef common::hash::ObHashMap<common::ObAddr, ServerSwitchLeaderInfo, common::hash::NoPthreadDefendMode>
      ServerSwitchLeaderInfoMap;

  ObServerSwitchLeaderInfoStat();
  virtual ~ObServerSwitchLeaderInfoStat()
  {}
  virtual int init(obrpc::ObSrvRpcProxy* srv_rpc_proxy);
  virtual int start_new_smooth_switch(const int64_t smooth_switch_start_time);
  virtual int can_switch_more_leader(const common::ObAddr& server, bool& can_switch);
  virtual int inc_leader_count(const common::ObAddr& leader, const bool is_new_leader);
  virtual int prepare_next_turn(
      const volatile bool& is_stop, ObServerManager& server_mgr, const int64_t switch_leader_duration_time);
  virtual int finish_smooth_turn(const bool force_finish);
  virtual void clear();
  virtual int copy_leader_stat(common::ObArray<ObILeaderCoordinator::ServerLeaderCount>& leader_stat);
  virtual int update_server_leader_cnt_status(ObServerManager* server_mgr);
  virtual void clear_switch_limited()
  {
    is_switch_limited_ = false;
  }
  virtual void set_switch_limited()
  {
    is_switch_limited_ = true;
  }
  virtual bool get_is_switch_limited() const
  {
    return is_switch_limited_;
  }
  virtual bool is_doing_smooth_switch() const
  {
    return 0 != smooth_switch_start_time_;
  }
  virtual int need_force_finish_smooth_switch(const int64_t merger_switch_leader_duration_time, bool& need_force) const;
  virtual int get_expect_switch_ts(const int64_t switch_leader_duration_time, int64_t& expect_ts) const;
  virtual void set_self_in_candidate(const bool in)
  {
    self_in_candidate_ = in;
  }
  virtual bool get_self_in_candidate() const
  {
    return self_in_candidate_;
  }
  int check_smooth_switch_condition(common::ObIArray<common::ObAddr>& candidate_leaders, bool& can_swith_more);

private:
  virtual int get_server_partition_count(common::ObAddr& server, int64_t& partition_count);
  virtual int prepare_next_smooth_turn(const volatile bool& is_stop,
      const common::ObIArray<common::ObAddr>& server_list, const int64_t switch_duration_time, const int64_t now);

  bool is_inited_;
  ServerSwitchLeaderInfoMap info_map_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  int64_t avg_partition_count_;
  bool is_switch_limited_;

  int64_t smooth_switch_start_time_;  // 0 means no in smooth switch
  int64_t switch_percent_;            // switch_percent >= ALL_SWITCH_PERCENT(100) means no limit
  bool self_in_candidate_;            // whether RS in candidate when SYS tenant do leader coordinate.
};

class ObLeaderCoordinatorIdling : public ObThreadIdling {
public:
  explicit ObLeaderCoordinatorIdling(volatile bool& stop) : ObThreadIdling(stop), idle_interval_us_(0)
  {}
  virtual ~ObLeaderCoordinatorIdling()
  {}

  virtual int64_t get_idle_interval_us()
  {
    return idle_interval_us_;
  }
  void set_idle_interval_us(const int64_t idle_interval_us)
  {
    idle_interval_us_ = idle_interval_us;
  }

private:
  int64_t idle_interval_us_;
};

class ObLeaderCoordinator;

class PartitionInfoContainer {
public:
  typedef common::hash::ObHashMap<common::ObPartitionKey, share::ObPartitionInfo, common::hash::NoPthreadDefendMode>
      PartitionInfoMap;
  const int64_t PART_INFO_MAP_SIZE = 1024 * 1024;

public:
  PartitionInfoContainer()
      : host_(nullptr), partition_info_map_(), pt_operator_(NULL), schema_service_(NULL), is_inited_(false)
  {}
  ~PartitionInfoContainer()
  {}  // no one shall derive from this class
public:
  int init(share::ObPartitionTableOperator* pt_operator, share::schema::ObMultiVersionSchemaService* schema_service,
      ObLeaderCoordinator* host);
  int build_tenant_partition_info(const uint64_t tenant_id);
  int build_partition_group_info(
      const uint64_t tenant_id, const uint64_t partition_entity_id, const int64_t partition_id);
  int get(const uint64_t table_id, const int64_t phy_partition_id, const share::ObPartitionInfo*& partition_info);

private:
  ObLeaderCoordinator* host_;
  PartitionInfoMap partition_info_map_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  bool is_inited_;
};

class ObLeaderCoordinator : public ObILeaderCoordinator, public ObRsReentrantThread {
  friend class ObAdminSwitchReplicaRole;
  friend class ObAllVirtualLeaderStat;
  friend class PartitionInfoContainer;

public:
  static const int64_t WAIT_SWITCH_LEADER_TIMEOUT = 16 * 1000 * 1000;  // 16s
  static const int64_t ALL_PARTITIONS = -1;

  ObLeaderCoordinator();
  virtual ~ObLeaderCoordinator();

  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& srv_rpc_proxy, ObZoneManager& zone_mgr,
      ObRebalanceTaskMgr& rebalance_task_mgr, ObUnitManager& unit_mgr, common::ObServerConfig& config,
      ObDailyMergeScheduler& daily_merge_scheduler);

  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

  void wakeup();
  void stop();

  // this interface is provided to daily merge, no one else shall invoke it anywhere
  virtual int check_daily_merge_switch_leader(
      const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results);
  virtual int coordinate();
  virtual int coordinate_tenants(const common::ObArray<common::ObZone>& excluded_zones,
      const common::ObIArray<common::ObAddr>& excluded_servers, const common::ObIArray<uint64_t>& tenant_ids,
      const bool force = false);
  void set_merge_status(bool is_in_merging)
  {
    is_in_merging_ = is_in_merging;
  }
  // Called by rebalance task executor to switch partition group leader to follower
  // before migrating.
  virtual int coordinate_partition_group(const uint64_t table_id, const int64_t partition_id,
      const common::ObIArray<common::ObAddr>& excluded_servers, const common::ObArray<common::ObZone>& excluded_zones);

  virtual int start_smooth_coordinate();
  virtual int is_doing_smooth_coordinate(bool& is_doing);
  virtual int is_last_switch_turn_succ(bool& is_succ);

  virtual void signal()
  {
    wakeup();
  }
  virtual int get_leader_stat(ServerLeaderStat& leader_stat);

  // for mock test
  virtual int get_tenant_ids(common::ObArray<uint64_t>& tenant_ids);

  virtual common::ObLatch& get_lock()
  {
    return lock_;
  }
  virtual common::ObLatch& get_switch_leader_lock()
  {
    return switch_leader_lock_;
  }
  virtual obrpc::ObSrvRpcProxy& get_rpc_proxy()
  {
    return *srv_rpc_proxy_;
  }

  void set_sys_balanced()
  {
    sys_balanced_ = true;
  }
  static int get_same_tablegroup_partition_entity_ids(share::schema::ObSchemaGetterGuard& schema_guard,
      const uint64_t partition_entity_id, common::ObIArray<uint64_t>& partition_entity_ids);
  int64_t get_schedule_interval() const;
  int get_excluded_zone_array(common::ObIArray<common::ObZone>& excluded_zone_array) const;
  int build_zone_region_score_array(const ObZone& primary_zone,
      common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score_array,
      common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array) const;

private:
  enum LastSwitchTurnStat {
    LAST_SWITCH_TURN_SUCCEED = 0,
    LAST_SWITCH_TURN_FAILED = 1,
    LAST_SWITCH_TURN_LIMITED = 2,
    LAST_SWITCH_TURN_INVALID = 3,
  };

  struct PreSwitchPgInfo {
    PreSwitchPgInfo() : index_(-1)
    {}
    PreSwitchPgInfo(const int64_t index) : index_(index)
    {}
    TO_STRING_KV(K_(index));
    bool is_valid() const
    {
      return index_ >= 0;
    }

    int64_t index_;
  };

  class IPartitionEntity {
  public:
    IPartitionEntity()
    {}
    virtual ~IPartitionEntity()
    {}

  public:
    virtual bool has_self_partition() const = 0;
    virtual uint64_t get_partition_entity_id() const = 0;
    virtual uint64_t get_tablegroup_id() const = 0;
    TO_STRING_KV("has_self_partition", has_self_partition(), "partition_entity_id", get_partition_entity_id(),
        "tablegroup_id", get_tablegroup_id());
  };

  class TablegroupEntity : public IPartitionEntity {
  public:
    TablegroupEntity(const share::schema::ObSimpleTablegroupSchema& tg_schema)
        : IPartitionEntity(), tg_schema_(tg_schema)
    {}
    virtual ~TablegroupEntity()
    {}

  public:
    virtual bool has_self_partition() const override;
    virtual uint64_t get_partition_entity_id() const override;
    virtual uint64_t get_tablegroup_id() const override;

  private:
    const share::schema::ObSimpleTablegroupSchema& tg_schema_;
  };

  class SATableEntity : public IPartitionEntity {
  public:
    SATableEntity(const share::schema::ObSimpleTableSchemaV2& table_schema)
        : IPartitionEntity(), table_schema_(table_schema)
    {}
    virtual ~SATableEntity()
    {}

  public:
    virtual bool has_self_partition() const override
    {
      return table_schema_.has_self_partition();
    }
    virtual uint64_t get_partition_entity_id() const override
    {
      return table_schema_.get_table_id();
    }
    virtual uint64_t get_tablegroup_id() const override
    {
      return table_schema_.get_tablegroup_id();
    }

  private:
    const share::schema::ObSimpleTableSchemaV2& table_schema_;
  };

  struct Partition {
  public:
    typedef common::ObSEArray<common::ObAddr, common::OB_MAX_MEMBER_NUMBER> ServerList;
    typedef common::ObSEArray<obrpc::CandidateStatus, common::OB_MAX_MEMBER_NUMBER> CandidateStatusList;
    Partition() : table_id_(0), tg_id_(0), info_(nullptr), candidates_(), prep_candidates_(), candidate_status_array_()
    {}

    inline bool is_valid() const
    {
      return (nullptr != info_ && info_->is_valid());
    }
    void reuse();
    int check_replica_can_be_elected(const common::ObAddr& server, bool& can_be_elected) const;
    int get_leader(common::ObAddr& leader) const;
    int get_partition_key(common::ObPartitionKey& pkey) const;
    int assign(const Partition& other);

    uint64_t table_id_;
    uint64_t tg_id_;  // set to tenant_id if small tenant
    const share::ObPartitionInfo* info_;
    ServerList candidates_;
    ServerList prep_candidates_;
    CandidateStatusList candidate_status_array_;
    TO_STRING_KV(K_(table_id), K_(tg_id), K_(info), K_(candidates), K_(prep_candidates), K_(candidate_status_array));

  private:
    DISALLOW_COPY_AND_ASSIGN(Partition);
  };

  class PartitionArray : public common::ObFixedArrayImpl<Partition, common::ObIAllocator> {
  public:
    PartitionArray(common::ObIAllocator& allocator)
        : common::ObFixedArrayImpl<Partition, common::ObIAllocator>(allocator),
          primary_zone_(),
          tablegroup_id_(OB_INVALID_ID),
          anchor_pos_(-1),
          advised_leader_()
    {}
    ~PartitionArray()
    {}  // no one shall derive from this
  public:
    inline void set_primary_zone(const ObZone& zone)
    {
      primary_zone_ = zone;
    }
    inline void set_tablegroup_id(const uint64_t tg_id)
    {
      tablegroup_id_ = tg_id;
    }
    inline const ObZone& get_primary_zone() const
    {
      return primary_zone_;
    }
    inline uint64_t get_tablegroup_id() const
    {
      return tablegroup_id_;
    }
    inline void set_anchor_pos(const int64_t anchor_pos)
    {
      anchor_pos_ = anchor_pos;
    }
    inline int64_t get_anchor_pos() const
    {
      return anchor_pos_;
    }
    inline void set_advised_leader(const common::ObAddr& leader)
    {
      advised_leader_ = leader;
    }
    inline const common::ObAddr& get_advised_leader() const
    {
      return advised_leader_;
    }

  private:
    common::ObZone primary_zone_;
    uint64_t tablegroup_id_;
    int64_t anchor_pos_;
    common::ObAddr advised_leader_;
  };

  // new leader strategy
  struct ObPartitionMsg : public common::ObDLinkBase<ObPartitionMsg> {
    ObPartitionMsg() : partition_msg_(nullptr), next_part_(nullptr)
    {}
    virtual ~ObPartitionMsg()
    {}
    Partition* partition_msg_;
    // Count that all replicas on the server are the leader's partition
    ObPartitionMsg* next_part_;
    TO_STRING_KV(K_(partition_msg), K_(next_part));
  };
  typedef common::ObDList<ObPartitionMsg> ObPartitionMsgList;
  // Record the leader information on the server
  struct ObServerLeaderMsg {
    ObServerLeaderMsg() : curr_leader_cnt_(0), leader_partition_list_(), zone_()
    {}
    virtual ~ObServerLeaderMsg()
    {}
    int curr_leader_cnt_;
    ObPartitionMsgList leader_partition_list_;
    common::ObZone zone_;
    TO_STRING_KV(K_(curr_leader_cnt), K_(zone), K_(leader_partition_list));
  };

  // Record which server the leader of the partition is finally switched to
  struct ObPartitionSwitchMsg {
    ObPartitionSwitchMsg() : original_leader_addr_(), final_leader_addr_()
    {}
    virtual ~ObPartitionSwitchMsg()
    {}
    common::ObAddr original_leader_addr_;
    common::ObAddr final_leader_addr_;
    TO_STRING_KV(K_(original_leader_addr), K_(final_leader_addr));
  };

  struct ObServerMsg {
    ObServerMsg() : self_addr_(), head_(nullptr), partition_cnt_(0), parent_node_(nullptr)
    {}
    virtual ~ObServerMsg()
    {}
    common::ObAddr self_addr_;
    ObPartitionMsg* head_;  // partition msg info
    int64_t partition_cnt_;
    ObServerMsg* parent_node_;  // parent server info
    TO_STRING_KV(K_(self_addr), K_(partition_cnt), K_(head), K_(parent_node));
  };
  // Count each server, which partition leaders are on the server
  typedef common::hash::ObHashMap<common::ObAddr, ObServerLeaderMsg*> ObAllServerLeaderMsg;
  // Count which server the leader of the partition was finally switched to
  typedef common::hash::ObHashMap<common::ObPartitionKey, ObPartitionSwitchMsg*> ObAllPartitionSwitchMsg;
  // Record all nodes and their child nodes
  typedef common::hash::ObHashMap<common::ObAddr, ObServerMsg*> ObSerMapInfo;

  struct LcBgPair {
    LcBgPair() : bg_array_idx_(-1), in_group_idx_(-1)
    {}
    LcBgPair(const int64_t bg_array_idx, const int64_t in_group_idx)
        : bg_array_idx_(bg_array_idx), in_group_idx_(in_group_idx)
    {}
    TO_STRING_KV(K_(bg_array_idx), K_(in_group_idx));
    bool is_valid() const
    {
      return bg_array_idx_ >= 0 && in_group_idx_ >= 0;
    }
    // bg_array_idx_ position index in the array of tenant partition
    int64_t bg_array_idx_;
    // Position index in its corresponding balance group
    int64_t in_group_idx_;
  };

  struct LcBalanceGroupInfo {
    LcBalanceGroupInfo(common::ObIAllocator& allocator) : pg_idx_array_(allocator), pg_cnt_(0), balance_group_id_(-1)
    {}
    TO_STRING_KV(K_(pg_cnt), K_(balance_group_id));

    // pg_idx_array_ records the index of this pg group in tenant_partition_array
    common::ObFixedArrayImpl<LcBgPair, common::ObIAllocator> pg_idx_array_;
    // Save the pg cnt in the balance group as a defensive check
    int64_t pg_cnt_;
    int64_t balance_group_id_;
  };

  struct ServerReplicaCounter {
  public:
    ServerReplicaCounter() : full_replica_cnt_(0), leader_cnt_(0), zone_()
    {}

    TO_STRING_KV(K_(full_replica_cnt), K_(leader_cnt), K_(zone));

    int64_t full_replica_cnt_;
    int64_t leader_cnt_;
    common::ObZone zone_;
  };

  struct ZoneReplicaCounter {
  public:
    ZoneReplicaCounter()
        : max_full_replica_cnt_(0),
          // The maximum number of full replicas on the server in the zone
          min_full_replica_cnt_(INT64_MAX),
          // The smallest number of full replicas on the server in the zone
          max_leader_cnt_(0),
          // Maximum number of leaders on the server in the zone (actual distribution)
          min_leader_cnt_(INT64_MAX),
          // smallest number of leaders on the server in the zone (actual distribution)
          max_exp_leader_cnt_(0),
          // The current maximum number of leaders on the server in the zone (expected distribution)
          min_exp_leader_cnt_(INT64_MAX),
          // The current smallest number of leaders on the server in the zone (expected distribution)
          max_leader_server_cnt_(0),
          // The number of servers whose leader number is max in the zone
          min_leader_server_cnt_(INT64_MAX),
          // The number of servers whose leader number is min in the zone
          curr_max_leader_server_cnt_(0),
          // The number of servers currently exceeding the maximum number of leaders in the zone
          total_leader_cnt_(0),     // in the zone
          expected_leader_cnt_(0),  // in the zone
          available_server_cnt_(0),
          seed_(-1)
    {}

    TO_STRING_KV(K_(max_full_replica_cnt), K_(min_full_replica_cnt), K_(max_leader_cnt), K_(min_leader_cnt),
        K_(max_exp_leader_cnt), K_(min_exp_leader_cnt), K_(max_leader_server_cnt), K_(min_leader_server_cnt),
        K_(curr_max_leader_server_cnt), K_(total_leader_cnt), K_(expected_leader_cnt), K_(available_server_cnt));

    int64_t max_full_replica_cnt_;
    int64_t min_full_replica_cnt_;
    int64_t max_leader_cnt_;
    int64_t min_leader_cnt_;
    int64_t max_exp_leader_cnt_;
    int64_t min_exp_leader_cnt_;
    int64_t max_leader_server_cnt_;
    int64_t min_leader_server_cnt_;
    int64_t curr_max_leader_server_cnt_;
    int64_t total_leader_cnt_;
    int64_t expected_leader_cnt_;
    int64_t available_server_cnt_;
    int64_t seed_;
  };

  class ServerReplicaMsgContainer {
  public:
    typedef common::hash::ObHashMap<common::ObAddr, ServerReplicaCounter, common::hash::NoPthreadDefendMode>
        ServerReplicaCounterMap;
    typedef common::hash::ObHashMap<common::ObZone, ZoneReplicaCounter, common::hash::NoPthreadDefendMode>
        ZoneReplicaCounterMap;

  public:
    ServerReplicaMsgContainer(common::ObIAllocator& allocator, ObLeaderCoordinator& host);
    virtual ~ServerReplicaMsgContainer();

  public:
    int init(const int64_t pg_cnt);
    int check_and_build_available_zones_and_servers(const int64_t balance_group_id,
        const common::ObIArray<common::ObZone>& primary_zone_array, const common::ObIArray<share::ObUnit>& tenant_unit,
        bool& need_balance);
    int collect_replica(Partition* partition, const share::ObPartitionReplica& replica);
    int check_need_balance_by_leader_cnt(bool& need_balance);
    int build_server_expect_leader_cnt();
    const common::ObIArray<common::ObZone>& get_valid_zone_array() const
    {
      return valid_zone_array_;
    }
    common::ObIArray<common::ObZone>& get_valid_zone_array()
    {
      return valid_zone_array_;
    }
    const ObAllServerLeaderMsg& get_server_leader_info() const
    {
      return server_leader_info_;
    }
    ObAllServerLeaderMsg& get_server_leader_info()
    {
      return server_leader_info_;
    }
    const ServerReplicaCounterMap& get_server_replica_counter_map() const
    {
      return server_replica_counter_map_;
    }
    ServerReplicaCounterMap& get_server_replica_counter_map()
    {
      return server_replica_counter_map_;
    }
    const ZoneReplicaCounterMap& get_zone_replica_counter_map() const
    {
      return zone_replica_counter_map_;
    }
    ZoneReplicaCounterMap& get_zone_replica_counter_map()
    {
      return zone_replica_counter_map_;
    }

  private:
    int check_and_build_available_servers(
        const int64_t balance_group_id, const common::ObIArray<share::ObUnit>& tenant_unit, bool& need_balance);
    int check_and_build_available_zones(const int64_t balance_group_id,
        const common::ObIArray<common::ObZone>& primary_zone_array, const common::ObIArray<share::ObUnit>& tenant_unit,
        bool& need_balance);
    int check_and_build_available_servers(const int64_t balance_group_id,
        const common::ObIArray<common::ObZone>& primary_zone_array, const common::ObIArray<share::ObUnit>& tenant_unit,
        bool& need_balance);
    void destroy_and_free_server_leader_msg(ObServerLeaderMsg* msg);

  private:
    common::ObIAllocator& inner_allocator_;
    ObLeaderCoordinator& host_;
    ObAllServerLeaderMsg server_leader_info_;
    ServerReplicaCounterMap server_replica_counter_map_;
    ZoneReplicaCounterMap zone_replica_counter_map_;
    common::ObArray<common::ObZone> valid_zone_array_;
    int64_t valid_zone_total_leader_cnt_;
    int64_t pg_cnt_;
    bool inited_;
  };

  class LcBalanceGroupContainer : public balancer::ObLeaderBalanceGroupContainer {
  public:
    typedef common::hash::ObHashMap<int64_t, LcBalanceGroupInfo*, common::hash::NoPthreadDefendMode> LcBgInfoMap;
    typedef common::hash::ObHashMap<common::ObPartitionKey, int64_t, common::hash::NoPthreadDefendMode> PkeyIndexMap;

  public:
    LcBalanceGroupContainer(ObLeaderCoordinator& host, share::schema::ObSchemaGetterGuard& schema_guard,
        balancer::ITenantStatFinder& stat_finder, common::ObIAllocator& allocator);
    virtual ~LcBalanceGroupContainer();

  public:
    int init(const uint64_t tenant_id);
    int build_base_info();
    int collect_balance_group_array_index(const common::ObIArray<PartitionArray*>& tenant_partition);
    int build_balance_groups_leader_info(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
        const common::ObIArray<PartitionArray*>& tenant_partition, const common::ObIArray<share::ObUnit>& tenant_unit);
    int check_need_balance_by_leader_cnt(ServerReplicaMsgContainer& server_replica_msg_container, bool& need_balance);
    int generate_leader_balance_plan(ServerReplicaMsgContainer& server_replica_msg_container,
        LcBalanceGroupInfo& bg_info, const common::ObIArray<PartitionArray*>& tenant_partition);

  private:
    int build_valid_zone_locality_map(const common::ObIArray<common::ObZone>& primary_zone_array,
        const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
        common::hash::ObHashMap<common::ObZone, const share::ObZoneReplicaAttrSet*>& map);
    int check_need_balance_by_locality_distribution(const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
        const common::ObIArray<common::ObZone>& primary_zone_array, PartitionArray& partition_array,
        ServerReplicaMsgContainer& server_replica_msg_container, bool& need_rebalance);
    int collect_balance_group_single_index(const int64_t balance_group_id, const int64_t bg_array_idx,
        const balancer::HashIndexMapItem& balance_group_item);
    int build_single_bg_leader_info(share::schema::ObSchemaGetterGuard& schema_guard,
        const common::ObIArray<common::ObZone>& excluded_zone_array, const uint64_t tenant_id,
        LcBalanceGroupInfo& bf_info, const common::ObIArray<PartitionArray*>& tenant_partition,
        const common::ObIArray<share::ObUnit>& tenant_unit);
    int get_pg_locality_and_valid_primary_zone(const PartitionArray& pa,
        share::schema::ObSchemaGetterGuard& schema_guard, const common::ObPartitionKey& pkey,
        const common::ObZone& sample_primary_zone, common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
        bool& need_balance);
    int get_balance_group_valid_primary_zone(const common::ObIArray<PartitionArray*>& tenant_partition,
        const common::ObIArray<common::ObZone>& excluded_zone_array, LcBalanceGroupInfo& bg_info,
        common::ObSEArray<common::ObZone, 7>& primary_zone, common::ObZone& sample_zone);
    int do_generate_leader_balance_plan(ObAllServerLeaderMsg& server_leader_info,
        ObAllPartitionSwitchMsg& switch_plan_map,
        ServerReplicaMsgContainer::ServerReplicaCounterMap& server_replica_counter_map,
        ServerReplicaMsgContainer::ZoneReplicaCounterMap& zone_replica_counter_map);
    int get_need_balance_addr(
        ObAllServerLeaderMsg& server_leader_info, ObIArray<common::ObAddr>& addr_arr, int64_t& total_partition_cnt);

  private:
    const int64_t BG_INFO_MAP_SIZE = 1024L * 1024L;
    ObLeaderCoordinator& host_;
    common::hash::ObHashMap<int64_t, LcBalanceGroupInfo*, common::hash::NoPthreadDefendMode> bg_info_map_;
    common::hash::ObHashMap<common::ObPartitionKey, int64_t, common::hash::NoPthreadDefendMode>
        pkey_to_tenant_partition_map_;
    // The memory of bg_info_map_ element is managed by itself,
    // try to save write memory.
    common::ObFIFOAllocator inner_allocator_;
    bool inited_;
  };

  class NewLeaderStrategy {
  public:
    NewLeaderStrategy(ObIAllocator& allocator, ObAllServerLeaderMsg& all_server_leader_msg,
        ObAllPartitionSwitchMsg& all_part_switch_msg,
        ServerReplicaMsgContainer::ServerReplicaCounterMap& server_replica_counter_map,
        ServerReplicaMsgContainer::ZoneReplicaCounterMap& zone_replica_counter_map_);

    virtual ~NewLeaderStrategy();

  public:
    virtual int execute();
    int init();

  private:
    int get_sorted_server(ObIArray<common::ObAddr>& server_arr);
    int switch_to_exp_max_leader(const ObIArray<common::ObAddr>& server_arr);
    int execute_new_leader_strategy(const common::ObAddr& addr, const bool to_max_exp_leader, bool& is_balance);
    int check_do_need_execute_new_strategy(ObServerLeaderMsg* server_leader_msg, bool& can_do_execute);
    int check_can_switch_leader(ObServerLeaderMsg* leader_server_leader_msg,
        ObServerLeaderMsg* follower_server_leader_msg, const share::ObPartitionReplica& partition_replica,
        bool& can_switch_leader);
    int check_can_do_switch_leader(ObServerLeaderMsg* leader_server_leader_msg,
        ObServerLeaderMsg* follower_server_leader_msg, const common::ObZone& zone, bool& can_do_switch);
    int push_first_server_in_map(const common::ObAddr& original_addr);
    int do_one_round_leader_switch(ObSerMapInfo& parent_map_info, ObSerMapInfo& children_map_info,
        const ObAddr& original_addr, const bool to_max_exp_leader, bool& has_switch_leader, bool& is_balance);
    int first_traversal_server_leader_info(ObServerMsg* server_msg, const common::ObAddr& original_addr,
        const bool to_max_exp_leader, bool& has_switch_leader, bool& is_balance);
    int second_traversal_server_leader_info(ObSerMapInfo& parent_map_info, ObSerMapInfo& children_map_info,
        const common::ObAddr& original_addr, ObServerMsg* parent_server_msg);
    int check_leader_can_switch(ObServerLeaderMsg* leader_server_leader_msg, ObPartitionMsg* partition_leader_msg,
        bool& has_switch_leader, ObServerMsg* server_msg);
    int change_server_leader_info(ObServerLeaderMsg* follower_server_leader_msg, ObPartitionMsg* partition_leader_msg,
        ObServerLeaderMsg* leader_server_leader_msg, const common::ObAddr& follower_addr);
    int change_parent_leader_info(ObServerMsg* server_msg);
    int erase_addr(const ObIArray<ObServerMsg*>& erase_addr_arr);
    int get_erase_addr(
        ObSerMapInfo& parent_map_info, ObIArray<ObServerMsg*>& erase_addr_arr, const common::ObAddr& original_addr);
    int change_partition_leader_address(ObServerLeaderMsg* follower_server_leader_msg,
        ObServerLeaderMsg* leader_server_leader_msg, ObPartitionMsg* partition_leader_msg,
        const common::ObAddr& follower_addr);
    int update_partition_switch_info(
        ObPartitionKey& leader_partition_key, ObPartitionMsg* partition_leader_msg, const ObAddr& follower_addr);
    int update_zone_replica_counter(
        ObServerLeaderMsg* leader_server_leader_msg, ObServerLeaderMsg* follower_server_leader_msg);
    int check_server_already_balance(const common::ObAddr& addr, const bool to_max_exp_leader, bool& is_balance);
    int update_server_map(ObPartitionMsg* partition_leader_msg, ObServerMsg* parent_server_msg,
        ObSerMapInfo& parent_map_info, ObSerMapInfo& children_map_info, const ObAddr& curr_leader_addr);
    int do_update_server_map(const common::ObAddr& follower_addr, ObPartitionMsg* partition_leader_msg,
        ObServerMsg* parent_server_msg, ObSerMapInfo& parent_map_info, ObSerMapInfo& children_map_info);
    int do_update_children_server_map(ObSerMapInfo& children_map_info, ObPartitionMsg* partition_leader_msg,
        ObServerMsg* parent_server_msg, const common::ObAddr& follower_addr);
    int do_map_reuse(const ObAddr& addr, ObSerMapInfo& map_info);
    int all_map_reuse();

  private:
    ObIAllocator& allocator_;
    ObAllServerLeaderMsg& all_server_leader_msg_;  // all server
    ObAllPartitionSwitchMsg& all_part_switch_msg_;
    // valid server
    ServerReplicaMsgContainer::ServerReplicaCounterMap& server_replica_counter_map_;
    // valid zone. eg:turn and merging z1, the value at this time contains z2 and z3.
    ServerReplicaMsgContainer::ZoneReplicaCounterMap& zone_replica_counter_map_;
    ObSerMapInfo global_map_info_;  // Record push server information
    // Record server information of each layer
    ObSerMapInfo first_local_map_info_;
    ObSerMapInfo second_local_map_info_;
    common::ObArenaAllocator self_allocator_;
    bool inited_;
  };

  class WaitElectionCycleIdling : public ObThreadIdling {
  public:
    explicit WaitElectionCycleIdling(volatile bool& stop) : ObThreadIdling(stop)
    {}
    virtual ~WaitElectionCycleIdling()
    {}

  public:
    static const int64_t ELECTION_CYCLE = 1500000;  // 1.5s
    virtual int64_t get_idle_interval_us()
    {
      return ELECTION_CYCLE;
    }
  };

  class ExpectedLeaderWaitOperator {
  public:
    typedef common::hash::ObHashMap<common::ObAddr, int64_t, common::hash::NoPthreadDefendMode> ServerLeaderCounter;
    static const int64_t MAX_SERVER_CNT = 256;

  public:
    ExpectedLeaderWaitOperator(share::ObPartitionTableOperator& pt_operator, common::ObMySQLProxy& mysql_proxy,
        volatile bool& stop, ObLeaderCoordinator& host)
        : sys_leader_waiter_(pt_operator, stop),
          user_leader_waiter_(mysql_proxy, stop),
          sys_tg_expected_leaders_(),
          non_sys_tg_expected_leaders_(),
          new_leader_counter_(),
          old_leader_counter_(),
          inited_(false),
          tenant_id_(OB_INVALID_ID),
          accumulate_cnt_(0),
          idling_(stop),
          host_(host),
          wait_ret_(common::OB_SUCCESS)
    {}
    virtual ~ExpectedLeaderWaitOperator()
    {}

  public:
    int init(const uint64_t tenant_id);
    int check_can_switch_more_leader(
        const common::ObAddr& new_leader, const common::ObAddr& old_leader, bool& can_switch);
    int check_can_switch_more_new_leader(const common::ObAddr& new_leader, bool& can_switch);
    int check_can_switch_more_old_leader(const common::ObAddr& old_leader, bool& can_switch);
    int wait();
    int finally_wait();
    int append_expected_leader(
        share::ObLeaderElectionWaiter::ExpectedLeader& expected_leader, const bool is_sys_tg_partition);
    int get_wait_ret() const;

  private:
    int64_t generate_wait_leader_timeout(const bool is_sys);

  private:
    static const int64_t WAIT_LEADER_US_PER_PARTITION = 10 * 1000;  // 10ms
    int get_non_sys_part_leader_cnt(const common::ObIArray<common::ObZone>& zones, int64_t& leader_cnt);
    int do_get_non_sys_part_leader_cnt(
        const uint64_t sql_tenant_id, const common::ObIArray<common::ObZone>& zones, int64_t& leader_cnt);
    int do_wait();

  private:
    share::ObLeaderElectionWaiter sys_leader_waiter_;
    share::ObUserPartitionLeaderWaiter user_leader_waiter_;
    common::ObArray<share::ObLeaderElectionWaiter::ExpectedLeader> sys_tg_expected_leaders_;
    common::ObArray<share::ObLeaderElectionWaiter::ExpectedLeader> non_sys_tg_expected_leaders_;
    ServerLeaderCounter new_leader_counter_;
    ServerLeaderCounter old_leader_counter_;
    bool inited_;
    uint64_t tenant_id_;
    int64_t accumulate_cnt_;
    WaitElectionCycleIdling idling_;
    ObLeaderCoordinator& host_;
    int wait_ret_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ExpectedLeaderWaitOperator);
  };
  class GetLeaderCandidatesAsyncV2Operator {
  public:
    struct PartIndex {
      PartIndex() : first_level_idx_(-1), second_level_idx_(-1)
      {}
      PartIndex(const int64_t first_level_idx, const int64_t second_level_idx)
          : first_level_idx_(first_level_idx), second_level_idx_(second_level_idx)
      {}
      bool is_valid() const
      {
        return first_level_idx_ >= 0 && second_level_idx_ >= 0;
      }
      TO_STRING_KV(K_(first_level_idx), K_(second_level_idx));
      int64_t first_level_idx_;
      int64_t second_level_idx_;
    };
    struct PartIndexInfo {
      PartIndexInfo() : part_index_array_(), dest_server_(), arg_()
      {}
      TO_STRING_KV(K_(part_index_array), K_(dest_server), K_(arg));
      void reuse()
      {
        part_index_array_.reuse();
        dest_server_.reset();
        arg_.reuse();
      }
      common::ObArray<PartIndex> part_index_array_;
      common::ObAddr dest_server_;
      obrpc::ObGetLeaderCandidatesV2Arg arg_;
    };
    typedef int (obrpc::ObSrvRpcProxy::*AsyncRpcFunc)(const obrpc::ObGetLeaderCandidatesV2Arg&,
        obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_GET_LEADER_CANDIDATES_ASYNC_V2>*, const obrpc::ObRpcOpts&);
    typedef common::ObArray<PartIndex> PartIndexArray;
    typedef common::hash::ObHashMap<common::ObAddr, int64_t, common::hash::NoPthreadDefendMode> IndexMap;

  public:
    GetLeaderCandidatesAsyncV2Operator(
        obrpc::ObSrvRpcProxy& rpc_proxy, const AsyncRpcFunc& async_rpc_func, common::ObServerConfig& config)
        : allocator_(ObModIds::OB_RS_LEADER_COORDINATOR_PA),
          async_proxy_(rpc_proxy, async_rpc_func),
          candidate_info_array_(allocator_),
          part_index_matrix_(),
          index_map_(),
          config_(config),
          send_requests_cnt_(0),
          inited_(false)
    {}
    ~GetLeaderCandidatesAsyncV2Operator()
    {}  // no one will derive from this class
  public:
    int init();
    void clear_after_wait();
    bool reach_accumulate_threshold(const PartIndexInfo& info) const;
    bool reach_send_to_wait_threshold() const;
    int process_request(const common::ObPartitionKey& pkey, const common::ObAddr& dest_server,
        common::ObArray<common::ObAddr>& prep_candidates, const int64_t first_part_idx, const int64_t second_part_idx,
        common::ObIArray<PartitionArray*>& partition_arrays);
    int process_request_server_exist(const int64_t ci_array_idx, const common::ObPartitionKey& pkey,
        const common::ObAddr& dest_server, common::ObArray<common::ObAddr>& prep_candidates,
        const int64_t first_part_idx, const int64_t second_part_idx,
        common::ObIArray<PartitionArray*>& partition_arrays);
    int process_request_server_not_exist(const common::ObPartitionKey& pkey, const common::ObAddr& dest_server,
        common::ObArray<common::ObAddr>& prep_candidates, const int64_t first_part_idx, const int64_t second_part_idx,
        common::ObIArray<PartitionArray*>& partition_arrays);
    int accumulate_request(common::ObArray<common::ObAddr>& prep_candidates, const common::ObAddr& dest_server,
        PartIndexInfo& part_index_info, const common::ObPartitionKey& pkey, const int64_t first_part_idx,
        const int64_t second_part_idx);
    int send_requests(PartIndexInfo& part_index_info, const common::ObAddr& dest_server,
        common::ObIArray<PartitionArray*>& partition_arrays);
    int finally_send_requests(common::ObIArray<PartitionArray*>& partition_arrays);
    int wait();
    int fill_partitions_leader_candidates(common::ObIArray<PartitionArray*>& partition_arrays);

  private:
    static const int64_t MAX_CANDIDATE_INFO_ARRAY_SIZE = 1024;
    static const int64_t ACCUMULATE_THRESHOLD = 1024;  // batch process threshold
    static const int64_t SEND_TO_WAIT_THRESHOLD = 32;  // cnt of send_requests() before wait()
    // The rpc timeout of get lead candidate is the configuration item get_leader_candidate_rpc_timeout
    // Its value range is [2s, 180s], When the read value is not within this value range,
    // using RPC_TIMEOUT = 9s
    static const int64_t RPC_TIMEOUT = 9L * 1000000L;
    static const int64_t MIN_RPC_TIMEOUT = 2L * 1000000L;
    static const int64_t MAX_RPC_TIMEOUT = 180L * 1000000L;

  private:
    common::ObArenaAllocator allocator_;
    ObGetLeaderCandidatesAsyncProxyV2 async_proxy_;
    // max count 1024
    common::ObFixedArrayImpl<PartIndexInfo, common::ObIAllocator> candidate_info_array_;
    common::ObArray<PartIndexArray> part_index_matrix_;
    IndexMap index_map_;  // addr->candidate_info_array_ subscripted map
    common::ObServerConfig& config_;
    int64_t send_requests_cnt_;
    bool inited_;
  };

  class SwitchLeaderListAsyncOperator {
    typedef int (obrpc::ObSrvRpcProxy::*AsyncRpcFunc)(const obrpc::ObSwitchLeaderListArg&,
        obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_SWITCH_LEADER_LIST_ASYNC>*, const obrpc::ObRpcOpts&);

  public:
    SwitchLeaderListAsyncOperator(
        obrpc::ObSrvRpcProxy& rpc_proxy, const AsyncRpcFunc& async_rpc_func, common::ObServerConfig& config)
        : async_proxy_(rpc_proxy, async_rpc_func),
          arg_(),
          is_sys_tg_partitions_(),
          config_(config),
          send_requests_cnt_(0),
          saved_cur_leader_(),
          saved_new_leader_()
    {}
    ~SwitchLeaderListAsyncOperator()
    {}  // no one will derive from this class
  public:
    void reuse();
    bool reach_accumulate_threshold() const;
    bool reach_send_to_wait_threshold() const;
    int accumulate_request(const common::ObPartitionKey& pkey, const bool is_sys_tg_partition);
    int send_requests(
        common::ObAddr& dest_server, common::ObAddr& dest_leader, ExpectedLeaderWaitOperator& leader_wait_operator);
    int wait();
    const common::ObAddr& get_saved_cur_leader() const
    {
      return saved_cur_leader_;
    }
    const common::ObAddr& get_saved_new_leader() const
    {
      return saved_new_leader_;
    }
    common::ObAddr& get_saved_cur_leader()
    {
      return saved_cur_leader_;
    }
    common::ObAddr& get_saved_new_leader()
    {
      return saved_new_leader_;
    }
    void set_saved_cur_leader(const common::ObAddr& leader)
    {
      saved_cur_leader_ = leader;
    }
    void set_saved_new_leader(const common::ObAddr& leader)
    {
      saved_new_leader_ = leader;
    }

  private:
    static const int64_t ACCUMULATE_THRESHOLD = 128;  // batch process threshold
    static const int64_t SEND_TO_WAIT_THRESHOLD = 4;  // cnt of send_requests() before wait()
    static const int64_t RPC_TIMEOUT = 9L * 1000000L;

  private:
    ObSwitchLeaderListAsyncProxy async_proxy_;
    obrpc::ObSwitchLeaderListArg arg_;
    common::ObArray<bool> is_sys_tg_partitions_;
    common::ObServerConfig& config_;
    int64_t send_requests_cnt_;
    common::ObAddr saved_cur_leader_;
    common::ObAddr saved_new_leader_;
  };
  struct CandidateZoneInfo {
  public:
    CandidateZoneInfo() : zone_(), region_score_(INT64_MAX), candidate_count_(0)
    {}
    TO_STRING_KV(K_(zone), K_(region_score), K_(candidate_count));
    common::ObZone zone_;
    int64_t region_score_;
    int64_t candidate_count_;
  };

  struct CandidateLeaderInfo {
    common::ObAddr server_addr_;
    common::ObZone zone_;

    int64_t balance_group_score_;
    int64_t original_leader_count_;
    int64_t cur_leader_count_;
    int64_t zone_candidate_count_;
    int64_t candidate_count_;
    int64_t in_normal_unit_count_;
    int64_t zone_migrate_out_or_transform_count_;
    int64_t migrate_out_or_transform_count_;
    share::ObRawPrimaryZoneUtil::ZoneScore zone_score_;
    share::ObRawPrimaryZoneUtil::RegionScore region_score_;
    bool is_candidate_;  // true when neither server nor zone has stop
    bool not_merging_;
    bool not_excluded_;  // true when server or zone is not excluded
    // random_score_ is introduced for the average distribution of the leader in the same tenant,
    // random_score_ is generated by RandomZoneSelector, the generated seed is
    // tablegroup_id and partition_id
    int64_t random_score_;
    // Read the observer's start_service_time_,
    // the priority is placed after not_merging_
    bool start_service_;
    int64_t in_revoke_blacklist_count_;
    int64_t partition_id_;

    CandidateLeaderInfo();
    virtual ~CandidateLeaderInfo()
    {}
    void reset();

    TO_STRING_KV(K_(server_addr), K_(zone), K_(balance_group_score), K_(region_score), K_(not_merging),
        K_(start_service), K_(zone_candidate_count), K_(candidate_count), K_(is_candidate), K_(not_excluded),
        K_(migrate_out_or_transform_count), K_(zone_migrate_out_or_transform_count), K_(in_normal_unit_count),
        K_(zone_score), K_(original_leader_count), K_(random_score), K_(cur_leader_count),
        K_(in_revoke_blacklist_count), K_(partition_id));
  };

  class ChooseLeaderCmp {
  public:
    ChooseLeaderCmp(const uint64_t tenant_id, const uint64_t tablegroup_id)
        : tenant_id_(tenant_id), tablegroup_id_(tablegroup_id), ret_(common::OB_SUCCESS)
    {}
    ~ChooseLeaderCmp()
    {}

  public:
    bool operator()(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    int get_ret() const
    {
      return ret_;
    }

  public:
    bool cmp_region_score(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_not_merging(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_start_service(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_candidate_cnt(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_in_revoke_blacklist(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_not_excluded(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_migrate_out_cnt(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_in_normal_unit(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_primary_zone(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_balance_group_score(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_original_leader(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_random_score(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);
    bool cmp_server_addr(const CandidateLeaderInfo& left, const CandidateLeaderInfo& right);

  private:
    const uint64_t tenant_id_;
    const uint64_t tablegroup_id_;
    int ret_;
  };

  struct CandidateZoneInfoCmp {
  public:
    CandidateZoneInfoCmp() : ret_(common::OB_SUCCESS)
    {}
    ~CandidateZoneInfoCmp()
    {}

  public:
    bool operator()(const CandidateZoneInfo& left, const CandidateZoneInfo& right);
    int get_ret()
    {
      return ret_;
    }
    bool same_level(const CandidateZoneInfo& left, const CandidateZoneInfo& right);

  private:
    int64_t cmp_candidate_cnt(const int64_t left_cnt, const int64_t right_cnt);

  private:
    int ret_;
  };

  struct PartitionArrayCursor {
    PartitionArrayCursor()
        : part_idx_(0), part_cnt_(0), array_idx_(0), cur_leader_(), advised_leader_(), ignore_switch_percent_(false)
    {}
    PartitionArrayCursor(int64_t part_idx, int64_t part_cnt, int64_t array_idx, const common::ObAddr& cur_leader,
        const common::ObAddr& advised_leader, bool ignore_switch_percent)
        : part_idx_(part_idx),
          part_cnt_(part_cnt),
          array_idx_(array_idx),
          cur_leader_(cur_leader),
          advised_leader_(advised_leader),
          ignore_switch_percent_(ignore_switch_percent)
    {}
    TO_STRING_KV(
        K(part_idx_), K(part_cnt_), K(array_idx_), K(cur_leader_), K(advised_leader_), K(ignore_switch_percent_));
    bool switch_finish()
    {
      return part_idx_ >= part_cnt_;
    }
    bool switch_start()
    {
      return part_idx_ > 0;
    }
    bool has_partition_not_handle() const
    {
      return part_idx_ < part_cnt_;
    }
    bool operator<(const PartitionArrayCursor& that) const;
    int64_t part_idx_;
    int64_t part_cnt_;
    int64_t array_idx_;
    common::ObAddr cur_leader_;
    common::ObAddr advised_leader_;
    bool ignore_switch_percent_;
  };
  struct CursorContainer {
  public:
    CursorContainer() : cursor_array_()
    {}
    virtual ~CursorContainer()
    {}

  public:
    int reorganize();

  public:
    TO_STRING_KV(K(cursor_array_));
    common::ObArray<PartitionArrayCursor> cursor_array_;
  };
  struct CandidateElectionPriority {
    common::ObAddr addr_;
    common::ObPartitionKey pkey_;
    int64_t role_;
    bool is_candidate_;
    int64_t membership_version_;
    uint64_t log_id_;
    uint64_t locality_;
    int64_t sys_score_;
    bool is_tenant_active_;
    bool on_revoke_blacklist_;
    bool on_loop_blacklist_;
    int64_t replica_type_;
    int64_t server_status_;
    bool is_clog_disk_full_;
    bool is_offline_;
    TO_STRING_KV(K_(addr), K_(pkey), K_(role), K_(is_candidate), K_(membership_version), K_(log_id), K_(locality),
        K_(sys_score), K_(is_tenant_active), K_(on_revoke_blacklist), K_(on_loop_blacklist), K_(replica_type),
        K_(server_status), K_(is_clog_disk_full), K_(is_offline));
  };
  typedef common::ObArray<PartitionArray*> TenantPartition;
  typedef common::ObArray<PartitionArray*> TablegroupPartition;
  typedef common::ObArray<share::ObLeaderElectionWaiter::ExpectedLeader> ExpectedLeaderArray;
  typedef common::hash::ObHashMap<common::ObAddr, CandidateLeaderInfo, common::hash::NoPthreadDefendMode>
      RawLeaderInfoMap;
  class CandidateLeaderInfoMap : public RawLeaderInfoMap {
  public:
    CandidateLeaderInfoMap(ObLeaderCoordinator& host) : host_(host)
    {}

  public:
    int locate_candidate_leader_info(const common::ObAddr& advised_leader, const share::ObPartitionReplica& replica,
        const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score,
        const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score,
        const ObRandomZoneSelector& random_selector, const ObZoneList& excluded_zones,
        const common::ObIArray<common::ObAddr>& excluded_servers, CandidateLeaderInfo& candidate_leader_info);

  private:
    int build_candidate_basic_statistic(const common::ObAddr& server_addr, const common::ObAddr& advised_leader,
        const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score,
        const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score,
        const ObRandomZoneSelector& random_selector, const ObZoneList& excluded_zones,
        const common::ObIArray<common::ObAddr>& excluded_servers, CandidateLeaderInfo& candidate_leader_info);
    int build_candidate_server_random_score(const common::ObAddr& server_addr,
        const ObRandomZoneSelector& random_selector, CandidateLeaderInfo& candidate_leader_info);
    int build_candidate_zone_region_score(const common::ObAddr& server_addr,
        const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score_array,
        const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
        CandidateLeaderInfo& candidate_leader_info);
    int build_candidate_server_zone_status(const common::ObAddr& server_addr, const ObZoneList& excluded_zones,
        const common::ObIArray<common::ObAddr>& excluded_servers, CandidateLeaderInfo& info);
    int build_candidate_balance_group_score(
        const common::ObAddr& server_addr, const common::ObAddr& advised_leader, CandidateLeaderInfo& info);

  private:
    ObLeaderCoordinator& host_;
  };
  typedef common::hash::ObHashMap<common::ObZone, int64_t, common::hash::NoPthreadDefendMode> ZoneCandidateCountMap;
  typedef common::hash::ObHashMap<common::ObZone, int64_t, common::hash::NoPthreadDefendMode> RawZoneMigrateCountMap;
  class ZoneMigrateCountMap : public RawZoneMigrateCountMap {
  public:
    ZoneMigrateCountMap() : RawZoneMigrateCountMap()
    {}

  public:
    int locate(const common::ObZone& zone, int64_t& count);
  };
  typedef CandidateLeaderInfoMap::iterator leader_info_iter;
  typedef CandidateLeaderInfoMap::const_iterator const_leader_info_iter;
  typedef common::hash::ObHashMap<common::ObZone, CandidateZoneInfo, common::hash::NoPthreadDefendMode>
      CandidateZoneInfoMap;
  typedef CandidateZoneInfoMap::iterator zone_info_iter;
  typedef CandidateZoneInfoMap::const_iterator const_zone_info_iter;
  typedef common::ObArray<share::ObUnit> TenantUnit;
  class SwitchLeaderStrategy {
  public:
    SwitchLeaderStrategy(ObLeaderCoordinator& leader_coordinator, common::ObArray<PartitionArray*>& partition_arrays,
        CursorContainer& cursor_container)
        : host_(leader_coordinator), partition_arrays_(partition_arrays), cursor_container_(cursor_container)
    {}
    virtual ~SwitchLeaderStrategy()
    {}

  public:
    virtual int execute(
        TenantUnit& tenant_unit, ExpectedLeaderWaitOperator& leader_wait_operator, const bool force) = 0;

  protected:
    int coordinate_partitions_per_tg(TenantUnit& tenant_unit, PartitionArray& partition_array,
        PartitionArrayCursor& cursor, const bool force, ExpectedLeaderWaitOperator& leader_wait_operator,
        SwitchLeaderListAsyncOperator& async_rpc_operator, bool& do_switch_leader);
    int check_before_coordinate_partition(const Partition& partition, const TenantUnit& tenant_unit, const bool force,
        const common::ObPartitionKey& part_key, common::ObAddr& advised_leader, common::ObAddr& cur_leader,
        bool& need_switch);
    int check_tenant_on_server(const TenantUnit& tenant_unit, const common::ObAddr& server, bool& tenant_on_server);

  protected:
    ObLeaderCoordinator& host_;
    common::ObArray<PartitionArray*>& partition_arrays_;
    CursorContainer& cursor_container_;
  };

  class ConcurrentSwitchLeaderStrategy : public SwitchLeaderStrategy {
    enum LinkType {
      START_SWITCH_LIST = 0,
      NOT_START_LIST,
      LINK_TYPE_MAX,
    };
    struct CursorLink : public common::ObDLinkBase<CursorLink> {
      CursorLink() : ObDLinkBase(), cursor_ptr_(NULL)
      {}
      virtual ~CursorLink()
      {}
      PartitionArrayCursor* cursor_ptr_;
    };
    typedef common::ObDList<CursorLink> CursorList;
    struct CursorQueue {
      CursorQueue() : cursor_list_()
      {}
      CursorList cursor_list_[LINK_TYPE_MAX];
      bool empty()
      {
        bool empty = true;
        for (int32_t i = 0; empty && i < LINK_TYPE_MAX; ++i) {
          empty = cursor_list_[i].get_size() <= 0;
        }
        return empty;
      }
    };
    typedef common::hash::ObHashMap<common::ObAddr, CursorQueue*, common::hash::NoPthreadDefendMode> CursorQueueMap;

  public:
    ConcurrentSwitchLeaderStrategy(ObLeaderCoordinator& leader_coordinator,
        common::ObArray<PartitionArray*>& partition_arrays, CursorContainer& cursor_container)
        : SwitchLeaderStrategy(leader_coordinator, partition_arrays, cursor_container),
          cursor_queue_map_(),
          link_node_allocator_()
    {}
    virtual ~ConcurrentSwitchLeaderStrategy()
    {}

  public:
    virtual int execute(TenantUnit& tenant_unit, ExpectedLeaderWaitOperator& leader_wait_operator, const bool force);

  private:
    int init_cursor_queue_map();
    int get_next_cursor_link(ExpectedLeaderWaitOperator& leader_wait_operator, CursorLink*& cursor_link,
        LinkType& link_type, bool& no_leader_pa);
    int get_cursor_link_from_list(ExpectedLeaderWaitOperator& leader_wait_operator, CursorList& cursro_list,
        CursorLink*& cursor_link, bool& no_leader_pa);
    int try_remove_cursor_queue_map(CursorLink* cursor_link, const LinkType link_type);
    bool is_switch_finished();
    int try_reconnect_cursor_queue(CursorLink* cursor_link, const LinkType link_type);

  private:
    CursorQueueMap cursor_queue_map_;
    common::ObArenaAllocator link_node_allocator_;
  };

  int auto_coordinate();
  int get_auto_leader_switch_idle_interval(int64_t& idle_interval_us) const;
  int coordinate_helper(PartitionInfoContainer& partition_info_container,
      const common::ObArray<common::ObZone>& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers,
      const common::ObIArray<uint64_t>& tenant_ids, const bool force = false);

  int move_sys_tenant_to_last(const common::ObIArray<uint64_t>& tenant_ids, common::ObIArray<uint64_t>& new_tenant_ids);

  // get zones active and not merging, if empty, get zones active
  int build_tenant_partition(share::schema::ObSchemaGetterGuard& schema_guard,
      PartitionInfoContainer& partition_info_container, const uint64_t tenant_id,
      common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition);
  int do_build_small_tenant_partition(const uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard,
      const ObIArray<const IPartitionEntity*>& partition_entity_array, PartitionInfoContainer& partition_info_container,
      common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition);
  int do_build_normal_tenant_partition(share::schema::ObSchemaGetterGuard& schema_guard,
      const ObIArray<const IPartitionEntity*>& partition_entity_array, PartitionInfoContainer& partition_info_container,
      common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition);
  // get tenant's partition entitys, sorted by (tg_id, table_id)
  int get_tenant_partition_entity_array(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      common::ObIAllocator& allocator, common::ObArray<const IPartitionEntity*>& partition_entity_array);
  int append_tg_partition(share::schema::ObSchemaGetterGuard& schema_guard,
      PartitionInfoContainer& partition_info_container, const common::ObArray<uint64_t>& table_ids,
      const bool small_tenant, common::ObIAllocator& partition_allocator, TenantPartition& tenant_partition);
  int build_tg_partition(share::schema::ObSchemaGetterGuard& schema_guard,
      PartitionInfoContainer& partition_info_container, const common::ObIArray<uint64_t>& table_ids,
      const bool small_tenant, common::ObIAllocator& partition_allocator, TablegroupPartition& tg_partition,
      const int64_t partition_idx);
  int fill_partition_tg_id(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id,
      const bool small_tenant, Partition& partition);
  int append_to_tg_partition(share::schema::ObSchemaGetterGuard& schema_guard, const bool is_single_partition_group,
      const int64_t partition_idx, TablegroupPartition& tg_partition, const Partition& partition,
      common::ObIAllocator& partition_allocator, const int64_t partition_array_capacity);
  int move_all_core_last(TenantPartition& tenant_partition);
  int get_tenant_unit(const uint64_t tenant_id, TenantUnit& tenant_unit);
  int build_leader_balance_info(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      LcBalanceGroupContainer& balance_group_container, TenantPartition& tenant_partition, TenantUnit& tenant_unit);
  int coordinate_partition_arrays(ExpectedLeaderWaitOperator& leader_wait_operator,
      common::ObArray<PartitionArray*>& partition_arrays, TenantUnit& tenant_unit, CursorContainer& cursor_container,
      const bool force = false);
  int build_single_pg_leader_candidates(GetLeaderCandidatesAsyncV2Operator& async_rpc_operator,
      const int64_t in_array_index, PartitionArray& partitions, common::ObIArray<PartitionArray*>& partition_arrays);
  int get_partition_prep_candidates(const common::ObAddr& leader,
      const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score_array,
      const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array, Partition& partition,
      ObIArray<common::ObAddr>& prep_partitions);
  int get_high_score_regions(const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
      common::ObIArray<common::ObRegion>& high_score_regions);
  static bool prep_candidates_match(
      ObArray<common::ObAddr>& prev_prep_candidates, ObSArray<common::ObAddr>& this_prep_candidates);

  int do_coordinate_partitions(const PartitionArray& partitions, const TenantUnit& tenant_unit,
      const common::ObAddr& advised_leader, const bool force, ExpectedLeaderWaitOperator& leader_wait_operator);
  // check advise leader can be switch, may update advise leader if needed.
  int build_leader_switch_cursor_container(const uint64_t tenant_id, TenantUnit& tenant_unit,
      common::ObArray<PartitionArray*>& partition_arrays, CursorContainer& cursor_array,
      const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers, const bool force);
  int build_pre_switch_pg_index_array(const uint64_t tenant_id, TenantUnit& tenant_unit,
      common::ObArray<PartitionArray*>& partition_arrays, common::ObIArray<PreSwitchPgInfo>& pre_switch_index_array,
      const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers);
  int build_pg_array_leader_candidates(const common::ObIArray<PreSwitchPgInfo>& pre_switch_index_array,
      common::ObArray<PartitionArray*>& partition_arrays);
  int do_build_leader_switch_cursor_container(const uint64_t tenant_id, TenantUnit& tenant_unit,
      const common::ObIArray<PreSwitchPgInfo>& pre_switch_index_array,
      common::ObArray<PartitionArray*>& partition_arrays, const ObZoneList& excluded_zones,
      const common::ObIArray<common::ObAddr>& excluded_servers, CursorContainer& cursor_container);
  // replica's server same with unit server and unit not migrating
  static int check_in_normal_unit(
      const share::ObPartitionReplica& replica, const TenantUnit& tenant_unit, bool& in_normal_unit);
  // misc functions
  int check_cancel() const;
  // remove replicas not alive
  int remove_lost_replicas(share::ObPartitionInfo& partition_info);
  // compare by tuple (tablegroup_id, table_id)
  static bool partition_entity_cmp(const IPartitionEntity* l, const IPartitionEntity* r);
  int get_cur_partition_leader(
      const common::ObIArray<Partition>& partitions, common::ObAddr& leader, bool& has_same_leader);
  int build_partition_statistic(const PartitionArray& partitions, const TenantUnit& tenant_unit,
      const ObZoneList& excluded_zones, const common::ObIArray<common::ObAddr>& excluded_servers,
      CandidateLeaderInfoMap& leader_info_map, bool& is_ignore_switch_percent);
  int update_candidate_leader_info(const Partition& partition, const share::ObPartitionReplica& replica,
      const int64_t replica_index, const TenantUnit& tenant_unit, CandidateLeaderInfo& info,
      int64_t& zone_migrate_out_or_transform_count, bool& is_ignore_switch_percent);
  int choose_leader(const uint64_t tenant_id, const uint64_t tablegroup_id,
      const CandidateLeaderInfoMap& leader_info_map, common::ObIArray<common::ObAddr>& candidate_leaders,
      const common::ObAddr& cur_leader);
  int try_update_switch_leader_event(const uint64_t tenant_id, const uint64_t tg_id, const int64_t partition_id,
      const common::ObAddr& advised_leader, const common::ObAddr& current_leader);
  int choose_server_by_candidate_count(const CandidateLeaderInfoMap& leader_info_map, const ObZoneList& excluded_zones,
      const common::ObIArray<common::ObAddr>& excluded_servers, common::ObIArray<common::ObAddr>& candidate_leaders);
  int update_leader_candidates(const common::ObIArray<Partition>& partitions, CandidateLeaderInfoMap& leader_info_map);
  int update_specific_candidate_statistic(const common::ObAddr& server, const common::ObZone& zone,
      const Partition& partition, CandidateLeaderInfo& info, CandidateLeaderInfoMap& leader_info_map,
      ZoneCandidateCountMap& zone_candidate_count_map);
  int check_in_election_revoke_blacklist(const common::ObAddr& server, const Partition& partition, bool& in_blacklist);

  int count_leader(const PartitionArray& partitions, const bool is_new_leader);
  int count_leader(const Partition& partition, const bool is_new_leader);
  int update_leader_stat();
  virtual uint32_t get_random();
  int fetch_smooth_switch_start_time(int64_t& merge_start_time);
  int alloc_partition_array(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id,
      const int64_t partition_array_capacity, common::ObIAllocator& partition_allocator, PartitionArray*& partitions);
  int get_pg_primary_zone(
      share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id, common::ObZone& primary_zone);
  int calculate_partition_array_capacity(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<uint64_t>& table_ids, const bool small_tenant, const int64_t partition_id,
      int64_t& partition_array_capacity);
  // for daily merge check switch leader
  int get_daily_merge_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int init_daily_merge_switch_leader_result(
      const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results);
  int check_daily_merge_switch_leader_by_tenant(PartitionInfoContainer& partition_info_container,
      const uint64_t tenant_id, const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results);
  int update_daily_merge_switch_leader_result(
      const common::ObIArray<bool>& intermediate_results, common::ObIArray<bool>& final_results);
  int check_daily_merge_switch_leader_need_continue(const common::ObIArray<bool>& results, bool& need_continue);
  int check_dm_pg_switch_leader_by_tenant(TenantPartition& tenant_partition, const uint64_t tenant_id,
      const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results);
  int build_dm_partition_candidates(TenantPartition& tenant_partition);
  int check_daily_merge_switch_leader_by_pg(PartitionArray& partition_array,
      const common::ObIArray<common::ObZone>& zone_list, common::ObIArray<bool>& results);
  void dump_partition_candidates_info(const uint64_t tenant_id, PartitionArray& partition_array);
  void dump_partition_candidates_to_rs_event(const uint64_t tenant_id, const Partition& partition);
  int dump_candidate_info_to_rs_event(const uint64_t tenant_id, const Partition& partition);
  int dump_non_candidate_info_to_rs_event(const uint64_t tenant_id, const Partition& partition);
  int construct_non_candidate_info_sql(const Partition& partition,
      const common::ObIArray<common::ObAddr>& non_candidates, common::ObSqlString& sql_string);
  int do_dump_non_candidate_info_to_rs_event(const Partition& partition,
      const common::ObIArray<common::ObAddr>& non_candidates, const common::ObSqlString& sql_string);
  int get_single_candidate_election_priority(
      sqlclient::ObMySQLResult& result, common::ObIArray<CandidateElectionPriority>& election_priority_array);
  int dump_exist_info_to_rs_event(const common::ObIArray<CandidateElectionPriority>& election_priority_array);
  int dump_non_exist_info_to_rs_event(const Partition& partition,
      const common::ObIArray<CandidateElectionPriority>& election_priority_array,
      const common::ObIArray<common::ObAddr>& non_candidates);
  int build_candidate_zone_info_map(
      const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
      const PartitionArray& partition_array, CandidateZoneInfoMap& candidate_zone_map);
  int update_candidate_zone_info_map(
      const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score_array,
      const common::ObAddr& candidate, CandidateZoneInfoMap& candidate_zone_map);
  int do_check_daily_merge_switch_leader_by_pg(const PartitionArray& partition_array,
      const CandidateZoneInfoMap& candidate_zone_map, const common::ObIArray<common::ObZone>& zone_list,
      common::ObIArray<bool>& results);

private:
  bool inited_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObServerManager* server_mgr_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  ObZoneManager* zone_mgr_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  ObUnitManager* unit_mgr_;
  common::ObServerConfig* config_;
  mutable ObLeaderCoordinatorIdling idling_;
  common::ObLatch lock_;
  ServerLeaderStat leader_stat_;
  common::ObLatch leader_stat_lock_;
  common::ObLatch switch_leader_lock_;
  ObServerSwitchLeaderInfoStat switch_info_stat_;
  ObDailyMergeScheduler* daily_merge_scheduler_;
  LastSwitchTurnStat last_switch_turn_stat_;
  // whether sys tenant balanced
  volatile bool sys_balanced_;
  PartitionInfoContainer partition_info_container_;
  bool is_in_merging_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_LEADER_COORDINATOR_H_
