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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_GTS_MONITOR_H_
#define OCEANBASE_ROOTSERVER_OB_RS_GTS_MONITOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/thread/ob_reentrant_thread.h"
#include "lib/list/ob_dlink_node.h"
#include "common/ob_unit_info.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "share/ob_gts_table_operator.h"
#include "share/ob_gts_info.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace rootserver {

class ObUnitManager;
class ObServerManager;
class ObZoneManager;
class ObRsGtsTaskMgr;

class ObGtsTaskManagerIdling : public ObThreadIdling {
public:
  explicit ObGtsTaskManagerIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  static const int64_t INTERVAL_US = 1000000;
  virtual int64_t get_idle_interval_us()
  {
    return INTERVAL_US;
  }
};

class UnitInfoStruct : public share::ObUnitInfo {
public:
  UnitInfoStruct() : ObUnitInfo(), outside_replica_cnt_(0)
  {}
  virtual ~UnitInfoStruct()
  {}

public:
  int assign(const ObUnitInfo& that)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObUnitInfo::assign(that))) {
      RS_LOG(WARN, "fail to assign base", K(ret));
    }
    return ret;
  }
  int assign(const UnitInfoStruct& that)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObUnitInfo::assign(that))) {
      RS_LOG(WARN, "fail to assign base", K(ret));
    } else {
      outside_replica_cnt_ = that.outside_replica_cnt_;
    }
    return ret;
  }
  int64_t get_outside_replica_cnt() const
  {
    return outside_replica_cnt_;
  }
  void inc_outside_replica_cnt()
  {
    ++outside_replica_cnt_;
  }

private:
  int64_t outside_replica_cnt_;
};

class ObRsGtsInfoCollector {
public:
  ObRsGtsInfoCollector(rootserver::ObUnitManager& unit_mgr)
      : inited_(false),
        sql_proxy_(nullptr),
        gts_table_operator_(),
        gts_info_array_(),
        unit_info_array_(),
        unit_info_map_(),
        unit_mgr_(unit_mgr)
  {}
  virtual ~ObRsGtsInfoCollector()
  {}

public:
  int init(common::ObMySQLProxy* sql_proxy);
  void reuse();
  int gather_stat();
  int get_unit_info(const common::ObAddr& server, UnitInfoStruct*& unit_info);
  const common::ObIArray<common::ObGtsInfo>& get_gts_info_array() const
  {
    return gts_info_array_;
  }
  const common::ObIArray<UnitInfoStruct>& get_unit_info_array() const
  {
    return unit_info_array_;
  }

private:
  typedef common::hash::ObHashMap<common::ObAddr, UnitInfoStruct*, common::hash::NoPthreadDefendMode> UnitInfoMap;
  static const int64_t UNIT_MAP_BUCKET_CNT = 50000;
  static int append_unit_info_array(
      common::ObIArray<UnitInfoStruct>& dst_array, const common::ObIArray<share::ObUnitInfo>& src_array);

private:
  bool inited_;
  common::ObMySQLProxy* sql_proxy_;
  share::ObGtsTableOperator gts_table_operator_;
  common::ObArray<common::ObGtsInfo> gts_info_array_;
  common::ObArray<UnitInfoStruct> unit_info_array_;
  UnitInfoMap unit_info_map_;
  rootserver::ObUnitManager& unit_mgr_;
};

/* 1 these are used to minotor the gts unit status:
 *   1.1 when unit is in migration, we check the unit migration progress by
 *       checking if the gts replicas migration on the unit is finished
 *   1.2 when unit server is offline, and no gts replicas on this unit, migrate the unit out;
 * 2 the logic of the Gts Unit management:
 *   2.1 a gts instance with one or two replicas(only for test) not in discussion
 *   2.2 a gts instance with three replicas, if there are three units, the following situations
 *       need to by processed:
 *       * unit migration: migrate unit from ServerA to ServerB, when ServerB is available,
 *         RS deletes ServerA from memberlist and designated ServerB as standby.
 *       * the server holding the unit collapses: RS will assign a new server when all members
 *         on this unit all removed.
 *   2.3 a gts instance has 3 normal replicas and a standby replica.
 *       * unit migration:migrate unit from ServerA to ServerB, when ServerB is available,
 *         the unit in migration may contain some standby replicas and normal replicas.
 *         do migrate directly
 *       * only the server holding standby collapses: standby will be migrated with the unit,
 *         whatever the server is active or not.
 *       * only servers holding replicas in member list collapse, these replicas in member list
 *         shall be deleted, unit will be migrated out directly
 *       * servers holding replicas in member list and standby replica collapse,
 *         processing according the above principles.
 */
class ObRsGtsUnitDistributor {
public:
  ObRsGtsUnitDistributor(rootserver::ObUnitManager& unit_mgr, rootserver::ObServerManager& server_mgr,
      rootserver::ObZoneManager& zone_mgr, volatile bool& stop)
      : inited_(false),
        sql_proxy_(nullptr),
        unit_mgr_(unit_mgr),
        server_mgr_(server_mgr),
        zone_mgr_(zone_mgr),
        rs_gts_info_collector_(unit_mgr),
        stop_(stop)
  {}
  virtual ~ObRsGtsUnitDistributor()
  {}

public:
  int init(common::ObMySQLProxy* sql_proxy);
  int distribute_unit_for_server_status_change();
  int unit_migrate_finish();
  int check_shrink_resource_pool();

private:
  int check_stop() const;
  int check_single_pool_shrinking_finished(const uint64_t tenant_id, const uint64_t pool_id, bool& is_finished);
  int commit_shrink_resource_pool(const uint64_t pool_id);

private:
  bool inited_;
  common::ObMySQLProxy* sql_proxy_;
  rootserver::ObUnitManager& unit_mgr_;
  rootserver::ObServerManager& server_mgr_;
  rootserver::ObZoneManager& zone_mgr_;
  ObRsGtsInfoCollector rs_gts_info_collector_;
  volatile bool& stop_;
};

enum class GtsReplicaTaskType : int64_t {
  GTS_RTY_INVALID = 0,
  GTS_RTY_MIGRATE,
  GTS_RTY_ALLOC_STANDBY,
  GTS_RTY_MAX,
};

class ObGtsReplicaTaskKey {
public:
  ObGtsReplicaTaskKey() : gts_id_(common::OB_INVALID_ID), hash_value_(0)
  {}
  virtual ~ObGtsReplicaTaskKey()
  {}

public:
  bool is_valid() const;
  bool operator==(const ObGtsReplicaTaskKey& that) const;
  ObGtsReplicaTaskKey& operator=(const ObGtsReplicaTaskKey& that);
  uint64_t hash() const;
  int init(const uint64_t gts_id);
  int init(const ObGtsReplicaTaskKey& that);
  TO_STRING_KV(K_(gts_id));

private:
  uint64_t inner_hash() const;

private:
  uint64_t gts_id_;
  uint64_t hash_value_;
};

class ObGtsReplicaTask : public common::ObDLinkBase<ObGtsReplicaTask> {
public:
  ObGtsReplicaTask()
  {}
  virtual ~ObGtsReplicaTask()
  {}

public:
  virtual int64_t get_deep_copy_size() const = 0;
  virtual int clone_new(void* ptr, ObGtsReplicaTask*& output_ptr) const = 0;
  virtual int execute(rootserver::ObServerManager& server_mgr, share::ObGtsTableOperator& gts_table_operator,
      obrpc::ObSrvRpcProxy& rpc_proxy) = 0;
  virtual int check_before_execute(rootserver::ObServerManager& server_mgr, rootserver::ObUnitManager& unit_mgr,
      share::ObGtsTableOperator& gts_table_operator, bool& can_execute) = 0;
  virtual GtsReplicaTaskType get_task_type() = 0;

public:
  int assign(const ObGtsReplicaTask& that)
  {
    task_key_ = that.task_key_;
    return common::OB_SUCCESS;
  }
  const ObGtsReplicaTaskKey& get_task_key() const
  {
    return task_key_;
  }
  TO_STRING_KV(K(task_key_));

protected:
  ObGtsReplicaTaskKey task_key_;
};

class GtsMigrateReplicaTask : public ObGtsReplicaTask {
public:
  enum class MigrateType : int64_t {
    MT_INVALID = 0,
    MT_MIGRATE_REPLICA,
    MT_MIGRATE_STANDBY,
    MT_SPREAD_REPLICA_AMONG_ZONE,
    MT_MAX
  };

public:
  GtsMigrateReplicaTask() : gts_info_(), src_(), dst_(), migrate_type_(MigrateType::MT_INVALID)
  {}
  virtual ~GtsMigrateReplicaTask()
  {}

public:
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(GtsMigrateReplicaTask);
  }
  virtual int clone_new(void* ptr, ObGtsReplicaTask*& output_ptr) const override;
  virtual int execute(rootserver::ObServerManager& server_mgr, share::ObGtsTableOperator& gts_table_operator,
      obrpc::ObSrvRpcProxy& rpc_proxy) override;
  virtual int check_before_execute(rootserver::ObServerManager& server_mgr, rootserver::ObUnitManager& unit_mgr,
      share::ObGtsTableOperator& gts_table_operator, bool& can_execute) override;
  virtual GtsReplicaTaskType get_task_type() override
  {
    return GtsReplicaTaskType::GTS_RTY_MIGRATE;
  }

public:
  int init(const common::ObGtsInfo& gts_info, const common::ObAddr& src, const common::ObAddr& dst,
      const MigrateType migrate_type);
  int assign(const GtsMigrateReplicaTask& that);

private:
  int try_remove_migrate_src(rootserver::ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy);

private:
  common::ObGtsInfo gts_info_;
  common::ObAddr src_;
  common::ObAddr dst_;
  MigrateType migrate_type_;
};

class GtsAllocStandbyTask : public ObGtsReplicaTask {
public:
  GtsAllocStandbyTask() : gts_info_(), new_standby_()
  {}
  virtual ~GtsAllocStandbyTask()
  {}

public:
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(GtsAllocStandbyTask);
  }
  virtual int clone_new(void* ptr, ObGtsReplicaTask*& output_ptr) const override;
  virtual int execute(rootserver::ObServerManager& server_mgr, share::ObGtsTableOperator& gts_table_operator,
      obrpc::ObSrvRpcProxy& rpc_proxy) override;
  virtual int check_before_execute(rootserver::ObServerManager& server_mgr, rootserver::ObUnitManager& unit_mgr,
      share::ObGtsTableOperator& gts_table_operator, bool& can_execute) override;
  virtual GtsReplicaTaskType get_task_type() override
  {
    return GtsReplicaTaskType::GTS_RTY_ALLOC_STANDBY;
  }

public:
  int init(const common::ObGtsInfo& gts_info, const common::ObAddr& new_standby);
  int assign(const GtsAllocStandbyTask& that);

private:
  common::ObGtsInfo gts_info_;
  common::ObAddr new_standby_;
};

class ObRsGtsReplicaTaskGenerator {
public:
  ObRsGtsReplicaTaskGenerator(rootserver::ObUnitManager& unit_mgr, rootserver::ObServerManager& server_mgr,
      rootserver::ObZoneManager& zone_mgr, volatile bool& stop)
      : inited_(false),
        sql_proxy_(nullptr),
        allocator_(common::ObModIds::OB_RS_GTS_MANAGER),
        output_task_array_(),
        unit_mgr_(unit_mgr),
        server_mgr_(server_mgr),
        zone_mgr_(zone_mgr),
        rs_gts_info_collector_(unit_mgr),
        stop_(stop)
  {}
  virtual ~ObRsGtsReplicaTaskGenerator()
  {}

public:
  int init(common::ObMySQLProxy* sql_proxy);
  int output_gts_replica_task_array(common::ObIArray<const ObGtsReplicaTask*>& output_task_array);
  void reuse();

private:
  struct ZoneReplicaCnt {
    ZoneReplicaCnt() : zone_(), cnt_(0)
    {}
    bool operator<(const ZoneReplicaCnt& that) const
    {
      return cnt_ < that.cnt_;
    }
    TO_STRING_KV(K_(zone), K_(cnt));
    common::ObZone zone_;
    int64_t cnt_;
  };
  struct MemberCandStat {
    MemberCandStat() : server_(), zone_()
    {}
    MemberCandStat(const common::ObAddr& server, const common::ObZone& zone) : server_(server), zone_(zone)
    {}
    TO_STRING_KV(K_(server), K_(zone));

    common::ObAddr server_;
    common::ObZone zone_;
  };

private:
  int check_stop();
  int check_can_migrate(const common::ObAddr& myself, const common::ObGtsInfo& gts_info, bool& can_migrate);
  int try_generate_gts_replica_task(const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);
  int try_generate_migrate_standby_task(const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);
  int try_alloc_standby_task(const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);
  int pick_new_gts_replica(const common::ObGtsInfo& gts_info, common::ObAddr& new_standby);
  int construct_alloc_standby_task(
      const common::ObGtsInfo& gts_info, const common::ObAddr& new_standby, GtsAllocStandbyTask*& new_task);
  int construct_migrate_replica_task(const common::ObGtsInfo& gts_info, const common::ObAddr& src,
      const common::ObAddr& dst, const GtsMigrateReplicaTask::MigrateType migrate_type,
      GtsMigrateReplicaTask*& new_task);
  int try_generate_spread_replica_between_zone_task(const common::ObGtsInfo& gts_info, const common::ObZone& src_zone,
      const common::ObZone& dst_zone, ObGtsReplicaTask*& gts_replica_task);
  int try_generate_spread_replica_among_zone_task(
      const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);
  int try_generate_migrate_replica_task(const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);
  int inner_generate_gts_replica_task(const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);
  int get_excluded_servers(const common::ObGtsInfo& gts_info, common::ObIArray<common::ObAddr>& excluded_servers);
  int get_zone_replica_cnts(
      const common::ObGtsInfo& gts_info, const bool count_standby, common::ObIArray<ZoneReplicaCnt>& zone_replica_cnts);
  int try_generate_server_or_zone_stopped_task(const common::ObGtsInfo& gts_info, ObGtsReplicaTask*& gts_replica_task);

public:
  static const int64_t GTS_QUORUM = 3;

private:
  bool inited_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObArenaAllocator allocator_;                    // output task allocator
  common::ObArray<ObGtsReplicaTask*> output_task_array_;  // used to destruct
  rootserver::ObUnitManager& unit_mgr_;
  rootserver::ObServerManager& server_mgr_;
  rootserver::ObZoneManager& zone_mgr_;
  ObRsGtsInfoCollector rs_gts_info_collector_;
  volatile bool& stop_;
};

class ObRsGtsMonitor : public ObRsReentrantThread {
public:
  ObRsGtsMonitor(rootserver::ObUnitManager& unit_mgr, rootserver::ObServerManager& server_mgr,
      rootserver::ObZoneManager& zone_mgr, rootserver::ObRsGtsTaskMgr& gts_task_mgr);
  virtual ~ObRsGtsMonitor()
  {}

public:
  int init(common::ObMySQLProxy* sql_proxy);
  virtual void run3() override;
  void wakeup();
  void stop();
  virtual int blocking_run() override
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int check_gts_replica_enough_when_stop_server(
      const common::ObIArray<common::ObAddr>& servers_need_stopped, bool& can_stop);
  int check_gts_replica_enough_when_stop_zone(const common::ObZone& zone_need_stopped, bool& can_stop);

public:
  static const int64_t GTS_QUORUM = 3;

private:
  bool inited_;
  mutable ObGtsTaskManagerIdling idling_;
  ObRsGtsUnitDistributor gts_unit_distributor_;
  ObRsGtsReplicaTaskGenerator gts_replica_task_generator_;
  ObRsGtsTaskMgr& gts_task_mgr_;
  rootserver::ObUnitManager& unit_mgr_;
  rootserver::ObServerManager& server_mgr_;
  rootserver::ObZoneManager& zone_mgr_;
  common::ObMySQLProxy* sql_proxy_;
};
}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_RS_GTS_MONITOR_H_
