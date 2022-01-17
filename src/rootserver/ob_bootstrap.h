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

#ifndef OCEANBASE_ROOTSERVER_OB_BOOTSTRAP_H_
#define OCEANBASE_ROOTSERVER_OB_BOOTSTRAP_H_

#include <typeinfo>
#include "share/ob_define.h"
#include "share/ob_leader_election_waiter.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_leader_coordinator.h"

namespace oceanbase {
namespace common {
class ObDataBuffer;
class ObMySQLProxy;
class ObAddr;
class ObServerConfig;
class ObISQLClient;
}  // namespace common

namespace obrpc {
class ObSrvRpcProxy;
}

namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObTenantSchema;
}  // namespace schema
}  // namespace share

namespace rootserver {
class ObRsGtsManager;
class ObSysStat;
class ObPartitionCreator;

class ObBaseBootstrap {
public:
  explicit ObBaseBootstrap(
      obrpc::ObSrvRpcProxy& rpc_proxy, const obrpc::ObServerInfoList& rs_list, common::ObServerConfig& config);
  virtual ~ObBaseBootstrap()
  {}

  static int gen_sys_unit_ids(const common::ObIArray<common::ObZone>& zones, common::ObIArray<uint64_t>& units);

  inline obrpc::ObSrvRpcProxy& get_rpc_proxy() const
  {
    return rpc_proxy_;
  }

protected:
  virtual int check_inner_stat() const;
  virtual int check_bootstrap_rs_list(const obrpc::ObServerInfoList& rs_list);
  virtual int check_multiple_zone_deployment_rslist(const obrpc::ObServerInfoList& rs_list);
  virtual int gen_sys_unit_ids(common::ObIArray<uint64_t>& unit_ids);
  virtual int gen_gts_unit_ids(common::ObIArray<uint64_t>& unit_ids);
  virtual int gen_sys_zone_list(common::ObIArray<common::ObZone>& zone_list);
  virtual int gen_sys_units(common::ObIArray<share::ObUnit>& units);
  virtual int gen_gts_units(common::ObIArray<share::ObUnit>& units);
  virtual int pick_sys_tenant_primary_zone(common::ObZone& primary_zone);
  virtual int pick_gts_tenant_primary_region(common::ObRegion& primary_region);
  virtual int pick_bootstrap_primary_zone_and_region(common::ObZone& primary_zone, common::ObRegion& primary_region);
  virtual int get_zones_in_primary_region(
      const common::ObRegion& primary_region, common::ObIArray<common::ObZone>& zones_in_primary_region);
  virtual int check_bootstrap_sys_tenant_primary_zone();
  virtual int fill_sys_unit_config(const share::ObUnitConfig& sample_config, share::ObUnitConfig& target_config);

public:
  int64_t step_id_;

protected:
  obrpc::ObSrvRpcProxy& rpc_proxy_;
  obrpc::ObServerInfoList rs_list_;
  common::ObServerConfig& config_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseBootstrap);
};

class ObPreBootstrap : public ObBaseBootstrap {
public:
  explicit ObPreBootstrap(obrpc::ObSrvRpcProxy& rpc_proxy, const obrpc::ObServerInfoList& rs_list,
      share::ObPartitionTableOperator& pt_operator, common::ObServerConfig& config, const obrpc::ObBootstrapArg& arg,
      obrpc::ObCommonRpcProxy& rs_rpc_proxy);
  virtual ~ObPreBootstrap()
  {}
  virtual int prepare_bootstrap(common::ObAddr& master_rs, int64_t& initial_frozen_version,
      int64_t& initial_schema_version, ObIArray<storage::ObFrozenStatus>& frozen_status,
      ObIArray<share::TenantIdAndSchemaVersion>& freeze_schema);

private:
  // wait leader elect time + root service start time
  static const int64_t WAIT_ELECT_SYS_LEADER_TIMEOUT_US = 30 * 1000 * 1000;
  static const int64_t NOTIFY_RESOURCE_RPC_TIMEOUT = 9 * 1000 * 1000;  // 9 second

  virtual int check_is_all_server_empty(bool& is_empty);
  virtual int check_all_server_bootstrap_mode_match(bool& match);
  virtual int wait_elect_master_partition(common::ObAddr& master_rs);
  virtual int notify_sys_tenant_server_unit_resource();
  virtual int create_partition(const int64_t initial_frozen_version, const int64_t initial_frozen_ts);

private:
  volatile bool stop_;
  share::ObLeaderElectionWaiter leader_waiter_;
  int64_t begin_ts_;
  const obrpc::ObBootstrapArg& arg_;
  obrpc::ObCommonRpcProxy& common_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObPreBootstrap);
};

class ObBootstrap : public ObBaseBootstrap {
public:
  class TableIdCompare {
  public:
    TableIdCompare() : ret_(common::OB_SUCCESS)
    {}
    ~TableIdCompare()
    {}
    bool operator()(const share::schema::ObTableSchema* left, const share::schema::ObTableSchema* right);
    int get_ret() const
    {
      return ret_;
    }

  private:
    int ret_;
  };
  explicit ObBootstrap(obrpc::ObSrvRpcProxy& rpc_proxy, ObDDLService& ddl_service, ObUnitManager& unit_mgr,
      ObILeaderCoordinator& leader_coordinator, common::ObServerConfig& config, const obrpc::ObBootstrapArg& arg,
      ObRsGtsManager& rs_gts_manager, obrpc::ObCommonRpcProxy& rs_rpc_proxy);

  virtual ~ObBootstrap()
  {}
  virtual int execute_bootstrap();
  static int create_all_schema(
      ObDDLService& ddl_service, common::ObIArray<share::schema::ObTableSchema>& table_schemas);
  int primary_construct_all_schema(common::ObIArray<share::schema::ObTableSchema>& table_schemas);
  int sort_schema(const common::ObIArray<share::schema::ObTableSchema>& table_schemas,
      common::ObIArray<share::schema::ObTableSchema>& sort_table_schemas);
  int check_schema_version(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::schema::ObTableSchema>& table_schemas, const bool need_update);

private:
  static const int64_t HEAT_BEAT_INTERVAL_US = 2 * 1000 * 1000;           // 2s
  static const int64_t WAIT_RS_IN_SERVICE_TIMEOUT_US = 40 * 1000 * 1000;  // 40s
  static const int64_t BATCH_INSERT_SCHEMA_CNT = 32;
  virtual int prepare_create_partition(
      ObPartitionCreator& creator, const share::schema_create_func func, share::schema::ObTableSchema& tschema);
  virtual int create_all_partitions();
  virtual int construct_all_schema(common::ObIArray<share::schema::ObTableSchema>& table_schemas);

  /*
virtual int set_table_locality(
    share::schema::ObTableSchema &table_schema);
*/
  virtual int construct_schema(const share::schema_create_func func, share::schema::ObTableSchema& tschema);
  virtual int broadcast_sys_schema(const common::ObSArray<share::schema::ObTableSchema>& table_schemas);
  static int batch_create_schema(ObDDLService& ddl_service,
      common::ObIArray<share::schema::ObTableSchema>& table_schemas, const int64_t begin, const int64_t end);
  virtual int check_is_already_bootstrap(bool& is_bootstrap);
  virtual int init_global_stat();
  virtual int init_sequence_id();
  virtual int init_system_data(const uint64_t server_id);
  virtual int init_all_zone_table();
  virtual int init_multiple_zone_deployment_table(common::ObISQLClient& sql_client);
  virtual int init_server_id(const uint64_t server_id);
  virtual int add_rs_list(uint64_t& server_id);
  virtual int wait_all_rs_online();
  virtual int wait_all_rs_in_service();
  virtual int insert_first_freeze_info();
  virtual int insert_first_freeze_schema();
  virtual int init_gts_service_data();
  int init_backup_inner_table();
  template <typename SCHEMA>
  int set_replica_options(const bool set_primary_zone, SCHEMA& schema);
  template <typename SCHEMA>
  int normalize_schema_primary_zone(SCHEMA& schema, const common::ObIArray<common::ObString>& zone_list);
  int build_zone_region_list(ObIArray<share::schema::ObZoneRegion>& zone_region_list);

  int init_sys_unit_config(share::ObUnitConfig& unit_config);
  int create_sys_unit_config();
  int create_sys_resource_pool();
  int gen_sys_resource_pool(share::ObResourcePool& pool);
  int create_sys_tenant();
  int gen_sys_tenant_locality_str(share::schema::ObTenantSchema& tenant_schema);
  int gen_multiple_zone_deployment_sys_tenant_locality_str(share::schema::ObTenantSchema& tenant_schema);
  // create gts service private method
  int create_gts_unit_config();
  int create_gts_resource_pool();
  int gen_gts_resource_pool(share::ObResourcePool& pool);
  int create_gts_tenant();
  int create_original_gts_instance();
  int init_gts_unit_config(share::ObUnitConfig& unit_config);
  // end create gts service private method
  int set_in_bootstrap();

private:
  ObDDLService& ddl_service_;
  ObUnitManager& unit_mgr_;
  ObILeaderCoordinator& leader_coordinator_;
  const obrpc::ObBootstrapArg& arg_;
  ObRsGtsManager& rs_gts_manager_;
  obrpc::ObCommonRpcProxy& common_proxy_;
  int64_t begin_ts_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBootstrap);
};

#define BOOTSTRAP_CHECK_SUCCESS_V2(function_name)                                                                   \
  do {                                                                                                              \
    step_id_++;                                                                                                     \
    int64_t major_step = 1;                                                                                         \
    if (NULL == strstr(typeid(*this).name(), "ObPreBootstrap")) {                                                   \
      major_step = 3;                                                                                               \
    } else {                                                                                                        \
      major_step = 2;                                                                                               \
    }                                                                                                               \
    int64_t end_ts = ObTimeUtility::current_time();                                                                 \
    int64_t cost = end_ts - begin_ts_;                                                                              \
    begin_ts_ = end_ts;                                                                                             \
    if (OB_SUCC(ret)) {                                                                                             \
      _BOOTSTRAP_LOG(INFO, "STEP_%ld.%ld:%s execute success, cost=%ld", major_step, step_id_, function_name, cost); \
    } else {                                                                                                        \
      _BOOTSTRAP_LOG(WARN, "STEP_%ld.%ld:%s execute fail, cost=%ld", major_step, step_id_, function_name, cost);    \
    }                                                                                                               \
  } while (0)

#define BOOTSTRAP_CHECK_SUCCESS()                                                                                \
  do {                                                                                                           \
    step_id_++;                                                                                                  \
    int64_t major_step = 1;                                                                                      \
    if (NULL == strstr(typeid(*this).name(), "ObPreBootstrap")) {                                                \
      major_step = 3;                                                                                            \
    } else {                                                                                                     \
      major_step = 2;                                                                                            \
    }                                                                                                            \
    int64_t end_ts = ObTimeUtility::current_time();                                                              \
    int64_t cost = end_ts - begin_ts_;                                                                           \
    begin_ts_ = end_ts;                                                                                          \
    if (OB_SUCC(ret)) {                                                                                          \
      _BOOTSTRAP_LOG(INFO, "STEP_%ld.%ld:%s execute success, cost=%ld", major_step, step_id_, _fun_name_, cost); \
    } else {                                                                                                     \
      _BOOTSTRAP_LOG(WARN, "STEP_%ld.%ld:%s execute fail, cost=%ld", major_step, step_id_, _fun_name_, cost);    \
    }                                                                                                            \
  } while (0)

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_BOOTSTRAP_H_
