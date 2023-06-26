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

namespace oceanbase
{
namespace common
{
class ObDataBuffer;
class ObMySQLProxy;
class ObAddr;
class ObServerConfig;
class ObISQLClient;
}

namespace obrpc
{
class ObSrvRpcProxy;
}

namespace share
{
class ObLSTableOperator;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObTenantSchema;
}
}

namespace rootserver
{
class ObRsGtsManager;
struct ObSysStat;
class ObTableCreator;
class ObServerZoneOpService;

class ObBaseBootstrap
{
public:
  explicit ObBaseBootstrap(obrpc::ObSrvRpcProxy &rpc_proxy,
                           const obrpc::ObServerInfoList &rs_list,
                           common::ObServerConfig &config);
  virtual ~ObBaseBootstrap() {}

  static int gen_sys_unit_ids(const common::ObIArray<common::ObZone> &zones,
                              common::ObIArray<uint64_t> &units);

  inline obrpc::ObSrvRpcProxy &get_rpc_proxy() const { return rpc_proxy_; }
protected:
  virtual int check_inner_stat() const;
  virtual int check_bootstrap_rs_list(const obrpc::ObServerInfoList &rs_list);
  virtual int check_multiple_zone_deployment_rslist(const obrpc::ObServerInfoList &rs_list);
  virtual int gen_sys_unit_ids(common::ObIArray<uint64_t> &unit_ids);
  virtual int gen_sys_zone_list(common::ObIArray<common::ObZone> &zone_list);
  virtual int gen_sys_units(common::ObIArray<share::ObUnit> &units);
public:
  int64_t step_id_;
protected:
  obrpc::ObSrvRpcProxy &rpc_proxy_;
  obrpc::ObServerInfoList rs_list_;
  common::ObServerConfig &config_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseBootstrap);
};

class ObPreBootstrap : public ObBaseBootstrap
{
public:
  explicit ObPreBootstrap(obrpc::ObSrvRpcProxy &rpc_proxy,
                          const obrpc::ObServerInfoList &rs_list,
                          share::ObLSTableOperator &lst_operator,
                          common::ObServerConfig &config,
                          const obrpc::ObBootstrapArg &arg,
                          obrpc::ObCommonRpcProxy &rs_rpc_proxy);
  virtual ~ObPreBootstrap() {}
  virtual int prepare_bootstrap(common::ObAddr &master_rs);

private:
  // wait leader elect time + root service start time
  static const int64_t WAIT_ELECT_SYS_LEADER_TIMEOUT_US = 30 * 1000 * 1000;
  static const int64_t NOTIFY_RESOURCE_RPC_TIMEOUT = 9 * 1000 * 1000; // 9 second

  virtual int check_is_all_server_empty(bool &is_empty);
  virtual int check_all_server_bootstrap_mode_match(bool &match);
  virtual int notify_sys_tenant_root_key();
  virtual int notify_sys_tenant_server_unit_resource();
  virtual int create_ls();
  virtual int wait_elect_ls(common::ObAddr &master_rs);

  int notify_sys_tenant_config_();
private:
  volatile bool stop_;
  share::ObLSLeaderElectionWaiter ls_leader_waiter_;
  int64_t begin_ts_;
  const obrpc::ObBootstrapArg &arg_;
  obrpc::ObCommonRpcProxy &common_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObPreBootstrap);
};

class ObBootstrap : public ObBaseBootstrap
{
public:
  class TableIdCompare
  {
  public:
    TableIdCompare() : ret_(common::OB_SUCCESS) {}
    ~TableIdCompare() {}
    bool operator() (const share::schema::ObTableSchema* left,
                     const share::schema::ObTableSchema* right);
    int get_ret() const { return ret_; }
  private:
    int ret_;
  };
  explicit ObBootstrap(obrpc::ObSrvRpcProxy &rpc_proxy,
                       share::ObLSTableOperator &lst_operator,
                       ObDDLService &ddl_service,
                       ObUnitManager &unit_mgr,
                       common::ObServerConfig &config,
                       const obrpc::ObBootstrapArg &arg,
                       obrpc::ObCommonRpcProxy &rs_rpc_proxy);

  virtual ~ObBootstrap() {}
  virtual int execute_bootstrap(rootserver::ObServerZoneOpService &server_zone_op_service);
  static int create_all_schema(
      ObDDLService &ddl_service,
      common::ObIArray<share::schema::ObTableSchema> &table_schemas);
  int construct_all_schema(
      common::ObIArray<share::schema::ObTableSchema> &table_schemas);
  int sort_schema(const common::ObIArray<share::schema::ObTableSchema> &table_schemas,
                  common::ObIArray<share::schema::ObTableSchema> &sort_table_schemas);
private:
  static const int64_t HEAT_BEAT_INTERVAL_US = 2 * 1000 * 1000; //2s
  static const int64_t WAIT_RS_IN_SERVICE_TIMEOUT_US = 40 * 1000 * 1000; //40s
  static const int64_t BATCH_INSERT_SCHEMA_CNT = 128;
  virtual int generate_table_schema_array_for_create_partition(
      const share::schema::ObTableSchema &tschema,
      common::ObIArray<share::schema::ObTableSchema> &table_schema_array);
  virtual int prepare_create_partition(
      ObTableCreator &creator,
      const share::schema_create_func func);
  virtual int create_all_partitions();
  virtual int create_all_core_table_partition();
  virtual int construct_schema(
      const share::schema_create_func func,
      share::schema::ObTableSchema &tschema);
  virtual int broadcast_sys_schema(
      const common::ObSArray<share::schema::ObTableSchema> &table_schemas);
  static int batch_create_schema(
      ObDDLService &ddl_service,
      common::ObIArray<share::schema::ObTableSchema> &table_schemas,
      const int64_t begin, const int64_t end);
  virtual int check_is_already_bootstrap(bool &is_bootstrap);
  virtual int init_global_stat();
  virtual int init_sequence_id();
  virtual int init_system_data();
  virtual int init_all_zone_table();
  virtual int init_multiple_zone_deployment_table(common::ObISQLClient &sql_client);
  virtual int add_servers_in_rs_list(rootserver::ObServerZoneOpService &server_zone_op_service);
  virtual int wait_all_rs_in_service();
  template<typename SCHEMA>
    int set_replica_options(SCHEMA &schema);
  int build_zone_region_list(
      ObIArray<share::schema::ObZoneRegion> &zone_region_list);

  int init_sys_unit_config(share::ObUnitConfig &unit_config);
  int create_sys_unit_config();
  int create_sys_resource_pool();
  int gen_sys_resource_pool(share::ObResourcePool &pool);
  int create_sys_tenant();
  int gen_sys_tenant_locality_str(
      share::schema::ObTenantSchema &tenant_schema);
  int gen_multiple_zone_deployment_sys_tenant_locality_str(
      share::schema::ObTenantSchema &tenant_schema);
  int set_in_bootstrap();
  int add_sys_table_lob_aux_table(
      uint64_t data_table_id,
      ObIArray<ObTableSchema> &table_schemas);
  int insert_sys_ls_(const share::schema::ObTenantSchema &tenant_schema,
                     const ObIArray<ObZone> &zone_list);

private:
  share::ObLSTableOperator &lst_operator_;
  ObDDLService &ddl_service_;
  ObUnitManager &unit_mgr_;
  const obrpc::ObBootstrapArg &arg_;
  obrpc::ObCommonRpcProxy &common_proxy_;
  int64_t begin_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBootstrap);
};

#define BOOTSTRAP_CHECK_SUCCESS_V2(function_name) \
    do { \
      step_id_ ++;   \
      int64_t major_step = 1; \
      if (NULL == strstr(typeid(*this).name(), "ObPreBootstrap")) { \
        major_step = 3; \
      } else { \
        major_step = 2; \
      } \
      int64_t end_ts = ObTimeUtility::current_time(); \
      int64_t cost = end_ts - begin_ts_; \
      begin_ts_ = end_ts ; \
      if (OB_SUCC(ret)) { \
        ObTaskController::get().allow_next_syslog(); \
        _BOOTSTRAP_LOG(INFO, "STEP_%ld.%ld:%s execute success, cost=%ld", \
                       major_step, step_id_, function_name, cost); \
      } else { \
        ObTaskController::get().allow_next_syslog(); \
        _BOOTSTRAP_LOG(WARN, "STEP_%ld.%ld:%s execute fail, ret=%d, cost=%ld", \
                       major_step, step_id_, function_name, ret, cost); \
      }\
    } while (0)

#define BOOTSTRAP_CHECK_SUCCESS() \
    do { \
      step_id_ ++;   \
      int64_t major_step = 1; \
      if (NULL == strstr(typeid(*this).name(), "ObPreBootstrap")) { \
        major_step = 3; \
      } else { \
        major_step = 2; \
      } \
      int64_t end_ts = ObTimeUtility::current_time(); \
      int64_t cost = end_ts - begin_ts_; \
      begin_ts_ = end_ts ; \
      if (OB_SUCC(ret)) { \
        ObTaskController::get().allow_next_syslog(); \
        _BOOTSTRAP_LOG(INFO, "STEP_%ld.%ld:%s execute success, cost=%ld", \
                       major_step, step_id_, _fun_name_, cost); \
      } else { \
        ObTaskController::get().allow_next_syslog(); \
        _BOOTSTRAP_LOG(WARN, "STEP_%ld.%ld:%s execute fail, ret=%d, cost=%ld", \
                       major_step, step_id_, _fun_name_, ret, cost); \
      }\
    } while (0)

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_BOOTSTRAP_H_
