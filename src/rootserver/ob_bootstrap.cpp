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

#define USING_LOG_PREFIX BOOTSTRAP

#include "rootserver/ob_bootstrap.h"

#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/utility/ob_print_utils.h"
#include "common/data_buffer.h"
#include "common/ob_role.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_srv_rpc_proxy.h"
#include "common/ob_member_list.h"
#include "share/ob_max_id_fetcher.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_ddl_sql_service.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_tenant_id_schema_version.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_server_status.h"
#include "lib/worker.h"
#include "share/config/ob_server_config.h"
#include "share/ob_primary_zone_util.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_table_operator.h"
#include "storage/ob_file_system_router.h"
#include "share/ls/ob_ls_creator.h"//ObLSCreator
#include "share/ls/ob_ls_life_manager.h"//ObLSLifeAgentManager
#include "share/ob_all_server_tracer.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_server_zone_op_service.h"
#include "observer/ob_server_struct.h"
#include "rootserver/freeze/ob_freeze_info_manager.h"
#include "rootserver/ob_table_creator.h"
#include "share/scn.h"
#include "rootserver/ob_heartbeat_service.h"
#include "rootserver/ob_root_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "close_modules/tde_security/share/ob_master_key_getter.h"
#endif

namespace oceanbase
{

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;
namespace rootserver
{

ObBaseBootstrap::ObBaseBootstrap(ObSrvRpcProxy &rpc_proxy,
                                 const ObServerInfoList &rs_list,
                                 common::ObServerConfig &config)
    : step_id_(0),
      rpc_proxy_(rpc_proxy),
      rs_list_(rs_list),
      config_(config)
{
  std::sort(rs_list_.begin(), rs_list_.end());
}

int ObBaseBootstrap::gen_sys_unit_ids(const ObIArray<ObZone> &zones,
                                      ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> sorted_zones;
  unit_ids.reuse();
  if (zones.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zones is empty", K(zones), K(ret));
  } else if (OB_FAIL(sorted_zones.assign(zones))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    std::sort(sorted_zones.begin(), sorted_zones.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < sorted_zones.count(); ++j) {
        if (sorted_zones.at(j) == zones.at(i)) {
          if (OB_FAIL(unit_ids.push_back(OB_SYS_UNIT_ID + static_cast<uint64_t>(j)))) {
            LOG_WARN("push_back failed", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (rs_list_.count() <= 0) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("rs_list is empty", K_(rs_list), K(ret));
  }
  return ret;
}

int ObBaseBootstrap::check_multiple_zone_deployment_rslist(
    const ObServerInfoList &rs_list)
{
  int ret = OB_SUCCESS;
  //In the multi zone deployment mode,
  //each server must come from a different zone,
  //and it will throw exception if there are duplicate zones
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list.count(); ++i) {
    const ObZone &zone = rs_list[i].zone_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rs_list.count(); ++j) {
      if (i != j) {
        if (zone == rs_list[j].zone_) {
          ret = OB_PARTITION_ZONE_DUPLICATED;
          LOG_WARN("should not choose two rs in same zone",
              "server1", to_cstring(rs_list[i].server_),
              "server2", to_cstring(rs_list[j].server_), K(zone), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::check_bootstrap_rs_list(
    const ObServerInfoList &rs_list)
{
  int ret = OB_SUCCESS;
  if (rs_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_list size must larger than 0", K(ret));
  } else {
    if (OB_FAIL(check_multiple_zone_deployment_rslist(rs_list))) {
      LOG_WARN("fail to check multiple zone deployment rslist", K(ret));
    }
  }
  //BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBaseBootstrap::gen_sys_unit_ids(ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  unit_ids.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(unit_ids.push_back(OB_SYS_UNIT_ID + static_cast<uint64_t>(i)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_sys_zone_list(ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs list count unexpected", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(zone_list.push_back(rs_list_.at(i).zone_))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_sys_units(ObIArray<share::ObUnit> &units)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> unit_ids;
  units.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_unit_ids(unit_ids))) {
    LOG_WARN("gen_sys_unit_ids failed", K(ret));
  } else {
    ObUnit unit;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      unit.reset();
      unit.unit_id_ = unit_ids.at(i);
      unit.resource_pool_id_ = OB_SYS_RESOURCE_POOL_ID;
      unit.unit_group_id_ = OB_SYS_UNIT_GROUP_ID;
      unit.zone_ = rs_list_.at(i).zone_;
      unit.server_ = rs_list_.at(i).server_;
      unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
      if (OB_FAIL(units.push_back(unit))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

ObPreBootstrap::ObPreBootstrap(ObSrvRpcProxy &rpc_proxy,
                               const ObServerInfoList &rs_list,
                               ObLSTableOperator &lst_operator,
                               common::ObServerConfig &config,
                               const ObBootstrapArg &arg,
                               obrpc::ObCommonRpcProxy &rs_rpc_proxy)
  : ObBaseBootstrap(rpc_proxy, rs_list, config),
    stop_(false),
    ls_leader_waiter_(lst_operator, stop_),
    begin_ts_(0),
    arg_(arg),
    common_proxy_(rs_rpc_proxy)
{
}

int ObPreBootstrap::prepare_bootstrap(ObAddr &master_rs)
{
  int ret = OB_SUCCESS;
  bool is_empty = false;
  bool match = false;
  begin_ts_ = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(check_bootstrap_rs_list(rs_list_))) {
    LOG_WARN("failed to check_bootstrap_rs_list", KR(ret), K_(rs_list));
  } else if (OB_FAIL(check_all_server_bootstrap_mode_match(match))) {
    LOG_WARN("fail to check all server bootstrap mode match", KR(ret));
  } else if (!match) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot do bootstrap with different bootstrap mode on servers", KR(ret));
  } else if (OB_FAIL(check_is_all_server_empty(is_empty))) {
    LOG_WARN("failed to check bootstrap stat", KR(ret));
  } else if (!is_empty) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot do bootstrap on not empty server", KR(ret));
  } else if (OB_FAIL(notify_sys_tenant_root_key())) {
    LOG_WARN("fail to notify sys tenant root key", KR(ret));
  } else if (OB_FAIL(notify_sys_tenant_server_unit_resource())) {
    LOG_WARN("fail to notify sys tenant server unit resource", KR(ret));
  } else if (OB_FAIL(notify_sys_tenant_config_())) {
    LOG_WARN("fail to notify sys tenant config", KR(ret));
  } else if (OB_FAIL(create_ls())) {
    LOG_WARN("failed to create core table partition", KR(ret));
  } else if (OB_FAIL(wait_elect_ls(master_rs))) {
    LOG_WARN("failed to wait elect master partition", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::notify_sys_tenant_root_key()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  ObArray<ObAddr> addrs;
  obrpc::ObRootKeyArg arg;
  arg.tenant_id_ = OB_SYS_TENANT_ID;
  arg.is_set_ = false;
  obrpc::ObRootKeyResult result;
  if (OB_FAIL(addrs.reserve(rs_list_.count()))) {
    LOG_WARN("fail to reserve array", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
    if (OB_FAIL(addrs.push_back(rs_list_[i].server_))) {
      LOG_WARN("fail to push back server", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID,
                                                                result.key_type_,
                                                                result.root_key_))) {
  } else if (obrpc::RootKeyType::INVALID != result.key_type_ || !result.root_key_.empty()) {
    LOG_INFO("root key existed in local");
  } else if (OB_FAIL(ObDDLService::notify_root_key(rpc_proxy_, arg, addrs, result, false))) {
    LOG_WARN("fail to notify root key", K(ret));
  } else if (obrpc::RootKeyType::INVALID != result.key_type_ || !result.root_key_.empty()) {
    LOG_INFO("root key existed in remote");
  } else if (OB_FAIL(ObDDLService::create_root_key(rpc_proxy_, OB_SYS_TENANT_ID, addrs))) {
    LOG_WARN("fail to create sys tenant root key", KR(ret), K(addrs));
  }
  BOOTSTRAP_CHECK_SUCCESS();
#endif
  return ret;
}

int ObPreBootstrap::notify_sys_tenant_server_unit_resource()
{
  int ret = OB_SUCCESS;
  ObUnitConfig unit_config;
  common::ObArray<uint64_t> sys_unit_id_array;
  const bool is_hidden_sys = false;

  if (OB_FAIL(unit_config.gen_sys_tenant_unit_config(is_hidden_sys))) {
    LOG_WARN("gen sys tenant unit config fail", KR(ret), K(is_hidden_sys));
  } else if (OB_FAIL(gen_sys_unit_ids(sys_unit_id_array))) {
    LOG_WARN("fail to gen sys unit ids", KR(ret));
  } else if (OB_UNLIKELY(sys_unit_id_array.count() != rs_list_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys unit id array and rs list count not match", KR(ret),
             "unit_id_array_cnt", sys_unit_id_array.count(), "rs_list_cnt", rs_list_.count());
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      rpc_proxy_,
                                      &ObSrvRpcProxy::notify_tenant_server_unit_resource);
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      obrpc::TenantServerUnitConfig tenant_unit_server_config;
      if (OB_FAIL(tenant_unit_server_config.init(
              OB_SYS_TENANT_ID,
              sys_unit_id_array.at(i),
              lib::Worker::CompatMode::MYSQL,
              unit_config,
              ObReplicaType::REPLICA_TYPE_FULL,
              false/*if not grant*/,
              false/*create new*/))) {
        LOG_WARN("fail to init tenant unit server config", KR(ret));
      } else if (OB_FAIL(notify_proxy.call(
              rs_list_[i].server_, rpc_timeout, tenant_unit_server_config))) {
        LOG_WARN("fail to call notify resource to server",
                 K(ret), "dst", rs_list_[i].server_, K(rpc_timeout));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::notify_sys_tenant_config_()
{
  int ret = OB_SUCCESS;
  common::ObConfigPairs config;
  common::ObSEArray<common::ObConfigPairs, 1> init_configs;
  ObArray<ObAddr> addrs;
  if (OB_FAIL(ObDDLService::gen_tenant_init_config(
      OB_SYS_TENANT_ID, DATA_CURRENT_VERSION, config))) {
  } else if (OB_FAIL(init_configs.push_back(config))) {
    LOG_WARN("fail to push back config", KR(ret), K(config));
  } else if (OB_FAIL(addrs.reserve(rs_list_.count()))) {
    LOG_WARN("fail to reserve array", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
    if (OB_FAIL(addrs.push_back(rs_list_[i].server_))) {
      LOG_WARN("fail to push back server", KR(ret));
    }
  } // end for
  if (FAILEDx(ObDDLService::notify_init_tenant_config(
              rpc_proxy_, init_configs, addrs))) {
    LOG_WARN("fail to notify init tenant config", KR(ret), K(init_configs), K(addrs));
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::create_ls()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    common::ObArray<share::ObUnit> unit_array;
    ObLSCreator ls_creator(rpc_proxy_, OB_SYS_TENANT_ID, SYS_LS);
    if (OB_FAIL(gen_sys_units(unit_array))) {
      LOG_WARN("fail to gen sys unit array", KR(ret));
    } else if (OB_FAIL(ls_creator.create_sys_tenant_ls(
            rs_list_, unit_array))) {
      LOG_WARN("fail to create sys log stream", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to create ls");
  } else {
    LOG_WARN("create ls failed.", K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::wait_elect_ls(
    common::ObAddr &master_rs)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  int64_t timeout = WAIT_ELECT_SYS_LEADER_TIMEOUT_US;
  if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
    timeout = max(timeout, THIS_WORKER.get_timeout_remain());
  }

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(ls_leader_waiter_.wait(
      tenant_id, SYS_LS, timeout, master_rs))) {
    LOG_WARN("leader_waiter_ wait failed", K(tenant_id), K(SYS_LS), K(timeout), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("succeed to wait elect log stream");
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::check_all_server_bootstrap_mode_match(
    bool &match)
{
  int ret = OB_SUCCESS;
  match = true;
  Bool is_match(false);

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else {
    ObCheckDeploymentModeArg arg;
    arg.single_zone_deployment_on_ = OB_FILE_SYSTEM_ROUTER.is_single_zone_deployment_on();
    for (int64_t i = 0; OB_SUCC(ret) && match && i < rs_list_.count(); ++i) {
      if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_).check_deployment_mode_match(
              arg, is_match))) {
        LOG_WARN("fail to check deployment mode match", K(ret));
      } else if (!is_match) {
        LOG_WARN("server deployment mode not match", "server", rs_list_[i].server_);
        match = false;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

int ObPreBootstrap::check_is_all_server_empty(bool &is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  Bool is_server_empty;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObCheckServerEmptyArg arg(ObCheckServerEmptyArg::BOOTSTRAP,
                              DATA_CURRENT_VERSION);
    for (int64_t i = 0; OB_SUCC(ret) && is_empty && i < rs_list_.count(); ++i) {
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_)
                            .timeout(rpc_timeout)
                            .is_empty_server(arg, is_server_empty))) {
        LOG_WARN("failed to check if server is empty",
            "server", rs_list_[i].server_, K(rpc_timeout), K(ret));
      } else if (!is_server_empty) {
        // don't need to set ret
        LOG_WARN("server is not empty", "server", rs_list_[i].server_);
        is_empty = false;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

bool ObBootstrap::TableIdCompare::operator() (const ObTableSchema* left, const ObTableSchema* right)
{
  bool bret = false;

  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K_(ret), KP(left), KP(right));
  } else {
    bool left_is_sys_index = left->is_index_table() && is_sys_table(left->get_table_id());
    bool right_is_sys_index = right->is_index_table() && is_sys_table(right->get_table_id());
    uint64_t left_table_id = left->get_table_id();
    uint64_t left_data_table_id = left->get_data_table_id();
    uint64_t right_table_id = right->get_table_id();
    uint64_t right_data_table_id = right->get_data_table_id();
    if (!left_is_sys_index && !right_is_sys_index) {
      bret = left_table_id < right_table_id;
    } else if (left_is_sys_index && right_is_sys_index) {
      bret = left_data_table_id < right_data_table_id;
    } else if (left_is_sys_index) {
      if (left_data_table_id == right_table_id) {
        bret = true;
      } else {
        bret = left_data_table_id < right_table_id;
      }
    } else {
      if (left_table_id == right_data_table_id) {
        bret = false;
      } else {
        bret = left_table_id < right_data_table_id;
      }
    }
  }
  return bret;
}


ObBootstrap::ObBootstrap(
    ObSrvRpcProxy &rpc_proxy,
    share::ObLSTableOperator &lst_operator,
    ObDDLService &ddl_service,
    ObUnitManager &unit_mgr,
    ObServerConfig &config,
    const obrpc::ObBootstrapArg &arg,
    obrpc::ObCommonRpcProxy &rs_rpc_proxy)
  : ObBaseBootstrap(rpc_proxy, arg.server_list_, config),
    lst_operator_(lst_operator),
    ddl_service_(ddl_service),
    unit_mgr_(unit_mgr),
    arg_(arg),
    common_proxy_(rs_rpc_proxy),
    begin_ts_(0)
{
}

int ObBootstrap::execute_bootstrap(rootserver::ObServerZoneOpService &server_zone_op_service)
{
  int ret = OB_SUCCESS;
  bool already_bootstrap = true;
  ObSArray<ObTableSchema> table_schemas;
  begin_ts_ = ObTimeUtility::current_time();

  BOOTSTRAP_LOG(INFO, "start do execute_bootstrap");

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(check_is_already_bootstrap(already_bootstrap))) {
    LOG_WARN("failed to check_is_already_bootstrap", K(ret));
  } else if (already_bootstrap) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ob system is already bootstrap, cannot bootstrap again", K(ret));
  } else if (OB_FAIL(check_bootstrap_rs_list(rs_list_))) {
    LOG_WARN("failed to check_bootstrap_rs_list", K_(rs_list), K(ret));
  } else if (OB_FAIL(create_all_core_table_partition())) {
    LOG_WARN("fail to create all core_table partition", KR(ret));
  } else if (OB_FAIL(set_in_bootstrap())) {
    LOG_WARN("failed to set in bootstrap", K(ret));
  } else if (OB_FAIL(init_global_stat())) {
    LOG_WARN("failed to init_global_stat", K(ret));
  } else if (OB_FAIL(construct_all_schema(table_schemas))) {
    LOG_WARN("construct all schema fail", K(ret));
  } else if (OB_FAIL(broadcast_sys_schema(table_schemas))) {
    LOG_WARN("broadcast_sys_schemas failed", K(table_schemas), K(ret));
  } else if (OB_FAIL(create_all_partitions())) {
    LOG_WARN("create all partitions fail", K(ret));
  } else if (OB_FAIL(create_all_schema(ddl_service_, table_schemas))) {
    LOG_WARN("create_all_schema failed",  K(table_schemas), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("create_all_schema");
  ObMultiVersionSchemaService &schema_service = ddl_service_.get_schema_service();

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_system_data())) {
      LOG_WARN("failed to init system data", KR(ret));
    } else if (OB_FAIL(ddl_service_.refresh_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to refresh_schema", K(ret));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("refresh_schema");

  if (FAILEDx(add_servers_in_rs_list(server_zone_op_service))) {
    LOG_WARN("fail to add servers in rs_list_", KR(ret));
  } else if (OB_FAIL(wait_all_rs_in_service())) {
    LOG_WARN("failed to wait all rs in service", KR(ret));
  } else {
    ROOTSERVICE_EVENT_ADD("bootstrap", "bootstrap_succeed");
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::sort_schema(const ObIArray<ObTableSchema> &table_schemas,
                             ObIArray<ObTableSchema> &sorted_table_schemas)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObTableSchema*> ptr_table_schemas;
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "count", table_schemas.count());
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ptr_table_schemas.push_back(&table_schemas.at(i)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      TableIdCompare compare;
      std::sort(ptr_table_schemas.begin(), ptr_table_schemas.end(), compare);
      if (OB_FAIL(compare.get_ret())) {
        LOG_WARN("fail to sort schema", KR(ret));
      } else {
        for (int64_t i = 0 ; i < ptr_table_schemas.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(sorted_table_schemas.push_back(*ptr_table_schemas.at(i)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBootstrap::generate_table_schema_array_for_create_partition(
    const share::schema::ObTableSchema &tschema,
    common::ObIArray<share::schema::ObTableSchema> &table_schema_array)
{
  int ret = OB_SUCCESS;
  table_schema_array.reset();
  const uint64_t table_id = tschema.get_table_id();
  int64_t tschema_idx = table_schema_array.count();

  if (OB_FAIL(table_schema_array.push_back(tschema))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(ObSysTableChecker::append_sys_table_index_schemas(
             OB_SYS_TENANT_ID, table_id, table_schema_array))) {
    LOG_WARN("fail to append sys table index schemas", KR(ret), K(table_id));
  } else if (OB_FAIL(add_sys_table_lob_aux_table(table_id, table_schema_array))) {
    LOG_WARN("fail to add lob table to sys table", KR(ret), K(table_id));
  }
  return ret;
}

int ObBootstrap::prepare_create_partition(
    ObTableCreator &creator,
    const share::schema_create_func func)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  const bool set_primary_zone = false;
  ObTableSchema tschema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (NULL == func) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func is null", KR(ret));
  } else if (OB_FAIL(func(tschema))) {
    LOG_WARN("failed to create table schema", KR(ret));
  } else if (tschema.has_partition()) {
    common::ObArray<share::schema::ObTableSchema> table_schema_array;
    common::ObArray<const share::schema::ObTableSchema*> table_schema_ptrs;
    common::ObArray<share::ObLSID> ls_id_array;
    if (OB_FAIL(generate_table_schema_array_for_create_partition(tschema, table_schema_array))) {
      LOG_WARN("fail to generate table schema array", KR(ret));
    } else if (OB_UNLIKELY(table_schema_array.count() < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate table schema count is unexpected", KR(ret));
    } else if (OB_FAIL(table_schema_ptrs.reserve(table_schema_array.count()))) {
      LOG_WARN("Fail to reserve rowkey column array", KR(ret));
    } else {
      for (int i = 0; i < table_schema_array.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(table_schema_ptrs.push_back(&table_schema_array.at(i)))) {
          LOG_WARN("fail to push back", KR(ret), K(table_schema_array));
        }
      }
      for (int i = 0; i < tschema.get_all_part_num() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ls_id_array.push_back(share::ObLSID(SYS_LS)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(creator.add_create_tablets_of_tables_arg(
            table_schema_ptrs,
            ls_id_array))) {
      LOG_WARN("fail to add create tablet arg", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed prepare create table partition",
             "table_id", tschema.get_table_id(),
             "table_name", tschema.get_table_name(),
             "cluster_role", cluster_role_to_str(arg_.cluster_role_));
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_core_table_partition()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObMySQLTransaction trans;
    ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
    ObTableCreator table_creator(OB_SYS_TENANT_ID,
                                 SCN::base_scn(),
                                 trans);
    if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(table_creator.init(false/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init tablet creator", KR(ret));
    } else {
      // create all core table partition
      for (int64_t i = 0; OB_SUCC(ret) && NULL != all_core_table_schema_creator[i]; ++i) {
        if (OB_FAIL(prepare_create_partition(
            table_creator, all_core_table_schema_creator[i]))) {
          LOG_WARN("prepare create partition fail", K(ret));
        }
      }
      // execute creating tablet
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_creator.execute())) {
          LOG_WARN("execute create partition failed", K(ret));
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }
  }

  LOG_INFO("finish creating all core table", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_partitions()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> sys_table_ids;
  ObArray<int64_t> partition_nums;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObMySQLTransaction trans;
    ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
    ObTableCreator table_creator(OB_SYS_TENANT_ID,
                                 SCN::base_scn(),
                                 trans);
    if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(table_creator.init(false/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init tablet creator", KR(ret));
    } else {
      // create core table partition
      for (int64_t i = 0; OB_SUCC(ret) && NULL != core_table_schema_creators[i]; ++i) {
        if (OB_FAIL(prepare_create_partition(
            table_creator, core_table_schema_creators[i]))) {
          LOG_WARN("prepare create partition fail", K(ret));
        }
      }
      // create sys table partition
      for (int64_t i = 0; OB_SUCC(ret) && NULL != sys_table_schema_creators[i]; ++i) {
        if (OB_FAIL(prepare_create_partition(
            table_creator, sys_table_schema_creators[i]))) {
          LOG_WARN("prepare create partition fail", K(ret));
        }
      }
      // execute creating tablet
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_creator.execute())) {
          LOG_WARN("execute create partition failed", K(ret));
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }
  }

  LOG_INFO("finish creating system tables", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::add_sys_table_lob_aux_table(
    uint64_t data_table_id,
    ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  if (is_system_table(data_table_id)) {
    HEAP_VARS_2((ObTableSchema, lob_meta_schema), (ObTableSchema, lob_piece_schema)) {
      if (OB_ALL_CORE_TABLE_TID == data_table_id) {
        // do nothing
      } else if (OB_FAIL(get_sys_table_lob_aux_schema(data_table_id, lob_meta_schema, lob_piece_schema))) {
        LOG_WARN("fail to get sys table lob aux schema", KR(ret), K(data_table_id));
      } else if (OB_FAIL(table_schemas.push_back(lob_meta_schema))) {
        LOG_WARN("fail to push lob meta into schemas", KR(ret), K(data_table_id));
      } else if (OB_FAIL(table_schemas.push_back(lob_piece_schema))) {
        LOG_WARN("fail to push lob piece into schemas", KR(ret), K(data_table_id));
      }
    }
  }
  return ret;
}

int ObBootstrap::construct_all_schema(ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const schema_create_func *creator_ptr_arrays[] = {
    core_table_schema_creators,
    sys_table_schema_creators,
    virtual_table_schema_creators,
    sys_view_schema_creators
  };

  ObTableSchema table_schema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(table_schemas.reserve(OB_SYS_TABLE_COUNT))) {
    LOG_WARN("reserve failed", "capacity", OB_SYS_TABLE_COUNT, KR(ret));
  } else {
    HEAP_VAR(ObTableSchema, data_schema) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
        for (const schema_create_func *creator_ptr = creator_ptr_arrays[i];
             OB_SUCCESS == ret && NULL != *creator_ptr; ++creator_ptr) {
          table_schema.reset();
          bool exist = false;
          if (OB_FAIL(construct_schema(*creator_ptr, table_schema))) {
            LOG_WARN("construct_schema failed", K(table_schema), KR(ret));
          } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
                     OB_SYS_TENANT_ID, table_schema, exist))) {
            LOG_WARN("fail to check inner table exist",
                     KR(ret), K(table_schema));
          } else if (!exist) {
            // skip
          } else if (ObSysTableChecker::is_sys_table_has_index(table_schema.get_table_id())) {
            const int64_t data_table_id = table_schema.get_table_id();
            if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(table_schema))) {
              LOG_WARN("fail to fill sys index infos", KR(ret), K(data_table_id));
            } else if (OB_FAIL(ObSysTableChecker::append_sys_table_index_schemas(
                       OB_SYS_TENANT_ID, data_table_id, table_schemas))) {
              LOG_WARN("fail to append sys table index schemas", KR(ret), K(data_table_id));
            }
          }

          const int64_t data_table_id = table_schema.get_table_id();
          if (OB_SUCC(ret) && exist) {
            // process lob aux table
            if (OB_FAIL(add_sys_table_lob_aux_table(data_table_id, table_schemas))) {
              LOG_WARN("fail to add lob table to sys table", KR(ret), K(data_table_id));
            }
            // push sys table
            if (OB_SUCC(ret) && OB_FAIL(table_schemas.push_back(table_schema))) {
              LOG_WARN("push_back failed", KR(ret), K(table_schema));
            }
          }
        }
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::broadcast_sys_schema(const ObSArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  obrpc::ObBatchBroadcastSchemaArg arg;
  obrpc::ObBatchBroadcastSchemaResult result;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", KR(ret), K(table_schemas));
  } else if (OB_FAIL(arg.init(OB_SYS_TENANT_ID,
                              OB_CORE_SCHEMA_VERSION,
                              table_schemas))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else {
    ObBatchBroadcastSchemaProxy proxy(rpc_proxy_,
                                      &ObSrvRpcProxy::batch_broadcast_schema);
    FOREACH_CNT_X(rs, rs_list_, OB_SUCC(ret)) {
      bool is_active = false;
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(proxy.call(rs->server_, rpc_timeout, arg))) {
        LOG_WARN("broadcast_sys_schema failed", KR(ret), K(rpc_timeout),
                 "server", rs->server_);
      }
    } // end foreach

    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS; // always wait all
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
      int res_ret = return_code_array.at(i);
      const ObAddr &addr = proxy.get_dests().at(i);
      if (OB_SUCCESS != res_ret) {
        ret = res_ret;
        LOG_WARN("broadcast schema failed", KR(ret), K(addr));
      }
    } // end for
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_schema(ObDDLService &ddl_service,
                                   ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("start create all schemas", "table count", table_schemas.count());
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", K(table_schemas), K(ret));
  } else {
    // persist __all_core_table's schema in inner table, which is only used for sys views.
    HEAP_VAR(ObTableSchema, core_table) {
       ObArray<ObTableSchema> tmp_tables;
      if (OB_FAIL(ObInnerTableSchema::all_core_table_schema(core_table))) {
        LOG_WARN("fail to construct __all_core_table's schema", KR(ret), K(core_table));
      } else if (OB_FAIL(tmp_tables.push_back(core_table))) {
        LOG_WARN("fail to push back __all_core_table's schema", KR(ret), K(core_table));
      } else if (OB_FAIL(batch_create_schema(ddl_service, tmp_tables, 0, 1))) {
        LOG_WARN("fail to create __all_core_table's schema", KR(ret), K(core_table));
      }
    }

    int64_t begin = 0;
    int64_t batch_count = BATCH_INSERT_SCHEMA_CNT;
    const int64_t MAX_RETRY_TIMES = 3;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      if (table_schemas.count() == (i + 1) || (i + 1 - begin) >= batch_count) {
        int64_t retry_times = 1;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(batch_create_schema(ddl_service, table_schemas, begin, i + 1))) {
            LOG_WARN("batch create schema failed", K(ret), "table count", i + 1 - begin);
            // bugfix:
            if ((OB_SCHEMA_EAGAIN == ret
                 || OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret)
                && retry_times <= MAX_RETRY_TIMES) {
              retry_times++;
              ret = OB_SUCCESS;
              LOG_INFO("schema error while create table, need retry", KR(ret), K(retry_times));
              ob_usleep(1 * 1000 * 1000L); // 1s
            }
          } else {
            break;
          }
        }
        if (OB_SUCC(ret)) {
          begin = i + 1;
        }
      }
    }
  }
  LOG_INFO("end create all schemas", K(ret), "table count", table_schemas.count(),
           "time_used", ObTimeUtility::current_time() - begin_time);
  return ret;
}

int ObBootstrap::batch_create_schema(ObDDLService &ddl_service,
                                     ObIArray<ObTableSchema> &table_schemas,
                                     const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObDDLSQLTransaction trans(&(ddl_service.get_schema_service()), true, true, false, false);
  if (begin < 0 || begin >= end || end > table_schemas.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin), K(end),
        "table count", table_schemas.count());
  } else {
    ObDDLOperator ddl_operator(ddl_service.get_schema_service(),
        ddl_service.get_sql_proxy());
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(trans.start(&ddl_service.get_sql_proxy(),
                            OB_SYS_TENANT_ID,
                            refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret));
    } else {
      bool is_truncate_table = false;
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        ObTableSchema &table = table_schemas.at(i);
        const ObString *ddl_stmt = NULL;
        bool need_sync_schema_version = !(ObSysTableChecker::is_sys_table_index_tid(table.get_table_id()) ||
                                          is_sys_lob_table(table.get_table_id()));
        int64_t start_time = ObTimeUtility::current_time();
        if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt,
                                              need_sync_schema_version,
                                              is_truncate_table))) {
          LOG_WARN("add table schema failed", K(ret),
              "table_id", table.get_table_id(),
              "table_name", table.get_table_name());
        } else {
          int64_t end_time = ObTimeUtility::current_time();
          LOG_INFO("add table schema succeed", K(i),
              "table_id", table.get_table_id(),
              "table_name", table.get_table_name(), "core_table", is_core_table(table.get_table_id()), "cost", end_time-start_time);
        }
      }
    }
  }

  const int64_t begin_commit_time = ObTimeUtility::current_time();
  if (trans.is_started()) {
    const bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end trans failed", K(tmp_ret), K(is_commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  const int64_t now = ObTimeUtility::current_time();
  LOG_INFO("batch create schema finish", K(ret), "table count", end - begin,
      "total_time_used", now - begin_time,
      "end_transaction_time_used", now - begin_commit_time);
  //BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::construct_schema(
    const share::schema_create_func func, ObTableSchema &tschema)
{
  int ret = OB_SUCCESS;
  BOOTSTRAP_CHECK_SUCCESS_V2("before construct schema");
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (NULL == func) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func is null", K(ret));
  } else if (OB_FAIL(func(tschema))) {
    LOG_WARN("failed to create table schema", K(ret));
  } else {} // no more to do
  return ret;
}

int ObBootstrap::add_servers_in_rs_list(rootserver::ObServerZoneOpService &server_zone_op_service) {
  int ret = OB_SUCCESS;
  ObArray<ObAddr> servers;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.root_service_ is null", KR(ret), KP(GCTX.root_service_));
  } else {
    if (!ObHeartbeatService::is_service_enabled()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
        const ObAddr &server = rs_list_.at(i).server_;
        const ObZone &zone = rs_list_.at(i).zone_;
        if (OB_FAIL(GCTX.root_service_->add_server_for_bootstrap_in_version_smaller_than_4_2_0(server, zone))) {
          LOG_WARN("fail to add server in version < 4.2", KR(ret), K(server), K(zone));
        }
        FLOG_INFO("add servers in rs_list_ in version < 4.2", KR(ret), K(server), K(zone));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
        servers.reuse();
        const ObAddr &server = rs_list_.at(i).server_;
        const ObZone &zone = rs_list_.at(i).zone_;
        if (OB_FAIL(servers.push_back(server))) {
          LOG_WARN("fail to push an element into servers", KR(ret), K(server));
        } else if (OB_FAIL(server_zone_op_service.add_servers(servers, zone, true /* is_bootstrap */))) {
          LOG_WARN("fail to add servers", KR(ret), K(servers), K(zone));
        }
        FLOG_INFO("add servers in rs_list_ in version >= 4.2", KR(ret), K(servers), K(zone));
      }
      if (FAILEDx(GCTX.root_service_->load_server_manager())) {
        LOG_WARN("fail to load server manager", KR(ret), KP(GCTX.root_service_));
      }
    }
  }
  return ret;
}

int ObBootstrap::wait_all_rs_in_service()
{
  int ret = OB_SUCCESS;
  const int64_t check_interval = 500 * 1000;
  int64_t left_time_can_sleep = WAIT_RS_IN_SERVICE_TIMEOUT_US;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (!ObRootServiceRoleChecker::is_rootserver()) {
      ret = OB_RS_SHUTDOWN;
      LOG_WARN("wait all rs in service fail, self is not master rootservice any more, check SYS LS leader revoke infos",
          KR(ret), K(left_time_can_sleep));
      break;
    }

    bool all_in_service = true;
    FOREACH_CNT_X(rs, rs_list_, all_in_service && OB_SUCCESS == ret) {
      bool in_service = false;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        left_time_can_sleep = max(left_time_can_sleep, THIS_WORKER.get_timeout_remain());
      }
      // mark
      if (OB_FAIL(SVR_TRACER.check_in_service(rs->server_, in_service))) {
        LOG_WARN("check_in_service failed", "server", rs->server_, K(ret));
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          all_in_service = false;
        }
      } else if (!in_service) {
        LOG_WARN("server is not in_service ", "server", rs->server_);
        all_in_service = false;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (all_in_service) {
      break;
    } else if (left_time_can_sleep > 0) {
      const int64_t time_to_sleep = min(check_interval, left_time_can_sleep);
      LOG_WARN("fail to wait all rs in service. wait a while", K(time_to_sleep), K(left_time_can_sleep));
      ob_usleep(static_cast<uint32_t>(time_to_sleep));
      left_time_can_sleep -= time_to_sleep;
    } else {
      ret = OB_WAIT_ALL_RS_ONLINE_TIMEOUT;
      LOG_WARN("wait all rs in service timeout", "timeout",
          static_cast<int64_t>(WAIT_RS_IN_SERVICE_TIMEOUT_US), K(ret));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::check_is_already_bootstrap(bool &is_bootstrap)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  is_bootstrap = true;
  ObMultiVersionSchemaService &schema_service = ddl_service_.get_schema_service();
  ObSchemaGetterGuard guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("get_schema_manager failed", K(ret));
  } else if (OB_FAIL(guard.get_schema_version(OB_SYS_TENANT_ID, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (OB_CORE_SCHEMA_VERSION == schema_version) {
    is_bootstrap = false;
  } else {
    // don't need to set ret
    //LOG_WARN("observer is bootstrap already", "schema_table_count", guard->get_table_count());
    LOG_WARN("observer is already bootstrap");
    //const bool is_verbose = false;
    //guard->print_info(is_verbose);
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_global_stat()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else {
    const int64_t baseline_schema_version = -1;
    const int64_t rootservice_epoch = 0;
    const SCN snapshot_gc_scn = SCN::min_scn();
    const int64_t snapshot_gc_timestamp = 0;
    const int64_t ddl_epoch = 0;
    ObGlobalStatProxy global_stat_proxy(trans, OB_SYS_TENANT_ID);
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
      LOG_WARN("trans start failed", KR(ret));
    } else if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", KR(ret));
    } else if (OB_FAIL(global_stat_proxy.set_init_value(
               OB_CORE_SCHEMA_VERSION, baseline_schema_version,
               rootservice_epoch, snapshot_gc_scn, snapshot_gc_timestamp, ddl_epoch,
               DATA_CURRENT_VERSION, DATA_CURRENT_VERSION))) {
      LOG_WARN("set_init_value failed", KR(ret), "schema_version", OB_CORE_SCHEMA_VERSION,
               K(baseline_schema_version), K(rootservice_epoch), K(ddl_epoch), "data_version", DATA_CURRENT_VERSION);
    }

    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("trans end failed", "commit", OB_SUCCESS == ret, KR(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }

    // Initializes a new state of refresh schema
    if (OB_SUCC(ret)) {
      ObRefreshSchemaStatus tenant_status(OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP,
          OB_INVALID_VERSION);
      if (OB_FAIL(schema_status_proxy->set_tenant_schema_status(tenant_status))) {
        LOG_WARN("fail to init create partition status", KR(ret), K(tenant_status));
      } else if (OB_FAIL(init_sequence_id())) {
        LOG_WARN("failed to init_sequence_id", KR(ret));
      } else {}
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_sequence_id()
{
  int ret = OB_SUCCESS;
  const int64_t rootservice_epoch = 0;
  ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
  ObSchemaService *schema_service = multi_schema_service.get_schema_service();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->init_sequence_id(rootservice_epoch))) {
    LOG_WARN("init sequence id failed", K(ret), K(rootservice_epoch));
  }
  return ret;
}

int ObBootstrap::gen_multiple_zone_deployment_sys_tenant_locality_str(
    share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone list count unexpected", K(ret));
  } else {
    const int64_t BUFF_SIZE = 256; // 256 is enough for sys tenant
    char locality_str[BUFF_SIZE] = "";
    bool first = true;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(databuff_printf(
              locality_str, BUFF_SIZE, pos, "%sF{1}@%s", first ? "": ", ", rs_list_.at(i).zone_.ptr()))) {
        LOG_WARN("fail to do databuff printf", K(ret));
      } else {
        first = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_locality(locality_str))) {
        LOG_WARN("fail to set locality", K(ret));
      }
    }
  }
  return ret;
}

int ObBootstrap::gen_sys_tenant_locality_str(
    share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    if (OB_FAIL(gen_multiple_zone_deployment_sys_tenant_locality_str(tenant_schema))) {
      LOG_WARN("fail to gen multiple zone deployment sys tenant locality str", K(ret));
    }
  }
  return ret;
}

int ObBootstrap::create_sys_tenant()
{
  // insert zero system stat value for create system tenant.
  int ret= OB_SUCCESS;
  ObTenantSchema tenant;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    obrpc::ObCreateTenantArg arg;
    arg.name_case_mode_ = OB_ORIGIN_AND_INSENSITIVE;
    tenant.set_tenant_id(OB_SYS_TENANT_ID);
    tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

    share::schema::ObSchemaGetterGuard dummy_schema_guard;
    ObArray<common::ObZone> zone_list;
    ObArray<share::schema::ObZoneRegion> zone_region_list;
    ObArenaAllocator allocator; // allocator for locality str
    int64_t pos = 0;
    ObLocalityDistribution locality_dist;
    common::ObArray<share::ObZoneReplicaAttrSet> zone_replica_num_array;
    char *locality_str = nullptr;
    if (OB_FAIL(gen_sys_tenant_locality_str(tenant))) {
      LOG_WARN("fail to gen sys tenant locality str", K(ret));
    } else if (OB_FAIL(gen_sys_zone_list(zone_list))) {
      LOG_WARN("fail to gen sys zone list", K(ret));
    } else if (OB_FAIL(build_zone_region_list(zone_region_list))) {
      LOG_WARN("fail to build zone region list", K(ret));
    } else if (OB_FAIL(locality_dist.init())) {
      LOG_WARN("fail to init locality distribution", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(
            tenant.get_locality_str(), zone_list, &zone_region_list))) {
      LOG_WARN("fail to parse tenant schema locality", K(ret));
    } else if (OB_ISNULL(locality_str = (char *)allocator.alloc(MAX_LOCALITY_LENGTH + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(locality_dist.output_normalized_locality(
            locality_str, MAX_LOCALITY_LENGTH, pos))) {
      LOG_WARN("fail to output normalized locality", K(ret));
    } else if (OB_FAIL(tenant.set_locality(locality_str))) {
      LOG_WARN("fail to set locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_num_array))) {
      LOG_WARN("fail to get zone region replica num array", K(ret));
    } else if (OB_FAIL(tenant.set_zone_replica_attr_array(zone_replica_num_array))) {
      LOG_WARN("fail to set zone replica_num array", K(ret));
    } else if (OB_FAIL(tenant.set_tenant_name(OB_SYS_TENANT_NAME))) {
      LOG_WARN("set_tenant_name failed", "tenant_name", OB_SYS_TENANT_NAME, K(ret));
    } else if (OB_FAIL(tenant.set_comment("system tenant"))) {
      LOG_WARN("set_comment failed", "comment", "system tenant", K(ret));
    } else if (OB_FAIL(set_replica_options(tenant))) {
      LOG_WARN("failed to set replica options", KR(ret));
    } else if (OB_FAIL(ddl_service_.check_primary_zone_locality_condition(
            tenant, zone_list, zone_region_list, dummy_schema_guard))) {
      LOG_WARN("fail to check primary zone region condition", K(ret));
    } else if (OB_FAIL(ddl_service_.create_sys_tenant(arg, tenant))) {
      LOG_WARN("create tenant failed", K(ret), K(tenant));
    } else if (OB_FAIL(insert_sys_ls_(tenant, zone_list))) {
      LOG_WARN("failed to insert sys ls", KR(ret), K(zone_list));
    } else {} // no more to do
  }

  LOG_INFO("create tenant", K(ret), K(tenant));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::insert_sys_ls_(const share::schema::ObTenantSchema &tenant_schema,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  ObZone primary_zone;
  ObSqlString primary_zone_str;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("rs_list_ count unexpected", K(ret), "rs list count", rs_list_.count());
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "rootserver list is empty");
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (rs_list_.at(i).server_ == GCTX.self_addr()) {
        if (found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("self addr is duplicate in rs list", KR(ret), K(rs_list_));
        } else {
          found = true;
          primary_zone = rs_list_.at(i).zone_;
        }
      }
      if (FAILEDx(primary_zone_str.append_fmt("%s", rs_list_.at(i).zone_.ptr()))) {
        LOG_WARN("failed to append fmt", KR(ret), K(i), K(rs_list_));
      } else if (rs_list_.count() - 1 != i) {
        if (OB_FAIL(primary_zone_str.append(","))) {
          LOG_WARN("failed to append fmt", KR(ret), K(i), K(rs_list_));
        }
      }
    }
    if (!found && OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can find self in rs list", KR(ret), K(rs_list_), "self", GCTX.self_addr());
    }
  }

  if (OB_SUCC(ret)) {
    ObLSLifeAgentManager life_agent(ddl_service_.get_sql_proxy());
    share::ObLSStatusInfo status_info;
    const uint64_t unit_group_id = 0;
    const uint64_t ls_group_id = 0;
    share::ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
    if (OB_FAIL(status_info.init(OB_SYS_TENANT_ID, SYS_LS, ls_group_id,
            share::OB_LS_NORMAL, unit_group_id, primary_zone, flag))) {
      LOG_WARN("failed to init ls info", KR(ret), K(primary_zone), K(flag));
    } else if (OB_FAIL(life_agent.create_new_ls(status_info, SCN::base_scn(), primary_zone_str.string(),
                                                share::NORMAL_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to get init member list", KR(ret), K(status_info), K(primary_zone_str));
    }
  }
  return ret;
}


int ObBootstrap::init_system_data()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(unit_mgr_.load())) {
    LOG_WARN("unit_mgr load failed", KR(ret));
  } else if (OB_FAIL(create_sys_unit_config())) {
    LOG_WARN("create_sys_unit_config failed", KR(ret));
  } else if (OB_FAIL(create_sys_resource_pool())) {
    LOG_WARN("create sys resource pool failed", KR(ret));
  } else if (OB_FAIL(create_sys_tenant())) {
    LOG_WARN("create system tenant failed", KR(ret));
  } else if (OB_FAIL(init_all_zone_table())) {
    LOG_WARN("failed to init all zone table", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_sys_unit_config(share::ObUnitConfig &unit_config)
{
  int ret = OB_SUCCESS;
  const bool is_hidden_sys = false;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(unit_config.gen_sys_tenant_unit_config(is_hidden_sys))) {
    LOG_WARN("gen sys tenant unit config fail", KR(ret), K(is_hidden_sys));
  } else {
    LOG_INFO("init sys tenant unit config succ", K(unit_config));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_sys_unit_config()
{
  int ret = OB_SUCCESS;
  ObUnitConfig unit_config;
  const bool if_not_exist = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(init_sys_unit_config(unit_config))) {
    LOG_WARN("init default sys unit config failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_unit_config(unit_config, if_not_exist))) {
    LOG_WARN("create_unit_config failed", K(unit_config), K(if_not_exist), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::gen_sys_resource_pool(
    share::ObResourcePool &pool)
{
  int ret = OB_SUCCESS;
  pool.resource_pool_id_ = OB_SYS_RESOURCE_POOL_ID;
  pool.name_ = "sys_pool";
  pool.unit_count_ = 1;
  pool.unit_config_id_ = ObUnitConfig::SYS_UNIT_CONFIG_ID;
  pool.tenant_id_ = OB_INVALID_ID;
  if (OB_FAIL(gen_sys_zone_list(pool.zone_list_))) {
    LOG_WARN("fail to gen sys zone list", K(ret));
  }
  return ret;
}

int ObBootstrap::create_sys_resource_pool()
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> sys_units;
  ObArray<ObResourcePoolName> pool_names;
  share::ObResourcePool pool;
  bool is_bootstrap = true;
  const bool if_not_exist = false;
  common::ObMySQLTransaction trans;
  common::ObArray<uint64_t> new_ug_id_array;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_resource_pool(pool))) {
    LOG_WARN("gen sys resource pool", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_resource_pool(
      pool, ObUnitConfig::SYS_UNIT_CONFIG_NAME, if_not_exist))) {
    LOG_WARN("create sys resource pool failed", K(pool),
             "unit_config", ObUnitConfig::SYS_UNIT_CONFIG_NAME, K(ret));
  } else if (OB_FAIL(gen_sys_units(sys_units))) {
    LOG_WARN("gen_sys_units failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_sys_units(sys_units))) {
    LOG_WARN("create_sys_units failed", K(sys_units), K(ret));
  } else if (OB_FAIL(pool_names.push_back(pool.name_))) {
    LOG_WARN("push_back failed", K(ret));
  } else if (OB_FAIL(trans.start(&unit_mgr_.get_sql_proxy(), OB_SYS_TENANT_ID))) {
    LOG_WARN("start transaction failed", KR(ret));
  } else if (OB_FAIL(unit_mgr_.grant_pools(
          trans, new_ug_id_array,
          lib::Worker::CompatMode::MYSQL, pool_names,
          OB_SYS_TENANT_ID, is_bootstrap))) {
    LOG_WARN("grant_pools_to_tenant failed", K(pool_names),
        "tenant_id", static_cast<uint64_t>(OB_SYS_TENANT_ID), K(ret));
  } else {
    if (OB_FAIL(unit_mgr_.load())) {
      LOG_WARN("unit_manager reload failed", K(ret));
    }
  }
  if (trans.is_started()) {
    const bool commit = (OB_SUCC(ret));
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
      LOG_WARN("trans end failed", K(commit), K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_multiple_zone_deployment_table(
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, zone_info) {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      zone_info.reset();
      zone_info.zone_ = rs_list_[i].zone_;
      // for compatibility with ob1.2, which has no region specified
      if (rs_list_[i].region_.is_empty()) {
        if (OB_FAIL(zone_info.region_.info_.assign(DEFAULT_REGION_NAME))) {
          LOG_WARN("fail assign default region info", K(ret), K(zone_info));
        }
      } else {
        if (OB_FAIL(zone_info.region_.info_.assign(
                ObString(rs_list_[i].region_.size(), rs_list_[i].region_.ptr())))) {
          LOG_WARN("fail assign region info", K(ret), K(zone_info));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_info.status_.info_.assign(
                ObString(ObZoneStatus::get_status_str(ObZoneStatus::ACTIVE))))) {
          LOG_WARN("fail to assign zone status str", KR(ret));
        } else {
          zone_info.status_.value_ = ObZoneStatus::ACTIVE;
        }
      }

      if (OB_SUCC(ret)) {
        zone_info.storage_type_.value_ = ObZoneInfo::STORAGE_TYPE_LOCAL;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObZoneTableOperation::insert_zone_info(sql_client, zone_info))) {
          LOG_WARN("insert zone info failed", K(ret), K(zone_info));
        }
      }
    }
  }
  return ret;
}

int ObBootstrap::init_all_zone_table()
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    HEAP_VAR(ObGlobalInfo, global_info) {
      if (OB_FAIL(ObZoneTableOperation::insert_global_info(trans, global_info))) {
        LOG_WARN("insert global info failed", K(ret));
      } else{
        if (OB_FAIL(init_multiple_zone_deployment_table(trans))) {
          LOG_WARN("fail to init multiple zone deployment table", K(ret));
        }
      }

      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", KR(tmp_ret), KR(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }

  LOG_INFO("init all zone table", KR(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

template<typename SCHEMA>
int ObBootstrap::set_replica_options(SCHEMA &schema)
{
  int ret = OB_SUCCESS;
  BOOTSTRAP_CHECK_SUCCESS_V2("before set replica options");
  ObArray<ObZone> zone_list;
  ObArray<ObString> zone_str_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema), K(ret));
  } else if (OB_FAIL(gen_sys_zone_list(zone_list))) {
    LOG_WARN("gen_zone_list failed", K(ret));
  } else if (zone_list.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_list is empty", K(zone_list), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      if (OB_FAIL(zone_str_list.push_back(ObString::make_string(zone_list.at(i).ptr())))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (zone_str_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone_str_list is empty", K(zone_str_list), K(ret));
    } else if (OB_FAIL(schema.set_zone_list(zone_str_list))) {
      LOG_WARN("set_zone_list failed", K(zone_str_list), K(ret));
    } else if (OB_FAIL(schema.set_primary_zone(OB_RANDOM_PRIMARY_ZONE))) {
      LOG_WARN("failed to set primary zone", KR(ret), K(schema));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::build_zone_region_list(
    ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  for (int64_t i = 0; i < rs_list_.count() && OB_SUCC(ret); ++i) {
    const common::ObZone &zone = rs_list_.at(i).zone_;
    const common::ObRegion &region = rs_list_.at(i).region_;
    if (OB_FAIL(zone_region_list.push_back(
            ObZoneRegion(zone, region, ObZoneRegion::CZY_NO_NEED_TO_CHECK)))) {
      LOG_WARN("fail to push back", K(ret), K(zone), K(region));
    } else {} // no more to do
  }
  return ret;
}

int ObBootstrap::set_in_bootstrap()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check inner stat error", K(ret));
  } else {
    ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
    ObSchemaService *schema_service = multi_schema_service.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else {
      schema_service->set_cluster_schema_status(
          ObClusterSchemaStatus::BOOTSTRAP_STATUS);
    }
  }
  return ret;
}


} // end namespace rootserver
} // end namespace oceanbase
