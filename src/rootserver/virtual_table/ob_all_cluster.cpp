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

#define USING_LOG_PREFIX RS

#include "ob_all_cluster.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_print_utils.h"  // databuff_printf
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_remote_sql_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_multi_cluster_util.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_server_manager.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver {
ObAllVirtualCluster::~ObAllVirtualCluster()
{
  inited_ = false;
}

int ObAllVirtualCluster::init(share::schema::ObMultiVersionSchemaService& schema_service, ObZoneManager& zone_manager,
    obrpc::ObCommonRpcProxy& common_rpc_proxy, ObMySQLProxy& sql_proxy, share::ObPartitionTableOperator& pt_operator,
    obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_manager, ObRootService& root_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    zone_manager_ = &zone_manager;
    common_rpc_proxy_ = &common_rpc_proxy;
    sql_proxy_ = &sql_proxy;
    pt_operator_ = &pt_operator;
    rpc_proxy_ = &rpc_proxy;
    server_manager_ = &server_manager;
    root_service_ = &root_service;
    inited_ = true;
  }
  return ret;
}

int ObAllVirtualCluster::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (!start_to_read_) {
    const share::schema::ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CLUSTER_TID);
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else {
      common::ObArray<Column> columns;
      bool full_columns = true;
      if (OB_FAIL(get_full_row(table_schema, columns, full_columns))) {
        LOG_WARN("failed to add row", K(ret));
      } else if (OB_FAIL(project_row(columns, cur_row_, full_columns))) {
        LOG_WARN("failed to project row", K(ret), K(columns), K(cur_row_));
      } else {
        start_to_read_ = true;
        row = &cur_row_;
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualCluster::get_full_row(
    const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns, bool& full_columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    common::ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH> cluster_name;
    char* cluster_name_str = NULL;
    const char* cluster_role = "";
    const char* cluster_status = "";
    const char* switch_status_str = "";
    const char* switchover_info_str = "";
    const char* protection_mode = "";
    const char* protection_level = "";
    ObString redo_transopt_str;
    int64_t current_scn = ObTimeUtility::current_time();
    int64_t primary_cluster_id = OB_INVALID_ID;
    int64_t standby_became_primary_scn = 0;
    const int64_t cluster_create_ts = zone_manager_->get_cluster_create_timestamp();
    obrpc::ObGetSwitchoverStatusRes get_switch_status_res;

    full_columns = true;
    arena_allocator_.reuse();

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_manager_->get_cluster(cluster_name))) {
      LOG_WARN("failed to get cluster name", K(ret));
    } else if (OB_ISNULL(cluster_name_str = static_cast<char*>(arena_allocator_.alloc(MAX_ZONE_INFO_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret), KP(cluster_name_str));
    } else if (OB_FAIL(databuff_printf(cluster_name_str, MAX_ZONE_INFO_LENGTH, "%s", cluster_name.ptr()))) {
      LOG_WARN("failed to set cluster name", KR(ret), K(cluster_name));
    } else {
      // set cluster role amd cluster status
      cluster_role = cluster_type_to_str(common::PRIMARY_CLUSTER);
      cluster_status = cluster_status_to_str(common::CLUSTER_VALID);
      protection_mode = cluster_protection_mode_to_str(common::MAXIMUM_PERFORMANCE_MODE);
      protection_level = cluster_protection_level_to_str(common::MAXIMUM_PERFORMANCE_LEVEL);
      switch_status_str = share::ObClusterInfo::in_memory_switchover_status_to_str(share::ObClusterInfo::I_NOT_ALLOW);
      const int64_t switchover_epoch = 0;
      const int64_t cluster_id = GCONF.cluster_id;
      //////////////////////// make columns ////////////////////////
      ADD_COLUMN(set_int, table, "cluster_id", cluster_id, columns);
      ADD_COLUMN(set_varchar, table, "cluster_name", cluster_name_str, columns);
      ADD_COLUMN(set_timestamp, table, "created", cluster_create_ts, columns);
      ADD_COLUMN(set_varchar, table, "cluster_role", cluster_role, columns);
      ADD_COLUMN(set_varchar, table, "cluster_status", cluster_status, columns);
      ADD_COLUMN(set_int, table, "switchover#", switchover_epoch, columns);
      ADD_COLUMN(set_varchar, table, "switchover_info", switchover_info_str, columns);
      if (full_columns) {
        ADD_COLUMN(set_varchar, table, "switchover_status", switch_status_str, columns);
      }
      ADD_COLUMN(set_int, table, "current_scn", current_scn, columns);
      ADD_COLUMN(set_int, table, "standby_became_primary_scn", standby_became_primary_scn, columns);
      if (OB_INVALID_ID != primary_cluster_id) {
        ADD_COLUMN(set_int, table, "primary_cluster_id", primary_cluster_id, columns);
      } else {
        // set primary_cluster_id to NULL if primary not exist
        ADD_NULL_COLUMN(table, "primary_cluster_id", columns);
      }
      ADD_COLUMN(set_varchar, table, "protection_mode", protection_mode, columns);
      ADD_COLUMN(set_varchar, table, "protection_level", protection_level, columns);
      ADD_COLUMN(set_varchar, table, "redo_transport_options", redo_transopt_str, columns);
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
