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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/thread/ob_work_queue.h"
#include "share/ob_virtual_table_projector.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
#include "src/rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
}
}

namespace rootserver
{
class ObZoneManager;
class ObRootService;

// class 2: trigger inspection by ALTER SYSTEM RUN JOB 'ROOT_INSPECTION'
class ObRootInspection
{
public:
  ObRootInspection();
  virtual ~ObRootInspection();
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           ObZoneManager &zone_mgr, common::ObMySQLProxy &sql_proxy, obrpc::ObCommonRpcProxy *rpc_proxy = NULL);
  inline bool is_inited() const { return inited_; }
  void start() { stopped_ = false; }
  void stop() { stopped_ = true; }
  virtual int check_all();
  // only called in upgrade status.
  // due to performance issues, not check sys table schema
  // the caller should call check sys table schema manually
  int check_tenant_in_upgrade(const uint64_t tenant_id);
  inline bool is_zone_passed() const { return zone_passed_; }
  inline bool is_sys_param_passed() const { return sys_param_passed_; }
  inline bool is_sys_stat_passed() const { return sys_stat_passed_; }
  inline bool is_sys_table_schema_passed() const { return sys_table_schema_passed_; }
  inline bool is_data_version_passed() const { return data_version_passed_; }
  inline bool is_all_checked() const { return all_checked_; }
  inline bool is_all_passed() const { return all_passed_; }
  int check_sys_table_schemas(const ObIArray<uint64_t> &tenant_ids);

  // !!! ATTENTION !!!
  // this function will return SUCCESS if inner table schema has error
  // remember to check result.error_table_ids to get all error tables
  static int check_sys_table_schema(const obrpc::ObCheckSysTableSchemaArg &arg,
    obrpc::ObCheckSysTableSchemaResult &result);

  // For system tables, check and get column schemas' difference
  // between table schema in memory and hard code table schema.
  // 1. Drop column: Not supported.
  // 2. Add column: Can only add columns at last.
  // 3. Alter column: Can only alter columns online.
  static int check_and_get_system_table_column_diff(
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObTableSchema &hard_code_schema,
    common::ObIArray<uint64_t> &add_column_ids,
    common::ObIArray<uint64_t> &alter_column_ids);
private:
  int construct_tenant_ids_(common::ObIArray<uint64_t> &tenant_ids);
  int check_all_on_rs_();
  int check_all_on_tenants_();
  int check_zone();
  int check_sys_stat_();
  int check_sys_param_();
  int check_sys_table_schemas_();
  int check_sys_table_schema_(const uint64_t tenant_id,
      rootserver::ObCheckSysTableSchemaProxy &proxy);

  int wait_and_check_rpc_response(rootserver::ObCheckSysTableSchemaProxy &proxy);
  int async_run_inspection_(const uint64_t tenant_id, rootserver::ObAsyncRunInspectionProxy &proxy);
  int wait_and_check_async_run_inspection_response(rootserver::ObAsyncRunInspectionProxy &proxy);

  int check_data_version_();

  int check_cancel();
private:
  bool inited_;
  volatile bool stopped_;

  bool zone_passed_;
  bool sys_param_passed_;
  bool sys_stat_passed_;
  bool sys_table_schema_passed_;
  bool data_version_passed_;

  bool all_checked_;
  bool all_passed_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObZoneManager *zone_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootInspection);
};

// virtual table __all_virtual_upgrade_inspection
// the result of ALTER SYSTEM RUN JOB 'ROOT_INSPECTION'
class ObUpgradeInspection : public common::ObVirtualTableProjector
{
public:
  ObUpgradeInspection();
  virtual ~ObUpgradeInspection();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           ObRootInspection &root_inspection);
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  struct ObInspectionItem
  {
    ObInspectionItem() : any_failed_(false), any_checking_(false) {}
    bool any_failed_;
    bool any_checking_;
  };
  static void merge_check_result_(ObInspectionItem &item, const bool passed);
  static void mark_checking_(ObInspectionItem &item);
  static const char *get_status_(const ObInspectionItem &item);
  int get_full_row(const share::schema::ObTableSchema *table,
                   const char *name, const char *info,
                   common::ObIArray<Column> &columns);
  int inner_get_next_row_on_rs_(common::ObNewRow *&row);
  int inner_get_next_row_on_tenants_(common::ObNewRow *&row);
private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObRootInspection *root_inspection_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeInspection);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_
