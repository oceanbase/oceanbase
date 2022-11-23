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

#ifndef OCEANBASE_STANDBY_OB_PRIMARY_STANDBY_SERVICE_H_
#define OCEANBASE_STANDBY_OB_PRIMARY_STANDBY_SERVICE_H_

#include "share/ob_rpc_struct.h"                          // ObAdminClusterArg
#include "share/ob_rs_mgr.h"                              // ObRsMgr
#include "lib/mysqlclient/ob_isql_client.h"               // ObISQLClient
#include "rootserver/ob_ddl_service.h"                    // ObDDLService
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService

namespace oceanbase
{

using namespace share;

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace standby
{

class ObPrimaryStandbyService
{
public:
  ObPrimaryStandbyService(): 
           sql_proxy_(NULL),
           schema_service_(NULL),
           inited_(false) {}
  virtual ~ObPrimaryStandbyService() {}

  int init(ObMySQLProxy *sql_proxy,
           share::schema::ObMultiVersionSchemaService *schema_service);
  void destroy();

  /**
   * @description:
   *    switch tenant role
   * @param[in] arg 
   * @return return code
   */
  int switch_tenant(const obrpc::ObSwitchTenantArg &arg);

private:
  int check_inner_stat_();

  /**
   * @description:
   *    failover standby tenant to primary tenant
   * @param[in] tenant_id the standby tenant id to failover
   * @param[in] arg tenant switch arguments
   * @return return code
   */
  int failover_to_primary(const uint64_t tenant_id, const obrpc::ObSwitchTenantArg &arg);

  const static int64_t SEC_UNIT = 1000L * 1000L;
  const static int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;

private:
  ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  bool inited_;
};

class ObPrimaryStandbyServiceGetter
{
public:
  static ObPrimaryStandbyService &get_instance()
  {
    static ObPrimaryStandbyService primary_standby_service;
    return primary_standby_service;
  }
};

#define OB_PRIMARY_STANDBY_SERVICE (oceanbase::standby::ObPrimaryStandbyServiceGetter::get_instance())

}  // end namespace standby
}  // end namespace oceanbase

#endif  // OCEANBASE_STANDBY_OB_PRIMARY_STANDBY_SERVICE_H_
