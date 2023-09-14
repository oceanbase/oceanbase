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

#ifndef OCEANBASE_ROOTSERVER_OB_SHRINK_EXPAND_RESOURCE_POOL_CHECKER_
#define OCEANBASE_ROOTSERVER_OB_SHRINK_EXPAND_RESOURCE_POOL_CHECKER_

#include "share/ob_define.h"
#include "ob_root_utils.h"

namespace oceanbase
{

namespace obrpc
{
class ObSrvRpcProxy;
}
namespace share
{
class ObLSTableOperator;
struct ObResourcePool;
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace rootserver
{
class DRLSInfo;
class ObUnitManager;
class ObServerManager;
class ObZoneManager;
class ObShrinkExpandResourcePoolChecker : public share::ObCheckStopProvider
{
public:
  ObShrinkExpandResourcePoolChecker(volatile bool &is_stop)
      : is_stop_(is_stop),
        sql_proxy_(NULL),
        unit_mgr_(NULL),
        schema_service_(NULL),
        lst_operator_(NULL),
        is_inited_(false) {}
  virtual ~ObShrinkExpandResourcePoolChecker() {}
public:
  int init(
      share::schema::ObMultiVersionSchemaService *schema_service,
      rootserver::ObUnitManager *unit_mgr,
      share::ObLSTableOperator &lst_operator,
      common::ObMySQLProxy &sql_proxy);
public:
  int check();
private:
  virtual int check_stop() const override;
  int check_shrink_resource_pool_finished_by_tenant_(
      const uint64_t tenant_id);
  int extract_units_servers_and_ids_(
      const ObIArray<share::ObUnit> &units,
      ObIArray<common::ObAddr> &servers,
      ObIArray<uint64_t> &unit_ids,
      ObIArray<uint64_t> &unit_group_ids);
  int check_shrink_resource_pool_finished_by_ls_(
    const uint64_t tenant_id,
    const ObIArray<common::ObAddr> &servers,
    const ObIArray<uint64_t> &unit_ids,
    const ObIArray<uint64_t> &unit_group_ids,
    bool &is_finished);
  int commit_tenant_shrink_resource_pool_(const uint64_t tenant_id);
private:
  const volatile bool &is_stop_;
  common::ObMySQLProxy *sql_proxy_;
  rootserver::ObUnitManager *unit_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLSTableOperator *lst_operator_;
  bool is_inited_;
};
} // end of namespace rootserver
} // end of namespace oceanbase
#endif
