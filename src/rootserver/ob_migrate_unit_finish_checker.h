/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVICE_MIGRATE_UNIT_FINISH_CHECKER_H_
#define OCEANBASE_ROOTSERVICE_MIGRATE_UNIT_FINISH_CHECKER_H_ 1
#include "share/ob_define.h"
#include "ob_root_utils.h"
#include "ob_disaster_recovery_info.h" // DRUnitStatInfoMap
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
class ObMultiVersionSchemaService;
}
class ObLSStatusInfo;
class ObLSTableOperator;
}
namespace rootserver
{
class ObUnitManager;
class ObZoneManager;
class DRLSInfo;

class ObMigrateUnitFinishChecker : public share::ObCheckStopProvider
{
public:
  ObMigrateUnitFinishChecker(volatile bool &stop);
  virtual ~ObMigrateUnitFinishChecker();
public:
  int init(
      ObUnitManager &unit_mgr,
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy,
      share::ObLSTableOperator &lst_operator);
public:
  int check();
private:
  virtual int check_stop() const override;
  int try_check_migrate_unit_finish_not_in_locality(
      const uint64_t &tenant_id);
  int try_check_migrate_unit_finish_not_in_tenant();
  int try_check_migrate_unit_finish_by_tenant(
      const uint64_t tenant_id);
  int statistic_migrate_unit_by_ls(
      DRLSInfo &dr_ls_info,
      share::ObLSStatusInfo &ls_status_info);
  int try_finish_migrate_unit(
      const UnitStatInfoMap &unit_stat_info_map);
private:
  // data members
  bool inited_;
  ObUnitManager *unit_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  share::ObLSTableOperator *lst_operator_;
  volatile bool &stop_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateUnitFinishChecker);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVICE_MIGRATE_UNIT_FINISH_CHECKER_H_ */
