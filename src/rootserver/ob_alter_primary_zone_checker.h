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
#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_PRIMARY_ZONE_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_PRIMARY_ZONE_CHECKER_H_
#include "share/ob_define.h"
#include "ob_root_utils.h"
namespace oceanbase
{
namespace rootserver
{
class ObAlterPrimaryZoneChecker : public share::ObCheckStopProvider
{
public:
  ObAlterPrimaryZoneChecker(volatile bool &is_stopped);
  virtual ~ObAlterPrimaryZoneChecker();
  int init(share::schema::ObMultiVersionSchemaService &schema_service);
  int check();
  static int create_alter_tenant_primary_zone_rs_job_if_needed(
    const obrpc::ObModifyTenantArg &arg,
    const uint64_t tenant_id,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    const share::schema::ObTenantSchema &new_tenant_schema,
    ObMySQLTransaction &trans);
private:
  virtual int check_stop() const override;
  int check_primary_zone_for_each_tenant_(uint64_t tenant_id);
  volatile bool &is_stopped_;
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterPrimaryZoneChecker);
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_PRIMARY_ZONE_CHECKER_H_