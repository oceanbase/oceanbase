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

#ifndef OCEANBASE_ROOTSERVER_OB_BALANCE_LS_PRIMARY_ZONE_H
#define OCEANBASE_ROOTSERVER_OB_BALANCE_LS_PRIMARY_ZONE_H
#include "lib/container/ob_array.h"//ObIArray
#include "common/ob_zone.h"//ObZone
namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObMySQLTransaction;
class ObSqlString;
}
namespace share
{
class ObLSTableOperator;
class SCN;
struct ObLSPrimaryZoneInfo;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}
}

namespace rootserver
{

class ObBalanceLSPrimaryZone
{
public:
  ObBalanceLSPrimaryZone() {};
  virtual ~ObBalanceLSPrimaryZone() {};

public:
  static int try_update_sys_ls_primary_zone(const uint64_t tenant_id);
  static int try_adjust_user_ls_primary_zone(const share::schema::ObTenantSchema &tenant_schema);
  static int try_update_ls_primary_zone(
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &new_zone_priority);
  static int need_update_ls_primary_zone (
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &new_zone_priority,
    bool &need_update);
  static int prepare_sys_ls_balance_primary_zone_info(
    const uint64_t tenant_id,
    share::ObLSPrimaryZoneInfo &primary_zone_info,
    common::ObZone &new_primary_zone,
    common::ObSqlString &new_zone_priority);
  static int check_sys_ls_primary_zone_balanced(const uint64_t tenant_id, int &check_ret);

private:
  static int adjust_primary_zone_by_ls_group_(const common::ObIArray<common::ObZone> &primary_zone_array,
                                       const ObIArray<share::ObLSPrimaryZoneInfo> &primary_zone_infos,
                                       const share::schema::ObTenantSchema &tenant_schema);
  static int set_ls_to_primary_zone_(const common::ObIArray<common::ObZone> &primary_zone_array,
                             const ObIArray<share::ObLSPrimaryZoneInfo> &primary_zone_infos,
                             common::ObIArray<common::ObZone> &ls_primary_zone,
                             common::ObIArray<uint64_t> &count_group_by_zone);
  static int balance_ls_primary_zone_(const common::ObIArray<common::ObZone> &primary_zone_array,
                              common::ObIArray<common::ObZone> &ls_primary_zone,
                              common::ObIArray<uint64_t> &count_group_by_zone);
};

}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_LS_PRIMARY_ZONE_H */
