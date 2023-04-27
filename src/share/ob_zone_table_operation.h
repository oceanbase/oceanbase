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

#ifndef OCEANBASE_SHARE_OB_ZONE_TABLE_OPERATION_H_
#define OCEANBASE_SHARE_OB_ZONE_TABLE_OPERATION_H_

#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "common/ob_zone.h"
#include "share/ob_lease_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
class ObZoneInfoItem;
class ObZoneInfo;
struct ObGlobalInfo;

class ObZoneTableOperation
{
public:
  static int update_info_item(common::ObISQLClient &sql_client,
      const common::ObZone &zone, const ObZoneInfoItem &item, bool insert = false);

  static int get_zone_list(
      common::ObISQLClient &sql_client, common::ObIArray<common::ObZone> &zone_list);

  static int get_zone_lease_info(common::ObISQLClient &sql_client, ObZoneLeaseInfo &info);
  // check_zone_exists is a newly added arg.
  // If the zone not exists,
  // in the previous implementation, OB_SUCCESS will be returned.
  // However, if check_zone_exists is set, OB_ZONE_INFO_NOT_EXIST will be returned.
  static int load_global_info(
      common::ObISQLClient &sql_client,
      ObGlobalInfo &info,
      const bool check_zone_exists = false);
  // check_zone_exists is a newly added arg.
  // If the zone not exists,
  // in the previous implementation, OB_SUCCESS will be returned.
  // However, if check_zone_exists is set, OB_ZONE_INFO_NOT_EXIST will be returned.
  static int load_zone_info(
      common::ObISQLClient &sql_client,
      ObZoneInfo &info,
      const bool check_zone_exists = false);

  static int insert_global_info(common::ObISQLClient &sql_client, ObGlobalInfo &info);
  static int insert_zone_info(common::ObISQLClient &sql_client, ObZoneInfo &info);

  static int remove_zone_info(common::ObISQLClient &sql_client, const common::ObZone &zone);
  static int get_region_list(
      common::ObISQLClient &sql_client, common::ObIArray<common::ObRegion> &region_list);
  static int check_encryption_zone(
      common::ObISQLClient &sql_client,
      const common::ObZone &zone,
      bool &encryption);
  static int check_zone_active(
      common::ObISQLClient &sql_client,
      const common::ObZone &zone,
      bool &is_active);
  static int get_inactive_zone_list(
      common::ObISQLClient &sql_client,
      common::ObIArray<common::ObZone> &zone_list);
  static int get_active_zone_list(
      common::ObISQLClient &sql_client,
      common::ObIArray<common::ObZone> &zone_list);
private:
  template <typename T>
      static int set_info_item(const char *name, const int64_t value, const char *info_str,
          T &info);
  template <typename T>
      static int load_info(
          common::ObISQLClient &sql_client,
          T &info,
          const bool check_zone_exists);
  template <typename T>
      static int insert_info(common::ObISQLClient &sql_client, T &info);
  static int get_zone_item_count(int64_t &cnt);
  static int get_zone_info(
      const ObZone &zone,
      common::ObISQLClient &sql_client,
      ObZoneInfo &zone_info);
  // if is_active, then get active zone_list
  // if !is_active, then get inactive zone_list
  static int get_zone_list_(
      common::ObISQLClient &sql_client,
      common::ObIArray<common::ObZone> &zone_list,
      const bool is_active);
};

}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_ZONE_TABLE_OPERATION_H_
