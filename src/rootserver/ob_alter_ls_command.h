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

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_LS_COMMAND_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_LS_COMMAND_H_

#include "share/ls/ob_alter_ls_struct.h"                         // ObLSID
#include "rootserver/ob_ls_service_helper.h"        //ObTenantLSInfo

namespace oceanbase
{
namespace rootserver
{
class ObAlterLSCommand
{
public:
  ObAlterLSCommand(){}
  ~ObAlterLSCommand(){}
  static int process(const ObAlterLSArg &arg, ObAlterLSRes &res);
private:
  static int modify_ls_(const ObAlterLSArg &arg);
  static int check_modify_ls_(
      const ObLSID &ls_id,
      const uint64_t unit_group_id,
      const common::ObZone &ls_primary_zone,
      ObTenantLSInfo &tenant_ls_info,
      ObLSStatusInfo &ls_status_info);
  static int create_ls_(const ObAlterLSArg &arg);
  static int check_create_ls_(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      const common::ObZone &ls_primary_zone,
      ObTenantLSInfo &tenant_ls_info);
  static bool is_ls_primary_zone_ok_(
      const ObIArray<common::ObZone> &tenant_primary_zone_arr,
      const common::ObZone &ls_primary_zone);
  static bool is_unit_group_ok_(
      ObUnitGroupInfoArray &tenant_unit_group_arr,
      const uint64_t unit_group_id);
  static bool is_unit_group_ok_(
      ObUnitGroupInfoArray &tenant_unit_group_arr,
      const uint64_t unit_group_id,
      const ObLSStatusInfo &ls_status_info);
  static int gen_ls_group_id_(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      ObLSGroupInfoArray &ls_group_info_arr,
      uint64_t &ls_group_id);
  static int modify_unit_group_id_(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const uint64_t old_unit_group_id,
      const uint64_t new_unit_group_id,
      const uint64_t old_ls_group_id,
      uint64_t &new_ls_group_id,
      ObLSGroupInfoArray &ls_group_info_arr);
  static int update_ls_group_id_in_all_ls_(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const uint64_t old_ls_group_id,
      const uint64_t new_ls_group_id);
  static int modify_ls_primary_zone_(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &old_ls_primary_zone,
      const common::ObZone &new_ls_primary_zone,
      share::schema::ObTenantSchema *tenant_schema);
  static int gen_ls_primary_zone_(
      const uint64_t &ls_group_id,
      ObTenantLSInfo &tenant_ls_info,
      ObZone &ls_primary_zone);
  static int insert_ls_(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      const uint64_t ls_group_id,
      const ObZone& ls_primary_zone,
      ObTenantLSInfo &tenant_ls_info,
      ObLSID &new_ls_id);
  static int print_insert_ls_error(const uint64_t tenant_id, const ObLSID &ls, const int orig_ret);

  DISALLOW_COPY_AND_ASSIGN(ObAlterLSCommand);
};

} // name space rootserver
} // name space oceanbase
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_MANAGEMENT_COMMAND_H_ */