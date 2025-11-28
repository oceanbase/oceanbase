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
  ObAlterLSCommand() : is_unit_list_enabled_(false), arg_(), in_hetero_deploy_mode_(false), unit_list_(), sql_proxy_() {}
  ~ObAlterLSCommand(){}
  int process(const ObAlterLSArg &arg, ObAlterLSRes &res);
private:
  int init_(const ObAlterLSArg &arg);
  int check_inner_stat_() const;
  int pre_check_for_zone_deploy_mode_();
  int generate_unit_list_for_homo_zone_();
  int check_unit_list_locality_for_hetero_zone_();
  int get_zone_list_and_units_(ObIArray<ObZone> &zone_list, common::ObIArray<ObUnit> &units_in_unit_list);
  int modify_ls_();
  int check_modify_ls_(ObTenantLSInfo &tenant_ls_info, ObLSStatusInfo &ls_status_info);

  int create_ls_();
  int check_create_ls_(ObTenantLSInfo &tenant_ls_info);
  bool is_ls_primary_zone_ok_(
      const ObIArray<common::ObZone> &tenant_primary_zone_arr,
      const common::ObZone &ls_primary_zone);
  int try_get_exist_ls_group_id_(ObTenantLSInfo &tenant_ls_info, uint64_t &ls_group_id);
  int gen_ls_group_id_(ObTenantLSInfo &tenant_ls_info, uint64_t &ls_group_id);
  int check_if_need_update_unit_(const ObLSStatusInfo &ls_status_info, bool &need_update);
  int modify_unit_(const ObLSStatusInfo &ls_status_info, ObTenantLSInfo &tenant_ls_info, uint64_t &new_ls_group_id);
  int update_ls_group_id_in_all_ls_(const uint64_t old_ls_group_id, const uint64_t new_ls_group_id);
  int modify_ls_primary_zone_(
      const common::ObZone &old_ls_primary_zone,
      const common::ObZone &new_ls_primary_zone,
      share::schema::ObTenantSchema *tenant_schema);
  int gen_ls_primary_zone_(
      const uint64_t &ls_group_id,
      ObTenantLSInfo &tenant_ls_info,
      ObZone &ls_primary_zone);
  int insert_ls_(
      const uint64_t ls_group_id,
      const ObZone& ls_primary_zone,
      ObTenantLSInfo &tenant_ls_info,
      ObLSID &new_ls_id);
  int get_unit_id_list_(ObUnitIDList &unit_id_list);
  // return true if old_unit_id_list == unit_list_
  bool is_same_unit_id_list_(const ObUnitIDList &old_unit_id_list);
  int print_insert_ls_error_(const ObLSID &ls, const int orig_ret);
  int print_deleting_unit_error_(const uint64_t unit_id);
  int print_invalid_unit_error_(const common::ObIArray<ObUnit> &units_in_unit_list);
  int print_unavailable_unit_group_error_(const uint64_t unit_group_id);
  int print_zone_error_(const ObZone &zone);
private:
  bool is_unit_list_enabled_;
  ObAlterLSArg arg_;
  bool in_hetero_deploy_mode_;
  // 同构zone需要将unit group 转换为 unit list。异构zone和同构zone最后要用的unit list都统一存在这里
  ObAlterLSArg::UnitListArg unit_list_;
  common::ObMySQLProxy *sql_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObAlterLSCommand);
};

} // name space rootserver
} // name space oceanbase
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_MANAGEMENT_COMMAND_H_ */
