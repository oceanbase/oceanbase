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

#include "share/ob_ls_id.h"                         // ObLSID
#include "common/ob_zone.h"                         // ObZone
#include "rootserver/ob_ls_service_helper.h"        //ObTenantLSInfo

namespace oceanbase
{
namespace rootserver
{
class ObAlterLSArg
{
  OB_UNIS_VERSION(1);
public:
  enum ObAlterLSOp {
    INVALID_ALTER_LS_OP = 0,
    CREATE_LS,
    MODIFY_LS,
    DROP_LS,
    MAX_ALTER_LS_OP
  };
  static const char *alter_ls_op_to_str(const ObAlterLSOp &op);
  ObAlterLSArg()
      : op_(INVALID_ALTER_LS_OP), tenant_id_(OB_INVALID_TENANT_ID),
        ls_id_(share::ObLSID::INVALID_LS_ID), unit_group_id_(OB_INVALID_ID), ls_primary_zone_() {}
  ~ObAlterLSArg() {}
  // empty ls_primary_zone means the user did not specify ls_primary_zone
  int init_create_ls(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      const common::ObZone &ls_primary_zone);
   // empty ls_primary_zone means the user did not specify ls_primary_zone
   // unit_group_id = -1 means the user did not specify unit_group_id
  int init_modify_ls(
      const uint64_t tenant_id,
      const int64_t ls_id,
      const uint64_t unit_group_id,
      const common::ObZone &ls_primary_zone);
  int init_drop_ls(const uint64_t tenant_id, const int64_t ls_id);
  bool is_valid() const;
  int assign(const ObAlterLSArg &other);
  bool is_create_service() const { return CREATE_LS == op_; }
  bool is_modify_service() const { return MODIFY_LS == op_; }
  bool is_drop_service() const { return DROP_LS == op_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  uint64_t get_unit_group_id () const { return unit_group_id_; }
  const common::ObZone &get_ls_primary_zone() const { return ls_primary_zone_; }
  TO_STRING_KV(K_(op), "op str", alter_ls_op_to_str(op_),
      K_(tenant_id), K_(ls_id), K_(unit_group_id), K_(ls_primary_zone));
private:
  ObAlterLSOp op_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  uint64_t unit_group_id_;
  common::ObZone ls_primary_zone_;
};
struct ObAlterLSRes
{
  OB_UNIS_VERSION(1);
public:
  ObAlterLSRes()
    : ret_(common::OB_SUCCESS),
      ls_id_() {}
  ~ObAlterLSRes() {}
  int ret_;
  ObLSID ls_id_;
  TO_STRING_KV(K_(ret), K_(ls_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterLSRes);
};
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
  static bool is_ls_primary_zone_ok_(
      const ObIArray<common::ObZone> &tenant_primary_zone_arr,
      const common::ObZone &ls_primary_zone);
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
      ObLSGroupInfoArray &ls_group_info_arr);
  static int update_ls_group_id_in_all_ls_(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const uint64_t new_ls_group_id);
  static int modify_ls_primary_zone_(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &old_ls_primary_zone,
      const common::ObZone &new_ls_primary_zone,
      share::schema::ObTenantSchema *tenant_schema);
  DISALLOW_COPY_AND_ASSIGN(ObAlterLSCommand);
};

} // name space rootserver
} // name space oceanbase
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_MANAGEMENT_COMMAND_H_ */