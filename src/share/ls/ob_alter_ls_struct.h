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
#ifndef OCEANBASE_SHARE_OB_ALTER_LS_STRUCT_H_
#define OCEANBASE_SHARE_OB_ALTER_LS_STRUCT_H_
#include "share/ob_ls_id.h"                         // ObLSID
#include "common/ob_zone.h"                         // ObZone
namespace oceanbase
{
namespace share
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
   // unit_group_id = -1 means the user did not specify unit_group_id
  int init_modify_ls(
      const uint64_t tenant_id,
      const ObLSID ls_id,
      const uint64_t unit_group_id,
      const common::ObZone &ls_primary_zone);
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
} // share
} // oceanbase
#endif // OCEANBASE_SHARE_OB_ALTER_LS_STRUCT_H_