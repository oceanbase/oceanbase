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

#define USING_LOG_PREFIX SHARE

#include "ob_alter_ls_struct.h"

#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace share
{
OB_SERIALIZE_MEMBER(ObAlterLSArg::UnitListArg, has_unit_id_list_, unit_id_list_);
OB_SERIALIZE_MEMBER(ObAlterLSArg, op_, tenant_id_, ls_id_, unit_group_id_, ls_primary_zone_, unit_list_);
OB_SERIALIZE_MEMBER(ObAlterLSRes, ret_, ls_id_);

int ObAlterLSArg::UnitListArg::init(const ObIArray<uint64_t> &unit_id_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(unit_id_list_.assign(unit_id_list))) {
    LOG_WARN("fail to assign unit id list", KR(ret), K(unit_id_list));
  } else {
    has_unit_id_list_ = true;
  }
  return ret;
}

int ObAlterLSArg::UnitListArg::assign(const UnitListArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(unit_id_list_.assign(other.unit_id_list_))) {
      LOG_WARN("fail to assign unit list", KR(ret), K(other));
    } else {
      has_unit_id_list_ = other.has_unit_id_list_;
    }
  }
  return ret;
}

void ObAlterLSArg::UnitListArg::reset()
{
  has_unit_id_list_ = false;
  unit_id_list_.reset();
}

int64_t ObAlterLSArg::UnitListArg::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const char * has_unit_id_list_str = has_unit_id_list_ ? "true" : "false";
  J_OBJ_START();
  BUF_PRINTF("has_unit_id_list_: %s", has_unit_id_list_str);
  if (has_unit_id_list_) {
    BUF_PRINTF(", unit_id_list_: ");
    J_ARRAY_START();
    int64_t unit_cnt = unit_id_list_.count();
    if (unit_cnt > 0) {
      int64_t arr_end = MAX_PRINT_UNIT_NUM > unit_cnt ? unit_cnt - 1 : MAX_PRINT_UNIT_NUM - 1;
      for (int64_t i = 0; i < arr_end; ++i) {
        BUF_PRINTO(unit_id_list_[i]);
        J_COMMA();
      }
      BUF_PRINTO(unit_id_list_[arr_end]);
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

static const char *ALTER_LS_OP_STR[] = {
    "INVALID ALTER LS OP",
    "CREATE LS",
    "MODIFY LS",
    "DROP LS",
};
const char *ObAlterLSArg::alter_ls_op_to_str(const ObAlterLSArg::ObAlterLSOp &op)
{
  STATIC_ASSERT(ARRAYSIZEOF(ALTER_LS_OP_STR) == MAX_ALTER_LS_OP, "array size mismatch");
  ObAlterLSOp returned_alter_ls_op = INVALID_ALTER_LS_OP;
  if (OB_UNLIKELY(op >= MAX_ALTER_LS_OP
                  || op < INVALID_ALTER_LS_OP)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown alter ls op", K(op));
  } else {
    returned_alter_ls_op = op;
  }
  return ALTER_LS_OP_STR[returned_alter_ls_op];
}
int ObAlterLSArg::init_create_ls(
    const uint64_t tenant_id,
    const uint64_t unit_group_id,
    const common::ObZone &ls_primary_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id)
      || OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or unit_group_id", KR(ret), K(tenant_id), K(unit_group_id));
  } else if (!ls_primary_zone.is_empty() && OB_FAIL(ls_primary_zone_.assign(ls_primary_zone))) {
    LOG_WARN("fail to assign ls_primary_zone", KR(ret), K(ls_primary_zone));
  } else {
    tenant_id_ = tenant_id;
    unit_group_id_ = unit_group_id;
    op_ = CREATE_LS;
  }
  return ret;
}

int ObAlterLSArg::init_modify_ls(
    const uint64_t tenant_id,
    const ObLSID ls_id,
    const uint64_t unit_group_id,
    const common::ObZone &ls_primary_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or ls_id", KR(ret), K(tenant_id), K(ls_id));
  } else if (!ls_primary_zone.is_empty() && OB_FAIL(ls_primary_zone_.assign(ls_primary_zone))) {
    LOG_WARN("fail to assign ls_primary_zone", KR(ret), K(ls_primary_zone));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    unit_group_id_ = unit_group_id;
    op_ = MODIFY_LS;
  }
  return ret;
}
int ObAlterLSArg::init_drop_ls(const uint64_t tenant_id, const int64_t ls_id)
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

bool ObAlterLSArg::is_valid() const
{
  bool bret = is_valid_tenant_id(tenant_id_);
  if (bret && MODIFY_LS == op_) {
    // either unit_group_id or ls_primary_zone can be invalid
    // they cannot be invalid at the same time
    bret = ls_id_.is_valid_with_tenant(tenant_id_) && !(OB_INVALID_ID == unit_group_id_ && ls_primary_zone_.is_empty());
  } else if (bret && CREATE_LS == op_) {
    bret = is_user_tenant(tenant_id_) && (OB_INVALID_ID != unit_group_id_);
  } else {
    bret = false;
  }
  return bret;
}

int ObAlterLSArg::assign(const ObAlterLSArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ls_primary_zone_.assign(other.ls_primary_zone_))) {
      LOG_WARN("fail to assign ls_primary_zone", KR(ret), K(other));
    } else {
      op_ = other.op_;
      tenant_id_ = other.tenant_id_;
      ls_id_ = other.ls_id_;
      unit_group_id_ = other.unit_group_id_;
    }
  }
  return ret;
}
} // share
} // oceanbase