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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_errno.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{


ObRevokeStmt::ObRevokeStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_GRANT),
      priv_set_(0),
      grant_level_(OB_PRIV_INVALID_LEVEL),
      database_(),
      table_(),
      tenant_id_(OB_INVALID_ID),
      revoke_all_(false),
      role_id_set_(),
      object_type_(share::schema::ObObjectType::INVALID),
      sys_priv_array_(),
      obj_priv_array_(),
      obj_id_(),
      obj_type_(),
      grantor_id_(),
      revoke_all_ora_(false),
      has_warning_(false),
      column_names_priv_(),
      table_schema_version_(0)
{
}

ObRevokeStmt::ObRevokeStmt()
    : ObDDLStmt(NULL, stmt::T_GRANT),
      priv_set_(0),
      grant_level_(OB_PRIV_INVALID_LEVEL),
      database_(),
      table_(),
      tenant_id_(OB_INVALID_ID),
      revoke_all_(false),
      role_id_set_(),
      object_type_(share::schema::ObObjectType::INVALID),
      sys_priv_array_(),
      obj_priv_array_(),
      obj_id_(),
      obj_type_(),
      grantor_id_(),
      revoke_all_ora_(false),
      has_warning_(false),
      column_names_priv_(),
      table_schema_version_(0)
{
}

ObRevokeStmt::~ObRevokeStmt()
{
}

int ObRevokeStmt::add_user(const uint64_t user_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(users_.push_back(user_id))) {
    LOG_WARN("failed to add user_id to users", K(user_id), K(ret));

  }
  return ret;
}

int ObRevokeStmt::add_role(const uint64_t role_id)
{
  int ret = OB_SUCCESS;
  if (OB_HASH_EXIST == role_id_set_.exist_refactored(role_id)) {
    ret = OB_PRIV_DUP;
    LOG_WARN("revoke duplicated role", K(ret), K(role_id));
  } else if (OB_FAIL(role_id_set_.set_refactored(role_id))) {
    LOG_WARN("set role to hash set failed", K(ret), K(role_id));
  } else if (OB_FAIL(user_arg_.role_ids_.push_back(role_id))) {
    LOG_WARN("failed to add role", K(ret), K(role_id));
  }
  return ret;
}

int ObRevokeStmt::add_role_ora(const uint64_t role_id)
{
  int ret = OB_SUCCESS;
  if (OB_HASH_EXIST == role_id_set_.exist_refactored(role_id)) {
    ret = OB_PRIV_DUP;
    LOG_WARN("revoke duplicated role", K(ret), K(role_id));
  } else if (OB_FAIL(role_id_set_.set_refactored(role_id))) {
    LOG_WARN("set role to hash set failed", K(ret), K(role_id));
  } else if (OB_FAIL(syspriv_arg_.role_ids_.push_back(role_id))) {
    LOG_WARN("failed to add role", K(ret), K(role_id));
  } else if (OB_FAIL(user_arg_.role_ids_.push_back(role_id))) {
    LOG_WARN("failed to add role", K(ret), K(role_id));
  }
  return ret;
}

void ObRevokeStmt::add_priv(const ObPrivType priv)
{
  priv_set_ |= priv;
}

void ObRevokeStmt::set_grant_level(ObPrivLevel grant_level)
{
  grant_level_ = grant_level;
}

void ObRevokeStmt::set_priv_set(ObPrivSet priv_set)
{
  priv_set_ = priv_set;
}

int ObRevokeStmt::set_database_name(const ObString &database)
{
  int ret = OB_SUCCESS;
  database_ = database;
  return ret;
}

int ObRevokeStmt::set_table_name(const ObString &table)
{
  int ret = OB_SUCCESS;
  table_ = table;
  return ret;
}

int ObRevokeStmt::set_priv_array(const share::ObRawPrivArray &array_in)
{
  int ret = OB_SUCCESS;
  OZ (sys_priv_array_.assign(array_in));
  OZ (syspriv_arg_.sys_priv_array_.assign(array_in));
  return ret;
}

int ObRevokeStmt::set_obj_priv_array(const share::ObRawObjPrivArray &array_in)
{
  int ret = OB_SUCCESS;
  OZ (obj_priv_array_.assign(array_in));
  OZ (table_arg_.obj_priv_array_.assign(array_in));
  return ret;
}

int ObRevokeStmt::add_grantee(const ObString &grantee)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(grantees_.add_string(grantee))) {
    LOG_WARN("failed to add user", K(ret));
  }
  return ret;
}

const ObIArray<uint64_t>& ObRevokeStmt::get_users() const
{
  return users_;
}

const ObIArray<uint64_t>& ObRevokeStmt::get_roles() const
{
  return user_arg_.role_ids_;
}

const ObString& ObRevokeStmt::get_database_name() const
{
  return database_;
}
const ObString& ObRevokeStmt::get_table_name() const
{
  return table_;
}
ObPrivSet ObRevokeStmt::get_priv_set() const
{
  return priv_set_;
}

int64_t ObRevokeStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "priv_set", share::schema::ObPrintPrivSet(priv_set_),
         "grant_level", ob_priv_level_str(grant_level_),
         "database", database_,
         "table", table_,
         "users", users_,
         "revoke_all", revoke_all_,
         "object_type", object_type_);
    J_OBJ_END();
  }
  return pos;
}

}
}


