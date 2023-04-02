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

#include "sql/resolver/ddl/ob_create_dblink_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObCreateDbLinkStmt::ObCreateDbLinkStmt()
   : ObDDLStmt(stmt::T_CREATE_DBLINK),
    create_dblink_arg_()
{
}

ObCreateDbLinkStmt::ObCreateDbLinkStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_CREATE_DBLINK),
    create_dblink_arg_()
{
}

ObCreateDbLinkStmt::~ObCreateDbLinkStmt()
{
}

int ObCreateDbLinkStmt::set_password(const common::ObString &pwd)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  uint64_t tenant_id = create_dblink_arg_.dblink_info_.get_tenant_id();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    if (OB_FAIL(create_dblink_arg_.dblink_info_.set_password(pwd))) {
      LOG_WARN("failed to deep copy plain_password_", K(ret));
    }
  } else if (OB_FAIL(create_dblink_arg_.dblink_info_.set_plain_password(pwd))) {
    LOG_WARN("failed to set plain password", K(ret));
  } else if (OB_FAIL(create_dblink_arg_.dblink_info_.do_encrypt_password())) {
    LOG_WARN("failed to encrypt password", K(ret), K(pwd));
  }
  return ret;
}

int ObCreateDbLinkStmt::set_reverse_password(const common::ObString &pwd)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  uint64_t tenant_id = create_dblink_arg_.dblink_info_.get_tenant_id();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only DATA_VERSION_4_1_0_0 or above suppprt reverse dblink", K(ret));
  } else if (OB_FAIL(create_dblink_arg_.dblink_info_.set_plain_reverse_password(pwd))) {
    LOG_WARN("failed to set plain_reverse_password", K(ret));
  } else if (OB_FAIL(create_dblink_arg_.dblink_info_.do_encrypt_reverse_password())) {
    LOG_WARN("failed to encrypt plain_reverse_password", K(ret), K(pwd));
  }
  return ret;
}

}//namespace sql
}//namespace oceanbase
