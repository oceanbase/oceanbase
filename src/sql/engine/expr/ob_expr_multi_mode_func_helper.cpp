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
 * This file is for implement of func multimode expr helper
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "ob_expr_multi_mode_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

uint64_t ObMultiModeExprHelper::get_tenant_id(ObSQLSessionInfo *session)
{
  uint64_t tenant_id = 0;
  if (OB_ISNULL(session)) {
    // If the session is not obtained, 500 tenant memory will be temporarily used,
    // but it will not affect subsequent execution.
    tenant_id = OB_SERVER_TENANT_ID;
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get session, set tenant_id 500.", K(ret), K(lbt()));
  } else if (session->get_ddl_info().is_ddl_check_default_value()) {
    tenant_id = OB_SERVER_TENANT_ID;
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return tenant_id;
}


};
};
