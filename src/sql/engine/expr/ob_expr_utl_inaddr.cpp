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

#define USING_LOG_PREFIX SQL_ENG
#include <stdio.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include "sql/engine/expr/ob_expr_utl_inaddr.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
static const int64_t HOSTADDR_MAX_LENGTH = 32;
const int64_t HOSTNAME_MAX_LENGTH = 256;
static const int64_t HOST_BUF_LENGTH = 1024;
ObExprUtlInaddrGetHostAddr::ObExprUtlInaddrGetHostAddr(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_UTL_INADDR_GET_HOST_ADDR, N_UTL_INADDR_GET_HOST_ADDR, 1,
                     VALID_FOR_GENERATED_COL, false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprUtlInaddrGetHostAddr::~ObExprUtlInaddrGetHostAddr()
{
}

int ObExprUtlInaddrGetHostAddr::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
  type.set_varchar();
  type.set_length_semantics(nls_param.nls_length_semantics_);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_collation_type(nls_param.nls_collation_);
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(nls_param.nls_collation_);
  return ret;
}

ObExprUtlInaddrGetHostName::ObExprUtlInaddrGetHostName(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_UTL_INADDR_GET_HOST_NAME, N_UTL_INADDR_GET_HOST_NAME, 1, VALID_FOR_GENERATED_COL,
                     false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprUtlInaddrGetHostName::~ObExprUtlInaddrGetHostName()
{
}

int ObExprUtlInaddrGetHostName::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
  type.set_varchar();
  type.set_length_semantics(nls_param.nls_length_semantics_);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_collation_type(nls_param.nls_collation_);
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(nls_param.nls_collation_);
  return ret;
}

}
}
