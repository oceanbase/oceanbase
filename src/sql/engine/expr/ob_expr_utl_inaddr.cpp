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

namespace oceanbase {
namespace sql {
static const int64_t HOSTADDR_MAX_LENGTH = 32;
const int64_t HOSTNAME_MAX_LENGTH = 256;
static const int64_t HOST_BUF_LENGTH = 1024;
ObExprUtlInaddrGetHostAddr::ObExprUtlInaddrGetHostAddr(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_FUN_SYS_UTL_INADDR_GET_HOST_ADDR, N_UTL_INADDR_GET_HOST_ADDR, 1)
{}

ObExprUtlInaddrGetHostAddr::~ObExprUtlInaddrGetHostAddr()
{}

int ObExprUtlInaddrGetHostAddr::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
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

int ObExprUtlInaddrGetHostAddr::calc_result1(
    common::ObObj& result, const common::ObObj& obj1, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const char* hostname = nullptr;
  char* ip_str = nullptr;
  char name_buf[HOSTNAME_MAX_LENGTH] = {0};
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj1.is_null()) {
    if (OB_UNLIKELY(gethostname(name_buf, sizeof(name_buf)) < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get local hostaddress failed", K(ret));
    } else {
      hostname = name_buf;
    }
  } else {
    ObString str = obj1.get_string();
    hostname = to_cstring(str);
  }
  if (OB_SUCC(ret)) {
    struct hostent host_buf;
    struct hostent* host_res = nullptr;
    char buf[HOST_BUF_LENGTH] = {0};
    int err = 0;
    if (OB_ISNULL(ip_str = static_cast<char*>(expr_ctx.calc_buf_->alloc(HOSTADDR_MAX_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(HOSTADDR_MAX_LENGTH));
    } else if (OB_FAIL(gethostbyname_r(hostname, &host_buf, buf, sizeof(buf), &host_res, &err)) ||
               OB_ISNULL(host_res)) {
      ret = OB_ERR_HOST_UNKNOWN;
      LOG_USER_ERROR(OB_ERR_HOST_UNKNOWN, static_cast<int>(strlen(hostname)), hostname);
      LOG_WARN("get host by name failed", K(ret), K(hostname), K(obj1), K(err));
    } else if (OB_ISNULL(inet_ntop(host_res->h_addrtype, host_res->h_addr_list[0], ip_str, HOSTADDR_MAX_LENGTH))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("print host address failed", K(ret));
    } else {
      ObString res(ip_str);
      result.set_string(result_type_.get_type(), res);
      result.set_collation(result_type_);
    }
    LOG_DEBUG("calc utl_inaddr.get_host_address", K(ret), K(hostname), K(obj1), K(result));
  }
  return ret;
}

ObExprUtlInaddrGetHostName::ObExprUtlInaddrGetHostName(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_FUN_SYS_UTL_INADDR_GET_HOST_NAME, N_UTL_INADDR_GET_HOST_NAME, 1)
{}

ObExprUtlInaddrGetHostName::~ObExprUtlInaddrGetHostName()
{}

int ObExprUtlInaddrGetHostName::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
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

int ObExprUtlInaddrGetHostName::calc_result1(
    common::ObObj& result, const common::ObObj& obj1, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const char* hostaddr = nullptr;
  char* hostname_str = nullptr;
  const int64_t HOSTADDR_MAX_LENGTH = 32;
  int res_len = 0;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj1.is_null()) {
    char hostname_buf[HOSTNAME_MAX_LENGTH] = {0};
    if (OB_UNLIKELY(gethostname(hostname_buf, sizeof(hostname_buf)) < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get local hostname failed", K(ret));
    } else if (FALSE_IT(res_len = strlen(hostname_buf))) {
    } else if (OB_ISNULL(hostname_str = static_cast<char*>(expr_ctx.calc_buf_->alloc(res_len + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(res_len));
    } else {
      memcpy(hostname_str, hostname_buf, res_len);
      hostname_str[res_len] = '\0';
    }
  } else {
    ObString str = obj1.get_string();
    hostaddr = to_cstring(str);
    struct hostent host_buf;
    struct hostent* host_res = nullptr;
    char paddr_buf[HOSTADDR_MAX_LENGTH] = {0};
    char buf[HOST_BUF_LENGTH] = {0};
    int err = 0;
    in_addr_t in_addr = inet_addr(hostaddr);
    if (OB_FAIL(gethostbyaddr_r((char*)&in_addr, 4, AF_INET, &host_buf, buf, sizeof(buf), &host_res, &err)) ||
        OB_ISNULL(host_res)) {
      ret = OB_ERR_HOST_UNKNOWN;
      LOG_USER_ERROR(OB_ERR_HOST_UNKNOWN, str.length(), str.ptr());
      LOG_WARN("get host by addr failed", K(ret), K(hostaddr), K(obj1), K(err));
    } else if (FALSE_IT(res_len = strlen(host_res->h_name))) {
    } else if (OB_ISNULL(hostname_str = static_cast<char*>(expr_ctx.calc_buf_->alloc(res_len + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(res_len));
    } else {
      memcpy(hostname_str, host_res->h_name, res_len);
      hostname_str[res_len] = '\0';
    }
    LOG_DEBUG("calc utl_inaddr.get_host_name", K(ret), K(hostaddr), K(obj1), K(result));
  }
  if (OB_SUCC(ret)) {
    ObString res(hostname_str);
    result.set_string(result_type_.get_type(), res);
    result.set_collation(result_type_);
  } else {
    result.set_null();
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
