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

#include "rpc/ob_sql_request_operator.h"
#include "rpc/obmysql/ob_sql_sock_session.h"
#include "rpc/obmysql/ob_easy_sql_request_operator.h"
#include "rpc/obmysql/ob_poc_sql_request_operator.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_opts.h"

namespace oceanbase
{
using namespace obmysql;
namespace rpc
{
ObEasySqlRequestOperator global_easy_sql_req_operator;
ObPocSqlRequestOperator global_poc_sql_req_operator;
ObISqlRequestOperator& ObSqlRequestOperator::get_operator(const ObRequest* req)
{
  ObISqlRequestOperator* op = NULL;
  switch(req->get_nio_protocol()) {
    case ObRequest::TRANSPORT_PROTO_POC:
      op = &global_poc_sql_req_operator;
      break;
    default:
      op = &global_easy_sql_req_operator;
  }
  return *op;
}

ObISqlRequestOperator& ObSqlRequestOperator::get_operator(const ObSqlSockDesc& desc)
{
  ObISqlRequestOperator* op = NULL;
  switch(desc.type_) {
    case ObRequest::TRANSPORT_PROTO_POC:
      op = &global_poc_sql_req_operator;
      break;
    default:
      op = &global_easy_sql_req_operator;
  }
  return *op;
}

void *ObSqlRequestOperator::get_sql_session(ObRequest* req)
{
  return (void *)(get_operator(req).get_sql_session(req));
}

void ObSqlSockDesc::clear_sql_session_info() {
  if (NULL == sock_desc_) {
  } else if (rpc::ObRequest::TRANSPORT_PROTO_POC == type_) {
    obmysql::ObSqlSockSession* sess = (obmysql::ObSqlSockSession *)sock_desc_;
    sess->clear_sql_session_info();
  } else {
    //TODO for easy
  }
}

ObSqlRequestOperator global_sql_req_operator;
}; // end namespace rpc
}; // end namespace oceanbase

