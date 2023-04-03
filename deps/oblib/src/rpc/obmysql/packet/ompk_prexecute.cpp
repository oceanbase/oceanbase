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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "ompk_prexecute.h"
#include "lib/oblog/ob_log_module.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
using namespace common;
namespace obmysql
{

int OMPKPrexecute::serialize(char* buffer, int64_t length, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow",  K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else {
    if (OB_FAIL(OMPKPrepare::serialize(buffer, length, pos))) {
      LOG_WARN("prepare failed", KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(buffer, length, extend_flag_.extend_flag_, pos))) {
      LOG_WARN("store failed", KP(buffer), K(length), K_(extend_flag_.extend_flag), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, has_result_set_, pos))) {
      LOG_WARN("store failed", KP(buffer), K(length), K_(has_result_set), K(pos));
    } 
  }
  return ret;
}

int64_t OMPKPrexecute::get_serialize_size() const
{
  int64_t len = 0;
  len += OMPKPrepare::get_serialize_size();   // prepare
  len += 4;                 // extend flag
  len += 1;                 // has result set
  return len;
}




} //end of obmysql
} //end of oceanbase
