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

#include "rpc/obmysql/packet/ompk_caching_sha2_response.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::obmysql;

int OMPKCachingSha2Response::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // MySQL protocol requires Authentication More Data packet format:
  // [0]: 0x01 (packet type identifier for Auth More Data)
  // [1]: response_byte (0x03 for FAST_AUTH_SUCCESS, 0x04 for PERFORM_FULL_AUTH)
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, 0x01, pos))) {
    LOG_WARN("store fail", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, response_byte_, pos))) {
    LOG_WARN("store fail", KP(buffer), K(length), K(pos), K(ret));
  }
  return ret;
}

int64_t OMPKCachingSha2Response::get_serialize_size() const
{
  return 2; // 0x01 prefix + response byte
}
