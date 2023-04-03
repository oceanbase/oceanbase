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

#include "rpc/obmysql/packet/ompk_ssl_request.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

void OMPKSSLRequest::reset()
{
  capability_.capability_ = 0;
  max_packet_size_ = 0;
  character_set_ = 0;
}

int OMPKSSLRequest::decode()
{
  int ret = OB_SUCCESS;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = pos + len;

  //OB_ASSERT(NULL != cdata_);
  if (OB_ISNULL(cdata_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", K(ret), KP(cdata_));
  } else if (OB_UNLIKELY(len < MIN_CAPABILITY_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error ssl request packet", K(len), KP(pos), KP(end), K(ret));
  } else {
    capability_.capability_ = uint2korr(pos);
    if (OB_UNLIKELY(!capability_.cap_flags_.OB_CLIENT_SSL)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error capability from ssl request packet", K(ret));
    } else if (OB_UNLIKELY(!capability_.cap_flags_.OB_CLIENT_PROTOCOL_41)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ob only support mysql client protocol 4.1", K(ret));
    } else {
      if (JDBC_SSL_MIN_SIZE == len) {
        // JConnector only sends server capabilities before starting SSL negotiation.  The below code is patch for this.
        ObMySQLUtil::get_uint4(pos, capability_.capability_);
        max_packet_size_ = 0xFFFFF;//unused
        character_set_ = 0;//unused
      } else {
        if (OB_UNLIKELY(len < HANDSHAKE_RESPONSE_MIN_SIZE)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error ssl request packet", K(len), KP(pos), KP(end), K(ret));
        } else {
          ObMySQLUtil::get_uint4(pos, capability_.capability_);
          ObMySQLUtil::get_uint4(pos, max_packet_size_); //16MB
          ObMySQLUtil::get_uint1(pos, character_set_);
          pos += HANDSHAKE_RESPONSE_RESERVED_SIZE;//23 bytes reserved
        }
      }
    }
  }
  return ret;
}

int OMPKSSLRequest::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow",  K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else {
    char reserved[HANDSHAKE_RESPONSE_RESERVED_SIZE] = {};
    if (OB_FAIL(ObMySQLUtil::store_int4(buffer, length, capability_ .capability_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(buffer, length, max_packet_size_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, character_set_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, length, reserved, HANDSHAKE_RESPONSE_RESERVED_SIZE, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    }
  }
  return ret;
}
