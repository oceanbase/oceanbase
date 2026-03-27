/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/packet/ompk_rsa_public_key.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::obmysql;

int OMPKRsaPublicKey::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // MySQL protocol for RSA public key response:
  // [0]: 0x01 (Authentication More Data packet type)
  // [1-n]: RSA public key in PEM format
  if (OB_ISNULL(buffer) || OB_ISNULL(public_key_) ||
      OB_UNLIKELY(length - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), KP(public_key_), K(length), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, 0x01, pos))) {
    LOG_WARN("store fail", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, length, public_key_, key_len_, pos))) {
    LOG_WARN("store fail", KP(buffer), K(length), K(pos), K(ret));
  }
  return ret;
}

int64_t OMPKRsaPublicKey::get_serialize_size() const
{
  return 1 + key_len_; // 0x01 prefix + public key
}
