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

#include "rpc/obmysql/packet/ompk_auth_switch.h"

#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

OMPKAuthSwitch::OMPKAuthSwitch()
    : status_(0xfe),
      plugin_name_(0),
      scramble_()
{}

int OMPKAuthSwitch::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer) || OB_UNLIKELY(len - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(len), K(pos), K(ret));
  } else if (OB_UNLIKELY(len - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid argument", K(len), K(pos), "need_size", get_serialize_size());
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, status_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(pos), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, len, plugin_name_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(pos), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_nzt(buffer, len, scramble_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(pos), K(ret));
    }
  }

  return ret;
}

int64_t OMPKAuthSwitch::get_serialize_size() const
{
  int64_t len = 0;
  len += 1;                 // field_count_
  len += plugin_name_.length() + 1;
  len += scramble_.length() + 1;
  return len;
}

int64_t OMPKAuthSwitch::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("header", hdr_,
       K_(status),
       K_(plugin_name),
       K_(scramble));
  J_OBJ_END();
  return pos;
}
