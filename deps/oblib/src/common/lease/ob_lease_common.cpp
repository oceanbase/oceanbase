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

#include "common/lease/ob_lease_common.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace lib;

namespace common {
bool ObLease::is_lease_valid(int64_t redun_time)
{
  bool ret = true;

  timeval time_val;
  (void)gettimeofday(&time_val, NULL);
  int64_t cur_time_us = time_val.tv_sec * 1000 * 1000 + time_val.tv_usec;
  if (lease_time + lease_interval + redun_time < cur_time_us) {
    _OB_LOG(INFO,
        "Lease expired, lease_time=%ld, lease_interval=%ld, cur_time_us=%ld",
        lease_time,
        lease_interval,
        cur_time_us);
    ret = false;
  }

  return ret;
}

DEFINE_SERIALIZE(ObLease)
{
  int err = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0 || pos < 0) {
    _OB_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    err = OB_INVALID_ARGUMENT;
  } else if (pos + (int64_t)sizeof(*this) > buf_len) {
    _OB_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
    err = OB_INVALID_ARGUMENT;
  } else {
    *((ObLease*)(buf + pos)) = *this;
    pos += sizeof(*this);
  }

  return err;
}

DEFINE_DESERIALIZE(ObLease)
{
  int err = OB_SUCCESS;
  if (NULL == buf || data_len <= 0 || pos < 0) {
    _OB_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld", buf, data_len, pos);
    err = OB_INVALID_ARGUMENT;
  } else if (pos + (int64_t)sizeof(*this) > data_len) {
    _OB_LOG(WARN, "data_len is not enough, pos=%ld, data_len=%ld", pos, data_len);
    err = OB_INVALID_ARGUMENT;
  } else {
    *this = *((ObLease*)(buf + pos));
    pos += sizeof(*this);
  }

  return err;
}

DEFINE_GET_SERIALIZE_SIZE(ObLease)
{
  return sizeof(*this);
}
}  // namespace common
}  // namespace oceanbase
