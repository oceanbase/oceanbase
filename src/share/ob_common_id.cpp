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

#define USING_LOG_PREFIX COMMON

#include "ob_common_id.h"

#include "lib/oblog/ob_log_module.h"        // LOG_*
#include "lib/oblog/ob_log.h"               // LOG_*
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace share
{

uint64_t ObCommonID::hash() const
{
  OB_ASSERT(id_ >= 0);
  return id_;
}

int ObCommonID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, id_))) {
    LOG_WARN("serialize ID failed", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObCommonID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &id_))) {
    LOG_WARN("deserialize ID failed", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObCommonID::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(id_);
  return size;
}

int ObCommonID::parse_from_display_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  errno = 0;
  if (OB_UNLIKELY(1 != sscanf(str.ptr(), "%ld", &id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObCommonID str", KR(ret), K(str), K(errno), KERRMSG, K(id_));
  }
  return ret;
}

int ObCommonID::to_display_str(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || pos < 0 || pos >= len || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(len), K(pos), KPC(this));
  } else if (OB_FAIL(databuff_printf(buf, len, pos, "%ld", id_))) {
    LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), KPC(this));
  }
  return ret;
}


} // end namespace share
} // end namespace oceanbase
