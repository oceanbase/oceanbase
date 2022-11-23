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

#define USING_LOG_PREFIX RPC
#include "common/data_buffer.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace common
{

OB_DEF_SERIALIZE(ObDataBuffer)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, position_);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(pos + position_ > buf_len)) {
    COMMON_LOG(WARN, "ObDataBuffer serialized error", K(pos), K_(position), K(buf_len));
    ret = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + pos, data_, position_);
    pos += position_;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDataBuffer)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, position_);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(pos + position_ > data_len)) {
    COMMON_LOG(WARN, "ObDataBuffer deserialized error", K(pos), K_(position), K(data_len));
    ret = OB_DESERIALIZE_ERROR;
  } else if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
  } else {
    // FIXME
    MEMCPY(data_, buf + pos, position_);
    pos += position_;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDataBuffer)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, position_);
  len += position_;
  return len;
}

int64_t ObDataBuffer::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "buffer=%p capacity=%ld position=%ld limit=%ld",
                  data_, capacity_, position_, limit_);
  return pos;
}

} /* common */
} /* oceanbase */
