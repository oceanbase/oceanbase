
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

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_space_usage.h"

namespace oceanbase
{
namespace storage
{

int ObTabletSpaceUsage::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, pos, occupy_bytes_))) {
    LOG_WARN("serialize occupy size failed", K(ret), KP(buf), K(len), K(occupy_bytes_));
  }else if (OB_FAIL(serialization::encode_i64(buf, len, pos, required_bytes_))) {
    LOG_WARN("serialize reqiured size failed", K(ret), KP(buf), K(len), K(required_bytes_));
  }
  return ret;
}
int ObTabletSpaceUsage::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, pos, &occupy_bytes_))) {
    LOG_WARN("deserialize LS ID failed", K(ret), KP(buf), K(len), K(occupy_bytes_));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, pos, &required_bytes_))) {
    LOG_WARN("deserialize LS ID failed", K(ret), KP(buf), K(len), K(required_bytes_));
  }
  return ret;
}
int64_t ObTabletSpaceUsage::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(occupy_bytes_);
  size += serialization::encoded_length_i64(required_bytes_);
  return size;
}

} // storage
} // oceanbase