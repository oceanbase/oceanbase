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

#include "storage/tablet/ob_tablet_status.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{
ObTabletStatus::ObTabletStatus()
  : status_(Status::MAX)
{
  static_assert(sizeof(status_str_array_) / sizeof(const char *) == static_cast<uint8_t>(ObTabletStatus::MAX) + 1,
      "status str array size does not equal to enum value count");
}

ObTabletStatus::ObTabletStatus(const Status &status)
  : status_(status)
{
}

const char *ObTabletStatus::get_str(const ObTabletStatus &status)
{
  const char *str = nullptr;
  if (status.status_ > ObTabletStatus::MAX) {
    str = "UNKNOWN";
  } else {
    str = status_str_array_[status.status_];
  }
  return str;
}

int ObTabletStatus::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(status_)))) {
    LOG_WARN("failed to serialize status", K(ret), K(len), K_(status));
  } else {
    pos = new_pos;
  }

  return ret;
}

int ObTabletStatus::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&status_)))) {
    LOG_WARN("failed to deserialize status", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObTabletStatus::get_serialize_size() const
{
  return serialization::encoded_length_i8(static_cast<int8_t>(status_));
}
} // namespace storage
} // namespace oceanbase
