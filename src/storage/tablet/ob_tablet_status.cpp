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
}

ObTabletStatus::ObTabletStatus(const Status &status)
  : status_(status)
{
}

static const char *TABLET_STATUS_STRS[] = {
    "CREATING",
    "NORMAL",
    "DELETING",
    "DELETED",
    "TRANSFER_OUT",
    "TRANSFER_IN",
    "TRANSFER_OUT_DELETED",
};

const char *ObTabletStatus::get_str(const ObTabletStatus &status)
{
  const char *str = NULL;

  if (status.status_ < 0 || status.status_ >= ObTabletStatus::MAX) {
    str = "UNKNOWN";
  } else {
    str = TABLET_STATUS_STRS[status.status_];
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

bool ObTabletStatus::is_valid_status(const Status current_status, const Status target_status)
{
  bool b_ret = true;

  switch (current_status) {
    case CREATING:
      if (target_status != NORMAL && target_status != DELETED) {
        b_ret = false;
      }
      break;
    case NORMAL:
      if (target_status != DELETING && target_status != NORMAL
          && target_status != TRANSFER_OUT && target_status != TRANSFER_IN) {
        b_ret = false;
      }
      break;
    case DELETING:
      if (target_status != NORMAL && target_status != DELETED
          && target_status != DELETING) {
        b_ret = false;
      }
      break;
    case DELETED:
      break;
    case MAX:
      if (target_status != CREATING && target_status != DELETED) {
        b_ret = false;
      }
      break;
    case TRANSFER_OUT:
      if (target_status != NORMAL && target_status != TRANSFER_OUT_DELETED && target_status != TRANSFER_OUT) {
        b_ret = false;
      }
      break;
    case TRANSFER_IN:
      if (target_status != NORMAL && target_status != DELETED && target_status != TRANSFER_IN) {
        b_ret = false;
      }
      break;
    case TRANSFER_OUT_DELETED:
      if (target_status != DELETED && target_status != TRANSFER_OUT_DELETED && target_status != TRANSFER_OUT_DELETED) {
        b_ret = false;
      }
      break;
    default:
      b_ret = false;
      break;
  }

  return b_ret;
}

} // namespace storage
} // namespace oceanbase
