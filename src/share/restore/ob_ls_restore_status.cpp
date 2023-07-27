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

#define USING_LOG_PREFIX SHARE
#include "share/restore/ob_ls_restore_status.h"

using namespace oceanbase;
using namespace share;


ObLSRestoreStatus::ObLSRestoreStatus(const Status &status)
  : status_(status)
{
}

ObLSRestoreStatus &ObLSRestoreStatus::operator=(const ObLSRestoreStatus &restore_status)
{
  if (this != &restore_status) {
    status_ = restore_status.status_;
  }
  return *this;
}

const char *ObLSRestoreStatus::get_restore_status_str(const ObLSRestoreStatus &status)
{
  const char *str = "UNKNOWN";
  const char *restore_status_str[] = {
      "RESTORE_NONE",
      "RESTORE_START",
      "RESTORE_SYS_TABLETS",
      "WAIT_RESTORE_SYS_TABLETS",
      "RESTORE_TABLETS_META",
      "WAIT_RESTORE_TABLETS_META",
      "RESTORE_TO_CONSISTENT_SCN",
      "WAIT_RESTORE_TO_CONSISTENT_SCN",
      "QUICK_RESTORE",
      "WAIT_QUICK_RESTORE",
      "QUICK_RESTORE_FINISH",
      "RESTORE_MAJOR_DATA",
      "WAIT_RESTORE_MAJOR_DATA",
      "RESTORE_FAILED",
  };
  STATIC_ASSERT(LS_RESTORE_STATUS_MAX == ARRAYSIZEOF(restore_status_str), "status count mismatch");
  if (status.status_ < 0 || status.status_ >= LS_RESTORE_STATUS_MAX) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid status", K(status));
  } else {
    str = restore_status_str[status.status_];
  }
  return str;

}

int ObLSRestoreStatus::set_status(int32_t status)
{
  int ret = OB_SUCCESS;
  if (RESTORE_NONE > status || LS_RESTORE_STATUS_MAX <= status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else {
    status_ = static_cast<Status>(status);
  }
  return ret;
}

ObLSRestoreStatus &ObLSRestoreStatus::operator=(const Status &status)
{
  status_ = status;
  return *this;
}

int ObLSRestoreStatus::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(status_)))) {
    LOG_WARN("failed to serialize log stream restore status", K(ret), K(len), K_(status));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObLSRestoreStatus::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&status_)))) {
    LOG_WARN("failed to decode log stream restore status", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObLSRestoreStatus::get_serialize_size() const
{
  return serialization::encoded_length_i8(static_cast<int8_t>(status_));
}
